"""Download TeraBox shares and upload their files to Telegram."""

import asyncio
import html
import json
import os
import re
from urllib.parse import urlparse

from natsort import natsorted
from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import MessageNotModified
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .. import (
    ADMIN_CHATS,
    ALL_CHATS,
    ForceDocumentFlag,
    SendAsZipFlag,
    help_dict,
    session,
)
from ..utils.file_split import TELEGRAM_SPLIT_SIZE
from ..utils.terabox import (
    DEFAULT_TERABOX_ENDPOINT,
    TERABOX_USER_AGENT,
    TeraboxError,
    TeraboxResolver,
    extract_surl,
)
from ..utils.terabox_config import TeraboxConfigStore
from ..utils.terabox_sessions import (
    FILE_DOWNLOADED,
    FILE_DOWNLOADING,
    FILE_FAILED,
    FILE_PENDING,
    FILE_RESOLVING,
    FILE_UPLOADED,
    SESSION_COMPLETED,
    SESSION_FAILED,
    SESSION_PAUSED,
    SESSION_RUNNING,
    infer_shared_folder_name,
    new_session_id,
    parse_size_limit,
    split_by_cumulative_size,
    terabox_session_store,
)
from .leech import initiate_directdl


XAPIVERSE_KEY = os.environ.get("XAPIVERSE_KEY", "")
TERABOX_BASE_URL = os.environ.get("TERABOX_BASE_URL", DEFAULT_TERABOX_ENDPOINT)
TERABOX_API_URL = "https://xapiverse.com/api/terabox"
TERABOX_CONFIG = TeraboxConfigStore()
terabox_session_tasks = {}
terabox_tasks = set()
terabox_chain_locks = {}
TERABOX_CHAIN_PAGE_BYTES = 3300


def _reported_size_bytes(value):
    if isinstance(value, (int, float)):
        return max(0, int(value))
    text = str(value or "").strip().replace(",", "")
    if text.isdigit():
        return int(text)
    match = re.fullmatch(
        r"(\d+(?:\.\d+)?)\s*(B|K(?:I?B)?|M(?:I?B)?|G(?:I?B)?|T(?:I?B)?)",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    powers = {
        "B": 0,
        "K": 1,
        "KB": 1,
        "KIB": 1,
        "M": 2,
        "MB": 2,
        "MIB": 2,
        "G": 3,
        "GB": 3,
        "GIB": 3,
        "T": 4,
        "TB": 4,
        "TIB": 4,
    }
    return int(float(match.group(1)) * (1024 ** powers[match.group(2).upper()]))


def _normalized_file(file_info, position, source_url):
    item = file_info.get("terabox_file")
    if item is not None:
        name = item.name
        relative_path = item.relative_path or item.name
        size_bytes = int(item.size or 0)
    else:
        name = str(file_info.get("name") or f"terabox-file-{position}")
        relative_path = str(file_info.get("path") or name)
        size_bytes = None
        for key in ("size_bytes", "size", "size_formatted"):
            size_bytes = _reported_size_bytes(file_info.get(key))
            if size_bytes is not None:
                break
        if size_bytes is None:
            raise TeraboxError(
                f"TeraBox did not report a size for {name}; it cannot be size-split"
            )
    return {
        "page_url": source_url,
        "filename": name,
        "relative_path": relative_path,
        "source_position": int(position),
        "size_bytes": max(0, int(size_bytes)),
    }


def _normalize_file_list(file_list, source_url):
    return [
        _normalized_file(file_info, position, source_url)
        for position, file_info in enumerate(file_list, 1)
    ]


def _resolved_file_for_session(file_doc, file_list, source_url):
    normalized = _normalize_file_list(file_list, source_url)
    expected_path = str(file_doc.get("relative_path") or file_doc["filename"])
    position = int(file_doc.get("source_position") or 0)
    if 1 <= position <= len(normalized):
        candidate = normalized[position - 1]
        if candidate["relative_path"] == expected_path:
            return file_list[position - 1]
    matches = [
        file_info
        for file_info, metadata in zip(file_list, normalized)
        if metadata["relative_path"] == expected_path
    ]
    if len(matches) == 1:
        return matches[0]
    raise TeraboxError(
        f"The share changed and {file_doc['filename']} can no longer be matched"
    )


def _human_size(size):
    value = float(size or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.2f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024


async def _resolve_with_cookie(link, cookie):
    resolver = TeraboxResolver(session, cookie, TERABOX_BASE_URL)
    files = await resolver.resolve(link)
    return resolver, [
        {
            "name": item.name,
            "size_formatted": _human_size(item.size),
            "terabox_file": item,
        }
        for item in files
    ]


async def _resolve_with_xapiverse(link):
    headers = {
        "Content-Type": "application/json",
        "xAPIverse-Key": XAPIVERSE_KEY,
    }
    async with session.post(
        TERABOX_API_URL,
        data=json.dumps({"url": link}),
        headers=headers,
    ) as response:
        if response.status != 200:
            raise TeraboxError(f"xAPIverse returned HTTP {response.status}")
        data = await response.json()
    if data.get("status") != "success":
        message = data.get("message") or data.get("error") or "Unknown error"
        raise TeraboxError(f"xAPIverse could not resolve the share: {message}")
    files = data.get("list") or []
    if not files:
        raise TeraboxError("No files were found in the TeraBox share")
    return None, files


async def _delete_secret_message(message):
    try:
        await message.delete()
        return True
    except Exception:
        return False


async def _is_cookie_admin(client, message):
    user = message.from_user
    if user is None:
        return False
    if message.chat.id == user.id:
        return user.id in ADMIN_CHATS
    try:
        member = await client.get_chat_member(message.chat.id, user.id)
    except Exception:
        return False
    return member.status in {
        ChatMemberStatus.OWNER,
        ChatMemberStatus.ADMINISTRATOR,
    }


@Client.on_message(
    filters.command(["setteraboxcookie", "settcookie"])
    & filters.chat(ALL_CHATS)
)
async def set_terabox_cookie_cmd(client, message):
    command = (message.text or message.caption or "").split(None, 1)
    deleted = await _delete_secret_message(message)
    if not await _is_cookie_admin(client, message):
        await client.send_message(
            message.chat.id,
            "❌ Only a Telegram administrator of this configured chat can "
            "replace the TeraBox cookie.",
        )
        return
    if len(command) != 2 or not command[1].strip():
        await client.send_message(
            message.chat.id,
            "Usage: <code>/setteraboxcookie &lt;ndus value&gt;</code>",
        )
        return

    candidate = command[1].strip()
    try:
        resolver = TeraboxResolver(session, candidate, TERABOX_BASE_URL)
        if not await resolver.validate_cookie():
            await client.send_message(
                message.chat.id,
                "❌ TeraBox rejected that cookie. The saved cookie was not changed.",
            )
            return
        await TERABOX_CONFIG.set_cookie(candidate, message.from_user.id)
    except Exception as error:
        await client.send_message(
            message.chat.id,
            f"❌ Cookie validation failed: {html.escape(str(error))}",
        )
        return

    persistence = "MongoDB" if TERABOX_CONFIG.persistent else "memory until restart"
    warning = "" if deleted else "\n⚠️ Delete your command message manually."
    await client.send_message(
        message.chat.id,
        f"✅ TeraBox cookie validated and saved in {persistence}.{warning}",
    )


@Client.on_message(
    filters.command("clearteraboxcookie")
    & filters.chat(ALL_CHATS)
)
async def clear_terabox_cookie_cmd(client, message):
    if not await _is_cookie_admin(client, message):
        await message.reply_text("❌ This command is restricted to chat administrators.")
        return
    await TERABOX_CONFIG.clear_cookie()
    fallback = bool(TERABOX_CONFIG.env_cookie)
    await message.reply_text(
        "✅ Database cookie override removed. "
        + (
            "The environment cookie is active again."
            if fallback
            else "No TeraBox cookie is currently configured."
        )
    )


@Client.on_message(
    filters.command("teraboxcookiestatus")
    & filters.chat(ALL_CHATS)
)
async def terabox_cookie_status_cmd(client, message):
    if not await _is_cookie_admin(client, message):
        await message.reply_text("❌ This command is restricted to chat administrators.")
        return
    state = await TERABOX_CONFIG.get_state()
    override = bool(state.get("cookie"))
    configured = bool(await TERABOX_CONFIG.get_cookie())
    source = "database override" if override else "environment fallback"
    persistence = "enabled" if TERABOX_CONFIG.persistent else "unavailable"
    await message.reply_text(
        f"TeraBox cookie configured: <b>{'yes' if configured else 'no'}</b>\n"
        f"Active source: <b>{source}</b>\n"
        f"MongoDB persistence: <b>{persistence}</b>"
    )


@Client.on_message(
    filters.command(["tera", "ziptera", "filetera"]) & filters.chat(ALL_CHATS)
)
async def tera_cmd(client, message):
    text = (message.text or message.caption).split(None, 1)
    command = text.pop(0).lower()
    if "zip" in command:
        flags = (SendAsZipFlag,)
    elif "file" in command:
        flags = (ForceDocumentFlag,)
    else:
        flags = ()

    link = None
    reply = message.reply_to_message
    if text:
        link = text[0].strip()
    elif not getattr(reply, "empty", True):
        link = (reply.text or reply.caption or "").strip()
    if link and not link.startswith("http"):
        link = "https://" + link

    if not link:
        await message.reply_text(
            "Usage:\n"
            "- /tera <i>&lt;TeraBox URL&gt;</i>\n"
            "- /ziptera <i>&lt;TeraBox URL&gt;</i>\n"
            "- /filetera <i>&lt;TeraBox URL&gt;</i> - send videos as files"
        )
        return
    cookie = await TERABOX_CONFIG.get_cookie()
    if not cookie and not XAPIVERSE_KEY:
        await message.reply_text(
            "❌ TeraBox is not configured. Set <code>TERABOX_COOKIE</code> "
            "or <code>XAPIVERSE_KEY</code>."
        )
        return

    status_msg = await message.reply_text("🔍 Resolving TeraBox link...")
    try:
        if cookie:
            resolver, file_list = await _resolve_with_cookie(link, cookie)
        else:
            resolver, file_list = await _resolve_with_xapiverse(link)

        info_text = f"📦 <b>Found {len(file_list)} file(s):</b>\n\n"
        for index, file_info in enumerate(file_list, 1):
            name = file_info.get("name", "Unknown")
            size = file_info.get("size_formatted", "Unknown")
            info_text += (
                f"<b>{index}.</b> <code>{html.escape(str(name))}</code>\n"
                f"    📏 {html.escape(str(size))}\n"
            )
        info_text += "\n⬇️ Starting download..."
        if len(info_text) > 4000:
            info_text = info_text[:3990] + "\n..."
        await status_msg.edit_text(info_text)

        for file_info in file_list:
            if resolver is not None:
                item = file_info["terabox_file"]
                download_url = await resolver.authorize_download_url(item.download_url)
                filename = item.name
                request_headers = [
                    f"User-Agent: {TERABOX_USER_AGENT}",
                    f"Referer: {TERABOX_BASE_URL}",
                ]
            else:
                download_url = file_info.get("normal_dlink") or file_info.get(
                    "zip_dlink"
                )
                filename = file_info.get("name")
                request_headers = None
            if not download_url:
                await message.reply_text(
                    "⚠️ No download link available for: "
                    f"<code>{html.escape(str(filename or 'Unknown'))}</code>"
                )
                continue
            await initiate_directdl(
                client,
                message,
                download_url,
                filename,
                flags,
                headers=request_headers,
            )
            if len(file_list) > 1:
                await asyncio.sleep(2)
    except asyncio.TimeoutError:
        await status_msg.edit_text("❌ TeraBox request timed out.")
    except Exception as error:
        await status_msg.edit_text(f"❌ Error: {html.escape(str(error))}")


async def _resolve_terabox_share(link):
    cookie = await TERABOX_CONFIG.get_cookie()
    if cookie:
        return await _resolve_with_cookie(link, cookie)
    if XAPIVERSE_KEY:
        return await _resolve_with_xapiverse(link)
    raise TeraboxError(
        "TeraBox is not configured. Set TERABOX_COOKIE or XAPIVERSE_KEY."
    )


def _terabox_flags_from_mode(mode):
    if mode == "zip":
        return (SendAsZipFlag,)
    if mode == "file":
        return (ForceDocumentFlag,)
    return ()


def _terabox_mode_from_command(command):
    command = command.lower()
    if "zip" in command:
        return "zip"
    if "file" in command:
        return "file"
    return "normal"


def _terabox_download_dir(session_doc, file_doc):
    return os.path.join(
        os.getcwd(),
        str(session_doc["owner_id"]),
        "terabox_sessions",
        session_doc["_id"],
        str(file_doc.get("source_position") or file_doc["position"]),
    )


def _start_terabox_session(client, message, session_id, resolved=None):
    existing = terabox_session_tasks.get(session_id)
    if existing and not existing.done():
        return existing
    task = asyncio.create_task(
        _run_terabox_session(client, message, session_id, resolved=resolved)
    )
    terabox_session_tasks[session_id] = task
    terabox_tasks.add(task)

    def _discard(finished):
        terabox_tasks.discard(finished)
        if terabox_session_tasks.get(session_id) is finished:
            terabox_session_tasks.pop(session_id, None)
        if not finished.cancelled() and finished.exception() is not None:
            asyncio.create_task(
                _record_terabox_session_crash(
                    message, session_id, finished.exception()
                )
            )

    task.add_done_callback(_discard)
    return task


async def _record_terabox_session_crash(message, session_id, error):
    await terabox_session_store.set_state(session_id, SESSION_FAILED)
    await message.reply_text(
        f"TeraBox session <code>{session_id}</code> stopped unexpectedly: "
        f"{html.escape(str(error))}. Resume it with "
        f"<code>/continuetera {session_id}</code>."
    )


async def _fail_terabox_session(message, session_id, file_doc, error):
    if file_doc is not None:
        await terabox_session_store.update_file_if_status(
            file_doc["_id"],
            (FILE_PENDING, FILE_RESOLVING, FILE_DOWNLOADING, FILE_DOWNLOADED),
            FILE_FAILED,
            gid=None,
            error=str(error),
        )
    await terabox_session_store.set_state(session_id, SESSION_FAILED)
    await message.reply_text(
        f"TeraBox session <code>{session_id}</code> stopped: "
        f"{html.escape(str(error))}. Resume with "
        f"<code>/continuetera {session_id}</code>."
    )


def _intelligent_workspace_bytes(file_info):
    """Estimate peak bytes while a source is prepared for Telegram."""
    source_bytes = max(0, int(file_info.get("size_bytes") or 0))
    if source_bytes > TELEGRAM_SPLIT_SIZE:
        # split_binary_file creates a complete second copy in numbered parts
        # before upload starts, so both copies coexist temporarily.
        return source_bytes * 2
    return source_bytes


def _valid_telegram_message_link(value):
    """Return a normalized Telegram message link, or None for unusable links."""
    link = str(value or "").strip()
    try:
        parsed = urlparse(link)
    except ValueError:
        return None
    path_parts = [part for part in parsed.path.split("/") if part]
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() not in {"t.me", "telegram.me"}
        or len(path_parts) < 2
    ):
        return None
    return link


def _ordered_telegram_files(sent_files):
    """Normalize successful upload records into stable numeric part order."""
    records = []
    for name, link in sent_files:
        valid_link = _valid_telegram_message_link(link)
        if valid_link:
            records.append({"name": str(name), "link": valid_link})
    return natsorted(records, key=lambda item: item["name"])


def _chain_index_lines(chain, files_by_session):
    chain_id = chain[0]["chain_id"]
    title = str(chain[0].get("name") or "").strip() or re.sub(
        r"\s*\(part \d+/\d+\)$", "", chain[0]["title"]
    )
    lines = [
        f"<b>TeraBox upload complete</b> — <code>{chain_id}</code>",
        f"📁 <b>{html.escape(title)}</b>/",
    ]

    def new_directory():
        return {"directories": {}, "entries": []}

    root = new_directory()
    for session_doc in chain:
        for file_doc in files_by_session.get(session_doc["_id"], []):
            relative_path = str(
                file_doc.get("relative_path") or file_doc["filename"]
            ).replace("\\", "/").strip("/")
            parts = [part for part in relative_path.split("/") if part]
            if len(parts) > 1 and parts[0] == title:
                # The chain heading already represents the shared root folder.
                # Do not render that directory twice in the final index.
                parts = parts[1:]
            source_name = parts.pop() if parts else file_doc["filename"]
            directory = root
            for part in parts:
                if part not in directory["directories"]:
                    child = new_directory()
                    directory["directories"][part] = child
                    directory["entries"].append(("directory", part, child))
                directory = directory["directories"][part]
            directory["entries"].append(("file", source_name, file_doc))

    item_index = 0

    def render_directory(directory, prefix=""):
        nonlocal item_index
        entries = directory["entries"]
        for entry_index, (kind, name, value) in enumerate(entries):
            last = entry_index == len(entries) - 1
            branch = "└──" if last else "├──"
            continuation = "    " if last else "│   "
            if kind == "directory":
                lines.append(
                    f"{prefix}{branch} 📁 <b>{html.escape(name)}/</b>"
                )
                render_directory(value, prefix + continuation)
                continue

            file_doc = value
            uploads = natsorted(
                file_doc.get("telegram_files") or [],
                key=lambda upload: str(upload.get("name") or ""),
            )
            item_index += 1
            if len(uploads) > 1:
                lines.append(
                    f"{prefix}{branch} 📦 {item_index}. "
                    f"<b>{html.escape(name)}</b>"
                )
                part_prefix = prefix + continuation
                for part_index, upload in enumerate(uploads, 1):
                    part_branch = "└──" if part_index == len(uploads) else "├──"
                    upload_name = html.escape(str(upload.get("name") or "part"))
                    link = _valid_telegram_message_link(upload.get("link"))
                    if link:
                        escaped_link = html.escape(link, quote=True)
                        lines.append(
                            f'{part_prefix}{part_branch} <a href="{escaped_link}">'
                            f"{item_index}.{part_index} {upload_name}</a>"
                        )
                    else:
                        lines.append(
                            f"{part_prefix}{part_branch} "
                            f"{item_index}.{part_index} {upload_name} (link missing)"
                        )
            elif uploads:
                upload = uploads[0]
                upload_name = html.escape(str(upload.get("name") or name))
                link = _valid_telegram_message_link(upload.get("link"))
                if link:
                    escaped_link = html.escape(link, quote=True)
                    lines.append(
                        f'{prefix}{branch} {item_index}. '
                        f'<a href="{escaped_link}">{upload_name}</a>'
                    )
                else:
                    lines.append(
                        f"{prefix}{branch} {item_index}. "
                        f"{upload_name} (link missing)"
                    )
            else:
                lines.append(
                    f"{prefix}{branch} {item_index}. "
                    f"{html.escape(name)} (upload missing)"
                )

    render_directory(root)
    return lines


async def _send_terabox_chain_index(message, chain):
    files_by_session = {
        session_doc["_id"]: await terabox_session_store.list_files(
            session_doc["_id"]
        )
        for session_doc in chain
    }
    lines = _chain_index_lines(chain, files_by_session)
    chunks = []
    current = ""
    for line in lines:
        candidate = current + line + "\n"
        if current and len(candidate.encode("utf-8")) > 3500:
            chunks.append(current.rstrip())
            current = (
                f"<b>TeraBox upload index (continued)</b> — "
                f"<code>{chain[0]['chain_id']}</code>\n{line}\n"
            )
        else:
            current = candidate
    if current:
        chunks.append(current.rstrip())
    for chunk in chunks:
        await message.reply_text(chunk, disable_web_page_preview=True)


async def _maybe_complete_terabox_session(client, message, session_id):
    session_doc = await terabox_session_store.get_session(session_id)
    if not session_doc:
        return False
    chain_id = session_doc.get("chain_id") or session_id
    lock = terabox_chain_locks.setdefault(chain_id, asyncio.Lock())
    async with lock:
        session_doc = await terabox_session_store.get_session(session_id)
        if not session_doc or session_doc["state"] != SESSION_RUNNING:
            return False
        counts = await terabox_session_store.counts(session_id)
        if counts.get(FILE_UPLOADED, 0) != session_doc["total_files"]:
            return False
        completed = await terabox_session_store.set_state(
            session_id, SESSION_COMPLETED
        )
        next_session = await terabox_session_store.activate_next_chain_part(
            session_id
        )
        if next_session is not None:
            _start_terabox_session(client, message, next_session["_id"])
            return True

        chain = await terabox_session_store.list_chain(completed["chain_id"])
        if chain and all(item["state"] == SESSION_COMPLETED for item in chain):
            last = chain[-1]
            if not last.get("index_sent"):
                await _send_terabox_chain_index(message, chain)
                await terabox_session_store.set_state(
                    last["_id"], SESSION_COMPLETED, index_sent=True
                )
        terabox_chain_locks.pop(chain_id, None)
        return True


async def _run_terabox_session(client, message, session_id, resolved=None):
    session_doc = await terabox_session_store.get_session(session_id)
    if not session_doc or session_doc["state"] != SESSION_RUNNING:
        return
    try:
        resolver, file_list = resolved or await _resolve_terabox_share(
            session_doc["source_url"]
        )
    except Exception as error:
        await _fail_terabox_session(message, session_id, None, error)
        return

    while True:
        session_doc = await terabox_session_store.get_session(session_id)
        if not session_doc or session_doc["state"] != SESSION_RUNNING:
            return
        file_doc = await terabox_session_store.claim_next_file(session_id)
        if file_doc is None:
            counts = await terabox_session_store.counts(session_id)
            queued_or_uploaded = (
                counts.get(FILE_DOWNLOADED, 0) + counts.get(FILE_UPLOADED, 0)
            )
            if queued_or_uploaded != session_doc["total_files"]:
                await _fail_terabox_session(
                    message, session_id, None, "one or more files did not download"
                )
                return
            # Upload callbacks complete the part after Telegram accepted every
            # file and cleanup released its temporary disk space.
            await _maybe_complete_terabox_session(
                client, message, session_id
            )
            return

        try:
            file_info = _resolved_file_for_session(
                file_doc, file_list, session_doc["source_url"]
            )
            if resolver is not None:
                item = file_info["terabox_file"]
                download_url = await resolver.authorize_download_url(
                    item.download_url
                )
                request_headers = [
                    f"User-Agent: {TERABOX_USER_AGENT}",
                    f"Referer: {TERABOX_BASE_URL}",
                ]
            else:
                download_url = file_info.get("normal_dlink") or file_info.get(
                    "zip_dlink"
                )
                request_headers = None
            if not download_url:
                raise TeraboxError(
                    f"No download link is available for {file_doc['filename']}"
                )

            async def on_gid(gid, current_file=file_doc):
                current_session = await terabox_session_store.get_session(session_id)
                if not current_session or current_session["state"] != SESSION_RUNNING:
                    return False
                await terabox_session_store.update_file(
                    current_file["_id"], FILE_DOWNLOADING, gid=gid, error=None
                )
                return True

            async def on_downloaded(current_file=file_doc):
                await terabox_session_store.update_file(
                    current_file["_id"], FILE_DOWNLOADED, gid=None, error=None
                )

            async def on_uploaded(
                sent_files, upload_error, current_file=file_doc
            ):
                source_size = max(0, int(current_file.get("size_bytes") or 0))
                expected_uploads = max(
                    1,
                    (source_size + TELEGRAM_SPLIT_SIZE - 1)
                    // TELEGRAM_SPLIT_SIZE,
                )
                telegram_files = _ordered_telegram_files(sent_files)
                uploader_complete = getattr(
                    sent_files,
                    "complete",
                    len(sent_files) == expected_uploads,
                )
                successful = (
                    uploader_complete
                    and bool(telegram_files)
                    and len(telegram_files) == len(sent_files)
                )
                if upload_error or not successful:
                    reason = upload_error or (
                        "Telegram upload did not return a valid message link "
                        f"for every part ({len(telegram_files)} valid link(s))"
                    )
                    await _fail_terabox_session(
                        message, session_id, current_file, reason
                    )
                    return
                updated = await terabox_session_store.update_file_if_status(
                    current_file["_id"],
                    FILE_DOWNLOADED,
                    FILE_UPLOADED,
                    gid=None,
                    error=None,
                    telegram_files=telegram_files,
                )
                if updated is not None:
                    await _maybe_complete_terabox_session(
                        client, message, session_id
                    )

            result = await initiate_directdl(
                client,
                message,
                download_url,
                file_doc["filename"],
                _terabox_flags_from_mode(session_doc["mode"]),
                headers=request_headers,
                on_gid=on_gid,
                on_downloaded=on_downloaded,
                download_dir=_terabox_download_dir(session_doc, file_doc),
                resume=True,
                on_uploaded=on_uploaded,
                suppress_upload_summary=True,
            )
            if result != "complete":
                current_session = await terabox_session_store.get_session(session_id)
                if not current_session or current_session["state"] != SESSION_RUNNING:
                    return
                await _fail_terabox_session(
                    message,
                    session_id,
                    file_doc,
                    result or "download failed",
                )
                return
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await _fail_terabox_session(message, session_id, file_doc, error)
            return


def _is_terabox_link_token(value):
    value = str(value or "").lower()
    return "terabox" in value or "1024tera" in value


def _split_terabox_request_from_message(message):
    args = list(message.command[1:])
    raw_link = None
    size_parts = []
    for argument in args:
        if raw_link is None and _is_terabox_link_token(argument):
            raw_link = argument
        else:
            size_parts.append(argument)

    reply = message.reply_to_message
    if raw_link is None and not getattr(reply, "empty", True):
        reply_text = (
            getattr(reply, "text", None) or getattr(reply, "caption", None) or ""
        )
        for token in reply_text.split():
            if _is_terabox_link_token(token):
                raw_link = token
                break
    if raw_link is None or not size_parts:
        return None
    try:
        max_bytes = parse_size_limit("".join(size_parts))
    except ValueError:
        return None
    if not raw_link.startswith(("http://", "https://")):
        raw_link = "https://" + raw_link
    try:
        extract_surl(raw_link)
    except TeraboxError:
        return None
    return raw_link, max_bytes


async def _create_split_terabox_sessions(
    client,
    message,
    source_url,
    max_bytes,
    reply,
    mode="normal",
    intelligent=False,
):
    try:
        resolver, file_list = await _resolve_terabox_share(source_url)
        normalized = _normalize_file_list(file_list, source_url)
        size_getter = _intelligent_workspace_bytes if intelligent else None
        groups = split_by_cumulative_size(
            normalized, max_bytes, size_getter=size_getter
        )
    except Exception as error:
        await reply.edit_text(
            f"TeraBox split failed: {html.escape(str(error))[:500]}"
        )
        return []
    if not groups:
        await reply.edit_text("No files were found in the TeraBox share.")
        return []

    chain_id = new_session_id()
    total_parts = len(groups)
    share_name = extract_surl(source_url)[:24]
    session_name = infer_shared_folder_name(
        normalized, fallback=f"TeraBox {share_name}"
    )
    session_docs = []
    chain_doc = None
    try:
        for part_index, group in enumerate(groups, 1):
            part_bytes = sum(item["size_bytes"] for item in group)
            workspace_bytes = sum(
                _intelligent_workspace_bytes(item) for item in group
            )
            session_docs.append(
                await terabox_session_store.create_session(
                    owner_id=message.from_user.id,
                    chat_id=message.chat.id,
                    source_message_id=message.id,
                    source_url=source_url,
                    title=f"{session_name} (part {part_index}/{total_parts})",
                    mode=mode,
                    custom_filename=None,
                    files=group,
                    initial_state=(
                        SESSION_RUNNING if part_index == 1 else SESSION_PAUSED
                    ),
                    session_fields={
                        "provider": "terabox",
                        "name": session_name,
                        "chain_id": chain_id,
                        "part_index": part_index,
                        "total_parts": total_parts,
                        "max_bytes": max_bytes,
                        "part_bytes": part_bytes,
                        "workspace_bytes": workspace_bytes,
                        "planning_mode": (
                            "intelligent_workspace" if intelligent else "source_size"
                        ),
                        "auto_continue": True,
                    },
                )
            )
        chain_doc = await terabox_session_store.create_chain(
            chain_id=chain_id,
            owner_id=message.from_user.id,
            chat_id=message.chat.id,
            source_message_id=message.id,
            source_url=source_url,
            name=session_name,
            mode=mode,
            total_parts=total_parts,
            total_files=len(normalized),
            session_ids=[doc["_id"] for doc in session_docs],
            chain_fields={
                "max_bytes": max_bytes,
                "total_bytes": sum(item["size_bytes"] for item in normalized),
                "workspace_bytes": sum(
                    _intelligent_workspace_bytes(item) for item in normalized
                ),
                "planning_mode": (
                    "intelligent_workspace" if intelligent else "source_size"
                ),
                "auto_continue": True,
            },
        )
    except Exception as error:
        for session_doc in session_docs:
            await terabox_session_store.delete_session(session_doc["_id"])
        if chain_doc is not None:
            await terabox_session_store.delete_chain(chain_id)
        await reply.edit_text(
            "Could not store the TeraBox sessions; partial records were removed. "
            f"Error: {html.escape(str(error))[:500]}"
        )
        return []

    lines = [
        f"<b>Name:</b> {html.escape(session_name)}",
        f"<b>TeraBox chain:</b> <code>{chain_id}</code>",
        f"<b>Files:</b> {len(normalized)} | "
        f"<b>{'Workspace' if intelligent else 'Source'} limit:</b> "
        f"{_human_size(max_bytes)}",
        f"<b>Parts:</b> {total_parts} (automatic, sequential)",
        "",
    ]
    for session_doc, group in zip(session_docs, groups):
        part_size = sum(item["size_bytes"] for item in group)
        workspace_size = sum(_intelligent_workspace_bytes(item) for item in group)
        state = "running now" if session_doc["part_index"] == 1 else "queued"
        size_text = _human_size(part_size)
        if intelligent:
            size_text += f" source, {_human_size(workspace_size)} peak workspace"
        lines.append(
            f"<b>Part {session_doc['part_index']}/{total_parts}</b> - "
            f"{len(group)} file(s), {size_text} - "
            f"<code>{session_doc['_id']}</code> ({state})"
        )
    if intelligent:
        split_count = sum(
            1 for item in normalized if item["size_bytes"] > TELEGRAM_SPLIT_SIZE
        )
        oversized = sum(
            1
            for item in normalized
            if _intelligent_workspace_bytes(item) > max_bytes
        )
        lines.extend(
            [
                "",
                f"<b>Telegram splitting:</b> {split_count} file(s) over "
                f"{_human_size(TELEGRAM_SPLIT_SIZE)}",
            ]
        )
        if oversized:
            lines.append(
                f"⚠️ {oversized} file(s) individually need more than the "
                "requested workspace limit and were placed alone."
            )
    persistence_note = (
        "MongoDB"
        if terabox_session_store.persistent
        else "memory only; configure DB_URL for restart persistence"
    )
    lines.extend(["", f"<b>Storage:</b> {persistence_note}"])
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "\n..."
    await reply.edit_text(text, disable_web_page_preview=True)
    _start_terabox_session(
        client,
        message,
        session_docs[0]["_id"],
        resolved=(resolver, file_list),
    )
    return session_docs


@Client.on_message(
    filters.command(
        ["splittera", "splitterabox", "splitziptera", "splitfiletera"]
    )
    & filters.chat(ALL_CHATS)
)
async def split_terabox_cmd(client, message):
    request = _split_terabox_request_from_message(message)
    if request is None:
        await message.reply_text(
            "Usage:\n"
            "<code>/splittera &lt;TeraBox URL&gt; &lt;size per session&gt;</code>\n"
            "Example: <code>/splittera https://terabox.com/s/... 40GB</code>\n"
            "You may also reply to a TeraBox URL with "
            "<code>/splittera 40GB</code>."
        )
        return
    source_url, max_bytes = request
    reply = await message.reply_text(
        "Resolving TeraBox files and creating size-based sessions..."
    )
    await _create_split_terabox_sessions(
        client,
        message,
        source_url,
        max_bytes,
        reply,
        mode=_terabox_mode_from_command(message.command[0]),
    )


@Client.on_message(
    filters.command(["teraintelligent", "teraintellegent"])
    & filters.chat(ALL_CHATS)
)
async def intelligent_terabox_cmd(client, message):
    request = _split_terabox_request_from_message(message)
    if request is None:
        await message.reply_text(
            "Usage:\n"
            "<code>/teraintelligent &lt;TeraBox URL&gt; "
            "&lt;available workspace&gt;</code>\n"
            "Example: <code>/teraintelligent https://terabox.com/s/... "
            "40GB</code>\n"
            "Files over Telegram's 2 GB boundary count twice because the "
            "original and all numbered split parts temporarily coexist."
        )
        return
    source_url, max_bytes = request
    reply = await message.reply_text(
        "Resolving TeraBox files and calculating peak split workspace..."
    )
    await _create_split_terabox_sessions(
        client,
        message,
        source_url,
        max_bytes,
        reply,
        intelligent=True,
    )


def _terabox_session_id_from_message(message):
    if len(message.command) > 1:
        return message.command[1].strip().lower()
    reply = message.reply_to_message
    if not getattr(reply, "empty", True):
        match = re.search(
            r"TeraBox session:\s*([0-9a-f]{12})", reply.text or "", re.I
        )
        if match:
            return match.group(1).lower()
    return None


async def _owned_terabox_session(message, default_states=None):
    session_id = _terabox_session_id_from_message(message)
    if session_id:
        return await terabox_session_store.get_session(
            session_id, owner_id=message.from_user.id
        )
    sessions = await terabox_session_store.list_sessions(
        owner_id=message.from_user.id,
        states=default_states,
        limit=2,
    )
    return sessions[0] if len(sessions) == 1 else None


def _chain_session_line(session_doc, *, is_last=False):
    branch = "└──" if is_last else "├──"
    part_index = int(session_doc.get("part_index") or 1)
    total_parts = int(session_doc.get("total_parts") or 1)
    state = html.escape(str(session_doc.get("state") or "unknown"))
    total_files = int(session_doc.get("total_files") or 0)
    size = _human_size(int(session_doc.get("part_bytes") or 0))
    return (
        f"{branch} <b>Part {part_index}/{total_parts}</b> · {state} · "
        f"{total_files} file(s) · {size} · "
        f"<code>{session_doc['_id']}</code>"
    )


def _chain_page_header(chain_doc, sessions, continued=False):
    name = html.escape(str(chain_doc.get("name") or "TeraBox"))
    completed = sum(
        1 for doc in sessions if doc.get("state") == SESSION_COMPLETED
    )
    suffix = " <i>(continued)</i>" if continued else ""
    return [
        f"📁 <b>{name}</b>{suffix}",
        f"Chain: <code>{chain_doc['_id']}</code>",
        f"Progress: {completed}/{len(sessions)} session(s) completed",
    ]


async def _terabox_chain_segments(chain_doc):
    """Render one parent chain, splitting very large child lists safely."""
    sessions = await terabox_session_store.list_chain(chain_doc["_id"])
    if not sessions:
        return ["\n".join(_chain_page_header(chain_doc, sessions) + ["└── No sessions"])]

    entries = [
        _chain_session_line(doc, is_last=index == len(sessions) - 1)
        for index, doc in enumerate(sessions)
    ]
    segments = []
    current_entries = []
    for entry in entries:
        header = _chain_page_header(
            chain_doc, sessions, continued=bool(segments)
        )
        candidate = "\n".join(header + current_entries + [entry])
        if current_entries and len(candidate.encode("utf-8")) > TERABOX_CHAIN_PAGE_BYTES:
            segments.append("\n".join(header + current_entries))
            current_entries = [entry]
        else:
            current_entries.append(entry)
    if current_entries:
        header = _chain_page_header(
            chain_doc, sessions, continued=bool(segments)
        )
        segments.append("\n".join(header + current_entries))
    return segments


async def _terabox_sessions_pages(owner_id):
    chains = await terabox_session_store.list_chains(owner_id=owner_id, limit=0)
    pages = []
    for chain_doc in chains:
        pages.extend(await _terabox_chain_segments(chain_doc))
    return pages


async def _terabox_sessions_page(owner_id, requested_page=1):
    pages = await _terabox_sessions_pages(owner_id)
    if not pages:
        return "You do not have any persistent TeraBox sessions.", None
    total_pages = len(pages)
    page = min(max(1, int(requested_page)), total_pages)
    text = (
        f"<b>Your TeraBox chains</b> — Page {page}/{total_pages}\n\n"
        f"{pages[page - 1]}\n\n"
        "Inspect: <code>/terasession SESSION_ID</code>\n"
        "Resume: <code>/continuetera CHAIN_OR_SESSION_ID</code>"
    )
    if not terabox_session_store.persistent:
        text += "\n\nDB_URL is not configured; these records are memory-only."

    buttons = []
    if page > 1:
        buttons.append(
            InlineKeyboardButton(
                "Previous",
                callback_data=f"terasessions_page:{int(owner_id)}:{page - 1}",
            )
        )
    buttons.append(
        InlineKeyboardButton(
            f"{page}/{total_pages}",
            callback_data=f"terasessions_page:{int(owner_id)}:{page}",
        )
    )
    if page < total_pages:
        buttons.append(
            InlineKeyboardButton(
                "Next",
                callback_data=f"terasessions_page:{int(owner_id)}:{page + 1}",
            )
        )
    return text, InlineKeyboardMarkup([buttons])


@Client.on_message(
    filters.command(["terasessions", "tsessions"]) & filters.chat(ALL_CHATS)
)
async def terabox_sessions_cmd(client, message):
    page = 1
    if len(message.command) > 1:
        try:
            page = int(message.command[1])
        except (TypeError, ValueError):
            await message.reply_text("Usage: <code>/terasessions [page]</code>")
            return
    text, reply_markup = await _terabox_sessions_page(
        message.from_user.id, page
    )
    await message.reply_text(text, reply_markup=reply_markup)


@Client.on_callback_query(filters.regex(r"^terasessions_page:\d+:\d+$"))
async def terabox_sessions_page_callback(client, callback_query):
    _, owner_text, page_text = callback_query.data.split(":", 2)
    owner_id = int(owner_text)
    if callback_query.from_user.id != owner_id:
        await callback_query.answer(
            "Only the user who opened this list can change its page.",
            show_alert=True,
        )
        return
    text, reply_markup = await _terabox_sessions_page(
        owner_id, int(page_text)
    )
    try:
        await callback_query.message.edit_text(text, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    await callback_query.answer()


@Client.on_message(
    filters.command(["terasession", "tsession"]) & filters.chat(ALL_CHATS)
)
async def terabox_session_cmd(client, message):
    session_doc = await _owned_terabox_session(
        message, default_states=(SESSION_RUNNING,)
    )
    if session_doc is None:
        await message.reply_text(
            "TeraBox session not found or more than one session is running. "
            "Use <code>/terasessions</code> and then "
            "<code>/terasession SESSION_ID</code>."
        )
        return
    session_id = session_doc["_id"]
    counts = await terabox_session_store.counts(session_id)
    await message.reply_text(
        f"<b>Name:</b> {html.escape(str(session_doc.get('name') or 'TeraBox'))}\n"
        f"<b>TeraBox session:</b> <code>{session_id}</code>\n"
        f"<b>Chain:</b> <code>{session_doc['chain_id']}</code>\n"
        f"<b>Part:</b> {session_doc['part_index']}/{session_doc['total_parts']}\n"
        f"<b>State:</b> {html.escape(session_doc['state'])}\n"
        f"<b>Downloaded:</b> "
        f"{counts[FILE_DOWNLOADED] + counts.get(FILE_UPLOADED, 0)}/"
        f"{session_doc['total_files']}\n"
        f"<b>Uploaded:</b> {counts.get(FILE_UPLOADED, 0)}/"
        f"{session_doc['total_files']}\n"
        f"<b>Part size:</b> {_human_size(session_doc['part_bytes'])}"
    )


@Client.on_message(
    filters.command(["continuetera", "resumetera"]) & filters.chat(ALL_CHATS)
)
async def continue_terabox_session_cmd(client, message):
    requested_id = _terabox_session_id_from_message(message)
    session_doc = (
        await terabox_session_store.get_session(
            requested_id, owner_id=message.from_user.id
        )
        if requested_id
        else None
    )
    chain_doc = None
    if session_doc is None and requested_id:
        chain_doc = await terabox_session_store.get_chain(
            requested_id, owner_id=message.from_user.id
        )
        if chain_doc is not None:
            chain_sessions = await terabox_session_store.list_chain(
                chain_doc["_id"]
            )
            session_doc = next(
                (
                    item
                    for item in chain_sessions
                    if item["state"] != SESSION_COMPLETED
                ),
                None,
            )
            if session_doc is None:
                await message.reply_text("That TeraBox chain is already complete.")
                return
    if session_doc is None:
        await message.reply_text(
            "Usage: <code>/continuetera &lt;chain or session ID&gt;</code>"
        )
        return
    session_id = session_doc["_id"]
    chain = await terabox_session_store.list_chain(session_doc["chain_id"])
    blockers = [
        item
        for item in chain
        if item["part_index"] < session_doc["part_index"]
        and item["state"] != SESSION_COMPLETED
    ]
    if blockers:
        await message.reply_text(
            f"Part {blockers[0]['part_index']} must complete before this part starts."
        )
        return
    running_task = terabox_session_tasks.get(session_id)
    if running_task and not running_task.done():
        await message.reply_text("That TeraBox session is already running.")
        return
    if session_doc["state"] == SESSION_COMPLETED:
        await message.reply_text("That TeraBox session is already complete.")
        return
    await terabox_session_store.prepare_continue(
        session_id, message.chat.id, message.id
    )
    _start_terabox_session(client, message, session_id)
    await message.reply_text(
        f"Resumed TeraBox session <code>{session_id}</code>."
    )


help_dict["terabox"] = (
    "TeraBox",
    """/tera <i>&lt;TeraBox URL&gt;</i>
/ziptera <i>&lt;TeraBox URL&gt;</i>
/filetera <i>&lt;TeraBox URL&gt;</i> - Sends videos as files
/splittera <i>&lt;TeraBox URL&gt; &lt;size&gt;</i> - Size-based sequential sessions
/splitziptera <i>&lt;TeraBox URL&gt; &lt;size&gt;</i> - Zip mode
/splitfiletera <i>&lt;TeraBox URL&gt; &lt;size&gt;</i> - File mode
/teraintelligent <i>&lt;TeraBox URL&gt; &lt;workspace&gt;</i> - Account for original + split-part disk usage
/terasessions <i>[page]</i> - List persistent parent chains and child sessions
/terasession <i>[session ID]</i> - Show one part or the only running part
/continuetera <i>&lt;chain or session ID&gt;</i> - Resume the next unfinished part

Downloads files from TeraBox shares and uploads them to Telegram.""",
)

help_dict["terabox_cookie"] = (
    "TeraBox Cookie (configured chat administrators only)",
    """/setteraboxcookie <i>&lt;ndus value&gt;</i> - Validate and persist a replacement cookie
/teraboxcookiestatus - Show whether an override is active without revealing it
/clearteraboxcookie - Remove the database override and use the environment fallback""",
)
