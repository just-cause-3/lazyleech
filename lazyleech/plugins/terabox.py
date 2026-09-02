"""Download TeraBox shares and upload their files to Telegram."""

import asyncio
import html
import json
import os
import re

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus

from .. import (
    ADMIN_CHATS,
    ALL_CHATS,
    ForceDocumentFlag,
    SendAsZipFlag,
    help_dict,
    session,
)
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
    SESSION_COMPLETED,
    SESSION_FAILED,
    SESSION_PAUSED,
    SESSION_RUNNING,
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
            (FILE_PENDING, FILE_RESOLVING, FILE_DOWNLOADING),
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
            if counts[FILE_DOWNLOADED] != session_doc["total_files"]:
                await _fail_terabox_session(
                    message, session_id, None, "one or more files did not download"
                )
                return
            completed = await terabox_session_store.set_state(
                session_id, SESSION_COMPLETED
            )
            await message.reply_text(
                f"TeraBox session <code>{session_id}</code> "
                f"(part {completed['part_index']}/{completed['total_parts']}) "
                "finished downloading."
            )
            next_session = await terabox_session_store.activate_next_chain_part(
                session_id
            )
            if next_session is not None:
                await message.reply_text(
                    f"Starting TeraBox part "
                    f"{next_session['part_index']}/{next_session['total_parts']}: "
                    f"<code>{next_session['_id']}</code>"
                )
                _start_terabox_session(client, message, next_session["_id"])
            else:
                chain = await terabox_session_store.list_chain(
                    completed["chain_id"]
                )
                if chain and all(
                    item["state"] == SESSION_COMPLETED for item in chain
                ):
                    await message.reply_text(
                        f"TeraBox split chain "
                        f"<code>{completed['chain_id']}</code> completed."
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
    client, message, source_url, max_bytes, reply, mode="normal"
):
    try:
        resolver, file_list = await _resolve_terabox_share(source_url)
        normalized = _normalize_file_list(file_list, source_url)
        groups = split_by_cumulative_size(normalized, max_bytes)
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
    session_docs = []
    try:
        for part_index, group in enumerate(groups, 1):
            part_bytes = sum(item["size_bytes"] for item in group)
            session_docs.append(
                await terabox_session_store.create_session(
                    owner_id=message.from_user.id,
                    chat_id=message.chat.id,
                    source_message_id=message.id,
                    source_url=source_url,
                    title=f"TeraBox {share_name} (part {part_index}/{total_parts})",
                    mode=mode,
                    custom_filename=None,
                    files=group,
                    initial_state=(
                        SESSION_RUNNING if part_index == 1 else SESSION_PAUSED
                    ),
                    session_fields={
                        "provider": "terabox",
                        "chain_id": chain_id,
                        "part_index": part_index,
                        "total_parts": total_parts,
                        "max_bytes": max_bytes,
                        "part_bytes": part_bytes,
                        "auto_continue": True,
                    },
                )
            )
    except Exception as error:
        for session_doc in session_docs:
            await terabox_session_store.delete_session(session_doc["_id"])
        await reply.edit_text(
            "Could not store the TeraBox sessions; partial records were removed. "
            f"Error: {html.escape(str(error))[:500]}"
        )
        return []

    lines = [
        f"<b>TeraBox chain:</b> <code>{chain_id}</code>",
        f"<b>Files:</b> {len(normalized)} | <b>Limit:</b> {_human_size(max_bytes)}",
        f"<b>Parts:</b> {total_parts} (automatic, sequential)",
        "",
    ]
    for session_doc, group in zip(session_docs, groups):
        part_size = sum(item["size_bytes"] for item in group)
        state = "running now" if session_doc["part_index"] == 1 else "queued"
        lines.append(
            f"<b>Part {session_doc['part_index']}/{total_parts}</b> - "
            f"{len(group)} file(s), {_human_size(part_size)} - "
            f"<code>{session_doc['_id']}</code> ({state})"
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


@Client.on_message(
    filters.command(["terasession", "tsession"]) & filters.chat(ALL_CHATS)
)
async def terabox_session_cmd(client, message):
    session_id = _terabox_session_id_from_message(message)
    session_doc = (
        await terabox_session_store.get_session(
            session_id, owner_id=message.from_user.id
        )
        if session_id
        else None
    )
    if session_doc is None:
        await message.reply_text("TeraBox session not found.")
        return
    counts = await terabox_session_store.counts(session_id)
    await message.reply_text(
        f"<b>TeraBox session:</b> <code>{session_id}</code>\n"
        f"<b>Chain:</b> <code>{session_doc['chain_id']}</code>\n"
        f"<b>Part:</b> {session_doc['part_index']}/{session_doc['total_parts']}\n"
        f"<b>State:</b> {html.escape(session_doc['state'])}\n"
        f"<b>Downloaded:</b> {counts[FILE_DOWNLOADED]}/"
        f"{session_doc['total_files']}\n"
        f"<b>Part size:</b> {_human_size(session_doc['part_bytes'])}"
    )


@Client.on_message(
    filters.command(["continuetera", "resumetera"]) & filters.chat(ALL_CHATS)
)
async def continue_terabox_session_cmd(client, message):
    session_id = _terabox_session_id_from_message(message)
    session_doc = (
        await terabox_session_store.get_session(
            session_id, owner_id=message.from_user.id
        )
        if session_id
        else None
    )
    if session_doc is None:
        await message.reply_text(
            "Usage: <code>/continuetera &lt;TeraBox session ID&gt;</code>"
        )
        return
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
/terasession <i>&lt;session ID&gt;</i> - Show part status
/continuetera <i>&lt;session ID&gt;</i> - Resume a stopped part

Downloads files from TeraBox shares and uploads them to Telegram.""",
)

help_dict["terabox_cookie"] = (
    "TeraBox Cookie (configured chat administrators only)",
    """/setteraboxcookie <i>&lt;ndus value&gt;</i> - Validate and persist a replacement cookie
/teraboxcookiestatus - Show whether an override is active without revealing it
/clearteraboxcookie - Remove the database override and use the environment fallback""",
)
