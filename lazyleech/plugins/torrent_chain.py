"""Workspace-bounded, persistent selective torrent chains."""

import asyncio
import html
import os
import re
import shutil
import tempfile
from urllib.parse import urlparse

from natsort import natsorted
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .. import (
    ALL_CHATS,
    IGNORE_PADDING_FILE,
    LEECH_TIMEOUT,
    ForceDocumentFlag,
    help_dict,
    session,
)
from ..utils.aria2 import (
    Aria2Error,
    aria2_add_torrent,
    aria2_remove,
    aria2_tell_status,
    aria2_unpause,
)
from ..utils.bunkr_sessions import (
    FILE_CANCELLED,
    FILE_DOWNLOADED,
    FILE_DOWNLOADING,
    FILE_FAILED,
    SESSION_CANCELLED,
    SESSION_COMPLETED,
    SESSION_FAILED,
    SESSION_PAUSED,
    SESSION_RUNNING,
    new_session_id,
)
from ..utils.file_split import TELEGRAM_SPLIT_SIZE
from ..utils.misc import format_bytes
from ..utils.terabox_sessions import FILE_UPLOADED
from ..utils.torrent_sessions import torrent_session_store
from .leech import _new_download_reference, _parse_workspace_size, handle_leech


TORRENT_FILE_MAX_BYTES = 4 * 1024**2
try:
    _workspace_reserve_mb = max(
        0, int(os.environ.get("TORRENT_WORKSPACE_RESERVE_MB", "512"))
    )
except (TypeError, ValueError):
    _workspace_reserve_mb = 512
TORRENT_WORKSPACE_RESERVE_BYTES = _workspace_reserve_mb * 1024**2
TORRENT_CHAIN_PAGE_SIZE = 5

torrent_session_tasks = {}
torrent_chain_locks = {}
torrent_pending_uploads = set()


def _torrent_file_split_extra(size_bytes):
    size = max(0, int(size_bytes or 0))
    return size if size > TELEGRAM_SPLIT_SIZE else 0


def _torrent_part_peak(files):
    """Peak bytes: all selected sources plus the largest split copy."""
    source_bytes = sum(max(0, int(item.get("size_bytes") or 0)) for item in files)
    split_extra = max(
        (_torrent_file_split_extra(item.get("size_bytes")) for item in files),
        default=0,
    )
    return source_bytes + split_extra


def _partition_torrent_files(files, workspace_bytes):
    workspace_bytes = int(workspace_bytes)
    if workspace_bytes <= 0:
        raise ValueError("Workspace must be greater than zero")
    groups = []
    current = []
    for item in files:
        if _torrent_part_peak([item]) > workspace_bytes:
            raise ValueError(
                f"{item['relative_path']} needs about "
                f"{format_bytes(_torrent_part_peak([item]))} of workspace"
            )
        candidate = current + [item]
        if current and _torrent_part_peak(candidate) > workspace_bytes:
            groups.append(current)
            current = [item]
        else:
            current = candidate
    if current:
        groups.append(current)
    return groups


def _relative_torrent_path(path, base_dir):
    path = os.path.normpath(str(path or ""))
    base_dir = os.path.normpath(str(base_dir or ""))
    try:
        relative = os.path.relpath(path, base_dir)
    except ValueError:
        relative = os.path.basename(path)
    if relative.startswith(".."):
        relative = os.path.basename(path)
    return relative.replace("\\", "/").lstrip("/")


def _torrent_metadata_files(torrent_info):
    base_dir = torrent_info.get("dir") or ""
    files = []
    for fallback_index, item in enumerate(torrent_info.get("files") or [], 1):
        relative_path = _relative_torrent_path(item.get("path"), base_dir)
        if not relative_path:
            continue
        if IGNORE_PADDING_FILE and re.match(
            r"(?i)^_+padding_file", os.path.basename(relative_path)
        ):
            continue
        try:
            torrent_index = int(item.get("index") or fallback_index)
            size_bytes = max(0, int(item.get("length") or 0))
        except (TypeError, ValueError):
            continue
        files.append(
            {
                "torrent_index": torrent_index,
                "relative_path": relative_path,
                "filename": os.path.basename(relative_path),
                "size_bytes": size_bytes,
            }
        )
    files.sort(key=lambda item: item["torrent_index"])
    return files


def _torrent_title(torrent_info):
    return str(
        ((torrent_info.get("bittorrent") or {}).get("info") or {}).get("name")
        or "Torrent"
    ).strip() or "Torrent"


async def _remove_torrent_gid(gid, *, cleanup=False):
    if not gid:
        return
    directory = None
    try:
        status = await aria2_tell_status(session, gid)
        directory = status.get("dir")
    except Aria2Error:
        pass
    try:
        await aria2_remove(session, gid)
    except Aria2Error:
        pass
    if cleanup and directory and os.path.isdir(directory):
        await asyncio.to_thread(shutil.rmtree, directory, True)


async def _inspect_torrent_bytes(owner_id, torrent_data):
    owner_dir = os.path.join(os.getcwd(), str(int(owner_id)))
    os.makedirs(owner_dir, exist_ok=True)
    metadata_dir = tempfile.mkdtemp(prefix="torrent-metadata-", dir=owner_dir)
    fd, torrent_path = tempfile.mkstemp(suffix=".torrent", dir=owner_dir)
    gid = None
    try:
        with os.fdopen(fd, "wb") as torrent_file:
            torrent_file.write(torrent_data)
        gid = await aria2_add_torrent(
            session,
            owner_id,
            torrent_path,
            LEECH_TIMEOUT,
            pause=True,
            download_dir=metadata_dir,
        )
        torrent_info = await aria2_tell_status(session, gid)
        files = _torrent_metadata_files(torrent_info)
        if not files:
            raise ValueError("The torrent does not contain downloadable files")
        return _torrent_title(torrent_info), files
    finally:
        if gid:
            await _remove_torrent_gid(gid, cleanup=True)
        if os.path.exists(torrent_path):
            os.remove(torrent_path)
        if os.path.isdir(metadata_dir):
            shutil.rmtree(metadata_dir, ignore_errors=True)


async def _download_torrent_bytes(message_with_document):
    downloaded = await message_with_document.download(in_memory=True)
    if hasattr(downloaded, "getvalue"):
        data = downloaded.getvalue()
    elif hasattr(downloaded, "read"):
        data = downloaded.read()
    else:
        raise ValueError("Telegram did not return the torrent file")
    return bytes(data)


async def _fetch_torrent_bytes(url):
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only HTTP(S) torrent URLs are supported")
    async with session.get(url, timeout=30) as response:
        if response.status >= 400:
            raise ValueError(f"Torrent URL returned HTTP {response.status}")
        data = await response.content.read(TORRENT_FILE_MAX_BYTES + 1)
    return bytes(data)


async def _torrent_chain_request(message):
    raw = message.text or message.caption or ""
    arguments = raw.split(None, 1)[1].strip() if len(raw.split(None, 1)) > 1 else ""
    reply = message.reply_to_message
    source_message = message if getattr(message, "document", None) else reply
    source_document = (
        getattr(source_message, "document", None)
        if not getattr(source_message, "empty", False)
        else None
    )

    tokens = arguments.rsplit(None, 1) if arguments else []
    if source_document:
        if int(source_document.file_size or 0) > TORRENT_FILE_MAX_BYTES:
            raise ValueError("The torrent metadata exceeds the 4 MiB safety limit")
        if not str(source_document.file_name or "").lower().endswith(".torrent"):
            raise ValueError("Reply to or attach a .torrent file")
        size_text = tokens[-1] if tokens else arguments
        workspace_bytes = _parse_workspace_size(size_text)
        torrent_data = await _download_torrent_bytes(source_message)
        source_url = f"telegram:{source_document.file_name or 'upload.torrent'}"
    else:
        if len(tokens) != 2:
            raise ValueError("Provide a torrent URL and workspace size")
        source_url, size_text = tokens
        workspace_bytes = _parse_workspace_size(size_text)
        torrent_data = await _fetch_torrent_bytes(source_url)

    if not torrent_data:
        raise ValueError("The torrent file is empty")
    if len(torrent_data) > TORRENT_FILE_MAX_BYTES:
        raise ValueError("The torrent metadata exceeds the 4 MiB safety limit")
    return torrent_data, source_url, workspace_bytes


async def _create_torrent_chain(message, torrent_data, source_url, workspace_bytes):
    title, files = await _inspect_torrent_bytes(message.from_user.id, torrent_data)
    groups = _partition_torrent_files(files, workspace_bytes)
    chain_id = new_session_id()
    total_parts = len(groups)
    session_docs = []
    try:
        for part_index, group in enumerate(groups, 1):
            initial_state = SESSION_RUNNING if part_index == 1 else SESSION_PAUSED
            session_doc = await torrent_session_store.create_session(
                owner_id=message.from_user.id,
                chat_id=message.chat.id,
                source_message_id=message.id,
                source_url=source_url,
                title=f"{title} (part {part_index}/{total_parts})",
                mode="document" if "file" in message.command[0].lower() else "normal",
                custom_filename=None,
                files=[
                    {
                        "page_url": source_url,
                        "filename": item["filename"],
                        "relative_path": item["relative_path"],
                        "torrent_index": item["torrent_index"],
                        "size_bytes": item["size_bytes"],
                        "telegram_files": [],
                    }
                    for item in group
                ],
                initial_state=initial_state,
                session_fields={
                    "chain_id": chain_id,
                    "name": title,
                    "part_index": part_index,
                    "total_parts": total_parts,
                    "workspace_bytes": int(workspace_bytes),
                    "peak_workspace_bytes": _torrent_part_peak(group),
                    "source_bytes": sum(item["size_bytes"] for item in group),
                    "provider": "torrent_chain",
                },
            )
            session_docs.append(session_doc)
        await torrent_session_store.create_chain(
            chain_id=chain_id,
            owner_id=message.from_user.id,
            chat_id=message.chat.id,
            source_message_id=message.id,
            source_url=source_url,
            name=title,
            mode=session_docs[0]["mode"],
            total_parts=total_parts,
            total_files=len(files),
            session_ids=[item["_id"] for item in session_docs],
            chain_fields={
                "workspace_bytes": int(workspace_bytes),
                "torrent_data": torrent_data,
                "provider": "torrent_chain",
            },
        )
    except Exception:
        for session_doc in session_docs:
            await torrent_session_store.delete_session(session_doc["_id"])
        raise
    return chain_id, title, files, session_docs


def _torrent_plan_chunks(chain_id, title, workspace_bytes, sessions):
    lines = [
        f"<b>Torrent chain:</b> <code>{chain_id}</code>",
        f"<b>Name:</b> {html.escape(title)}",
        f"<b>Workspace:</b> {format_bytes(workspace_bytes)}",
        f"<b>Parts:</b> {len(sessions)} (automatic, sequential)",
        "",
    ]
    for item in sessions:
        state = "running now" if item["part_index"] == 1 else "queued"
        lines.append(
            f"Part {item['part_index']}/{item['total_parts']} — "
            f"{item['total_files']} file(s), "
            f"{format_bytes(item['source_bytes'])} source, "
            f"{format_bytes(item['peak_workspace_bytes'])} peak — "
            f"<code>{item['_id']}</code> ({state})"
        )
    chunks = []
    current = ""
    for line in lines:
        candidate = current + line + "\n"
        if current and len(candidate.encode("utf-8")) > 3500:
            chunks.append(current.rstrip())
            current = (
                f"<b>Torrent chain plan (continued)</b> — "
                f"<code>{chain_id}</code>\n{line}\n"
            )
        else:
            current = candidate
    if current:
        chunks.append(current.rstrip())
    return chunks


@Client.on_message(
    filters.command(["splittorrent", "splitfiletorrent"])
    & filters.chat(ALL_CHATS)
)
async def split_torrent_cmd(client, message):
    try:
        torrent_data, source_url, workspace_bytes = await _torrent_chain_request(
            message
        )
        chain_id, title, _files, sessions = await _create_torrent_chain(
            message, torrent_data, source_url, workspace_bytes
        )
    except (ValueError, Aria2Error) as error:
        await message.reply_text(
            "Could not create torrent chain: " + html.escape(str(error))
        )
        return
    for chunk in _torrent_plan_chunks(chain_id, title, workspace_bytes, sessions):
        await message.reply_text(chunk)
    _start_torrent_session(client, message, sessions[0]["_id"])


def _torrent_download_dir(session_doc):
    return os.path.join(
        os.getcwd(),
        str(int(session_doc["owner_id"])),
        "torrent_sessions",
        str(session_doc["_id"]),
    )


def _directory_allocated_bytes(path):
    total = 0
    if not os.path.isdir(path):
        return total
    for root, directories, files in os.walk(path):
        directories[:] = [
            name for name in directories if not os.path.islink(os.path.join(root, name))
        ]
        for name in files:
            filepath = os.path.join(root, name)
            try:
                if os.path.islink(filepath):
                    continue
                stat = os.stat(filepath)
                total += int(getattr(stat, "st_blocks", 0) or 0) * 512
                if not getattr(stat, "st_blocks", None):
                    total += int(stat.st_size)
            except OSError:
                continue
    return total


def _valid_telegram_link(value):
    link = str(value or "").strip()
    parsed = urlparse(link)
    if parsed.scheme != "https" or (parsed.hostname or "").lower() not in {
        "t.me",
        "telegram.me",
    }:
        return None
    return link


def _map_torrent_uploads(file_docs, sent_files):
    source_results = list(getattr(sent_files, "source_results", None) or [])
    if source_results:
        by_relative_path = {
            str(item.get("relative_path") or item["filename"])
            .replace("\\", "/")
            .strip("/"): item
            for item in file_docs
        }
        mapped = {}
        for result in source_results:
            relative_name = str(result.get("relative_name") or "")
            relative_name = relative_name.replace("\\", "/").strip("/")
            file_doc = by_relative_path.get(relative_name)
            if file_doc is None or not result.get("complete"):
                return None
            uploads = [
                {"name": str(name), "link": _valid_telegram_link(link)}
                for name, link in result.get("uploads") or []
            ]
            if not uploads or any(not upload["link"] for upload in uploads):
                return None
            mapped[file_doc["_id"]] = uploads
        if len(mapped) != len(file_docs):
            return None
        return mapped

    links = [
        {"name": str(name), "link": _valid_telegram_link(link)}
        for name, link in sent_files
    ]
    if any(not item["link"] for item in links):
        return None
    ordered_files = natsorted(
        file_docs, key=lambda item: str(item.get("relative_path") or item["filename"])
    )
    mapped = {}
    offset = 0
    for file_doc in ordered_files:
        size = max(0, int(file_doc.get("size_bytes") or 0))
        count = max(1, (size + TELEGRAM_SPLIT_SIZE - 1) // TELEGRAM_SPLIT_SIZE)
        mapped[file_doc["_id"]] = links[offset : offset + count]
        offset += count
    if offset != len(links) or any(not value for value in mapped.values()):
        return None
    return mapped


async def _mark_torrent_files(file_docs, status, **fields):
    for file_doc in file_docs:
        await torrent_session_store.update_file(
            file_doc["_id"], status, **fields
        )


def _torrent_flags(mode):
    return (ForceDocumentFlag,) if mode == "document" else ()


def _start_torrent_session(client, message, session_id):
    running = torrent_session_tasks.get(session_id)
    if running and not running.done():
        return running
    task = asyncio.create_task(_run_torrent_session(client, message, session_id))
    torrent_session_tasks[session_id] = task

    def discard(completed):
        if torrent_session_tasks.get(session_id) is completed:
            torrent_session_tasks.pop(session_id, None)

    task.add_done_callback(discard)
    return task


async def _run_torrent_session(client, message, session_id):
    session_doc = await torrent_session_store.get_session(session_id)
    if not session_doc or session_doc.get("state") != SESSION_RUNNING:
        return
    chain_doc = await torrent_session_store.get_chain(session_doc["chain_id"])
    if not chain_doc or not chain_doc.get("torrent_data"):
        await torrent_session_store.set_state(session_id, SESSION_FAILED)
        await message.reply_text(
            f"Torrent session <code>{session_id}</code> has no persisted metadata."
        )
        return
    file_docs = await torrent_session_store.list_files(session_id)
    unfinished = [
        item for item in file_docs if item.get("status") != FILE_UPLOADED
    ]
    if not unfinished:
        await _complete_torrent_session(client, message, session_id)
        return

    peak = int(session_doc.get("peak_workspace_bytes") or 0)
    download_dir = _torrent_download_dir(session_doc)
    os.makedirs(os.path.dirname(download_dir), exist_ok=True)
    free = shutil.disk_usage(os.path.dirname(download_dir)).free
    allocated = await asyncio.to_thread(_directory_allocated_bytes, download_dir)
    additional = max(0, peak - allocated)
    required_free = additional + TORRENT_WORKSPACE_RESERVE_BYTES
    if free < required_free:
        await torrent_session_store.set_state(session_id, SESSION_FAILED)
        await message.reply_text(
            f"Torrent session <code>{session_id}</code> needs about "
            f"{format_bytes(required_free)} additional free space "
            f"but only {format_bytes(free)} is available. Clear space and use "
            f"<code>/continuetorrent {session_id}</code>."
        )
        return

    owner_dir = os.path.join(os.getcwd(), str(int(session_doc["owner_id"])))
    os.makedirs(owner_dir, exist_ok=True)
    fd, torrent_path = tempfile.mkstemp(suffix=".torrent", dir=owner_dir)
    gid = None
    try:
        with os.fdopen(fd, "wb") as torrent_file:
            torrent_file.write(bytes(chain_doc["torrent_data"]))
        selected = [int(item["torrent_index"]) for item in unfinished]
        gid = await aria2_add_torrent(
            session,
            session_doc["owner_id"],
            torrent_path,
            LEECH_TIMEOUT,
            pause=True,
            download_dir=download_dir,
            selected_files=selected,
        )
        await torrent_session_store.set_state(
            session_id, SESSION_RUNNING, gid=gid
        )
        await _mark_torrent_files(
            unfinished, FILE_DOWNLOADING, gid=gid, error=None
        )
        await aria2_unpause(session, gid)

        async def on_downloaded():
            await _mark_torrent_files(
                unfinished, FILE_DOWNLOADED, gid=None, error=None
            )
            torrent_pending_uploads.add(session_id)

        async def on_uploaded(sent_files, upload_error):
            torrent_pending_uploads.discard(session_id)
            mapped = _map_torrent_uploads(unfinished, sent_files)
            complete = bool(getattr(sent_files, "complete", False))
            if upload_error or not complete or mapped is None:
                reason = upload_error or (
                    "Telegram upload did not return the expected links "
                    f"({len(sent_files)} result(s))"
                )
                await _mark_torrent_files(
                    unfinished, FILE_FAILED, gid=None, error=str(reason)
                )
                await torrent_session_store.set_state(
                    session_id, SESSION_FAILED, gid=None
                )
                await message.reply_text(
                    f"Torrent session <code>{session_id}</code> stopped: "
                    f"{html.escape(str(reason))}. Resume with "
                    f"<code>/continuetorrent {session_id}</code>."
                )
                return
            for file_doc in unfinished:
                await torrent_session_store.update_file(
                    file_doc["_id"],
                    FILE_UPLOADED,
                    gid=None,
                    error=None,
                    telegram_files=mapped[file_doc["_id"]],
                )
            await _complete_torrent_session(client, message, session_id)

        result = await handle_leech(
            client,
            message,
            gid,
            _new_download_reference(message),
            session_doc["owner_id"],
            _torrent_flags(session_doc.get("mode")),
            None,
            on_downloaded=on_downloaded,
            on_uploaded=on_uploaded,
            suppress_upload_summary=True,
        )
        if result != "complete":
            await _mark_torrent_files(
                unfinished,
                FILE_FAILED if result != "removed" else FILE_CANCELLED,
                gid=None,
                error=str(result or "torrent download failed"),
            )
            await torrent_session_store.set_state(
                session_id,
                SESSION_CANCELLED if result == "removed" else SESSION_FAILED,
                gid=None,
            )
    except asyncio.CancelledError:
        if gid:
            await _remove_torrent_gid(gid, cleanup=False)
        raise
    except Exception as error:
        await _mark_torrent_files(
            unfinished, FILE_FAILED, gid=None, error=str(error)
        )
        await torrent_session_store.set_state(
            session_id, SESSION_FAILED, gid=None
        )
        await message.reply_text(
            f"Torrent session <code>{session_id}</code> stopped: "
            f"{html.escape(str(error))}. Resume with "
            f"<code>/continuetorrent {session_id}</code>."
        )
    finally:
        if os.path.exists(torrent_path):
            os.remove(torrent_path)


async def _complete_torrent_session(client, message, session_id):
    session_doc = await torrent_session_store.get_session(session_id)
    if not session_doc:
        return False
    chain_id = session_doc.get("chain_id") or session_id
    lock = torrent_chain_locks.setdefault(chain_id, asyncio.Lock())
    async with lock:
        session_doc = await torrent_session_store.get_session(session_id)
        if not session_doc or session_doc.get("state") != SESSION_RUNNING:
            return False
        counts = await torrent_session_store.counts(session_id)
        if counts.get(FILE_UPLOADED, 0) != session_doc["total_files"]:
            return False
        completed = await torrent_session_store.set_state(
            session_id, SESSION_COMPLETED, gid=None
        )
        next_session = await torrent_session_store.activate_next_chain_part(
            completed["_id"]
        )
        if next_session:
            _start_torrent_session(client, message, next_session["_id"])
            return True
        chain = await torrent_session_store.list_chain(chain_id)
        if chain and all(item.get("state") == SESSION_COMPLETED for item in chain):
            last = chain[-1]
            if not last.get("index_sent"):
                await _send_torrent_chain_index(message, chain)
                await torrent_session_store.set_state(
                    last["_id"], SESSION_COMPLETED, index_sent=True
                )
        return True


def _torrent_index_lines(chain, files_by_session):
    chain_id = chain[0]["chain_id"]
    title = str(chain[0].get("name") or "Torrent")
    lines = [
        f"<b>Torrent upload complete</b> — <code>{chain_id}</code>",
        f"📁 <b>{html.escape(title)}/</b>",
    ]
    root = {"dirs": {}, "entries": []}
    for session_doc in chain:
        for file_doc in files_by_session.get(session_doc["_id"], []):
            parts = [
                part
                for part in str(
                    file_doc.get("relative_path") or file_doc["filename"]
                ).replace("\\", "/").split("/")
                if part
            ]
            if len(parts) > 1 and parts[0] == title:
                parts = parts[1:]
            name = parts.pop() if parts else file_doc["filename"]
            node = root
            for directory in parts:
                if directory not in node["dirs"]:
                    child = {"dirs": {}, "entries": []}
                    node["dirs"][directory] = child
                    node["entries"].append(("dir", directory, child))
                node = node["dirs"][directory]
            node["entries"].append(("file", name, file_doc))

    number = 0

    def render(node, prefix=""):
        nonlocal number
        for index, (kind, name, value) in enumerate(node["entries"]):
            last = index == len(node["entries"]) - 1
            branch = "└──" if last else "├──"
            continuation = "    " if last else "│   "
            if kind == "dir":
                lines.append(f"{prefix}{branch} 📁 <b>{html.escape(name)}/</b>")
                render(value, prefix + continuation)
                continue
            number += 1
            uploads = value.get("telegram_files") or []
            if len(uploads) > 1:
                lines.append(f"{prefix}{branch} 📦 {number}. <b>{html.escape(name)}</b>")
                for part_index, upload in enumerate(uploads, 1):
                    part_last = part_index == len(uploads)
                    part_branch = "└──" if part_last else "├──"
                    link = html.escape(str(upload["link"]), quote=True)
                    upload_name = html.escape(str(upload["name"]))
                    lines.append(
                        f'{prefix}{continuation}{part_branch} '
                        f'<a href="{link}">{number}.{part_index} {upload_name}</a>'
                    )
            elif uploads:
                link = html.escape(str(uploads[0]["link"]), quote=True)
                upload_name = html.escape(str(uploads[0]["name"]))
                lines.append(
                    f'{prefix}{branch} {number}. <a href="{link}">{upload_name}</a>'
                )
            else:
                lines.append(f"{prefix}{branch} {number}. {html.escape(name)} (missing)")

    render(root)
    return lines


async def _send_torrent_chain_index(message, chain):
    files_by_session = {
        item["_id"]: await torrent_session_store.list_files(item["_id"])
        for item in chain
    }
    lines = _torrent_index_lines(chain, files_by_session)
    chunks = []
    current = ""
    for line in lines:
        candidate = current + line + "\n"
        if current and len(candidate.encode("utf-8")) > 3500:
            chunks.append(current.rstrip())
            current = (
                f"<b>Torrent upload index (continued)</b> — "
                f"<code>{chain[0]['chain_id']}</code>\n{line}\n"
            )
        else:
            current = candidate
    if current:
        chunks.append(current.rstrip())
    for chunk in chunks:
        await message.reply_text(chunk, disable_web_page_preview=True)


def _torrent_id_from_message(message):
    return message.command[1].strip() if len(message.command) > 1 else None


async def _resolve_owned_torrent_session(owner_id, requested_id):
    if requested_id:
        session_doc = await torrent_session_store.get_session(
            requested_id, owner_id=owner_id
        )
        if session_doc:
            return session_doc
        chain_doc = await torrent_session_store.get_chain(
            requested_id, owner_id=owner_id
        )
        if chain_doc:
            chain = await torrent_session_store.list_chain(chain_doc["_id"])
            return next(
                (item for item in chain if item.get("state") != SESSION_COMPLETED),
                chain[-1] if chain else None,
            )
    return None


@Client.on_message(filters.command("continuetorrent") & filters.chat(ALL_CHATS))
async def continue_torrent_cmd(client, message):
    session_doc = await _resolve_owned_torrent_session(
        message.from_user.id, _torrent_id_from_message(message)
    )
    if not session_doc:
        await message.reply_text(
            "Usage: <code>/continuetorrent &lt;chain or session ID&gt;</code>"
        )
        return
    if session_doc.get("state") == SESSION_COMPLETED:
        await message.reply_text("That torrent chain is already complete.")
        return
    chain = await torrent_session_store.list_chain(session_doc["chain_id"])
    blocker = next(
        (
            item
            for item in chain
            if int(item.get("part_index") or 0)
            < int(session_doc.get("part_index") or 0)
            and item.get("state") != SESSION_COMPLETED
        ),
        None,
    )
    if blocker:
        await message.reply_text(
            f"Part {blocker['part_index']} must complete before this part starts."
        )
        return
    running = torrent_session_tasks.get(session_doc["_id"])
    if running and not running.done():
        await message.reply_text("That torrent session is already running.")
        return
    if session_doc["_id"] in torrent_pending_uploads:
        await message.reply_text(
            "That torrent session is waiting for its Telegram uploads and "
            "verified cleanup to finish."
        )
        return
    if session_doc.get("gid"):
        await _remove_torrent_gid(session_doc["gid"], cleanup=False)
    await torrent_session_store.prepare_continue(
        session_doc["_id"], message.chat.id, message.id
    )
    _start_torrent_session(client, message, session_doc["_id"])
    await message.reply_text(
        f"Resumed torrent session <code>{session_doc['_id']}</code>."
    )


async def _torrent_chains_page(owner_id, requested_page):
    total = await torrent_session_store.count_chains(owner_id=owner_id)
    if not total:
        return "You do not have any torrent chains.", None
    pages = max(1, (total + TORRENT_CHAIN_PAGE_SIZE - 1) // TORRENT_CHAIN_PAGE_SIZE)
    page = min(max(1, int(requested_page)), pages)
    chains = await torrent_session_store.list_chains(
        owner_id=owner_id,
        limit=TORRENT_CHAIN_PAGE_SIZE,
        skip=(page - 1) * TORRENT_CHAIN_PAGE_SIZE,
    )
    lines = [f"<b>Your torrent chains — Page {page}/{pages}</b>", ""]
    for chain_doc in chains:
        children = await torrent_session_store.list_chain(chain_doc["_id"])
        completed = sum(
            item.get("state") == SESSION_COMPLETED for item in children
        )
        lines.extend(
            [
                f"📁 <b>{html.escape(str(chain_doc.get('name') or 'Torrent'))}</b>",
                f"Chain: <code>{chain_doc['_id']}</code>",
                f"Progress: {completed}/{len(children)} part(s) completed",
            ]
        )
        for child in children[:8]:
            lines.append(
                f"├── Part {child.get('part_index')}/{child.get('total_parts')} · "
                f"{html.escape(str(child.get('state')))} · "
                f"{child.get('total_files')} file(s) · <code>{child['_id']}</code>"
            )
        if len(children) > 8:
            lines.append(f"└── … {len(children) - 8} more part(s)")
        lines.append("")
    rows = []
    if page > 1:
        rows.append(
            InlineKeyboardButton(
                "Previous", callback_data=f"torrentchains_page:{owner_id}:{page - 1}"
            )
        )
    if page < pages:
        rows.append(
            InlineKeyboardButton(
                "Next", callback_data=f"torrentchains_page:{owner_id}:{page + 1}"
            )
        )
    markup = InlineKeyboardMarkup([rows]) if rows else None
    return "\n".join(lines).rstrip(), markup


@Client.on_message(filters.command("torrentchains") & filters.chat(ALL_CHATS))
async def torrent_chains_cmd(client, message):
    try:
        page = int(message.command[1]) if len(message.command) > 1 else 1
    except ValueError:
        page = 1
    text, markup = await _torrent_chains_page(message.from_user.id, page)
    await message.reply_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^torrentchains_page:\d+:\d+$"))
async def torrent_chains_page_callback(client, callback):
    _, owner_id, page = callback.data.split(":")
    if callback.from_user.id != int(owner_id):
        await callback.answer("Only the owner can change this page.", show_alert=True)
        return
    text, markup = await _torrent_chains_page(int(owner_id), int(page))
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@Client.on_message(filters.command("torrentchain") & filters.chat(ALL_CHATS))
async def torrent_chain_cmd(client, message):
    requested_id = _torrent_id_from_message(message)
    try:
        page = int(message.command[2]) if len(message.command) > 2 else 1
    except ValueError:
        page = 1
    text, markup = await _torrent_chain_detail_page(
        message.from_user.id, requested_id, page
    )
    await message.reply_text(text, reply_markup=markup)


async def _torrent_chain_detail_page(owner_id, requested_id, requested_page):
    session_doc = await _resolve_owned_torrent_session(owner_id, requested_id)
    if not session_doc:
        return (
            "Usage: <code>/torrentchain &lt;chain or session ID&gt; [page]</code>",
            None,
        )
    chain = await torrent_session_store.list_chain(session_doc["chain_id"])
    page_size = 10
    pages = max(1, (len(chain) + page_size - 1) // page_size)
    page = min(max(1, int(requested_page)), pages)
    visible = chain[(page - 1) * page_size : page * page_size]
    lines = [
        f"<b>Name:</b> {html.escape(str(session_doc.get('name') or 'Torrent'))}",
        f"<b>Chain:</b> <code>{session_doc['chain_id']}</code>",
        f"<b>Workspace:</b> {format_bytes(session_doc.get('workspace_bytes'))}",
        f"<b>Page:</b> {page}/{pages}",
        "",
    ]
    for child in visible:
        counts = await torrent_session_store.counts(child["_id"])
        lines.append(
            f"Part {child.get('part_index')}/{child.get('total_parts')} — "
            f"{html.escape(str(child.get('state')))} — "
            f"{counts.get(FILE_UPLOADED, 0)}/{child.get('total_files')} uploaded — "
            f"<code>{child['_id']}</code>"
        )
    buttons = []
    if page > 1:
        buttons.append(
            InlineKeyboardButton(
                "Previous",
                callback_data=(
                    f"torrentchain_page:{owner_id}:"
                    f"{session_doc['chain_id']}:{page - 1}"
                ),
            )
        )
    if page < pages:
        buttons.append(
            InlineKeyboardButton(
                "Next",
                callback_data=(
                    f"torrentchain_page:{owner_id}:"
                    f"{session_doc['chain_id']}:{page + 1}"
                ),
            )
        )
    markup = InlineKeyboardMarkup([buttons]) if buttons else None
    return "\n".join(lines), markup


@Client.on_callback_query(
    filters.regex(r"^torrentchain_page:\d+:[0-9a-f]+:\d+$")
)
async def torrent_chain_page_callback(client, callback):
    _, owner_id, chain_id, page = callback.data.split(":")
    if callback.from_user.id != int(owner_id):
        await callback.answer("Only the owner can change this page.", show_alert=True)
        return
    text, markup = await _torrent_chain_detail_page(
        int(owner_id), chain_id, int(page)
    )
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


async def _stop_torrent_session(session_doc):
    task = torrent_session_tasks.get(session_doc["_id"])
    if session_doc.get("gid"):
        await _remove_torrent_gid(session_doc["gid"], cleanup=False)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@Client.on_message(
    filters.command("deletetorrentchain") & filters.chat(ALL_CHATS)
)
async def delete_torrent_chain_cmd(client, message):
    requested_id = _torrent_id_from_message(message)
    session_doc = await _resolve_owned_torrent_session(
        message.from_user.id, requested_id
    )
    chain_id = session_doc.get("chain_id") if session_doc else requested_id
    chain_doc = (
        await torrent_session_store.get_chain(
            chain_id, owner_id=message.from_user.id
        )
        if chain_id
        else None
    )
    if not chain_doc:
        await message.reply_text("Torrent chain not found.")
        return
    for child in await torrent_session_store.list_chain(chain_id):
        await _stop_torrent_session(child)
    result = await torrent_session_store.delete_chain_tree(
        chain_id, message.from_user.id
    )
    await message.reply_text(
        f"Deleted torrent chain <code>{chain_id}</code> and "
        f"{result['deleted_sessions']} child session(s)."
    )


@Client.on_message(
    filters.command("deletealltorrentchains") & filters.chat(ALL_CHATS)
)
async def delete_all_torrent_chains_cmd(client, message):
    chains = await torrent_session_store.list_chains(
        owner_id=message.from_user.id, limit=0
    )
    for chain_doc in chains:
        for child in await torrent_session_store.list_chain(chain_doc["_id"]):
            await _stop_torrent_session(child)
    result = await torrent_session_store.delete_all_for_owner(message.from_user.id)
    await message.reply_text(
        f"Deleted {result['deleted_chains']} torrent chain(s) and "
        f"{result['deleted_sessions']} child session(s)."
    )


help_dict["torrent-chain"] = (
    "<b>Torrent workspace chains</b>\n\n"
    "/splittorrent <i>&lt;torrent URL&gt; &lt;workspace&gt;</i> or reply to a .torrent\n"
    "/splitfiletorrent <i>&lt;torrent URL&gt; &lt;workspace&gt;</i> - Send videos as files\n"
    "/torrentchains <i>[page]</i> - List persistent torrent chains\n"
    "/torrentchain <i>&lt;chain or session ID&gt;</i> - Inspect a chain\n"
    "/continuetorrent <i>&lt;chain or session ID&gt;</i> - Resume the next part\n"
    "/deletetorrentchain <i>&lt;chain ID&gt;</i> - Delete one chain\n"
    "/deletealltorrentchains - Delete all your torrent chains"
)
