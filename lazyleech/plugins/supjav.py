# lazyleech - SupJav plugin
# Downloads videos from SupJav and uploads them to Telegram

import asyncio
import html
import logging
import os
import re
import shutil
import tempfile
import threading
import time
from uuid import uuid4

import yt_dlp
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from .. import (
    ADMIN_CHATS,
    ALL_CHATS,
    PROGRESS_UPDATE_DELAY,
    ForceDocumentFlag,
    SendAsZipFlag,
    help_dict,
)
from ..utils.misc import calculate_eta, format_bytes, return_progress_string
from ..utils.supjav import (
    SupJavError,
    extract_supjav_url,
    is_supjav_url,
    resolve_supjav,
    sanitize_filename,
)
from ..utils.upload_worker import upload_queue
from .leech import _new_download_reference, initiate_directdl

LOGGER = logging.getLogger(__name__)

# Track active HLS downloads: task_id -> {cancel_event, user_id, download_dir, reply_msg}
active_supdl_tasks = {}
active_tasks_lock = asyncio.Lock()


def parse_flags(command: str, text: str):
    """Parse upload mode flags and options from command and arguments."""
    flags = []
    if "zip" in command.lower() or " -z" in f" {text} ":
        flags.append(SendAsZipFlag)
    if "file" in command.lower() or " -d" in f" {text} ":
        flags.append(ForceDocumentFlag)
    return tuple(flags)


def parse_quality(text: str) -> tuple[int | None, str]:
    """Extract requested video quality (e.g. 1080, 720, 480, 360) and return (quality, cleaned_text)."""
    match = re.search(r"(?i)(?:^|\s)-?(1080|720|480|360)p?(?:\s|$)", text)
    if match:
        quality = int(match.group(1))
        cleaned = re.sub(r"(?i)(?:^|\s)-?(?:1080|720|480|360)p?(?:\s|$)", " ", text).strip()
        return quality, cleaned
    return None, text


@Client.on_message(
    filters.command(["supdl", "zipsupdl", "filesupdl"]) & filters.chat(ALL_CHATS)
)
async def supdl_cmd(client: Client, message: Message):
    text_parts = (message.text or message.caption or "").split(None, 1)
    command = text_parts[0].lower()
    raw_args = text_parts[1].strip() if len(text_parts) > 1 else ""

    flags = parse_flags(command, raw_args)
    prefer_st = " -st" in f" {raw_args} "
    prefer_fst = " -fst" in f" {raw_args} "

    # Clean flags from args
    cleaned_args = raw_args
    for flag_token in ("-z", "-d", "-st", "-fst"):
        cleaned_args = re_sub_token(cleaned_args, flag_token)

    target_quality, cleaned_args = parse_quality(cleaned_args)

    target_url = None
    custom_filename = None

    if "|" in cleaned_args:
        url_part, _, name_part = cleaned_args.partition("|")
        target_url = extract_supjav_url(url_part)
        custom_filename = name_part.strip() or None
    else:
        target_url = extract_supjav_url(cleaned_args)

    reply_to = message.reply_to_message
    if not target_url and not getattr(reply_to, "empty", True):
        reply_text = reply_to.text or reply_to.caption or ""
        target_url = extract_supjav_url(reply_text)

    if not target_url:
        await message.reply_text(
            "<b>Usage:</b>\n"
            "• <code>/supdl &lt;SupJav URL&gt; [quality]</code> - Download & upload video\n"
            "• <code>/supdl &lt;SupJav URL&gt; 720p</code> - Download in 720p\n"
            "• <code>/supdl &lt;SupJav URL&gt; 480p</code> - Download in 480p\n"
            "• <code>/supdl &lt;SupJav URL&gt; | custom_name.mp4</code> - Custom filename\n"
            "• <code>/zipsupdl &lt;SupJav URL&gt;</code> - Upload as zip archive\n"
            "• <code>/filesupdl &lt;SupJav URL&gt;</code> - Send as document\n\n"
            "<b>Options:</b>\n"
            "• <code>720p</code> / <code>480p</code> / <code>1080p</code> - Target video quality\n"
            "• <code>-st</code> - Force Streamtape server (direct MP4 via Aria2)\n"
            "• <code>-fst</code> - Force FST server (original HLS)\n\n"
            "<i>You can also reply to a message containing a SupJav URL with /supdl</i>"
        )
        return

    status_msg = await message.reply_text("🔎 Resolving SupJav video streams...")

    try:
        resolved = await asyncio.to_thread(resolve_supjav, target_url)
    except SupJavError as exc:
        await status_msg.edit_text(f"❌ <b>SupJav Error:</b> {html.escape(str(exc))}")
        return
    except Exception as exc:
        LOGGER.exception("Unexpected error resolving SupJav: %s", exc)
        await status_msg.edit_text(f"❌ <b>Resolution Failed:</b> {html.escape(str(exc))}")
        return

    servers = resolved.get("servers", {})
    if not servers:
        await status_msg.edit_text("❌ No available video streams found on this SupJav page.")
        return

    filename = custom_filename or resolved["filename"]
    if not os.path.splitext(filename)[1]:
        filename += ".mp4"
    filename = sanitize_filename(filename)

    # Server selection:
    # 1. User forced -st -> use Streamtape
    # 2. User forced -fst -> use FST
    # 3. Default -> prefer FST (1080p/720p HLS), fallback to Streamtape
    selected_server = None
    if prefer_st and "st" in servers:
        selected_server = "st"
    elif prefer_fst and "fst" in servers:
        selected_server = "fst"
    elif "fst" in servers:
        selected_server = "fst"
    elif "st" in servers:
        selected_server = "st"
    elif "tv" in servers:
        selected_server = "tv"
    else:
        selected_server = next(iter(servers))

    server_info = servers[selected_server]
    LOGGER.info(
        "SupJav resolved: title='%s', server='%s', file='%s', quality=%s",
        resolved["title"],
        selected_server,
        filename,
        target_quality,
    )

    # Path A: Direct MP4 via Aria2 (Streamtape)
    if selected_server == "st" and server_info.get("type") == "direct":
        st_url = server_info["url"]
        st_headers = [
            f"Referer: {server_info['headers'].get('Referer', 'https://streamtape.com/')}",
            f"User-Agent: {server_info['headers'].get('User-Agent', '')}",
        ]
        await status_msg.edit_text(
            f"🎬 <b>{html.escape(resolved['title'])}</b>\n"
            f"📄 <code>{html.escape(filename)}</code>\n"
            f"🌐 <b>Server:</b> Streamtape (Direct MP4)\n\n"
            "⬇️ Initiating download via Aria2..."
        )
        try:
            await initiate_directdl(
                client,
                message,
                st_url,
                filename,
                flags,
                headers=st_headers,
            )
        except Exception as exc:
            LOGGER.exception("Aria2 directdl failed for Streamtape: %s", exc)
            await status_msg.edit_text(
                f"❌ Failed to start Streamtape download: {html.escape(str(exc))}"
            )
        return

    # Path B: HLS stream (FST / TV) via yt-dlp, with auto-fallback to Streamtape if FST fails
    await _download_and_upload_hls(
        client=client,
        message=message,
        status_msg=status_msg,
        resolved=resolved,
        server_key=selected_server,
        filename=filename,
        flags=flags,
        quality=target_quality,
    )


async def _download_and_upload_hls(
    client: Client,
    message: Message,
    status_msg: Message,
    resolved: dict,
    server_key: str,
    filename: str,
    flags: tuple,
    quality: int | None = None,
):
    """Download HLS stream via yt-dlp with live progress and upload to Telegram."""
    server_info = resolved["servers"][server_key]
    stream_url = server_info["url"]
    headers = server_info.get("headers", {})

    task_id = str(uuid4())[:8]
    cancel_event = threading.Event()
    download_dir = tempfile.mkdtemp(prefix="supdl_")

    async with active_tasks_lock:
        active_supdl_tasks[task_id] = {
            "cancel_event": cancel_event,
            "user_id": message.from_user.id,
            "download_dir": download_dir,
            "message": status_msg,
        }

    cancel_btn = InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Cancel Download", callback_data=f"supdl_cancel_{task_id}")]]
    )

    if quality:
        format_spec = f"bestvideo[height<={quality}]+bestaudio/best[height<={quality}]/best"
        server_label = (
            f"FST ({quality}p HLS)"
            if server_key == "fst"
            else f"{server_key.upper()} ({quality}p HLS)"
        )
    else:
        format_spec = "bestvideo+bestaudio/best"
        server_label = (
            "FST (1080p HLS)"
            if server_key == "fst"
            else f"{server_key.upper()} (HLS)"
        )
    await status_msg.edit_text(
        f"🎬 <b>{html.escape(resolved['title'])}</b>\n"
        f"📄 <code>{html.escape(filename)}</code>\n"
        f"🌐 <b>Server:</b> {server_label}\n\n"
        "⬇️ <i>Starting download...</i>",
        reply_markup=cancel_btn,
    )

    progress_data = {
        "downloaded": 0,
        "total": 0,
        "speed": 0,
        "eta": 0,
        "status": "starting",
        "last_update": 0,
    }

    def ytdl_progress_hook(data):
        if cancel_event.is_set():
            raise yt_dlp.utils.DownloadCancelled("Download cancelled by user")
        status = data.get("status")
        if status == "downloading":
            total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
            downloaded = data.get("downloaded_bytes") or 0
            speed = data.get("speed") or 0
            eta = data.get("eta") or 0
            progress_data["downloaded"] = downloaded
            progress_data["total"] = total
            progress_data["speed"] = speed
            progress_data["eta"] = eta
            progress_data["status"] = "downloading"
        elif status == "finished":
            progress_data["status"] = "finished"

    # Async monitor task to update Telegram progress periodically
    stop_monitor = asyncio.Event()

    async def progress_monitor():
        while not stop_monitor.is_set():
            await asyncio.sleep(PROGRESS_UPDATE_DELAY)
            if stop_monitor.is_set():
                break
            if progress_data["status"] != "downloading":
                continue
            downloaded = progress_data["downloaded"]
            total = progress_data["total"]
            speed = progress_data["speed"]
            eta = progress_data["eta"]
            prog_bar = return_progress_string(downloaded, total)
            percent = f"{(downloaded / total * 100):.1f}%" if total > 0 else "N/A"
            speed_str = format_bytes(speed) + "/s" if speed else "N/A"
            eta_str = calculate_eta(speed, total - downloaded) if (speed and total) else (
                f"{eta}s" if eta else "N/A"
            )
            text = (
                f"🎬 <b>{html.escape(resolved['title'][:80])}</b>\n"
                f"📄 <code>{html.escape(filename)}</code>\n"
                f"🌐 <b>Server:</b> {server_label}\n"
                f"<code>{prog_bar}</code> {percent}\n"
                f"<b>Downloaded:</b> {format_bytes(downloaded)} of {format_bytes(total) if total else 'Unknown'}\n"
                f"<b>Speed:</b> {speed_str} | <b>ETA:</b> {eta_str}"
            )
            try:
                await status_msg.edit_text(text, reply_markup=cancel_btn)
            except (FloodWait, MessageNotModified):
                pass
            except Exception:
                pass

    monitor_task = asyncio.create_task(progress_monitor())

    target_path = os.path.join(download_dir, filename)

    def run_download():
        ydl_opts = {
            "outtmpl": target_path,
            "format": format_spec,
            "http_headers": headers,
            "quiet": True,
            "no_warnings": True,
            "nocheckcertificate": True,
            "progress_hooks": [ytdl_progress_hook],
            "concurrent_fragment_downloads": 4,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([stream_url])

    download_success = False
    try:
        await asyncio.to_thread(run_download)
        download_success = True
    except yt_dlp.utils.DownloadCancelled:
        LOGGER.info("SupJav download %s cancelled by user", task_id)
        await status_msg.edit_text("❌ Download cancelled.")
        await asyncio.to_thread(shutil.rmtree, download_dir, True)
        return
    except Exception as exc:
        LOGGER.warning("HLS download failed with %s: %s", server_key, exc)
        # Check if Streamtape fallback is available
        st_server = resolved.get("servers", {}).get("st")
        if server_key != "st" and st_server and st_server.get("url"):
            await status_msg.edit_text(
                f"⚠️ {server_label} download failed. Falling back to Streamtape (direct MP4)..."
            )
            await asyncio.to_thread(shutil.rmtree, download_dir, True)
            try:
                st_headers = [
                    f"Referer: {st_server['headers'].get('Referer', 'https://streamtape.com/')}",
                    f"User-Agent: {st_server['headers'].get('User-Agent', '')}",
                ]
                await initiate_directdl(
                    client,
                    message,
                    st_server["url"],
                    filename,
                    flags,
                    headers=st_headers,
                )
            except Exception as st_exc:
                await status_msg.edit_text(
                    f"❌ Streamtape fallback also failed: {html.escape(str(st_exc))}"
                )
            return
        await status_msg.edit_text(f"❌ Download failed: {html.escape(str(exc))}")
        await asyncio.to_thread(shutil.rmtree, download_dir, True)
        return
    finally:
        stop_monitor.set()
        monitor_task.cancel()
        async with active_tasks_lock:
            active_supdl_tasks.pop(task_id, None)

    # Locate the output file in download_dir
    files_in_dir = [
        f for f in os.listdir(download_dir)
        if os.path.isfile(os.path.join(download_dir, f))
    ]
    if not files_in_dir:
        await status_msg.edit_text("❌ Download completed but output file was not found.")
        await asyncio.to_thread(shutil.rmtree, download_dir, True)
        return

    output_file = os.path.join(download_dir, files_in_dir[0])
    file_size = os.path.getsize(output_file)

    if file_size == 0:
        await status_msg.edit_text("❌ Downloaded file is empty.")
        await asyncio.to_thread(shutil.rmtree, download_dir, True)
        return

    await status_msg.edit_text(
        f"✅ <b>Download Complete!</b> ({format_bytes(file_size)})\n"
        f"📄 <code>{html.escape(os.path.basename(output_file))}</code>\n\n"
        "⬆️ <i>Queued for Telegram upload...</i>"
    )

    torrent_info = {
        "name": os.path.basename(output_file),
        "dir": download_dir,
        "files": [{"path": output_file, "length": file_size, "selected": "true"}],
    }

    # Queue for upload via upload_worker
    upload_ref = _new_download_reference(message)
    upload_queue.put_nowait(
        (
            client,
            message,
            upload_ref,
            torrent_info,
            message.from_user.id,
            flags,
            None,
            {"workspace_temp_root": download_dir},
        )
    )


@Client.on_callback_query(filters.regex(r"^supdl_cancel_([a-zA-Z0-9]+)$") & filters.chat(ALL_CHATS))
async def supdl_cancel_callback(client: Client, callback_query: CallbackQuery):
    task_id = callback_query.matches[0].group(1)
    async with active_tasks_lock:
        task_info = active_supdl_tasks.get(task_id)

    if not task_info:
        await callback_query.answer("Download already finished or expired.", show_alert=True)
        return

    caller_id = callback_query.from_user.id
    if caller_id != task_info["user_id"] and caller_id not in ADMIN_CHATS:
        await callback_query.answer("You cannot cancel another user's download.", show_alert=True)
        return

    task_info["cancel_event"].set()
    await callback_query.answer("Cancelling download...", show_alert=False)


def re_sub_token(text: str, token: str) -> str:
    """Safely remove a command flag token from text."""
    parts = text.split()
    return " ".join(part for part in parts if part.lower() != token.lower())


help_dict["supjav"] = (
    "SupJav",
    "<b>SupJav Video Downloader</b>\n"
    "• /supdl <i>&lt;SupJav URL&gt; [quality]</i> - Download and upload SupJav video\n"
    "• /supdl <i>&lt;SupJav URL&gt; 720p</i> - Download in 720p\n"
    "• /supdl <i>&lt;SupJav URL&gt; 480p</i> - Download in 480p\n"
    "• /supdl <i>&lt;SupJav URL&gt; | custom_name.mp4</i> - Specify custom filename\n"
    "• /zipsupdl <i>&lt;SupJav URL&gt;</i> - Upload as a zip file\n"
    "• /filesupdl <i>&lt;SupJav URL&gt;</i> - Send video as document\n\n"
    "<b>Options:</b>\n"
    "• <code>720p</code> / <code>480p</code> / <code>1080p</code> - Target video quality\n"
    "• <code>-st</code> - Force Streamtape server (direct MP4 via Aria2)\n"
    "• <code>-fst</code> - Force FST server (original HLS)\n\n"
    "<i>Tip: You can also reply to a message containing a SupJav URL with /supdl</i>",
)
