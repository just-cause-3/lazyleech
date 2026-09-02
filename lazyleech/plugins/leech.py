# lazyleech - Telegram bot primarily to leech from torrents and upload to Telegram
# Copyright (c) 2021 lazyleech developers <theblankx protonmail com, meliodas_bot protonmail com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio
import html
import itertools
import os
import re
import tempfile
import time
from collections import deque
from types import SimpleNamespace
from urllib.parse import unquote as urldecode
from urllib.parse import urlparse, urlunparse

from pyrogram import Client, filters
from pyrogram.parser import html as pyrogram_html
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .. import (
    ADMIN_CHATS,
    ALL_CHATS,
    LEECH_TIMEOUT,
    MAGNET_TIMEOUT,
    PROGRESS_UPDATE_DELAY,
    ForceDocumentFlag,
    SelectFilesFlag,
    SendAsZipFlag,
    help_dict,
    session,
)
from ..utils.aria2 import (
    Aria2Error,
    aria2_add_directdl,
    aria2_add_magnet,
    aria2_add_torrent,
    aria2_change_option,
    aria2_force_pause_all,
    aria2_pause,
    aria2_remove,
    aria2_tell_active,
    aria2_tell_status,
    aria2_tell_waiting,
    aria2_unpause,
    is_gid_owner,
)
from ..utils.bunkr import extract_album_urls, is_bunkr_url, resolve_bunkr_file
from ..utils.bunkr_sessions import (
    FILE_CANCELLED,
    FILE_DOWNLOADED,
    FILE_DOWNLOADING,
    FILE_FAILED,
    FILE_PENDING,
    FILE_RESOLVING,
    SESSION_CANCELLED,
    SESSION_COMPLETED,
    SESSION_FAILED,
    SESSION_PAUSED,
    SESSION_RUNNING,
    bunkr_session_store,
)
from ..utils.misc import (
    allow_admin_cancel,
    calculate_eta,
    format_bytes,
    get_file_mimetype,
    return_progress_string,
)
from ..utils.status import send_status_message
from ..utils.upload_worker import (
    progress_callback_data,
    stop_uploads,
    upload_queue,
    upload_statuses,
    upload_waits,
)

bunkr_tasks = set()
bunkr_session_tasks = {}
bunkr_host_semaphores = {}
download_reference_ids = itertools.count(1)


def _new_download_reference(message):
    """Return a unique internal task key without sending a Telegram message."""
    return SimpleNamespace(chat=message.chat, id=-next(download_reference_ids))


async def _delete_download_placeholder(reference):
    delete = getattr(reference, "delete", None)
    if delete is not None:
        await delete()


def _nonnegative_int_env(name, default):
    try:
        return max(0, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


def _positive_int_env(name, default, maximum=None):
    try:
        value = max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        value = default
    return min(value, maximum) if maximum else value


BUNKR_SLOW_SPEED_KBPS = _nonnegative_int_env("BUNKR_SLOW_SPEED_KBPS", 650)
BUNKR_SLOW_GRACE_SECONDS = _nonnegative_int_env(
    "BUNKR_SLOW_GRACE_SECONDS", 30
)
BUNKR_SLOW_DURATION_SECONDS = _nonnegative_int_env(
    "BUNKR_SLOW_DURATION_SECONDS", 30
)
BUNKR_SLOW_WINDOW_SECONDS = _positive_int_env(
    "BUNKR_SLOW_WINDOW_SECONDS", 20, maximum=120
)
BUNKR_SLOW_PEAK_PERCENT = _positive_int_env(
    "BUNKR_SLOW_PEAK_PERCENT", 20, maximum=100
)
BUNKR_MAX_AUTO_SKIPS = _nonnegative_int_env("BUNKR_MAX_AUTO_SKIPS", 3)
BUNKR_CONNECTIONS = _positive_int_env("BUNKR_CONNECTIONS", 4, maximum=16)
BUNKR_RECOVERY_CONNECTIONS = _positive_int_env(
    "BUNKR_RECOVERY_CONNECTIONS", 2, maximum=16
)
BUNKR_MAX_DOWNLOADS_PER_HOST = _positive_int_env(
    "BUNKR_MAX_DOWNLOADS_PER_HOST", 1, maximum=8
)
BUNKR_SLOW_HOST_COOLDOWN_SECONDS = _positive_int_env(
    "BUNKR_SLOW_HOST_COOLDOWN_SECONDS", 300, maximum=3600
)
BUNKR_MAX_HOST_COOLDOWN_SECONDS = _positive_int_env(
    "BUNKR_MAX_HOST_COOLDOWN_SECONDS", 1200, maximum=21600
)
BUNKR_COOLDOWN_POLL_SECONDS = 15
BUNKR_SIGNED_URL_REFRESH_SECONDS = 60
BUNKR_SESSIONS_PAGE_SIZE = 10
BUNKR_VIDEO_EXTENSIONS = (
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".wmv",
    ".flv",
    ".webm",
    ".m4v",
    ".m4a",
    ".ts",
)


def _bunkr_cdn_host(download_url):
    return (urlparse(download_url).hostname or "").lower().rstrip(".")


def _bunkr_host_semaphore(download_url):
    host = _bunkr_cdn_host(download_url)
    semaphore = bunkr_host_semaphores.get(host)
    if semaphore is None:
        semaphore = asyncio.Semaphore(BUNKR_MAX_DOWNLOADS_PER_HOST)
        bunkr_host_semaphores[host] = semaphore
    return semaphore


async def _active_bunkr_cooldowns(session_id):
    session_hosts, global_hosts, known_hosts = await asyncio.gather(
        bunkr_session_store.active_host_cooldowns(session_id),
        bunkr_session_store.active_global_host_cooldowns(),
        bunkr_session_store.session_cdn_hosts(session_id),
    )
    return session_hosts | (global_hosts & known_hosts)


def _bunkr_download_dir(owner_id, session_id, file_id):
    position = str(file_id).rsplit(":", 1)[-1]
    return os.path.join(
        os.getcwd(), str(int(owner_id)), "bunkr_sessions", session_id, position
    )


async def _extract_bunkr_video_files(link, filename=None):
    """Resolve one Bunkr source into the files stored by a session."""
    if "/a/" not in urlparse(link).path:
        file_name = filename or os.path.basename(urlparse(link).path) or link
        return [(link, file_name)]

    files = await extract_album_urls(link, session)
    return [
        (file_url, file_name)
        for file_url, file_name in files
        if file_name and file_name.lower().endswith(BUNKR_VIDEO_EXTENSIONS)
    ]


def _bunkr_connection_count(file_doc, cdn_health=None):
    file_strikes = max(0, int(file_doc.get("auto_defer_count", 0)))
    host_strikes = max(0, int((cdn_health or {}).get("slow_strikes", 0)))
    adaptive_level = max(file_strikes, host_strikes)
    if adaptive_level >= 2:
        return 1
    if adaptive_level == 1:
        return min(BUNKR_CONNECTIONS, BUNKR_RECOVERY_CONNECTIONS)
    return BUNKR_CONNECTIONS


class _BunkrSlowDownloadMonitor:
    def __init__(
        self,
        speed_limit_bps,
        grace_seconds,
        duration_seconds,
        *,
        window_seconds=20,
        peak_ratio=0.20,
        clock=None,
    ):
        self.speed_limit_bps = speed_limit_bps
        self.grace_seconds = grace_seconds
        self.duration_seconds = duration_seconds
        self.window_seconds = max(1, window_seconds)
        self.peak_ratio = max(0.0, min(float(peak_ratio), 1.0))
        self.clock = clock or time.monotonic
        self.started_at = self.clock()
        self.slow_since = None
        self.samples = deque()
        self.rolling_speed_bps = 0.0
        self.peak_speed_bps = 0.0
        self.effective_limit_bps = float(speed_limit_bps)
        self.triggered = False

    def _sample_speed(self, now, speed):
        self.samples.append((now, speed))
        cutoff = now - self.window_seconds
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()
        self.rolling_speed_bps = sum(value for _, value in self.samples) / len(
            self.samples
        )
        self.peak_speed_bps = max(self.peak_speed_bps, self.rolling_speed_bps)
        # A short CDN burst must not create an unreasonably high permanent floor.
        # Cap peak-relative detection at four times the configured absolute limit.
        relative_limit = self.peak_speed_bps * self.peak_ratio
        relative_limit = min(relative_limit, self.speed_limit_bps * 4)
        self.effective_limit_bps = max(self.speed_limit_bps, relative_limit)

    def should_defer(self, torrent_info):
        if self.speed_limit_bps <= 0 or torrent_info.get("status") != "active":
            self.slow_since = None
            return False
        now = self.clock()
        try:
            speed = int(torrent_info.get("downloadSpeed", 0))
        except (TypeError, ValueError):
            speed = 0
        self._sample_speed(now, speed)
        if now - self.started_at < self.grace_seconds:
            return False
        if (
            speed >= self.effective_limit_bps
            or self.rolling_speed_bps >= self.effective_limit_bps
        ):
            self.slow_since = None
            return False
        if self.slow_since is None:
            self.slow_since = now
        self.triggered = now - self.slow_since >= self.duration_seconds
        return self.triggered

    def completed_healthy(self):
        return bool(
            self.speed_limit_bps <= 0
            or (
                self.peak_speed_bps >= self.speed_limit_bps
                and not self.triggered
            )
        )


def _bunkr_mode_from_flags(flags):
    if SendAsZipFlag in flags:
        return "zip"
    if ForceDocumentFlag in flags:
        return "document"
    return "normal"


def _bunkr_flags_from_mode(mode):
    if mode == "zip":
        return (SendAsZipFlag,)
    if mode == "document":
        return (ForceDocumentFlag,)
    return ()


@Client.on_message(
    filters.command(["torrent", "ziptorrent", "filetorrent"]) & filters.chat(ALL_CHATS)
)
async def torrent_cmd(client, message):
    text = (message.text or message.caption).split(None, 1)
    command = text.pop(0).lower()

    flags = []
    if "zip" in command:
        flags.append(SendAsZipFlag)
    elif "file" in command:
        flags.append(ForceDocumentFlag)

    if text and text[0].startswith("-s "):
        flags.append(SelectFilesFlag)
        text[0] = text[0][3:].strip()
    elif text and text[0] == "-s":
        flags.append(SelectFilesFlag)
        text[0] = ""

    flags = tuple(flags)

    link = None
    reply = message.reply_to_message
    document = message.document
    if document:
        if (
            document.file_size < 1048576
            and document.file_name.endswith(".torrent")
            and (
                not document.mime_type
                or document.mime_type == "application/x-bittorrent"
            )
        ):
            os.makedirs(str(message.from_user.id), exist_ok=True)
            fd, link = tempfile.mkstemp(
                dir=str(message.from_user.id), suffix=".torrent"
            )
            os.fdopen(fd).close()
            await message.download(link)
            mimetype = await get_file_mimetype(link)
            if mimetype != "application/x-bittorrent":
                os.remove(link)
                link = None
    nf = None
    if not link:
        if text:
            x = text[0].split(" | ", 1)
            link = x[0].strip()
            if len(x) == 2:
                nf = x[1]
        elif not getattr(reply, "empty", True):
            document = reply.document
            link = reply.text
            if document:
                if (
                    document.file_size < 1048576
                    and document.file_name.endswith(".torrent")
                    and (
                        not document.mime_type
                        or document.mime_type == "application/x-bittorrent"
                    )
                ):
                    os.makedirs(str(message.from_user.id), exist_ok=True)
                    fd, link = tempfile.mkstemp(
                        dir=str(message.from_user.id), suffix=".torrent"
                    )
                    os.fdopen(fd).close()
                    await reply.download(link)
                    mimetype = await get_file_mimetype(link)
                    if mimetype != "application/x-bittorrent":
                        os.remove(link)
                        link = reply.text or reply.caption
    if not link:
        await message.reply_text("""Usage:
- /torrent <i>&lt;Torrent URL or File&gt;</i>
- /torrent <i>(as reply to a Torrent URL or file)</i>

- /ziptorrent <i>&lt;Torrent URL or File&gt;</i>
- /ziptorrent <i>(as reply to a Torrent URL or File)</i>

- /filetorrent <i>&lt;Torrent URL or File&gt;</i> - Sends videos as files
- /filetorrent <i>(as reply to a Torrent URL or file)</i> - Sends videos as files""")
        return

    if link.startswith("magnet:"):
        prefix = (
            "zip"
            if SendAsZipFlag in flags
            else "file"
            if ForceDocumentFlag in flags
            else ""
        )
        await message.reply_text(f"Use /{prefix}magnet instead")
        return

    await initiate_torrent(client, message, link, flags, nf)
    await message.stop_propagation()


async def initiate_torrent(client, message, link, flags, newFile: str = None):
    user_id = message.from_user.id
    pause = SelectFilesFlag in flags
    reply = None
    if pause:
        # Selective torrents need a real message which becomes the file prompt.
        reply = await message.reply_text("Loading torrent file list...")
    try:
        gid = await aria2_add_torrent(
            session, user_id, link, LEECH_TIMEOUT, pause=pause
        )
    except Aria2Error as ex:
        await message.reply_text(
            f"Aria2 Error Occured!\n{ex.error_code}: {html.escape(ex.error_message)}"
        )
        if reply is not None:
            await _delete_download_placeholder(reply)
        return
    finally:
        if os.path.isfile(link):
            os.remove(link)

    if pause:
        await handle_file_selection(
            client, message, gid, reply, user_id, flags, newFile
        )
    else:
        await handle_leech(
            client,
            message,
            gid,
            _new_download_reference(message),
            user_id,
            flags,
            newFile,
        )


@Client.on_message(
    filters.command(["magnet", "zipmagnet", "filemagnet"]) & filters.chat(ALL_CHATS)
)
async def magnet_cmd(client, message):
    text = (message.text or message.caption).split(None, 1)
    command = text.pop(0).lower()

    flags = []
    if "zip" in command:
        flags.append(SendAsZipFlag)
    elif "file" in command:
        flags.append(ForceDocumentFlag)

    if text and text[0].startswith("-s "):
        flags.append(SelectFilesFlag)
        text[0] = text[0][3:].strip()
    elif text and text[0] == "-s":
        flags.append(SelectFilesFlag)
        text[0] = ""

    flags = tuple(flags)

    nf = None
    link = None
    reply = message.reply_to_message
    if text:
        x = text[0].split(" | ", 1)
        link = x[0].strip()
        if len(x) == 2:
            nf = x[1]
    elif not getattr(reply, "empty", True):
        link = reply.text or reply.caption
    if not link:
        await message.reply_text("""Usage:
- /magnet <i>&lt;Magnet URL&gt;</i>
- /magnet <i>(as reply to a Magnet URL)</i>

- /zipmagnet <i>&lt;Magnet URL&gt;</i>
- /zipmagnet <i>(as reply to a Magnet URL)</i>

- /filemagnet <i>&lt;Magnet URL&gt;</i> - Sends videos as files
- /filemagnet <i>(as reply to a Magnet URL)</i> - Sends videos as files""")
        return
    await initiate_magnet(client, message, link, flags, nf)


async def initiate_magnet(client, message, link, flags, nf: str = None):
    user_id = message.from_user.id
    pause = SelectFilesFlag in flags
    reply = None
    if pause:
        reply = await message.reply_text("Loading magnet file list...")
    try:
        gid = await asyncio.wait_for(
            aria2_add_magnet(session, user_id, link, LEECH_TIMEOUT, pause=pause),
            MAGNET_TIMEOUT,
        )
    except Aria2Error as ex:
        await message.reply_text(
            f"Aria2 Error Occured!\n{ex.error_code}: {html.escape(ex.error_message)}"
        )
        if reply is not None:
            await _delete_download_placeholder(reply)
    except asyncio.TimeoutError:
        await message.reply_text("Magnet timed out")
        if reply is not None:
            await _delete_download_placeholder(reply)
    else:
        if pause:
            await handle_file_selection(client, message, gid, reply, user_id, flags, nf)
        else:
            await handle_leech(
                client,
                message,
                gid,
                _new_download_reference(message),
                user_id,
                flags,
                nf,
            )


@Client.on_message(
    filters.command(
        ["directdl", "direct", "zipdirectdl", "zipdirect", "filedirectdl", "filedirect"]
    )
    & filters.chat(ALL_CHATS)
)
async def directdl_cmd(client, message):
    text = message.text.split(None, 1)
    command = text.pop(0).lower()
    if "zip" in command:
        flags = (SendAsZipFlag,)
    elif "file" in command:
        flags = (ForceDocumentFlag,)
    else:
        flags = ()
    link = filename = None
    reply = message.reply_to_message
    if text:
        link = text[0].strip()
    elif not getattr(reply, "empty", True):
        link = reply.text
    if not link:
        await message.reply_text("""Usage:
- /directdl <i>&lt;Direct URL&gt; | optional custom file name</i>
- /directdl <i>(as reply to a Direct URL) | optional custom file name</i>
- /direct <i>&lt;Direct URL&gt; | optional custom file name</i>
- /direct <i>(as reply to a Direct URL) | optional custom file name</i>

- /zipdirectdl <i>&lt;Direct URL&gt; | optional custom file name</i>
- /zipdirectdl <i>(as reply to a Direct URL) | optional custom file name</i>
- /zipdirect <i>&lt;Direct URL&gt; | optional custom file name</i>
- /zipdirect <i>(as reply to a Direct URL) | optional custom file name</i>

- /filedirectdl <i>&lt;Direct URL&gt; | optional custom file name</i> - Sends videos as files
- /filedirectdl <i>(as reply to a Direct URL) | optional custom file name</i> - Sends videos as files
- /filedirect <i>&lt;Direct URL&gt; | optional custom file name</i> - Sends videos as files
- /filedirect <i>(as reply to a Direct URL) | optional custom file name</i> - Sends videos as files""")
        return
    split = link.split("|", 1)
    if len(split) > 1:
        filename = os.path.basename(split[1].strip())
        link = split[0].strip()

    await process_link(client, message, link, filename, flags)


async def process_link(client, message, link, filename, flags, reply_msg=None):
    parsed = list(urlparse(link, "https"))
    if parsed[0] == "magnet":
        if not reply_msg:
            prefix = (
                "zip"
                if SendAsZipFlag in flags
                else "file"
                if ForceDocumentFlag in flags
                else ""
            )
            await message.reply_text(f"Use /{prefix}magnet instead")
        return
    if not parsed[0]:
        parsed[0] = "https"
    if parsed[0] not in ("http", "https"):
        if not reply_msg:
            await message.reply_text("Invalid scheme")
        return
    link = urlunparse(parsed)

    if is_bunkr_url(link):
        reply = reply_msg or await message.reply_text(
            "Fetching Bunkr metadata..."
        )
        try:
            files = await _extract_bunkr_video_files(link, filename)
            if not files:
                if reply_msg:
                    await message.reply_text("No video files found in the album.")
                else:
                    await reply.edit_text("No video files found in the album.")
                return

            title = os.path.basename(urlparse(link).path.rstrip("/")) or "Bunkr"
            session_doc = await bunkr_session_store.create_session(
                owner_id=message.from_user.id,
                chat_id=message.chat.id,
                source_message_id=message.id,
                source_url=link,
                title=title,
                mode=_bunkr_mode_from_flags(flags),
                custom_filename=filename,
                files=files,
            )
            session_id = session_doc["_id"]
            persistence_note = (
                ""
                if bunkr_session_store.persistent
                else "\n\n⚠️ <code>DB_URL</code> is not configured; this session "
                "will not survive a bot restart."
            )
            session_text = (
                f"<b>Bunkr session:</b> <code>{session_id}</code>\n"
                f"Found <b>{len(files)}</b> video file(s). Downloading sequentially.\n\n"
                f"Pause: <code>/pause {session_id}</code>\n"
                f"Continue: <code>/continue {session_id}</code>\n"
                f"Details: <code>/bsession {session_id}</code>"
                f"{persistence_note}"
            )
            if reply_msg:
                await message.reply_text(session_text)
            else:
                await reply.edit_text(session_text)
            _start_bunkr_session(client, message, session_id)
            return
        except Exception as e:
            if reply_msg:
                await message.reply_text(f"Bunkr session creation failed: {str(e)}")
            else:
                await reply.edit_text(f"Bunkr extraction failed: {str(e)}")
            return

    await initiate_directdl(client, message, link, filename, flags)


bunkr_semaphore = asyncio.Semaphore(1)


def _start_bunkr_session(client, message, session_id):
    existing = bunkr_session_tasks.get(session_id)
    if existing and not existing.done():
        return existing

    task = asyncio.create_task(_run_bunkr_session(client, message, session_id))
    bunkr_session_tasks[session_id] = task
    bunkr_tasks.add(task)

    def _discard(finished):
        bunkr_tasks.discard(finished)
        if bunkr_session_tasks.get(session_id) is finished:
            bunkr_session_tasks.pop(session_id, None)
        if not finished.cancelled():
            error = finished.exception()
            if error is not None:
                asyncio.create_task(
                    _record_bunkr_session_crash(message, session_id, error)
                )

    task.add_done_callback(_discard)
    return task


async def _record_bunkr_session_crash(message, session_id, error):
    await bunkr_session_store.set_state(session_id, SESSION_FAILED)
    await message.reply_text(
        f"Bunkr session <code>{session_id}</code> stopped unexpectedly: "
        f"{html.escape(str(error))}. Resume it with "
        f"<code>/continue {session_id}</code>."
    )


async def _run_bunkr_session(client, message, session_id):
    while True:
        session_doc = await bunkr_session_store.get_session(session_id)
        if not session_doc or session_doc["state"] != SESSION_RUNNING:
            return

        cooled_hosts = await _active_bunkr_cooldowns(session_id)
        file_doc = await bunkr_session_store.claim_next_file(
            session_id,
            excluded_hosts=cooled_hosts,
            allow_excluded_fallback=False,
        )
        if file_doc is None:
            counts = await bunkr_session_store.counts(session_id)
            if counts[FILE_PENDING] and cooled_hosts:
                # No eligible host is available. Leave partial files parked while
                # uploads continue, and periodically re-check the circuit breakers.
                await asyncio.sleep(BUNKR_COOLDOWN_POLL_SECONDS)
                continue
            final_state = (
                SESSION_COMPLETED
                if counts[FILE_DOWNLOADED] == session_doc["total_files"]
                else SESSION_FAILED
            )
            await bunkr_session_store.set_state(session_id, final_state)
            updated = await bunkr_session_store.get_session(session_id)
            await _send_bunkr_session_summary(message, updated)
            return

        current = await bunkr_session_store.get_session(session_id)
        if not current or current["state"] != SESSION_RUNNING:
            await bunkr_session_store.update_file(
                file_doc["_id"], FILE_PENDING, gid=None
            )
            return
        await process_bunkr_download(
            client,
            message,
            current,
            file_doc,
            _bunkr_flags_from_mode(current["mode"]),
        )


async def _bunkr_session_chunks(session_doc, include_files=True):
    counts = await bunkr_session_store.counts(session_doc["_id"])
    cooled_hosts = await _active_bunkr_cooldowns(session_doc["_id"])
    downloaded_count = counts[FILE_DOWNLOADED]
    not_downloaded_count = session_doc["total_files"] - downloaded_count
    persistence = "MongoDB" if bunkr_session_store.persistent else "memory only"
    cooling_line = (
        f"<b>Cooling CDNs:</b> "
        f"{html.escape(', '.join(sorted(cooled_hosts)))}\n"
        if cooled_hosts
        else ""
    )
    source_urls = session_doc.get("source_urls") or [session_doc["source_url"]]
    if len(source_urls) > 1:
        source_line = (
            f"<b>Sources:</b> {len(source_urls)} queued Bunkr links "
            f"(<a href=\"{html.escape(source_urls[0], quote=True)}\">first source</a>)\n"
        )
    else:
        source_line = (
            f"<b>Source:</b> "
            f"<a href=\"{html.escape(source_urls[0], quote=True)}\">album/link</a>\n"
        )
    header = (
        f"<b>Bunkr session:</b> <code>{session_doc['_id']}</code>\n"
        f"<b>State:</b> {html.escape(session_doc['state'].title())}\n"
        f"<b>Downloaded:</b> {downloaded_count}/{session_doc['total_files']} | "
        f"<b>Not downloaded:</b> {not_downloaded_count}\n"
        f"<b>Storage:</b> {persistence}\n"
        f"{cooling_line}"
        f"{source_line}\n"
    )
    if not include_files:
        return [header.rstrip()]

    files = await bunkr_session_store.list_files(session_doc["_id"])
    downloaded = [item for item in files if item["status"] == FILE_DOWNLOADED]
    unfinished = [item for item in files if item["status"] != FILE_DOWNLOADED]
    entries = []
    for title, items, icon in (
        ("Downloaded", downloaded, "✅"),
        ("Not downloaded", unfinished, "⏳"),
    ):
        entries.append(f"<b>{title} ({len(items)}):</b>\n")
        if not items:
            entries.append("<i>None</i>\n")
            continue
        for item in items:
            error = item.get("error")
            error_text = f" — {html.escape(str(error))[:300]}" if error else ""
            defer_count = item.get("defer_count", 0)
            defer_text = f" | deferred {defer_count}x" if defer_count else ""
            host_defer_count = item.get("host_defer_count", 0)
            host_defer_text = (
                f" | CDN-routed {host_defer_count}x" if host_defer_count else ""
            )
            cdn_text = (
                f" | CDN {html.escape(str(item['cdn_host']))}"
                if item.get("cdn_host")
                else ""
            )
            entries.append(
                f"{icon} <b>{item['position']}.</b> "
                f"<code>{html.escape(str(item['filename']))}</code>\n"
                f"{html.escape(item['status'])}{error_text}{defer_text}"
                f"{host_defer_text}{cdn_text} | "
                f"<a href=\"{html.escape(item['page_url'], quote=True)}\">source link</a>\n"
            )
        entries.append("\n")

    chunks = []
    current = header
    for entry in entries:
        if len(current) + len(entry) > 3900 and current != header:
            chunks.append(current.rstrip())
            current = (
                f"<b>Bunkr session:</b> <code>{session_doc['_id']}</code> "
                f"(continued)\n\n"
            )
        current += entry
    if current.strip():
        chunks.append(current.rstrip())
    return chunks


async def _send_bunkr_session_summary(message, session_doc, include_files=True):
    if not session_doc:
        return
    for chunk in await _bunkr_session_chunks(session_doc, include_files):
        await message.reply_text(chunk, disable_web_page_preview=True)


@Client.on_message(filters.command("listqueue") & filters.chat(ALL_CHATS))
async def listqueue_cmd(client, message):
    sessions = await bunkr_session_store.list_sessions(
        owner_id=message.from_user.id,
        states=(SESSION_RUNNING, SESSION_PAUSED),
    )
    if not sessions:
        await message.reply_text("The download queue is currently empty.")
        return

    text = "<b>Current Bunkr sessions:</b>\n\n"
    for session_doc in sessions:
        counts = await bunkr_session_store.counts(session_doc["_id"])
        title = html.escape(str(session_doc.get("title") or "Bunkr"))
        text += (
            f"<b>{title}</b>\n"
            f"• <code>{session_doc['_id']}</code> — "
            f"{html.escape(session_doc['state'])} — "
            f"{counts[FILE_DOWNLOADED]}/{session_doc['total_files']} downloaded\n"
        )

    await message.reply_text(text, disable_web_page_preview=True)


def _bunkr_session_id_from_message(message):
    if len(message.command) > 1:
        return message.command[1].strip().lower()
    reply = message.reply_to_message
    if not getattr(reply, "empty", True):
        match = re.search(r"Bunkr session:\s*([0-9a-f]{12})", reply.text or "", re.I)
        if match:
            return match.group(1).lower()
    return None


async def _owned_bunkr_session(message, default_states=None):
    session_id = _bunkr_session_id_from_message(message)
    if session_id:
        return await bunkr_session_store.get_session(
            session_id, owner_id=message.from_user.id
        )
    sessions = await bunkr_session_store.list_sessions(
        owner_id=message.from_user.id,
        states=default_states,
        limit=2,
    )
    return sessions[0] if len(sessions) == 1 else None


async def _remove_bunkr_download(gid, *, cleanup=True):
    try:
        torrent_info = await aria2_tell_status(session, gid)
        dir_path = torrent_info.get("dir")
        await aria2_remove(session, gid)
        if cleanup and dir_path and os.path.exists(dir_path):
            import shutil

            shutil.rmtree(dir_path, ignore_errors=True)
            parent_dir = os.path.dirname(dir_path)
            if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                os.rmdir(parent_dir)
    except (Aria2Error, OSError):
        pass


async def _cancel_bunkr_session_runtime(session_id):
    active_files = await bunkr_session_store.active_files(session_id)
    await bunkr_session_store.cancel_session(session_id)
    for file_doc in active_files:
        if file_doc.get("gid"):
            await _remove_bunkr_download(file_doc["gid"], cleanup=False)
    task = bunkr_session_tasks.get(session_id)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def _defer_active_bunkr_download(session_doc, reason, *, automatic=False):
    active_files = await bunkr_session_store.active_files(session_doc["_id"])
    if not active_files:
        return None, "no_active"
    file_doc = sorted(active_files, key=lambda item: item["position"])[0]
    cdn_host = str(file_doc.get("cdn_host") or "").lower()
    gid = file_doc.get("gid")
    if gid:
        try:
            torrent_info = await aria2_tell_status(session, gid)
        except Aria2Error:
            torrent_info = None
        if torrent_info and torrent_info.get("status") == "complete":
            return None, "complete"
    deferred = await bunkr_session_store.defer_file_to_bottom(
        file_doc["_id"],
        reason,
        automatic=automatic,
        max_auto_defers=BUNKR_MAX_AUTO_SKIPS if automatic else None,
    )
    if deferred is None and automatic:
        # If every remaining item uses this CDN, park the active file in place.
        # This opens a real quiet period instead of downloading through a throttle.
        deferred = await bunkr_session_store.park_file_for_cooldown(
            file_doc["_id"],
            reason,
            max_auto_defers=BUNKR_MAX_AUTO_SKIPS,
        )
    if deferred is None:
        return None, "auto_limit" if automatic else "no_next"
    if gid:
        # Keep aria2's partial data. A later pass uses the same stable directory
        # and a freshly signed URL, so range-capable CDN downloads can resume.
        await _remove_bunkr_download(gid, cleanup=False)
    if automatic and cdn_host:
        health = await bunkr_session_store.record_cdn_slowdown(
            cdn_host,
            BUNKR_SLOW_HOST_COOLDOWN_SECONDS,
            BUNKR_MAX_HOST_COOLDOWN_SECONDS,
        )
        cooldown_seconds = int(
            (health or {}).get(
                "cooldown_seconds", BUNKR_SLOW_HOST_COOLDOWN_SECONDS
            )
        )
        await bunkr_session_store.set_host_cooldown(
            session_doc["_id"],
            cdn_host,
            cooldown_seconds,
        )
        moved = await bunkr_session_store.move_pending_host_to_bottom(
            session_doc["_id"], cdn_host
        )
        deferred["_cdn_host"] = cdn_host
        deferred["_same_host_moved"] = max(0, moved - 1)
        deferred["_cooldown_seconds"] = cooldown_seconds
        deferred["_host_strikes"] = int((health or {}).get("slow_strikes", 1))
    return deferred, None


@Client.on_message(
    filters.command(["bsessions", "bunkrsessions"]) & filters.chat(ALL_CHATS)
)
async def bunkr_sessions_cmd(client, message):
    page = 1
    if len(message.command) > 1:
        try:
            page = int(message.command[1])
        except (TypeError, ValueError):
            await message.reply_text("Usage: <code>/bsessions [page]</code>")
            return
    text, reply_markup = await _bunkr_sessions_page(message.from_user.id, page)
    await message.reply_text(text, reply_markup=reply_markup)


async def _bunkr_sessions_page(owner_id, requested_page=1):
    total = await bunkr_session_store.count_sessions(owner_id=owner_id)
    if not total:
        return "You do not have any Bunkr sessions.", None

    total_pages = max(
        1, (total + BUNKR_SESSIONS_PAGE_SIZE - 1) // BUNKR_SESSIONS_PAGE_SIZE
    )
    page = min(max(1, int(requested_page)), total_pages)
    sessions = await bunkr_session_store.list_sessions(
        owner_id=owner_id,
        limit=BUNKR_SESSIONS_PAGE_SIZE,
        skip=(page - 1) * BUNKR_SESSIONS_PAGE_SIZE,
    )
    text = f"<b>Your Bunkr sessions</b> - Page {page}/{total_pages}\n\n"
    for session_doc in sessions:
        counts = await bunkr_session_store.counts(session_doc["_id"])
        title = html.escape(str(session_doc.get("title") or "Bunkr"))[:120]
        state = html.escape(str(session_doc["state"]))
        text += (
            f"<b>{title}</b>\n"
            f"• <code>{session_doc['_id']}</code> - {state} - "
            f"{counts[FILE_DOWNLOADED]}/{session_doc['total_files']} downloaded\n"
        )
    text += "\nUse <code>/bsession ID</code> for every stored file link."
    if not bunkr_session_store.persistent:
        text += "\n\nDB_URL is not configured; these sessions are memory-only."

    buttons = []
    if page > 1:
        buttons.append(
            InlineKeyboardButton(
                "Previous",
                callback_data=f"bsessions_page:{int(owner_id)}:{page - 1}",
            )
        )
    buttons.append(
        InlineKeyboardButton(
            f"{page}/{total_pages}",
            callback_data=f"bsessions_page:{int(owner_id)}:{page}",
        )
    )
    if page < total_pages:
        buttons.append(
            InlineKeyboardButton(
                "Next",
                callback_data=f"bsessions_page:{int(owner_id)}:{page + 1}",
            )
        )
    return text, InlineKeyboardMarkup([buttons])


@Client.on_callback_query(filters.regex(r"^bsessions_page:\d+:\d+$"))
async def bunkr_sessions_page_callback(client, callback_query):
    _, owner_text, page_text = callback_query.data.split(":", 2)
    owner_id = int(owner_text)
    if callback_query.from_user.id != owner_id:
        await callback_query.answer(
            "Only the user who opened this list can change its page.",
            show_alert=True,
        )
        return
    text, reply_markup = await _bunkr_sessions_page(owner_id, int(page_text))
    await callback_query.message.edit_text(text, reply_markup=reply_markup)
    await callback_query.answer()


@Client.on_message(
    filters.command(["bsession", "bunkrsession"]) & filters.chat(ALL_CHATS)
)
async def bunkr_session_cmd(client, message):
    session_doc = await _owned_bunkr_session(message)
    if not session_doc:
        await message.reply_text(
            "Session not found. Use <code>/bsession &lt;session_id&gt;</code> "
            "or reply to a Bunkr session message."
        )
        return
    await _send_bunkr_session_summary(message, session_doc)


@Client.on_message(
    filters.command(["pause", "pausesession"]) & filters.chat(ALL_CHATS)
)
async def pause_bunkr_session_cmd(client, message):
    session_doc = await _owned_bunkr_session(
        message, default_states=(SESSION_RUNNING,)
    )
    if not session_doc:
        await message.reply_text(
            "Running session not found. Use <code>/pause &lt;session_id&gt;</code>."
        )
        return
    if session_doc["state"] == SESSION_COMPLETED:
        await message.reply_text("That Bunkr session is already complete.")
        return

    await bunkr_session_store.set_state(session_doc["_id"], SESSION_PAUSED)
    active_files = await bunkr_session_store.active_files(session_doc["_id"])
    for file_doc in active_files:
        gid = file_doc.get("gid")
        if gid:
            try:
                await aria2_pause(session, gid)
            except Aria2Error:
                pass
    updated = await bunkr_session_store.get_session(session_doc["_id"])
    await message.reply_text(
        f"Paused Bunkr session <code>{session_doc['_id']}</code>. "
        "Already queued Telegram uploads will continue normally."
    )
    await _send_bunkr_session_summary(message, updated)


@Client.on_message(filters.command("skip") & filters.chat(ALL_CHATS))
async def skip_bunkr_file_cmd(client, message):
    session_doc = await _owned_bunkr_session(
        message, default_states=(SESSION_RUNNING,)
    )
    if not session_doc or session_doc["state"] != SESSION_RUNNING:
        await message.reply_text(
            "Running session not found. Use <code>/skip &lt;session_id&gt;</code> "
            "or reply to its Bunkr session message."
        )
        return
    deferred, reason = await _defer_active_bunkr_download(
        session_doc, "Skipped by user"
    )
    if deferred is None:
        if reason == "complete":
            text = "That file has already completed and is entering the upload queue."
        elif reason == "no_next":
            text = (
                "The active file is the only pending file, so moving it to the "
                "bottom would not change the queue."
            )
        else:
            text = "No Bunkr file is currently downloading in that session."
        await message.reply_text(text)
        return
    await message.reply_text(
        f"Moved <code>{html.escape(str(deferred['filename']))}</code> to the "
        f"bottom of Bunkr session <code>{session_doc['_id']}</code>. "
        "The next queued file is starting now."
    )


@Client.on_message(filters.command("continue") & filters.chat(ALL_CHATS))
async def continue_bunkr_session_cmd(client, message):
    session_doc = await _owned_bunkr_session(
        message,
        default_states=(
            SESSION_RUNNING,
            SESSION_PAUSED,
            SESSION_CANCELLED,
            SESSION_FAILED,
        ),
    )
    if not session_doc:
        await message.reply_text(
            "Resumable session not found or more than one session needs attention. "
            "Use <code>/continue &lt;session_id&gt;</code> or reply to its session message."
        )
        return
    if session_doc["state"] == SESSION_COMPLETED:
        await message.reply_text("That Bunkr session is already complete.")
        return

    task = bunkr_session_tasks.get(session_doc["_id"])
    if task and not task.done():
        await bunkr_session_store.set_state(
            session_doc["_id"],
            SESSION_RUNNING,
            chat_id=message.chat.id,
            source_message_id=message.id,
        )
        for file_doc in await bunkr_session_store.active_files(session_doc["_id"]):
            gid = file_doc.get("gid")
            if gid:
                try:
                    await aria2_unpause(session, gid)
                except Aria2Error:
                    pass
    else:
        # A bot-only restart can leave aria2 alive while the in-memory session
        # task is gone. Remove any persisted stale GIDs before re-claiming them.
        for file_doc in await bunkr_session_store.active_files(session_doc["_id"]):
            if file_doc.get("gid"):
                await _remove_bunkr_download(file_doc["gid"], cleanup=False)
        await bunkr_session_store.prepare_continue(
            session_doc["_id"], message.chat.id, message.id
        )
        _start_bunkr_session(client, message, session_doc["_id"])

    await message.reply_text(
        f"Continuing Bunkr session <code>{session_doc['_id']}</code> from its "
        "first unfinished link."
    )


@Client.on_message(
    filters.command(["cancelsession", "cancelbunkr"]) & filters.chat(ALL_CHATS)
)
async def cancel_bunkr_session_cmd(client, message):
    session_doc = await _owned_bunkr_session(
        message,
        default_states=(SESSION_RUNNING, SESSION_PAUSED, SESSION_FAILED),
    )
    if not session_doc:
        await message.reply_text(
            "Session not found. Use <code>/cancelsession &lt;session_id&gt;</code>."
        )
        return
    await _cancel_bunkr_session_runtime(session_doc["_id"])
    updated = await bunkr_session_store.get_session(session_doc["_id"])
    await _send_bunkr_session_summary(message, updated)
    await message.reply_text(
        f"Continue later with <code>/continue {session_doc['_id']}</code>. "
        "Already queued Telegram uploads were not cancelled."
    )


@Client.on_message(
    filters.command(["deletesession", "delsession", "deletebunkrsession"])
    & filters.chat(ALL_CHATS)
)
async def delete_bunkr_session_cmd(client, message):
    session_doc = await _owned_bunkr_session(message)
    if not session_doc:
        await message.reply_text(
            "Session not found. Use <code>/deletesession &lt;session_id&gt;</code>."
        )
        return
    await _cancel_bunkr_session_runtime(session_doc["_id"])
    await bunkr_session_store.delete_session(session_doc["_id"])
    await message.reply_text(
        f"Deleted Bunkr session <code>{session_doc['_id']}</code> and its link "
        "history from the database. Downloaded and queued Telegram files were "
        "not deleted."
    )


@Client.on_message(
    filters.command(["deleteallsessions", "deleteallbunkrsessions"])
    & filters.chat(ALL_CHATS)
)
async def delete_all_bunkr_sessions_cmd(client, message):
    sessions = await bunkr_session_store.list_sessions(
        owner_id=message.from_user.id, limit=0
    )
    if not sessions:
        await message.reply_text("You do not have any Bunkr sessions to delete.")
        return
    for session_doc in sessions:
        await _cancel_bunkr_session_runtime(session_doc["_id"])
        await bunkr_session_store.delete_session(session_doc["_id"])
    await message.reply_text(
        f"Deleted {len(sessions)} Bunkr session(s) and their link history from "
        "the database. Downloaded and queued Telegram files were not deleted."
    )


def _split_bunkr_request_from_message(message):
    """Return ``(album_url, files_per_session)`` for /splitbunkr."""
    args = list(message.command[1:])
    reply = message.reply_to_message
    if len(args) == 1 and args[0].isdigit() and not getattr(reply, "empty", True):
        reply_text = (getattr(reply, "text", None) or getattr(reply, "caption", None) or "")
        reply_parts = reply_text.split()
        if reply_parts:
            args.append(reply_parts[0])

    if len(args) != 2:
        return None
    if args[0].isdigit():
        files_per_session, raw_link = int(args[0]), args[1]
    elif args[1].isdigit():
        raw_link, files_per_session = args[0], int(args[1])
    else:
        return None
    if not 1 <= files_per_session <= 1000:
        return None

    parsed = list(urlparse(raw_link, "https"))
    if not parsed[0]:
        parsed[0] = "https"
    if parsed[0] not in ("http", "https"):
        return None
    link = urlunparse(parsed)
    if not is_bunkr_url(link) or "/a/" not in urlparse(link).path:
        return None
    return link, files_per_session


async def _create_split_bunkr_sessions(
    client, message, album_url, files_per_session, reply
):
    """Create all album parts, leaving every part except the first paused."""
    try:
        files = await _extract_bunkr_video_files(album_url)
    except Exception as error:
        await reply.edit_text(
            f"Bunkr album extraction failed: {html.escape(str(error))[:500]}"
        )
        return []
    if not files:
        await reply.edit_text("No video files found in the Bunkr album.")
        return []

    file_groups = [
        files[index : index + files_per_session]
        for index in range(0, len(files), files_per_session)
    ]
    album_title = os.path.basename(urlparse(album_url).path.rstrip("/")) or "Bunkr"
    session_docs = []
    try:
        for index, group in enumerate(file_groups, 1):
            session_docs.append(
                await bunkr_session_store.create_session(
                    owner_id=message.from_user.id,
                    chat_id=message.chat.id,
                    source_message_id=message.id,
                    source_url=album_url,
                    source_urls=[album_url],
                    title=f"{album_title} (part {index}/{len(file_groups)})",
                    mode="normal",
                    custom_filename=None,
                    files=group,
                    initial_state=(
                        SESSION_RUNNING if index == 1 else SESSION_PAUSED
                    ),
                )
            )
    except Exception as error:
        for session_doc in session_docs:
            await bunkr_session_store.delete_session(session_doc["_id"])
        await reply.edit_text(
            "Could not store the split Bunkr sessions; any partial database "
            f"records were removed. Error: {html.escape(str(error))[:500]}"
        )
        return []

    lines = [
        f"<b>Split Bunkr album into {len(session_docs)} sessions</b>",
        f"<b>Files:</b> {len(files)} | "
        f"<b>Maximum per session:</b> {files_per_session}",
        "",
    ]
    for index, (session_doc, group) in enumerate(
        zip(session_docs, file_groups), 1
    ):
        if index == 1:
            action = "running now"
        else:
            action = f"paused - <code>/continue {session_doc['_id']}</code>"
        lines.append(
            f"<b>Part {index}/{len(session_docs)}</b> ({len(group)} files): "
            f"<code>{session_doc['_id']}</code> - {action}"
        )
    lines.extend(
        [
            "",
            "Delete a part with <code>/deletesession SESSION_ID</code>.",
        ]
    )

    chunks = []
    current = ""
    for line in lines:
        entry = f"{line}\n"
        if current and len(current) + len(entry) > 3900:
            chunks.append(current.rstrip())
            current = ""
        current += entry
    if current:
        chunks.append(current.rstrip())
    first_session = session_docs[0]
    _start_bunkr_session(client, message, first_session["_id"])
    await reply.edit_text(chunks[0], disable_web_page_preview=True)
    for chunk in chunks[1:]:
        await message.reply_text(chunk, disable_web_page_preview=True)

    return session_docs


@Client.on_message(filters.command("splitbunkr") & filters.chat(ALL_CHATS))
async def split_bunkr_cmd(client, message):
    request = _split_bunkr_request_from_message(message)
    if request is None:
        await message.reply_text(
            "Usage:\n"
            "<code>/splitbunkr &lt;album URL&gt; &lt;files per session&gt;</code>\n"
            "<code>/splitbunkr &lt;files per session&gt; &lt;album URL&gt;</code>\n"
            "Or reply to a Bunkr album URL with "
            "<code>/splitbunkr &lt;files per session&gt;</code>."
        )
        return
    album_url, files_per_session = request
    reply = await message.reply_text("Fetching and splitting Bunkr album metadata...")
    await _create_split_bunkr_sessions(
        client, message, album_url, files_per_session, reply
    )


@Client.on_message(
    filters.command(["queue", "zipqueue", "filequeue"]) & filters.chat(ALL_CHATS)
)
async def queue_cmd(client, message):
    text = message.text.split(None, 1)
    command = text.pop(0).lower()
    if "zip" in command:
        flags = (SendAsZipFlag,)
    elif "file" in command:
        flags = (ForceDocumentFlag,)
    else:
        flags = ()

    links = []
    if text:
        links = text[0].split()
    elif not getattr(message.reply_to_message, "empty", True):
        links = message.reply_to_message.text.split()

    if not links:
        await message.reply_text("""Usage:
- /queue <i>&lt;URL1&gt; &lt;URL2&gt; ...</i>
- /zipqueue <i>&lt;URL1&gt; &lt;URL2&gt; ...</i>
- /filequeue <i>&lt;URL1&gt; &lt;URL2&gt; ...</i>""")
        return

    reply = await message.reply_text(
        f"Added {len(links)} links to queue. Processing..."
    )

    task = asyncio.create_task(
        _process_queue_links(client, message, links, flags, reply)
    )
    bunkr_tasks.add(task)
    task.add_done_callback(bunkr_tasks.discard)


async def _process_queue_links(client, message, links, flags, reply):
    """Process one /queue invocation, grouping all Bunkr sources together."""
    bunkr_files = []
    bunkr_sources = []
    failed_bunkr_sources = []
    non_bunkr_count = 0

    for raw_link in links:
        parsed = list(urlparse(raw_link, "https"))
        normalized_link = None
        if parsed[0] != "magnet":
            if not parsed[0]:
                parsed[0] = "https"
            if parsed[0] in ("http", "https"):
                normalized_link = urlunparse(parsed)

        if normalized_link and is_bunkr_url(normalized_link):
            try:
                files = await _extract_bunkr_video_files(normalized_link)
            except Exception as error:
                failed_bunkr_sources.append((normalized_link, str(error)))
            else:
                if files:
                    bunkr_sources.append(normalized_link)
                    bunkr_files.extend(files)
                else:
                    failed_bunkr_sources.append(
                        (normalized_link, "No video files found")
                    )
        else:
            non_bunkr_count += 1
            await process_link(
                client, message, raw_link, None, flags, reply_msg=reply
            )
        await asyncio.sleep(2.5)

    session_doc = None
    session_error = None
    if bunkr_files:
        source_count = len(bunkr_sources)
        try:
            session_doc = await bunkr_session_store.create_session(
                owner_id=message.from_user.id,
                chat_id=message.chat.id,
                source_message_id=message.id,
                source_url=bunkr_sources[0],
                source_urls=bunkr_sources,
                title=f"Bunkr queue ({source_count} sources)",
                mode=_bunkr_mode_from_flags(flags),
                custom_filename=None,
                files=bunkr_files,
            )
        except Exception as error:
            session_error = str(error)
            await message.reply_text(
                "Bunkr queue session creation failed: "
                f"{html.escape(session_error)[:500]}"
            )
        else:
            session_id = session_doc["_id"]
            persistence_note = (
                ""
                if bunkr_session_store.persistent
                else "\n\nDB_URL is not configured; this session will not survive "
                "a bot restart."
            )
            await message.reply_text(
                f"<b>Bunkr queue session:</b> <code>{session_id}</code>\n"
                f"Combined <b>{len(bunkr_files)}</b> video file(s) from "
                f"<b>{source_count}</b> queued Bunkr link(s).\n\n"
                f"Pause: <code>/pause {session_id}</code>\n"
                f"Continue: <code>/continue {session_id}</code>\n"
                f"Delete: <code>/deletesession {session_id}</code>\n"
                f"Details: <code>/bsession {session_id}</code>"
                f"{persistence_note}"
            )
            _start_bunkr_session(client, message, session_id)

    summary_parts = [f"Processed queue of {len(links)} links."]
    if session_doc:
        summary_parts.append(
            f"Bunkr: {len(bunkr_files)} files in one session "
            f"<code>{session_doc['_id']}</code>."
        )
    if non_bunkr_count:
        summary_parts.append(
            f"Other downloads started separately: {non_bunkr_count}."
        )
    if failed_bunkr_sources:
        summary_parts.append(
            f"Bunkr links with no usable files/errors: "
            f"{len(failed_bunkr_sources)}."
        )
    if session_error:
        summary_parts.append("The combined Bunkr session could not be created.")
    try:
        await reply.edit_text("\n".join(summary_parts))
    except Exception:
        pass
    return session_doc


async def process_bunkr_download(client, message, session_doc, file_doc, flags):
    file_id = file_doc["_id"]
    slow_monitor = None
    auto_defer_unavailable = False
    try:
        async with bunkr_semaphore:
            direct_url, resolved_name, referer = await resolve_bunkr_file(
                file_doc["page_url"], session
            )
            # Metadata requests have their own shared pacing/backoff in bunkr.py.
            # Keep this lock only to prevent simultaneous page/signing resolution.
            resolved_at = time.monotonic()
        cdn_host = _bunkr_cdn_host(direct_url)
        await bunkr_session_store.update_file(
            file_id,
            filename=resolved_name or file_doc["filename"],
            cdn_host=cdn_host or None,
        )

        current = await bunkr_session_store.get_session(session_doc["_id"])
        latest_file = await bunkr_session_store.get_file(file_id)
        if not latest_file or latest_file["status"] != FILE_RESOLVING:
            return "deferred"
        if not current or current["state"] != SESSION_RUNNING:
            if current and current["state"] == SESSION_PAUSED:
                await bunkr_session_store.update_file_if_status(
                    file_id, FILE_RESOLVING, FILE_PENDING, gid=None
                )
            return

        cooled_hosts = await _active_bunkr_cooldowns(session_doc["_id"])
        if cdn_host in cooled_hosts:
            routed = await bunkr_session_store.route_file_to_bottom(
                file_id,
                f"Deferred while CDN {cdn_host} is cooling down",
            )
            if routed is None:
                routed = await bunkr_session_store.update_file_if_status(
                    file_id,
                    FILE_RESOLVING,
                    FILE_PENDING,
                    gid=None,
                    error=f"Waiting for CDN {cdn_host} cooldown",
                )
            if routed is not None:
                return "deferred"

        async def on_gid(gid):
            nonlocal slow_monitor
            updated = await bunkr_session_store.update_file_if_status(
                file_id,
                FILE_RESOLVING,
                FILE_DOWNLOADING,
                gid=gid,
                error=None,
            )
            if updated is None:
                return False
            slow_monitor = _BunkrSlowDownloadMonitor(
                BUNKR_SLOW_SPEED_KBPS * 1024 if BUNKR_MAX_AUTO_SKIPS else 0,
                BUNKR_SLOW_GRACE_SECONDS,
                BUNKR_SLOW_DURATION_SECONDS,
                window_seconds=BUNKR_SLOW_WINDOW_SECONDS,
                peak_ratio=BUNKR_SLOW_PEAK_PERCENT / 100,
            )
            latest = await bunkr_session_store.get_session(session_doc["_id"])
            if latest and latest["state"] == SESSION_PAUSED:
                try:
                    await aria2_pause(session, gid)
                except Aria2Error:
                    pass
            return True

        async def on_downloaded():
            await bunkr_session_store.update_file(
                file_id, FILE_DOWNLOADED, gid=None, error=None
            )
            if cdn_host and (slow_monitor is None or slow_monitor.completed_healthy()):
                await bunkr_session_store.record_cdn_success(cdn_host)

        async def on_removed():
            latest = await bunkr_session_store.get_file(file_id)
            return bool(latest and latest["status"] == FILE_PENDING)

        async def on_status(torrent_info):
            nonlocal auto_defer_unavailable
            if (
                auto_defer_unavailable
                or slow_monitor is None
                or not slow_monitor.should_defer(torrent_info)
            ):
                return None
            try:
                speed = int(torrent_info.get("downloadSpeed", 0))
            except (TypeError, ValueError):
                speed = 0
            reason = (
                f"Automatically deferred: {int(slow_monitor.rolling_speed_bps / 1024)} "
                f"KiB/s rolling speed stayed below the adaptive "
                f"{int(slow_monitor.effective_limit_bps / 1024)} KiB/s floor for "
                f"{BUNKR_SLOW_DURATION_SECONDS}s"
            )
            deferred, _ = await _defer_active_bunkr_download(
                session_doc, reason, automatic=True
            )
            if deferred and deferred["_id"] == file_id:
                cdn_host = html.escape(str(deferred.get("_cdn_host") or "unknown"))
                same_host_moved = int(deferred.get("_same_host_moved", 0))
                cooldown_minutes = max(
                    1, int(deferred.get("_cooldown_seconds", 0) / 60)
                )
                host_strikes = int(deferred.get("_host_strikes", 1))
                await message.reply_text(
                    f"Slow Bunkr download parked/requeued: "
                    f"<code>{html.escape(str(deferred['filename']))}</code> "
                    f"({format_bytes(speed)}/s). CDN <code>{cdn_host}</code> "
                    f"is cooling down for about {cooldown_minutes} minute(s) "
                    f"(adaptive level {host_strikes}); moved {same_host_moved} additional known "
                    f"same-CDN file(s) behind alternate hosts."
                )
                return "deferred"
            auto_defer_unavailable = True
            return None

        download_dir = _bunkr_download_dir(
            session_doc["owner_id"], session_doc["_id"], file_id
        )
        # Cyberdrop-DL uses a server lock for Bunkr. Do the equivalent here so
        # multiple sessions do not pile independent range requests onto one CDN
        # host. Different CDN hosts may still make progress concurrently.
        async with _bunkr_host_semaphore(direct_url):
            latest_session = await bunkr_session_store.get_session(session_doc["_id"])
            latest_file = await bunkr_session_store.get_file(file_id)
            if not latest_file or latest_file["status"] != FILE_RESOLVING:
                return "deferred"
            if not latest_session or latest_session["state"] != SESSION_RUNNING:
                if latest_session and latest_session["state"] == SESSION_PAUSED:
                    await bunkr_session_store.update_file_if_status(
                        file_id, FILE_RESOLVING, FILE_PENDING, gid=None
                    )
                return
            current = latest_session

            # Another session may have opened this host's circuit breaker while
            # this file waited for the one-per-CDN semaphore.
            cooled_hosts = await _active_bunkr_cooldowns(session_doc["_id"])
            if cdn_host in cooled_hosts:
                await bunkr_session_store.update_file_if_status(
                    file_id,
                    FILE_RESOLVING,
                    FILE_PENDING,
                    gid=None,
                    error=f"Waiting for CDN {cdn_host} cooldown",
                )
                return "deferred"

            # Waiting behind a large same-host file can outlive the signed URL.
            # Refresh only after acquiring the slot so addUri gets a fresh token.
            if time.monotonic() - resolved_at >= BUNKR_SIGNED_URL_REFRESH_SECONDS:
                async with bunkr_semaphore:
                    direct_url, refreshed_name, referer = await resolve_bunkr_file(
                        file_doc["page_url"], session
                    )
                if refreshed_name:
                    resolved_name = refreshed_name
                    await bunkr_session_store.update_file(
                        file_id, filename=refreshed_name
                    )

            cdn_health = await bunkr_session_store.get_cdn_health(cdn_host)
            max_connections = _bunkr_connection_count(latest_file, cdn_health)

            result = await initiate_directdl(
                client,
                message,
                direct_url,
                current.get("custom_filename") or resolved_name,
                flags,
                headers=f"Referer: {referer}",
                on_gid=on_gid,
                on_downloaded=on_downloaded,
                on_status=on_status,
                on_removed=on_removed,
                max_connections=max_connections,
                download_dir=download_dir,
                resume=True,
            )
        if result == "complete":
            await bunkr_session_store.update_file(
                file_id, FILE_DOWNLOADED, gid=None, error=None
            )
        elif result == "deferred":
            return result
        elif result == "removed":
            latest_file = await bunkr_session_store.get_file(file_id)
            if latest_file and latest_file["status"] == FILE_PENDING:
                return "deferred"
            await bunkr_session_store.update_file_if_status(
                file_id,
                (FILE_RESOLVING, FILE_DOWNLOADING),
                FILE_CANCELLED,
                gid=None,
                error="Download cancelled",
            )
        else:
            await bunkr_session_store.update_file_if_status(
                file_id,
                (FILE_RESOLVING, FILE_DOWNLOADING),
                FILE_FAILED,
                gid=None,
                error=result or "Download could not be started",
            )
    except asyncio.CancelledError:
        latest_file = await bunkr_session_store.get_file(file_id)
        if latest_file and latest_file["status"] != FILE_DOWNLOADED:
            await bunkr_session_store.update_file(
                file_id, FILE_CANCELLED, gid=None, error="Session cancelled"
            )
        raise
    except Exception as e:
        latest_file = await bunkr_session_store.get_file(file_id)
        if latest_file and latest_file["status"] == FILE_PENDING:
            return "deferred"
        await bunkr_session_store.update_file_if_status(
            file_id,
            (FILE_RESOLVING, FILE_DOWNLOADING),
            FILE_FAILED,
            gid=None,
            error=str(e),
        )
        await message.reply_text(
            f"Failed to download {html.escape(str(file_doc['filename']))}: "
            f"{html.escape(str(e))}"
        )


async def initiate_directdl(
    client,
    message,
    link,
    filename,
    flags,
    headers=None,
    on_gid=None,
    on_downloaded=None,
    on_status=None,
    on_removed=None,
    max_connections=8,
    download_dir=None,
    resume=False,
    on_uploaded=None,
    suppress_upload_summary=False,
):
    user_id = message.from_user.id
    reply = _new_download_reference(message)
    try:
        gid = await asyncio.wait_for(
            aria2_add_directdl(
                session,
                user_id,
                link,
                filename,
                LEECH_TIMEOUT,
                headers=headers,
                max_connections=max_connections,
                download_dir=download_dir,
                resume=resume,
            ),
            MAGNET_TIMEOUT,
        )
    except Aria2Error as ex:
        await message.reply_text(
            f"Aria2 Error Occured!\n{ex.error_code}: {html.escape(ex.error_message)}"
        )
        return f"Aria2 {ex.error_code}: {ex.error_message}"
    except asyncio.TimeoutError:
        await message.reply_text("Connection timed out")
        return "Connection timed out"
    else:
        if on_gid is not None:
            try:
                accepted = await on_gid(gid)
            except Exception:
                await _remove_bunkr_download(gid)
                raise
            if accepted is False:
                await _remove_bunkr_download(gid)
                return "deferred"
        return await handle_leech(
            client,
            message,
            gid,
            reply,
            user_id,
            flags,
            None,
            on_downloaded=on_downloaded,
            on_status=on_status,
            on_removed=on_removed,
            on_uploaded=on_uploaded,
            suppress_upload_summary=suppress_upload_summary,
        )


leech_statuses = dict()


async def handle_leech(
    client,
    message,
    gid,
    reply,
    user_id,
    flags,
    newFile,
    on_downloaded=None,
    on_status=None,
    on_removed=None,
    on_uploaded=None,
    suppress_upload_summary=False,
):
    torrent_info = await aria2_tell_status(session, gid)
    message_identifier = (reply.chat.id, reply.id)
    leech_statuses[message_identifier] = gid

    # Trigger sending the initial unified status message when a task is added
    await send_status_message(client, message)
    await _delete_download_placeholder(reply)

    while torrent_info["status"] in ("active", "waiting", "paused"):
        if torrent_info.get("seeder") == "true":
            break
        if on_status is not None:
            action = await on_status(torrent_info)
            if action == "deferred":
                leech_statuses.pop(message_identifier, None)
                return "deferred"
        await asyncio.sleep(1)
        try:
            torrent_info = await aria2_tell_status(session, gid)
        except Aria2Error:
            if on_removed is not None and await on_removed():
                leech_statuses.pop(message_identifier, None)
                return "deferred"
            raise

    if torrent_info["status"] == "error":
        leech_statuses.pop(message_identifier, None)
        error_code = torrent_info["errorCode"]
        error_message = torrent_info["errorMessage"]
        text = f"Aria2 Error Occured!\n{error_code}: {html.escape(error_message)}"
        if (
            error_code == "7"
            and not error_message
            and torrent_info["downloadSpeed"] == "0"
        ):
            text += (
                "\n\nThis error may have been caused due to the torrent being too slow"
            )
        await message.reply_text(text)
        return "error"
    elif torrent_info["status"] == "removed":
        leech_statuses.pop(message_identifier, None)
        if on_removed is not None and await on_removed():
            return "deferred"
        await message.reply_text("Your download has been manually cancelled.")
        return "removed"
    else:
        leech_statuses.pop(message_identifier, None)
        task = None

        tor_name = "Unknown"
        if torrent_info.get("bittorrent"):
            tor_name = torrent_info["bittorrent"]["info"]["name"]
        elif torrent_info.get("files"):
            tor_name = os.path.basename(torrent_info["files"][0]["path"])

        # Insert a 'Waiting' state block so it remains on the status board instantly
        from ..utils.status import update_upload_status_state

        await update_upload_status_state(reply.chat.id, reply.id, tor_name, "Waiting")

        if on_uploaded is not None and on_downloaded is not None:
            await on_downloaded()
        upload_queue.put_nowait(
            (
                client,
                message,
                reply,
                torrent_info,
                user_id,
                flags,
                newFile,
                {
                    "on_uploaded": on_uploaded,
                    "suppress_summary": suppress_upload_summary,
                },
            )
        )
        if on_uploaded is None and on_downloaded is not None:
            await on_downloaded()
        try:
            await aria2_remove(session, gid)
        except Aria2Error as ex:
            if not (
                ex.error_code == 1
                and ex.error_message == f"Active Download not found for GID#{gid}"
            ):
                raise
        finally:
            if task:
                await task
        return "complete"


selection_waits = dict()


async def handle_file_selection(client, message, gid, reply, user_id, flags, newFile):
    torrent_info = await aria2_tell_status(session, gid)
    files = torrent_info.get("files", [])

    if len(files) <= 1:
        await aria2_unpause(session, gid)
        await handle_leech(client, message, gid, reply, user_id, flags, newFile)
        return

    base_dir = torrent_info.get("dir", "")

    file_list_text = "<b>Select files to download:</b>\n"
    file_list_text += "Reply to this message with a comma-separated list of numbers (e.g., <code>1, 3-5, 8</code>).\nTo cancel, reply with <code>/cancel</code>.\n\n"

    import re
    from .. import IGNORE_PADDING_FILE

    tree = {}
    valid_files_count = 0
    for i, file in enumerate(files, 1):
        path = file.get("path", "")
        if path.startswith(base_dir):
            path = path[len(base_dir) :].lstrip("/")

        if IGNORE_PADDING_FILE and re.match(
            r"(?i)^_+padding_file", os.path.basename(path)
        ):
            continue

        valid_files_count += 1
        parts = path.split("/")
        current = tree
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = {
            "__id": i,
            "__size": format_bytes(int(file.get("length", 0))),
        }

    if valid_files_count == 0:
        await aria2_unpause(session, gid)
        await handle_leech(client, message, gid, reply, user_id, flags, newFile)
        return

    def render_tree(node, prefix=""):
        lines = []
        keys = list(node.keys())
        keys.sort(key=lambda k: (0 if "__id" not in node[k] else 1, k))
        for idx, k in enumerate(keys):
            is_last = idx == len(keys) - 1
            connector = "└── " if is_last else "├── "
            child_prefix = "    " if is_last else "│   "

            if "__id" in node[k]:
                file_id = node[k]["__id"]
                size = node[k]["__size"]
                lines.append(
                    f"{prefix}{connector}<b>{file_id}.</b> <code>{html.escape(k)}</code> ({size})"
                )
            else:
                lines.append(f"{prefix}{connector}📁 <b>{html.escape(k)}</b>")
                lines.extend(render_tree(node[k], prefix + child_prefix))
        return lines

    rendered_lines = render_tree(tree)

    text_chunks = []
    current_chunk = file_list_text

    for line in rendered_lines:
        if len(current_chunk) + len(line) + 1 > 4000:
            text_chunks.append(current_chunk)
            current_chunk = line + "\n"
        else:
            current_chunk += line + "\n"

    if current_chunk:
        text_chunks.append(current_chunk)

    await reply.delete()
    sent_msgs = []
    for chunk in text_chunks:
        msg = await message.reply_text(chunk)
        sent_msgs.append(msg)

    state = {
        "gid": gid,
        "user_id": user_id,
        "flags": flags,
        "newFile": newFile,
        "reply_msg": sent_msgs[-1],
        "sent_msgs": [m.id for m in sent_msgs],
    }

    for msg in sent_msgs:
        selection_waits[(msg.chat.id, msg.id)] = state
        leech_statuses[(msg.chat.id, msg.id)] = gid


@Client.on_message(filters.reply & filters.text & filters.chat(ALL_CHATS))
async def file_selection_reply(client, message):
    reply_to_id = message.reply_to_message.id
    chat_id = message.chat.id

    if (chat_id, reply_to_id) not in selection_waits:
        return

    state = selection_waits[(chat_id, reply_to_id)]
    user_id = state["user_id"]

    if message.from_user.id != user_id:
        await message.reply_text("You didn't initiate this download.")
        return

    # User is valid, remove all tracked message IDs for this selection
    for msg_id in state["sent_msgs"]:
        selection_waits.pop((chat_id, msg_id), None)
        leech_statuses.pop((chat_id, msg_id), None)

    selection_text = message.text.strip()
    gid = state["gid"]

    selected_indices = set()
    for part in selection_text.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = map(int, part.split("-"))
                selected_indices.update(range(start, end + 1))
            except ValueError:
                pass
        else:
            try:
                selected_indices.add(int(part))
            except ValueError:
                pass

    if not selected_indices:
        await message.reply_text("Invalid selection. Please reply with valid numbers.")
        for msg_id in state["sent_msgs"]:
            selection_waits[(chat_id, msg_id)] = state
            leech_statuses[(chat_id, msg_id)] = gid
        return

    select_file_str = ",".join(map(str, sorted(selected_indices)))

    try:
        await aria2_change_option(session, gid, {"select-file": select_file_str})
        await aria2_unpause(session, gid)
    except Aria2Error as ex:
        await message.reply_text(f"Error applying selection: {ex.error_message}")
        return

    for msg_id in state["sent_msgs"]:
        try:
            await client.delete_messages(chat_id, msg_id)
        except Exception:
            pass

    reply_msg = await message.reply_text("Selection applied! Starting download...")
    await handle_leech(
        client, message, gid, reply_msg, user_id, state["flags"], state["newFile"]
    )


@Client.on_message(filters.command(["list", "status"]) & filters.chat(ALL_CHATS))
async def list_leeches(client, message):
    await send_status_message(client, message)


@Client.on_message(filters.command("cancelall") & filters.chat(ALL_CHATS))
async def cancelall_leech(client, message):
    user_id = message.from_user.id
    if not await allow_admin_cancel(message.chat.id, user_id):
        await message.reply_text("You are not authorized to use this command.")
        return

    count = 0
    affected_bunkr_sessions = await bunkr_session_store.list_sessions(
        states=(SESSION_RUNNING, SESSION_PAUSED), limit=0
    )
    for session_doc in affected_bunkr_sessions:
        await bunkr_session_store.cancel_session(session_doc["_id"])
    count += len(affected_bunkr_sessions)

    # Cancel all pending asyncio tasks related to the Bunkr queue
    cancelled_bunkr_tasks = []
    for t in list(bunkr_tasks):
        if not t.done():
            t.cancel()
            cancelled_bunkr_tasks.append(t)
            count += 1
    if cancelled_bunkr_tasks:
        await asyncio.gather(*cancelled_bunkr_tasks, return_exceptions=True)
    bunkr_tasks.clear()

    # Pause everything first: with -j5 only a handful of downloads are active and
    # aria2 promotes a waiting one the moment we remove an active one, so without
    # this the sweep below races against the queue refilling itself.
    try:
        await aria2_force_pause_all(session)
    except Exception:
        pass

    # Cancel all active *and* queued Aria2 downloads
    downloads = await aria2_tell_active(session)
    try:
        downloads += await aria2_tell_waiting(session)
    except Exception:
        pass
    for i in downloads:
        gid = i["gid"]
        try:
            torrent_info = await aria2_tell_status(session, gid)
            dir_path = torrent_info.get("dir")
            await aria2_remove(session, gid)
            count += 1
            if dir_path and os.path.exists(dir_path):
                import shutil

                shutil.rmtree(dir_path, ignore_errors=True)
                parent_dir = os.path.dirname(dir_path)
                if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                    os.rmdir(parent_dir)
        except Exception:
            pass

    # Cancel all active Telegram Uploads
    for identifier, tasks in list(upload_statuses.items()):
        stop_uploads.add(identifier)
        for task, _ in tasks:
            task.cancel()
            count += 1

    if count > 0:
        await message.reply_text(
            f"Successfully cancelled {count} active tasks and purged the queue."
        )
    else:
        await message.reply_text("No active tasks or queue to cancel.")

    for session_doc in affected_bunkr_sessions:
        if session_doc["owner_id"] == user_id:
            updated = await bunkr_session_store.get_session(session_doc["_id"])
            await _send_bunkr_session_summary(message, updated)


@Client.on_message(
    (filters.command("cancel") | filters.regex(r"^/cancel_([a-zA-Z0-9_-]+)"))
    & filters.chat(ALL_CHATS)
)
async def cancel_leech(client, message):
    user_id = message.from_user.id
    gid = None
    reply = message.reply_to_message
    reply_identifier = None

    if message.text and message.text.startswith("/cancel_"):
        # Strip potential @bot_username appended by Telegram in groups
        cmd_text = message.text.split("@")[0]
        args = cmd_text.split("_")
        if len(args) == 2:
            gid = args[1]
        elif len(args) == 3:
            try:
                reply_identifier = (int(args[1]), int(args[2]))
            except ValueError:
                gid = args[1] + "_" + args[2]
    elif len(message.command) == 2:
        gid = message.command[1]
    elif len(message.command) == 3 or not getattr(reply, "empty", True):
        if len(message.command) == 3:
            try:
                reply_identifier = (int(message.command[1]), int(message.command[2]))
            except ValueError:
                reply_identifier = None
        else:
            reply_identifier = (reply.chat.id, reply.id)

    if reply_identifier:
        tasks = upload_statuses.get(reply_identifier)
        if tasks:
            unauthorized = False
            for task, starter_id in tasks:
                if user_id != starter_id and not await allow_admin_cancel(
                    message.chat.id, user_id
                ):
                    unauthorized = True
                    continue
                task.cancel()

            if unauthorized and len(tasks) == 1:
                await message.reply_text("You did not start this leech.")
            else:
                stop_uploads.add(reply_identifier)
            return

        result = progress_callback_data.get(reply_identifier)
        if result:
            if user_id != result[3] and not await allow_admin_cancel(
                message.chat.id, user_id
            ):
                await message.reply_text("You did not start this leech.")
            else:
                stop_uploads.add(reply_identifier)
                await message.reply_text("Cancelled!")
            return

        starter_id = upload_waits.get(reply_identifier)
        if starter_id:
            if user_id != starter_id[0] and not await allow_admin_cancel(
                message.chat.id, user_id
            ):
                await message.reply_text("You did not start this leech.")
            else:
                stop_uploads.add(reply_identifier)
                await message.reply_text("Cancelled!")
            return

        gid = leech_statuses.get(reply_identifier)

    if not gid:
        await message.reply_text("""Usage:
/cancel <i>&lt;GID&gt;</i>
/cancel <i>&lt;chat id&gt;</i> <i>&lt;message id&gt;</i>
/cancel <i>(as reply to status message)</i>""")
        return

    # Check for upload task cancel via direct string ID
    if gid and "_" in str(gid):
        # We assume it's the chat_id _ message_id upload pattern format
        args = str(gid).split("_")
        if len(args) == 2:
            try:
                target_id = (int(args[0]), int(args[1]))
                stop_uploads.add(target_id)
                await message.reply_text("Upload cancelled!")
                return
            except ValueError:
                pass

    if not is_gid_owner(user_id, gid) and not await allow_admin_cancel(
        message.chat.id, user_id
    ):
        await message.reply_text("You did not start this leech.")
        return

    bunkr_file = await bunkr_session_store.find_file_by_gid(gid)
    if bunkr_file:
        session_id = bunkr_file["session_id"]
        task = bunkr_session_tasks.get(session_id)
        await _cancel_bunkr_session_runtime(session_id)
        if task and not task.done():
            await asyncio.gather(task, return_exceptions=True)
        session_doc = await bunkr_session_store.get_session(session_id)
        await _send_bunkr_session_summary(message, session_doc)
        await message.reply_text(
            f"Bunkr session cancelled. Continue later with "
            f"<code>/continue {session_id}</code>. Already queued uploads are "
            "still running."
        )
        return

    try:
        # If it's still downloading, try to get the directory info to clean it up before removing the task
        torrent_info = await aria2_tell_status(session, gid)
        dir_path = torrent_info.get("dir")
        await aria2_remove(session, gid)

        # Clean up the directory of the cancelled Aria2 task
        if dir_path and os.path.exists(dir_path):
            import shutil

            shutil.rmtree(dir_path, ignore_errors=True)
            parent_dir = os.path.dirname(dir_path)
            if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                os.rmdir(parent_dir)

    except Aria2Error as ex:
        if not (
            ex.error_code == 1
            and ex.error_message == f"Active Download not found for GID#{gid}"
        ):
            await message.reply_text(
                f"Aria2 Error Occured!\n{ex.error_code}: {html.escape(ex.error_message)}"
            )


@Client.on_callback_query(
    filters.regex(r"^status_(next|prev)$") & filters.chat(ALL_CHATS)
)
async def status_callback(client, callback_query):
    chat_id = callback_query.message.chat.id
    from ..utils.status import status_pages, update_status_message

    if callback_query.data == "status_next":
        status_pages[chat_id] = status_pages.get(chat_id, 1) + 1
    elif callback_query.data == "status_prev":
        status_pages[chat_id] = max(1, status_pages.get(chat_id, 1) - 1)

    await update_status_message(client, chat_id)
    await callback_query.answer()


help_dict["leech"] = (
    "Leech",
    """/torrent <i>&lt;Torrent URL or File&gt;</i>
/torrent <i>(as reply to a Torrent URL or file)</i>

/ziptorrent <i>&lt;Torrent URL or File&gt;</i>
/ziptorrent <i>(as reply to a Torrent URL or File)</i>

/filetorrent <i>&lt;Torrent URL or File&gt;</i> - Sends videos as files
/filetorrent <i>(as reply to a Torrent URL or File)</i> - Sends videos as files

/magnet <i>&lt;Magnet URL&gt;</i>
/magnet <i>(as reply to a Magnet URL)</i>

/zipmagnet <i>&lt;Magnet URL&gt;</i>
/zipmagnet <i>(as reply to a Magnet URL)</i>

/filemagnet <i>&lt;Magnet URL&gt;</i> - Sends videos as files
/filemagnet <i>(as reply to a Magnet URL)</i> - Sends videos as files

/directdl <i>&lt;Direct URL&gt; | optional custom file name</i>
/directdl <i>(as reply to a Direct URL) | optional custom file name</i>
/direct <i>&lt;Direct URL&gt; | optional custom file name</i>
/direct <i>(as reply to a Direct URL) | optional custom file name</i>

/zipdirectdl <i>&lt;Direct URL&gt; | optional custom file name</i>
/zipdirectdl <i>(as reply to a Direct URL) | optional custom file name</i>
/zipdirect <i>&lt;Direct URL&gt; | optional custom file name</i>
/zipdirect <i>(as reply to a Direct URL) | optional custom file name</i>

/filedirectdl <i>&lt;Direct URL&gt; | optional custom file name</i> - Sends videos as files
/filedirectdl <i>(as reply to a Direct URL) | optional custom file name</i> - Sends videos as files
/filedirect <i>&lt;Direct URL&gt; | optional custom file name</i> - Sends videos as files
/filedirect <i>(as reply to a Direct URL) | optional custom file name</i> - Sends videos as files

/queue <i>&lt;URL1&gt; &lt;URL2&gt; ...</i> - One session for all queued Bunkr links
/zipqueue <i>&lt;URL1&gt; &lt;URL2&gt; ...</i> - Same, uploaded as ZIP
/filequeue <i>&lt;URL1&gt; &lt;URL2&gt; ...</i> - Same, videos sent as files
/splitbunkr <i>&lt;album URL&gt; &lt;files per session&gt;</i> - Stores split sessions; part 1 starts

/bsessions <i>[page]</i> - Lists your Bunkr sessions with Previous/Next buttons
/bsession <i>&lt;session ID&gt;</i> - Lists downloaded and unfinished file links
/pause <i>&lt;session ID&gt;</i> - Pauses Bunkr downloading; queued uploads continue
/skip <i>&lt;session ID&gt;</i> - Moves the active Bunkr file to the queue bottom
/continue <i>&lt;session ID&gt;</i> - Resumes unfinished Bunkr files
/cancelsession <i>&lt;session ID&gt;</i> - Cancels downloading but retains the session
/deletesession <i>&lt;session ID&gt;</i> - Deletes session history from the database
/deleteallsessions - Deletes all of your session histories from the database

/cancel <i>&lt;GID&gt;</i>
/cancel <i>&lt;chat id&gt;</i> <i>&lt;message id&gt;</i>
/cancel <i>(as reply to status message)</i>

/list - Lists all current leeches""",
)
