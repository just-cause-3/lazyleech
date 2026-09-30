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
import logging
import os
import re
import shutil
import tempfile
import time
import traceback
import unicodedata
import zipfile
from collections import defaultdict
from itertools import count

from natsort import natsorted
from pyrogram import StopTransmission
from pyrogram.errors.exceptions.bad_request_400 import (
    MessageIdInvalid,
    MessageNotModified,
)
from pyrogram.parser import html as pyrogram_html

from .. import (
    ADMIN_CHATS,
    IGNORE_PADDING_FILE,
    LICHER_CHAT,
    LICHER_FOOTER,
    LICHER_PARSE_EPISODE,
    LICHER_STICKER,
    PROGRESS_UPDATE_DELAY,
    TESTMODE,
    ForceDocumentFlag,
    SendAsZipFlag,
    preserved_logs,
)
from .file_cleanup import remove_uploaded_source
from .file_split import TELEGRAM_SPLIT_SIZE
from .misc import (
    calculate_eta,
    format_bytes,
    generate_thumbnail,
    get_file_mimetype,
    get_video_info,
    return_progress_string,
    split_files,
    watermark_photo,
)
from .status import (
    remove_upload_status,
    update_upload_status,
    update_upload_status_state,
)

upload_queue = asyncio.Queue()
upload_statuses = dict()
upload_tamper_lock = asyncio.Lock()
_upload_id_sequence = count(int(time.time() * 1_000_000))


def _new_upload_identifier(chat_id):
    """Return a process-unique status/cancellation identifier."""
    return chat_id, next(_upload_id_sequence)


class UploadResult(list):
    """Uploaded file links plus whether the complete job succeeded."""

    def __init__(
        self, values=(), *, complete=False, source_results=None, source_removed=False
    ):
        super().__init__(values)
        self.complete = bool(complete)
        self.source_results = list(source_results or [])
        self.source_removed = bool(source_removed)


def _usable_thumbnail(path):
    """Return a thumbnail path only when it contains actual image bytes."""
    if not path or not os.path.isfile(path):
        return None
    try:
        return path if os.path.getsize(path) > 0 else None
    except OSError:
        return None


def _truncate_utf8_filename(filename, max_bytes=250):
    """Shorten a filename without cutting a Unicode code point or extension."""
    if len(filename.encode("utf-8")) <= max_bytes:
        return filename

    stem, extension = os.path.splitext(filename)
    extension_bytes = extension.encode("utf-8")
    if len(extension_bytes) >= max_bytes:
        extension = ""
        stem = filename
        stem_budget = max_bytes
    else:
        stem_budget = max_bytes - len(extension_bytes)

    while stem and len(stem.encode("utf-8")) > stem_budget:
        stem = stem[:-1]
    return stem + extension


def sanitize_upload_filename(filename, max_bytes=250):
    """Keep valid Unicode while removing characters unsafe for an upload path."""
    original = str(filename or "")
    basename = os.path.basename(original.replace("\\", "/"))
    normalized = unicodedata.normalize("NFC", basename)
    cleaned = "".join(
        character
        for character in normalized
        if character not in ("/", "\\")
        and unicodedata.category(character) not in {"Cc", "Cs"}
    ).strip()
    if not cleaned or not cleaned.strip(". "):
        extension = os.path.splitext(basename)[1]
        cleaned = f"download_{int(time.time())}{extension}"
    return _truncate_utf8_filename(cleaned, max_bytes=max_bytes)


async def upload_worker():
    while True:
        queue_item = await upload_queue.get()
        (
            client,
            message,
            reply,
            torrent_info,
            user_id,
            flags,
            newFile,
        ) = queue_item[:7]
        upload_options = queue_item[7] if len(queue_item) > 7 else {}
        try:
            message_identifier = (reply.chat.id, reply.id)
            if SendAsZipFlag not in flags:
                pass
            task = asyncio.create_task(
                _upload_worker(
                    client,
                    message,
                    reply,
                    torrent_info,
                    user_id,
                    flags,
                    newFile,
                    upload_options,
                )
            )
            if message_identifier not in upload_statuses:
                upload_statuses[message_identifier] = []
            upload_statuses[message_identifier].append((task, user_id))

            # Allow the background worker to move on immediately instead of waiting for the upload to finish!
            # The cleanup logic runs in a wrapper inside the task itself.
            task.add_done_callback(
                lambda t,
                mi=message_identifier,
                ti=torrent_info,
                r=reply,
                uid=user_id,
                options=upload_options: asyncio.create_task(
                    cleanup_upload(t, mi, ti, r, uid, options)
                )
            )

        except asyncio.CancelledError:
            text = "Your leech has been cancelled."
            await message.reply_text(text)
        except Exception as ex:
            preserved_logs.append((message, torrent_info, ex))
            logging.exception("%s %s", message, torrent_info)
            await message.reply_text(traceback.format_exc(), parse_mode=None)
            for admin_chat in ADMIN_CHATS:
                await client.send_message(
                    admin_chat, traceback.format_exc(), parse_mode=None
                )
        finally:
            upload_queue.task_done()


async def cleanup_upload(
    task, message_identifier, torrent_info, reply, user_id, upload_options=None
):
    sent_files = []
    upload_error = None
    try:
        sent_files = await task
    except asyncio.CancelledError:
        upload_error = "upload cancelled"
    except Exception as ex:
        upload_error = str(ex)
        logging.exception("Background upload task failed")

    worker_identifier = (reply.chat.id, reply.id)
    # Drop any cancellation flag for this finished worker so stop_uploads
    # doesn't accumulate stale identifiers over the process lifetime.
    stop_uploads.discard(worker_identifier)
    async with upload_tamper_lock:
        for key in list(upload_waits.keys()):
            _, iworker_identifier = upload_waits[key]
            if iworker_identifier == worker_identifier:
                upload_waits.pop(key, None)

    if message_identifier in upload_statuses:
        upload_statuses[message_identifier] = [
            (t, uid) for t, uid in upload_statuses[message_identifier] if t != task
        ]
        if not upload_statuses[message_identifier]:
            upload_statuses.pop(message_identifier, None)
            remove_upload_status(message_identifier)

    # Clean up the actual download directory completely. Do not acknowledge
    # workspace release to session callbacks until deletion is verified.
    upload_complete = bool(getattr(sent_files, "complete", False))
    if upload_complete and not TESTMODE and torrent_info and "dir" in torrent_info:
        dir_path = torrent_info["dir"]
        cleanup_error = None
        try:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
            if os.path.exists(dir_path):
                raise OSError("download directory still exists after removal")
        except Exception as e:
            if os.path.exists(dir_path):
                cleanup_error = f"download cleanup failed for {dir_path}: {e}"
                logging.exception(
                    "Failed to completely clean up %s", torrent_info.get("dir")
                )
        if cleanup_error is None:
            logging.info("Successfully cleaned up download directory: %s", dir_path)
            # Parent pruning is optional; only the job directory controls
            # whether the workspace reservation may be released.
            parent_dir = os.path.dirname(dir_path)
            try:
                if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                    os.rmdir(parent_dir)
                    logging.info("Cleaned up empty parent directory: %s", parent_dir)
            except OSError:
                logging.warning(
                    "Could not prune empty parent directory: %s", parent_dir
                )
        else:
            if upload_error is None:
                upload_error = cleanup_error

    on_uploaded = (upload_options or {}).get("on_uploaded")
    if on_uploaded is not None:
        try:
            await on_uploaded(sent_files, upload_error)
        except Exception:
            logging.exception("Upload completion callback failed")


upload_waits = dict()


async def _upload_worker(
    client,
    message,
    reply,
    torrent_info,
    user_id,
    flags,
    newFile,
    upload_options=None,
):
    files = dict()
    sent_files = []
    source_results = []
    upload_complete = True
    try:
        parallel_files = max(
            1, int((upload_options or {}).get("parallel_files") or 1)
        )
    except (TypeError, ValueError):
        parallel_files = 1

    temp_root = (upload_options or {}).get("workspace_temp_root") or str(user_id)
    os.makedirs(temp_root, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=temp_root) as zip_tempdir:
        if SendAsZipFlag in flags:
            if torrent_info.get("bittorrent"):
                filename = torrent_info["bittorrent"]["info"]["name"]
            else:
                filename = os.path.basename(torrent_info["files"][0]["path"])
            filename = sanitize_upload_filename(filename + ".zip")
            filepath = os.path.join(zip_tempdir, filename)

            def _zip_files():
                with zipfile.ZipFile(filepath, "x") as zipf:
                    for file in torrent_info["files"]:
                        # Skip unselected files in zip for selective downloads
                        if file.get("selected") == "false":
                            continue
                        filename = file["path"].replace(
                            os.path.join(torrent_info["dir"], ""), "", 1
                        )
                        if (
                            IGNORE_PADDING_FILE
                            and re.match(r"(?i)^_+padding_file", filename) is not None
                        ):
                            continue
                        zipf.write(file["path"], filename)

            await asyncio.gather(
                update_upload_status_state(
                    reply.chat.id, reply.id, filename, "Zipping", 0, 1
                ),
                client.loop.run_in_executor(None, _zip_files),
            )
            await update_upload_status_state(
                reply.chat.id, reply.id, filename, "Zipping", 1, 1
            )
            files[filepath] = filename
        else:
            for file in torrent_info["files"]:
                # Skip unselected files in loop for selective downloads
                if file.get("selected") == "false":
                    continue
                filepath = file["path"]
                filename = filepath.replace(
                    os.path.join(torrent_info["dir"], ""), "", 1
                )
                if (
                    IGNORE_PADDING_FILE
                    and re.match(r"(?i)^_+padding_file", filename) is not None
                ):
                    continue
                if LICHER_PARSE_EPISODE:
                    filename = (
                        re.sub(
                            r"\s*(?:\[.+?\]|\(.+?\))\s*|\.[a-z][a-z0-9]{2}$",
                            "",
                            os.path.basename(filepath),
                        ).strip()
                        or filename
                    )
                files[filepath] = filename
        ordered_paths = natsorted(files)
        if SendAsZipFlag in flags:
            parallel_files = 1
        file_slots = asyncio.Semaphore(parallel_files)
        transfer_slots = (
            asyncio.Semaphore(parallel_files) if parallel_files > 1 else None
        )
        # Telegram split staging can temporarily duplicate a large source.
        # Keep preparation serial so the planner's source-total + largest-copy
        # peak remains valid even while transfers themselves run in parallel.
        split_slots = asyncio.Semaphore(1) if parallel_files > 1 else None

        async def upload_source(source_index, filepath):
            async with file_slots:
                remove_upload_status((reply.chat.id, reply.id))
                try:
                    uploaded = await _upload_file(
                        client,
                        message,
                        reply,
                        files[filepath],
                        filepath,
                        ForceDocumentFlag in flags,
                        newFile,
                        source_index + 1,
                        cleanup_source=SendAsZipFlag not in flags,
                        download_root=torrent_info.get("dir"),
                        transfer_semaphore=transfer_slots,
                        split_semaphore=split_slots,
                        workspace_temp_root=(upload_options or {}).get(
                            "workspace_temp_root"
                        ),
                    )
                except Exception:
                    # One source must not cancel successful sibling uploads.
                    # Its incomplete source result lets a persistent session
                    # retry only this file while retaining sibling links.
                    logging.exception("Source upload failed: %s", filepath)
                    uploaded = UploadResult([], complete=False)
                on_source_uploaded = (upload_options or {}).get("on_source_uploaded")
                if on_source_uploaded is not None:
                    try:
                        await on_source_uploaded(
                            {
                                "relative_name": files[filepath],
                                "uploads": list(uploaded),
                                "complete": bool(uploaded.complete),
                                "source_removed": bool(uploaded.source_removed),
                            }
                        )
                    except Exception:
                        logging.exception(
                            "Source completion callback failed: %s", filepath
                        )
                return source_index, filepath, uploaded

        tasks = [
            asyncio.create_task(upload_source(source_index, filepath))
            for source_index, filepath in enumerate(ordered_paths)
        ]
        try:
            source_uploads = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        for _source_index, filepath, uploaded in sorted(source_uploads):
            sent_files.extend(uploaded)
            source_results.append(
                {
                    "source_path": filepath,
                    "relative_name": files[filepath],
                    "uploads": list(uploaded),
                    "complete": bool(uploaded.complete),
                }
            )
            upload_complete = upload_complete and uploaded.complete
    if bool((upload_options or {}).get("suppress_summary")):
        return UploadResult(
            sent_files,
            complete=upload_complete,
            source_results=source_results,
        )

    text = "Files:\n"
    parser = pyrogram_html.HTML(client)
    quote = None
    first_index = None
    all_amount = 1
    for filename, filelink in sent_files:
        if filelink:
            atext = f'- <a href="{filelink}">{html.escape(filename)}</a>'
        else:
            atext = f"- {html.escape(filename)} (empty)"
        atext += "\n"
        futtext = text + atext
        if all_amount > 100 or len((await parser.parse(futtext))["message"]) > 4096:
            thing = await message.reply_text(
                text, quote=quote, disable_web_page_preview=True
            )
            if first_index is None:
                first_index = thing
            quote = False
            futtext = atext
            all_amount = 1
            await asyncio.sleep(PROGRESS_UPDATE_DELAY)
        all_amount += 1
        text = futtext
    if not sent_files:
        text = "Files: None"
    elif LICHER_CHAT and LICHER_STICKER and message.chat.id in ADMIN_CHATS:
        await client.send_sticker(LICHER_CHAT, LICHER_STICKER)

    # Only send the summary index message if there is an error, or if there are multiple files
    if (
        len(sent_files) != 1
        or (len(sent_files) == 1 and sent_files[0][1] is None)
    ):
        await message.reply_text(text, quote=quote, disable_web_page_preview=True)

    return UploadResult(
        sent_files,
        complete=upload_complete,
        source_results=source_results,
    )


async def _upload_file(
    client,
    message,
    reply,
    filename,
    filepath,
    force_document,
    newFile,
    count,
    cleanup_source=False,
    download_root=None,
    transfer_semaphore=None,
    split_semaphore=None,
    workspace_temp_root=None,
):
    if not os.path.getsize(filepath):
        return UploadResult([(os.path.basename(filename), None)], complete=False)
    worker_identifier = (reply.chat.id, reply.id)
    user_id = message.from_user.id
    user_thumbnail = os.path.join(str(user_id), "thumbnail.jpg")
    user_watermark = os.path.join(str(user_id), "watermark.jpg")
    user_watermarked_thumbnail = os.path.join(str(user_id), "watermarked_thumbnail.jpg")
    file_has_big = os.path.getsize(filepath) > TELEGRAM_SPLIT_SIZE

    safe_filename = sanitize_upload_filename(filename)

    if safe_filename != filename:
        safe_path = os.path.join(os.path.dirname(filepath), safe_filename)
        try:
            os.rename(filepath, safe_path)
            filepath = safe_path
            filename = safe_filename
        except Exception as e:
            logging.error(f"Failed to sanitize filename path: {e}")

    upload_identifier = _new_upload_identifier(message.chat.id)
    async with upload_tamper_lock:
        upload_waits[upload_identifier] = user_id, worker_identifier

    to_upload = []
    sent_files = []
    split_task = None
    try:
        ss = ""
        ps = ""
        if newFile is not None:
            regcheck = re.match(".*{(.*)}$", newFile)
            if regcheck is not None:
                sd = str(regcheck.groups()[0])
                sds = sd.split(",")
                sr = 3
                if len(sds) == 2:
                    sr = int(sds[1])
                if "p" in sds[0] or "P" in sds[0]:
                    ps = ("0" * (sr - len(str(count)))) + (str(count)) + " "
                if "s" in sds[0] or "S" in sds[0]:
                    ss = " " + ("0" * (sr - len(str(count)))) + (str(count))
            newFile = re.sub(r"{.*}$", "", newFile)
            nf = newFile.split(".")
            file_ext = nf.pop().strip()
            newFile = ".".join(nf).strip()
            newFileName = (
                os.path.dirname(filepath) + "/" + ps + newFile + ss + "." + file_ext
            )
            os.rename(filepath, newFileName)
            filepath = newFileName
        source_filepath = filepath
        with tempfile.TemporaryDirectory(
            dir=workspace_temp_root or str(user_id)
        ) as tempdir:
            if file_has_big:

                async def _split_files():
                    async def prepare_parts():
                        splitted = await split_files(
                            filepath, tempdir, force_document
                        )
                        for split in splitted:
                            # Use the physical part name for the Telegram document,
                            # caption, progress board, and final link summary.
                            to_upload.append((split, os.path.basename(split)))
                        if to_upload and cleanup_source and download_root:
                            await _remove_source_file(
                                source_filepath,
                                download_root,
                                lifecycle="successfully split and queued",
                            )

                    if split_semaphore is None:
                        await prepare_parts()
                    else:
                        async with split_semaphore:
                            await prepare_parts()

                split_task = asyncio.create_task(_split_files())
            else:
                to_upload.append((filepath, filename))
            for _ in range(PROGRESS_UPDATE_DELAY):
                if upload_identifier in stop_uploads:
                    return UploadResult(sent_files, complete=False)
                await asyncio.sleep(1)
            if upload_identifier in stop_uploads:
                return UploadResult(sent_files, complete=False)
            if split_task and not split_task.done():
                await update_upload_status_state(
                    upload_identifier[0],
                    upload_identifier[1],
                    filename,
                    "Splitting",
                    0,
                    1,
                )
                while not split_task.done():
                    if upload_identifier in stop_uploads:
                        return UploadResult(sent_files, complete=False)
                    await asyncio.sleep(1)
            if split_task:
                try:
                    await split_task
                except Exception as error:
                    logging.exception(
                        "Failed to split %s for Telegram upload", filepath
                    )
                    await message.reply_text(
                        "Could not split "
                        f"<code>{html.escape(str(filename))}</code> for upload: "
                        f"{html.escape(str(error))}"
                    )
                    return UploadResult(sent_files, complete=False)
                if not to_upload:
                    await message.reply_text(
                        "Could not split "
                        f"<code>{html.escape(str(filename))}</code>: "
                        "no parts were created."
                    )
                    return UploadResult(sent_files, complete=False)
            if upload_identifier in stop_uploads:
                return UploadResult(sent_files, complete=False)
            if file_has_big:
                # Every numbered part now exists and the source was released
                # inside the serialized preparation section. A failed split
                # never reaches that cleanup and therefore retains its source.
                # The placeholder represents the split operation. Each part
                # gets its own status/cancel ID once all parts are ready.
                remove_upload_status(upload_identifier)
                async with upload_tamper_lock:
                    upload_waits.pop(upload_identifier, None)
                thumbnail = None
                for candidate in (user_thumbnail, user_watermarked_thumbnail):
                    thumbnail = _usable_thumbnail(candidate) or thumbnail
                split_upload_args = (
                    client,
                    message,
                    worker_identifier,
                    user_id,
                    to_upload,
                    thumbnail,
                    tempdir,
                )
                if transfer_semaphore is None:
                    split_uploads = await _upload_split_parts(*split_upload_args)
                else:
                    split_uploads = await _upload_split_parts(
                        *split_upload_args,
                        transfer_semaphore=transfer_semaphore,
                    )
                sent_files.extend(split_uploads)
                upload_complete = (
                    len(sent_files) == len(to_upload)
                    and all(link for _, link in sent_files)
                )
                return UploadResult(
                    sent_files,
                    complete=upload_complete,
                    source_removed=(
                        upload_complete and cleanup_source and not TESTMODE
                        and not os.path.exists(source_filepath)
                    ),
                )
            for a, (filepath, filename) in enumerate(to_upload):
                while True:
                    if a:
                        async with upload_tamper_lock:
                            upload_waits.pop(upload_identifier, None)
                            upload_identifier = _new_upload_identifier(
                                message.chat.id
                            )
                            upload_waits[upload_identifier] = user_id, worker_identifier
                        for _ in range(PROGRESS_UPDATE_DELAY):
                            if upload_identifier in stop_uploads:
                                return UploadResult(sent_files, complete=False)
                            await asyncio.sleep(1)
                        if upload_identifier in stop_uploads:
                            return UploadResult(sent_files, complete=False)
                    thumbnail = None
                    for i in (user_thumbnail, user_watermarked_thumbnail):
                        thumbnail = _usable_thumbnail(i) or thumbnail
                    mimetype = await get_file_mimetype(filepath)
                    progress_args = (
                        client,
                        message,
                        upload_identifier,
                        filename,
                        user_id,
                    )
                    try:
                        if not force_document and mimetype.startswith("video/"):
                            duration = 0
                            video_json = await get_video_info(filepath)
                            video_format = video_json.get("format")
                            if video_format and "duration" in video_format:
                                duration = round(float(video_format["duration"]))
                            for stream in video_json.get("streams", ()):
                                if stream["codec_type"] == "video":
                                    width = stream.get("width")
                                    height = stream.get("height")
                                    if width and height:
                                        if not thumbnail:
                                            thumbnail = os.path.join(tempdir, "0.jpg")
                                            await generate_thumbnail(
                                                filepath, thumbnail
                                            )
                                            if os.path.isfile(
                                                thumbnail
                                            ) and os.path.isfile(user_watermark):
                                                othumbnail = thumbnail
                                                thumbnail = os.path.join(
                                                    tempdir, "1.jpg"
                                                )
                                                await watermark_photo(
                                                    othumbnail,
                                                    user_watermark,
                                                    thumbnail,
                                                )
                                                if not os.path.isfile(thumbnail):
                                                    thumbnail = othumbnail
                                            if not os.path.isfile(thumbnail):
                                                thumbnail = None
                                        break
                            else:
                                width = height = 0
                            thumbnail = _usable_thumbnail(thumbnail)
                            async def send_video():
                                return await message.reply_video(
                                    filepath,
                                    thumb=thumbnail,
                                    caption=filename,
                                    duration=duration,
                                    width=width,
                                    height=height,
                                    parse_mode=None,
                                    progress=progress_callback,
                                    progress_args=progress_args,
                                )

                            if transfer_semaphore is None:
                                resp = await send_video()
                            else:
                                async with transfer_semaphore:
                                    resp = await send_video()
                        else:
                            thumbnail = _usable_thumbnail(thumbnail)
                            async def send_document():
                                return await message.reply_document(
                                    filepath,
                                    thumb=thumbnail,
                                    caption=filename,
                                    parse_mode=None,
                                    progress=progress_callback,
                                    progress_args=progress_args,
                                )

                            if transfer_semaphore is None:
                                resp = await send_document()
                            else:
                                async with transfer_semaphore:
                                    resp = await send_document()
                    except StopTransmission:
                        resp = None
                    except Exception:
                        await message.reply_text(
                            traceback.format_exc(), parse_mode=None
                        )
                        break
                    if resp:
                        sent_files.append((os.path.basename(filename), resp.link))
                        if (
                            LICHER_CHAT
                            and message.chat.id in ADMIN_CHATS
                            and mimetype.startswith("video/")
                            and resp.video
                        ):
                            await client.send_video(
                                LICHER_CHAT,
                                resp.video.file_id,
                                thumb=thumbnail,
                                caption=filename + LICHER_FOOTER,
                                duration=duration,
                                width=width,
                                height=height,
                                parse_mode=None,
                            )
                        break

                    remove_upload_status(upload_identifier)
                    return UploadResult(sent_files, complete=False)
                remove_upload_status(upload_identifier)
        upload_complete = (
            bool(to_upload)
            and len(sent_files) == len(to_upload)
            and all(link for _, link in sent_files)
        )
        if upload_complete and cleanup_source and download_root:
            await _remove_source_file(
                source_filepath,
                download_root,
                lifecycle="successfully uploaded",
            )
        return UploadResult(
            sent_files,
            complete=upload_complete,
            source_removed=(
                upload_complete and cleanup_source and not TESTMODE
                and not os.path.exists(source_filepath)
            ),
        )
    finally:
        remove_upload_status(upload_identifier)
        stop_uploads.discard(upload_identifier)
        if split_task:
            split_task.cancel()
        async with upload_tamper_lock:
            upload_waits.pop(upload_identifier, None)


async def _remove_source_file(filepath, download_root, *, lifecycle):
    removed = await asyncio.to_thread(remove_uploaded_source, filepath, download_root)
    if removed:
        logging.info("Removed %s source file: %s", lifecycle, filepath)
    else:
        logging.warning("Could not remove %s source file: %s", lifecycle, filepath)
    return removed


async def _upload_split_part(
    client,
    message,
    worker_identifier,
    user_id,
    filepath,
    filename,
    thumbnail,
    tempdir,
    transfer_semaphore=None,
):
    """Upload one numbered part and free that part as soon as Telegram accepts it."""
    upload_identifier = _new_upload_identifier(message.chat.id)
    async with upload_tamper_lock:
        upload_waits[upload_identifier] = user_id, worker_identifier
    await update_upload_status_state(
        upload_identifier[0],
        upload_identifier[1],
        filename,
        "Waiting",
        0,
        os.path.getsize(filepath),
    )
    try:
        if upload_identifier in stop_uploads:
            return None
        try:
            async def send_document():
                return await message.reply_document(
                    filepath,
                    thumb=_usable_thumbnail(thumbnail),
                    caption=filename,
                    parse_mode=None,
                    progress=progress_callback,
                    progress_args=(
                        client,
                        message,
                        upload_identifier,
                        filename,
                        user_id,
                    ),
                )

            if transfer_semaphore is None:
                response = await send_document()
            else:
                async with transfer_semaphore:
                    response = await send_document()
        except StopTransmission:
            return None
        except Exception:
            logging.exception("Failed to upload split part %s", filepath)
            await message.reply_text(traceback.format_exc(), parse_mode=None)
            return None

        if not response:
            return None

        # A successfully accepted temporary part no longer needs disk space.
        # The source was already removed after the full split was staged.
        removed = await asyncio.to_thread(remove_uploaded_source, filepath, tempdir)
        if not removed:
            logging.warning("Could not remove uploaded split part: %s", filepath)
        return os.path.basename(filename), response.link
    finally:
        remove_upload_status(upload_identifier)
        stop_uploads.discard(upload_identifier)
        async with upload_tamper_lock:
            upload_waits.pop(upload_identifier, None)


async def _upload_split_parts(
    client,
    message,
    worker_identifier,
    user_id,
    to_upload,
    thumbnail,
    tempdir,
    transfer_semaphore=None,
):
    """Make every split part eligible together and return links in part order."""

    async def upload_indexed(part_index, part_path, part_name):
        args = (
            client,
            message,
            worker_identifier,
            user_id,
            part_path,
            part_name,
            thumbnail,
            tempdir,
        )
        if transfer_semaphore is None:
            result = await _upload_split_part(*args)
        else:
            result = await _upload_split_part(
                *args, transfer_semaphore=transfer_semaphore
            )
        return part_index, result

    results = await asyncio.gather(
        *(
            upload_indexed(part_index, part_path, part_name)
            for part_index, (part_path, part_name) in enumerate(to_upload)
        )
    )
    return [
        result
        for _, result in sorted(results, key=lambda item: item[0])
        if result is not None
    ]


progress_callback_data = dict()
stop_uploads = set()


async def progress_callback(
    current, total, client, message, upload_identifier, filename, user_id
):
    try:
        if upload_identifier in stop_uploads:
            client.stop_transmission()
            return

        if current == total:
            remove_upload_status(upload_identifier)
        else:
            update_upload_status(
                upload_identifier, current, total, filename, upload_identifier[0]
            )

    # stop_transmission() raises StopTransmission to abort the upload; it must
    # propagate to pyrogram (and up to the caller, which sets resp = None) or the
    # transfer never stops and the callback keeps firing/erroring on every chunk.
    except StopTransmission:
        raise
    except Exception as e:
        logging.error(f"Error in progress callback: {e}")
