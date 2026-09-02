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
import zipfile
from collections import defaultdict

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


async def upload_worker():
    while True:
        (
            client,
            message,
            reply,
            torrent_info,
            user_id,
            flags,
            newFile,
        ) = await upload_queue.get()
        try:
            message_identifier = (reply.chat.id, reply.id)
            if SendAsZipFlag not in flags:
                pass
            task = asyncio.create_task(
                _upload_worker(
                    client, message, reply, torrent_info, user_id, flags, newFile
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
                uid=user_id: asyncio.create_task(cleanup_upload(t, mi, ti, r, uid))
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


async def cleanup_upload(task, message_identifier, torrent_info, reply, user_id):
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as ex:
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

    # Clean up the actual download directory completely
    if not TESTMODE and torrent_info and "dir" in torrent_info:
        try:
            dir_path = torrent_info["dir"]
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path, ignore_errors=True)
                logging.info(f"Successfully cleaned up download directory: {dir_path}")

            # Try to clean up parent directory if empty
            parent_dir = os.path.dirname(dir_path)
            if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                os.rmdir(parent_dir)
                logging.info(f"Cleaned up empty parent directory: {parent_dir}")
        except Exception as e:
            logging.error(
                f"Failed to completely clean up {torrent_info.get('dir')}: {e}"
            )


upload_waits = dict()


async def _upload_worker(client, message, reply, torrent_info, user_id, flags, newFile):
    files = dict()
    sent_files = []

    with tempfile.TemporaryDirectory(dir=str(user_id)) as zip_tempdir:
        if SendAsZipFlag in flags:
            if torrent_info.get("bittorrent"):
                filename = torrent_info["bittorrent"]["info"]["name"]
            else:
                filename = os.path.basename(torrent_info["files"][0]["path"])
            filename = filename[-251:] + ".zip"
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
        fcount = 0
        for filepath in natsorted(files):
            fcount += 1
            remove_upload_status((reply.chat.id, reply.id))
            sent_files.extend(
                await _upload_file(
                    client,
                    message,
                    reply,
                    files[filepath],
                    filepath,
                    ForceDocumentFlag in flags,
                    newFile,
                    fcount,
                    cleanup_source=SendAsZipFlag not in flags,
                    download_root=torrent_info.get("dir"),
                )
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
    if len(sent_files) != 1 or (len(sent_files) == 1 and sent_files[0][1] is None):
        await message.reply_text(text, quote=quote, disable_web_page_preview=True)


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
):
    if not os.path.getsize(filepath):
        return [(os.path.basename(filename), None)]
    worker_identifier = (reply.chat.id, reply.id)
    user_id = message.from_user.id
    user_thumbnail = os.path.join(str(user_id), "thumbnail.jpg")
    user_watermark = os.path.join(str(user_id), "watermark.jpg")
    user_watermarked_thumbnail = os.path.join(str(user_id), "watermarked_thumbnail.jpg")
    file_has_big = os.path.getsize(filepath) > TELEGRAM_SPLIT_SIZE

    import re

    # Strip complex emojis/unicode from the physical filename path to prevent Pyrogram Base64 decode crashes
    # Using a whitelist regex to strictly allow only standard alphanumeric, dashes, dots, and spaces
    safe_filename = re.sub(r"[^A-Za-z0-9_\-\. ]+", "", filename).strip()
    if not safe_filename:
        # Fallback if filename was completely wiped out by regex
        ext = os.path.splitext(filename)[1]
        safe_filename = f"bunkr_file_{int(time.time())}{ext}"

    if safe_filename != filename:
        # Construct new safe path and rename it on disk
        safe_path = os.path.join(os.path.dirname(filepath), safe_filename)
        try:
            os.rename(filepath, safe_path)
            filepath = safe_path
            # Important: also update the filename parameter so the renaming logic below inherits the correct extension/name!
            filename = safe_filename
        except Exception as e:
            logging.error(f"Failed to sanitize filename path: {e}")

    upload_identifier = (message.chat.id, int(time.time() * 1000))
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
        with tempfile.TemporaryDirectory(dir=str(user_id)) as tempdir:
            if file_has_big:

                async def _split_files():
                    splitted = await split_files(filepath, tempdir, force_document)
                    for split in splitted:
                        # Use the physical part name for the Telegram document,
                        # caption, progress board, and final link summary.
                        to_upload.append((split, os.path.basename(split)))

                split_task = asyncio.create_task(_split_files())
            else:
                to_upload.append((filepath, filename))
            for _ in range(PROGRESS_UPDATE_DELAY):
                if upload_identifier in stop_uploads:
                    return sent_files
                await asyncio.sleep(1)
            if upload_identifier in stop_uploads:
                return sent_files
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
                        return sent_files
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
                    return sent_files
                if not to_upload:
                    await message.reply_text(
                        "Could not split "
                        f"<code>{html.escape(str(filename))}</code>: "
                        "no parts were created."
                    )
                    return sent_files
            if upload_identifier in stop_uploads:
                return sent_files
            for a, (filepath, filename) in enumerate(to_upload):
                while True:
                    if a:
                        async with upload_tamper_lock:
                            upload_waits.pop(upload_identifier, None)
                            upload_identifier = (
                                message.chat.id,
                                int(time.time() * 1000),
                            )
                            upload_waits[upload_identifier] = user_id, worker_identifier
                        for _ in range(PROGRESS_UPDATE_DELAY):
                            if upload_identifier in stop_uploads:
                                return sent_files
                            await asyncio.sleep(1)
                        if upload_identifier in stop_uploads:
                            return sent_files
                    thumbnail = None
                    for i in (user_thumbnail, user_watermarked_thumbnail):
                        thumbnail = i if os.path.isfile(i) else thumbnail
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
                            resp = await message.reply_video(
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
                        else:
                            resp = await message.reply_document(
                                filepath,
                                thumb=thumbnail,
                                caption=filename,
                                parse_mode=None,
                                progress=progress_callback,
                                progress_args=progress_args,
                            )
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
                    return sent_files
                remove_upload_status(upload_identifier)
        upload_complete = bool(to_upload) and len(sent_files) == len(to_upload)
        if upload_complete and cleanup_source and download_root:
            removed = await asyncio.to_thread(
                remove_uploaded_source, source_filepath, download_root
            )
            if removed:
                logging.info(
                    "Removed successfully uploaded source file: %s",
                    source_filepath,
                )
            else:
                logging.warning(
                    "Could not remove successfully uploaded source file: %s",
                    source_filepath,
                )
        return sent_files
    finally:
        remove_upload_status(upload_identifier)
        stop_uploads.discard(upload_identifier)
        if split_task:
            split_task.cancel()
        async with upload_tamper_lock:
            upload_waits.pop(upload_identifier, None)


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
