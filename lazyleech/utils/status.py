import asyncio
import html
import math
import time

from pyrogram.errors import MessageIdInvalid, MessageNotModified
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .. import PROGRESS_UPDATE_DELAY, session
from .aria2 import aria2_tell_active
from .misc import calculate_eta, format_bytes, return_progress_string

status_messages = {}  # chat_id -> Message
active_uploads = {}  # identifier -> dict
status_pages = {}  # chat_id -> page_int


def update_upload_status(identifier, current, total, filename, chat_id):
    if identifier not in active_uploads:
        active_uploads[identifier] = {
            "start_time": time.time(),
            "filename": filename,
            "chat_id": chat_id,
            "state": "Uploading",
        }
    active_uploads[identifier]["current"] = current
    active_uploads[identifier]["total"] = total
    active_uploads[identifier]["state"] = "Uploading"


async def update_upload_status_state(
    chat_id, message_id, filename, state, current=0, total=1
):
    identifier = (chat_id, message_id)
    if identifier not in active_uploads:
        active_uploads[identifier] = {
            "start_time": time.time(),
            "filename": filename,
            "chat_id": chat_id,
            "state": "Waiting",
        }
    active_uploads[identifier]["state"] = state
    active_uploads[identifier]["current"] = current
    active_uploads[identifier]["total"] = total


def remove_upload_status(identifier):
    active_uploads.pop(identifier, None)


async def get_status_text(chat_id):
    blocks = []
    # Add downloads
    try:
        downloads = await aria2_tell_active(session)
        for i in downloads:
            if i.get("bittorrent") and i["bittorrent"].get("info"):
                tor_name = i["bittorrent"]["info"]["name"]
            else:
                import os
                from urllib.parse import unquote, urlparse

                if i["files"] and i["files"][0]["path"]:
                    tor_name = os.path.basename(i["files"][0]["path"])
                elif i["files"] and i["files"][0]["uris"]:
                    tor_name = unquote(
                        os.path.basename(urlparse(i["files"][0]["uris"][0]["uri"]).path)
                    )
                else:
                    tor_name = "Unknown"

            status = i["status"].capitalize()
            total_length = int(i["totalLength"])
            completed_length = int(i["completedLength"])
            download_speed = format_bytes(int(i["downloadSpeed"])) + "/s"

            formatted_total = format_bytes(total_length) if total_length else "Unknown"
            formatted_completed = format_bytes(completed_length)

            block = f"<b>{html.escape(tor_name)}</b>\n"
            block += f"<code>{html.escape(return_progress_string(completed_length, total_length))}</code>\n"
            block += f"<b>Status:</b> {status} | <b>Downloaded:</b> {formatted_completed} of {formatted_total}\n"
            block += f"<b>Speed:</b> {download_speed} | /cancel_{i['gid']}\n\n"
            blocks.append(block)
    except Exception:
        pass

    # Add uploads
    for uid, data in list(active_uploads.items()):
        current = data.get("current", 0)
        total = data.get("total", 0)
        start_time = data.get("start_time", time.time())
        filename = data.get("filename", "Unknown")
        state = data.get("state", "Uploading")

        speed = (
            format_bytes((current) / (time.time() - start_time))
            if (time.time() - start_time) > 0 and state == "Uploading"
            else "0 B"
        )
        formatted_total = format_bytes(total) if total else "Unknown"
        formatted_completed = format_bytes(current)

        block = f"<b>{html.escape(filename)}</b>\n"
        if state == "Uploading":
            block += (
                f"<code>{html.escape(return_progress_string(current, total))}</code>\n"
            )
            block += f"<b>Status:</b> {state} | <b>Uploaded:</b> {formatted_completed} of {formatted_total}\n"
            block += f"<b>Speed:</b> {speed}/s | /cancel_{uid[0]}_{uid[1]}\n\n"
        elif state == "Waiting":
            block += f"<b>Status:</b> {state} in Queue...\n\n"
        else:
            block += f"<b>Status:</b> {state}...\n\n"
        blocks.append(block)

    if not blocks:
        return "No active tasks.", None

    TASKS_PER_PAGE = 4
    total_tasks = len(blocks)
    total_pages = math.ceil(total_tasks / TASKS_PER_PAGE)

    page = status_pages.get(chat_id, 1)
    if page > total_pages:
        page = total_pages
        status_pages[chat_id] = page

    start = (page - 1) * TASKS_PER_PAGE
    end = start + TASKS_PER_PAGE

    text = "".join(blocks[start:end])
    reply_markup = None

    if total_pages > 1:
        text += f"<b>Page:</b> {page}/{total_pages} | <b>Tasks:</b> {total_tasks}"
        buttons = []
        if page > 1:
            buttons.append(
                InlineKeyboardButton("⬅️ Previous", callback_data="status_prev")
            )
        if page < total_pages:
            buttons.append(InlineKeyboardButton("Next ➡️", callback_data="status_next"))
        reply_markup = InlineKeyboardMarkup([buttons])

    return text, reply_markup


async def update_status_message(client, chat_id):
    text, reply_markup = await get_status_text(chat_id)
    if chat_id in status_messages:
        msg = status_messages[chat_id]
        try:
            if msg.text != text or getattr(msg, "reply_markup", None) != reply_markup:
                await msg.edit_text(text, reply_markup=reply_markup)
                msg.text = text
                msg.reply_markup = reply_markup
        except MessageNotModified:
            pass
        except MessageIdInvalid:
            status_messages.pop(chat_id, None)
        except Exception:
            pass


async def send_status_message(client, message):
    chat_id = message.chat.id
    if chat_id in status_messages:
        try:
            await status_messages[chat_id].delete()
        except Exception:
            pass
    text, reply_markup = await get_status_text(chat_id)
    msg = await client.send_message(chat_id, text, reply_markup=reply_markup)
    msg.text = text
    msg.reply_markup = reply_markup
    status_messages[chat_id] = msg


async def status_worker(client):
    while True:
        await asyncio.sleep(PROGRESS_UPDATE_DELAY)
        for chat_id in list(status_messages.keys()):
            await update_status_message(client, chat_id)
