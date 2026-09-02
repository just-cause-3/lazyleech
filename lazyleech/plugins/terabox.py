"""Download TeraBox shares and upload their files to Telegram."""

import asyncio
import html
import json
import os

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
)
from ..utils.terabox_config import TeraboxConfigStore
from .leech import initiate_directdl


XAPIVERSE_KEY = os.environ.get("XAPIVERSE_KEY", "")
TERABOX_BASE_URL = os.environ.get("TERABOX_BASE_URL", DEFAULT_TERABOX_ENDPOINT)
TERABOX_API_URL = "https://xapiverse.com/api/terabox"
TERABOX_CONFIG = TeraboxConfigStore()


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
    & filters.chat(ADMIN_CHATS)
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
    & filters.chat(ADMIN_CHATS)
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
    & filters.chat(ADMIN_CHATS)
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


help_dict["terabox"] = (
    "TeraBox",
    """/tera <i>&lt;TeraBox URL&gt;</i>
/ziptera <i>&lt;TeraBox URL&gt;</i>
/filetera <i>&lt;TeraBox URL&gt;</i> - Sends videos as files

Downloads files from TeraBox shares and uploads them to Telegram.""",
)

help_dict["terabox_cookie"] = (
    "TeraBox Cookie (configured chat administrators only)",
    """/setteraboxcookie <i>&lt;ndus value&gt;</i> - Validate and persist a replacement cookie
/teraboxcookiestatus - Show whether an override is active without revealing it
/clearteraboxcookie - Remove the database override and use the environment fallback""",
)
