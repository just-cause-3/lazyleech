# lazyleech - Mega-Debrid plugin
# Unrestricts premium hoster links and caches torrents via mega-debrid.eu,
# then downloads the resulting direct links and uploads to Telegram

import asyncio
import html
import os
import time
from urllib.parse import unquote, urlparse

import aiohttp
from pyrogram import Client, filters

from .. import (
    ALL_CHATS,
    PROGRESS_UPDATE_DELAY,
    ForceDocumentFlag,
    SendAsZipFlag,
    help_dict,
    session,
)
from ..utils.misc import format_bytes
from .leech import initiate_directdl

MEGADEBRID_API_URL = "https://www.mega-debrid.eu/api.php"
MEGADEBRID_TOKEN = os.environ.get("MEGADEBRID_TOKEN", "")
MEGADEBRID_LOGIN = os.environ.get("MEGADEBRID_LOGIN", "")
MEGADEBRID_PASSWORD = os.environ.get("MEGADEBRID_PASSWORD", "")
# How long to wait for mega-debrid to finish caching a torrent (seconds)
MEGADEBRID_TORRENT_TIMEOUT = int(os.environ.get("MEGADEBRID_TORRENT_TIMEOUT", 3600))
TORRENT_POLL_INTERVAL = max(PROGRESS_UPDATE_DELAY, 5)

NOT_CONFIGURED_TEXT = (
    "❌ Mega-Debrid is not configured. Set <code>MEGADEBRID_TOKEN</code> "
    "or <code>MEGADEBRID_LOGIN</code> + <code>MEGADEBRID_PASSWORD</code> in .env"
)


class MegaDebridError(Exception):
    pass


_token = MEGADEBRID_TOKEN


async def megadebrid_connect():
    """Log in with MEGADEBRID_LOGIN/MEGADEBRID_PASSWORD and cache a fresh token"""
    global _token
    if not (MEGADEBRID_LOGIN and MEGADEBRID_PASSWORD):
        raise MegaDebridError(
            "Token expired or invalid and no MEGADEBRID_LOGIN/MEGADEBRID_PASSWORD "
            "set to fetch a new one"
        )
    params = {
        "action": "connectUser",
        "login": MEGADEBRID_LOGIN,
        "password": MEGADEBRID_PASSWORD,
    }
    async with session.get(MEGADEBRID_API_URL, params=params) as resp:
        data = await resp.json(content_type=None)
    if data.get("response_code") != "ok":
        raise MegaDebridError(data.get("response_text") or "Login failed")
    vip_end = data.get("vip_end")
    try:
        if vip_end and int(vip_end) < time.time():
            raise MegaDebridError(
                "Premium subscription has expired (free accounts cannot use the API)"
            )
    except (TypeError, ValueError):
        pass
    _token = data["token"]
    return _token


def _is_token_error(data):
    # API returns {"response_code": "TOKEN_ERROR", "response_text": "Token error, please log-in"}
    code = str(data.get("response_code") or "").upper()
    text = (data.get("response_text") or "").lower()
    return "TOKEN" in code or "token" in text or "log-in" in text


# Friendly explanations for known API error codes that come back with an empty
# response_text (the API often returns only a code, e.g. UNRESTRICTING_ERROR_1).
ERROR_MESSAGES = {
    "UNRESTRICTING_ERROR": (
        "Couldn't unrestrict this link. The hoster may be temporarily down for "
        "Mega-Debrid, the link may be dead/invalid, or your premium subscription "
        "or daily traffic may be exhausted - check your account at mega-debrid.eu"
    ),
}


def _friendly_error(result):
    """Turn a non-ok API result into a human-readable message."""
    code = str(result.get("response_code") or "")
    text = (result.get("response_text") or "").strip()
    if text:
        return text
    upper = code.upper()
    friendly = ERROR_MESSAGES.get(upper)
    if not friendly:
        # Match code families like UNRESTRICTING_ERROR_1 / _2 / ...
        for prefix, message in ERROR_MESSAGES.items():
            if upper.startswith(prefix):
                friendly = message
                break
    if friendly:
        return f"{friendly} (code: {code})" if code else friendly
    return code or None


async def megadebrid_api(action, data=None, needs_token=True, _retried=False):
    """Call a mega-debrid API action, transparently refreshing the token once"""
    global _token
    params = {"action": action}
    if needs_token:
        if not _token:
            await megadebrid_connect()
        params["token"] = _token
    if data is None:
        request = session.get(MEGADEBRID_API_URL, params=params)
    else:
        request = session.post(MEGADEBRID_API_URL, params=params, data=data)
    async with request as resp:
        result = await resp.json(content_type=None)
    if result.get("response_code") != "ok":
        if needs_token and not _retried and _is_token_error(result):
            await megadebrid_connect()
            return await megadebrid_api(action, data, needs_token, _retried=True)
        # Errors put the code in response_code, often with empty response_text
        # e.g. {"response_code": "UNRESTRICTING_ERROR_1", "response_text": ""}
        error = _friendly_error(result)
        raise MegaDebridError(error or f"Unknown error ({action})")
    return result


def filename_from_url(url):
    name = unquote(os.path.basename(urlparse(url).path)).strip()
    return name or None


def parse_flags(command):
    if "zip" in command:
        return (SendAsZipFlag,)
    if "file" in command:
        return (ForceDocumentFlag,)
    return ()


def is_configured():
    return bool(_token or (MEGADEBRID_LOGIN and MEGADEBRID_PASSWORD))


@Client.on_message(filters.command(["md", "zipmd", "filemd"]) & filters.chat(ALL_CHATS))
async def megadebrid_cmd(client, message):
    text = (message.text or message.caption).split()
    command = text.pop(0).lower()
    flags = parse_flags(command)

    link = password = None
    reply = message.reply_to_message
    if text:
        link = text[0].strip()
        if len(text) > 1:
            password = text[1]
    elif not getattr(reply, "empty", True):
        link = (reply.text or reply.caption or "").split()
        password = link[1] if len(link) > 1 else None
        link = link[0].strip() if link else None
    if link and not link.startswith("http"):
        link = "https://" + link

    if not link:
        await message.reply_text(
            """Usage:
- /md <i>&lt;hoster URL&gt; [password]</i>
- /md <i>(as reply to a hoster URL)</i>

- /zipmd <i>&lt;hoster URL&gt; [password]</i> - Uploads as a zip
- /filemd <i>&lt;hoster URL&gt; [password]</i> - Sends videos as files"""
        )
        return

    if not is_configured():
        await message.reply_text(NOT_CONFIGURED_TEXT)
        return

    status_msg = await message.reply_text("🔓 Unrestricting link via Mega-Debrid...")
    try:
        payload = {"link": link}
        if password:
            payload["password"] = password
        data = await megadebrid_api("getLink", data=payload)
        debrid_link = data.get("debridLink", "").strip('"')
        if not debrid_link or not debrid_link.startswith("http"):
            await status_msg.edit_text(
                "❌ Mega-Debrid did not return a valid direct link."
            )
            return

        filename = filename_from_url(debrid_link) or filename_from_url(link)
        await status_msg.edit_text(
            f"✅ Link unrestricted\n📄 <code>{html.escape(filename or 'Unknown')}</code>"
            "\n\n⬇️ Starting download..."
        )
        await initiate_directdl(client, message, debrid_link, filename, flags)
    except MegaDebridError as ex:
        await status_msg.edit_text(f"❌ Mega-Debrid error: {html.escape(str(ex))}")
    except asyncio.TimeoutError:
        await status_msg.edit_text("❌ Mega-Debrid API request timed out.")
    except aiohttp.ClientError as ex:
        await status_msg.edit_text(f"❌ Network error: {html.escape(str(ex))}")


@Client.on_message(
    filters.command(["mdtorrent", "zipmdtorrent", "filemdtorrent"])
    & filters.chat(ALL_CHATS)
)
async def megadebrid_torrent_cmd(client, message):
    text = (message.text or message.caption).split(None, 1)
    command = text.pop(0).lower()
    flags = parse_flags(command)

    magnet = None
    torrent_file = None
    reply = message.reply_to_message
    if text:
        magnet = text[0].strip()
    elif not getattr(reply, "empty", True):
        if reply.document and reply.document.file_name.endswith(".torrent"):
            torrent_file = reply
        else:
            magnet = (reply.text or reply.caption or "").strip()

    if not magnet and not torrent_file:
        await message.reply_text(
            """Usage:
- /mdtorrent <i>&lt;magnet link&gt;</i>
- /mdtorrent <i>(as reply to a magnet link or .torrent file)</i>

- /zipmdtorrent - Uploads as a zip
- /filemdtorrent - Sends videos as files

Caches the torrent on Mega-Debrid, then downloads the direct link."""
        )
        return

    if not is_configured():
        await message.reply_text(NOT_CONFIGURED_TEXT)
        return

    status_msg = await message.reply_text("📤 Sending torrent to Mega-Debrid...")
    try:
        if torrent_file:
            file = await client.download_media(torrent_file, in_memory=True)
            file.seek(0)
            form = aiohttp.FormData()
            form.add_field(
                "file",
                file.read(),
                filename=torrent_file.document.file_name,
                content_type="application/x-bittorrent",
            )
            data = await megadebrid_api("uploadTorrent", data=form)
        else:
            data = await megadebrid_api("uploadTorrent", data={"magnet": magnet})

        new_torrent = data.get("newTorrent") or {}
        torrent_hash = new_torrent.get("hash")
        torrent_name = new_torrent.get("name", "Unknown")
        if not torrent_hash:
            await status_msg.edit_text(
                "❌ Mega-Debrid did not return a torrent hash."
            )
            return

        # Poll until mega-debrid finishes caching the torrent
        deadline = time.time() + MEGADEBRID_TORRENT_TIMEOUT
        ub_link = None
        last_status_text = None
        while time.time() < deadline:
            data = await megadebrid_api("getTorrent", data={"hash": torrent_hash})
            status = data.get("status") or {}
            ub_link = (status.get("ub_link") or "").strip()
            state = str(status.get("status", "unknown"))
            if ub_link.startswith("http"):
                break
            if state.lower() in ("error", "failed"):
                await status_msg.edit_text(
                    f"❌ Mega-Debrid failed to process the torrent "
                    f"(status: {html.escape(state)})"
                )
                return
            ub_link = None

            size = status.get("size")
            try:
                size = format_bytes(int(size))
            except (TypeError, ValueError):
                size = size or "Unknown"
            status_text = (
                f"⏳ <b>Caching torrent on Mega-Debrid...</b>\n"
                f"📄 <code>{html.escape(str(status.get('name') or torrent_name))}</code>\n"
                f"📏 {html.escape(str(size))} | "
                f"📊 {html.escape(str(status.get('progress', '?')))}% | "
                f"🚀 {html.escape(str(status.get('speed', '?')))} | "
                f"👥 {html.escape(str(status.get('peers', '?')))} peers\n"
                f"ℹ️ Status: {html.escape(state)}"
            )
            if status_text != last_status_text:
                await status_msg.edit_text(status_text)
                last_status_text = status_text
            await asyncio.sleep(TORRENT_POLL_INTERVAL)

        if not ub_link:
            await status_msg.edit_text(
                "❌ Timed out waiting for Mega-Debrid to cache the torrent."
            )
            return

        # ub_link is a hoster link - unrestrict it before downloading
        download_link = ub_link
        try:
            data = await megadebrid_api("getLink", data={"link": ub_link})
            debrid_link = data.get("debridLink", "").strip('"')
            if debrid_link.startswith("http"):
                download_link = debrid_link
        except MegaDebridError:
            pass  # fall back to downloading ub_link directly

        filename = filename_from_url(download_link) or torrent_name
        await status_msg.edit_text(
            f"✅ Torrent cached\n📄 <code>{html.escape(filename or 'Unknown')}</code>"
            "\n\n⬇️ Starting download..."
        )
        await initiate_directdl(client, message, download_link, filename, flags)
    except MegaDebridError as ex:
        await status_msg.edit_text(f"❌ Mega-Debrid error: {html.escape(str(ex))}")
    except asyncio.TimeoutError:
        await status_msg.edit_text("❌ Mega-Debrid API request timed out.")
    except aiohttp.ClientError as ex:
        await status_msg.edit_text(f"❌ Network error: {html.escape(str(ex))}")


@Client.on_message(filters.command("mdhosters") & filters.chat(ALL_CHATS))
async def megadebrid_hosters_cmd(client, message):
    status_msg = await message.reply_text("🔍 Fetching supported hosters...")
    try:
        data = await megadebrid_api("getHostersList", needs_token=False)
        hosters = data.get("hosters", [])
        up = sorted(
            h["name"]
            for h in hosters
            if str(h.get("status", "")).lower() in ("up", "1", "ok", "true")
        )
        down = len(hosters) - len(up)
        text = (
            f"🌐 <b>Mega-Debrid hosters ({len(up)} up, {down} down):</b>\n\n"
            + html.escape(", ".join(up))
        )
        if len(text) > 4000:
            text = text[:3990] + "\n..."
        await status_msg.edit_text(text)
    except MegaDebridError as ex:
        await status_msg.edit_text(f"❌ Mega-Debrid error: {html.escape(str(ex))}")
    except aiohttp.ClientError as ex:
        await status_msg.edit_text(f"❌ Network error: {html.escape(str(ex))}")


help_dict["megadebrid"] = (
    "Mega-Debrid",
    """/md <i>&lt;hoster URL&gt; [password]</i>
/md <i>(as reply to a hoster URL)</i>
/zipmd - Uploads as a zip
/filemd - Sends videos as files

/mdtorrent <i>&lt;magnet link&gt;</i>
/mdtorrent <i>(as reply to a magnet link or .torrent file)</i>
/zipmdtorrent - Uploads as a zip
/filemdtorrent - Sends videos as files

/mdhosters - List supported hosters

Unrestricts premium hoster links (and caches torrents) via mega-debrid.eu, then downloads and uploads to Telegram.
Requires <code>MEGADEBRID_TOKEN</code> or <code>MEGADEBRID_LOGIN</code>/<code>MEGADEBRID_PASSWORD</code>.""",
)
