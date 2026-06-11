# lazyleech - Terabox download plugin
# Downloads files from Terabox via xAPIverse API and uploads to Telegram

import asyncio
import html
import json
import os

from pyrogram import Client, filters

from .. import ALL_CHATS, ForceDocumentFlag, SendAsZipFlag, help_dict, session
from .leech import initiate_directdl

XAPIVERSE_KEY = os.environ.get("XAPIVERSE_KEY", "")
TERABOX_API_URL = "https://xapiverse.com/api/terabox"


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
            """Usage:
- /tera <i>&lt;Terabox URL&gt;</i>
- /tera <i>(as reply to a Terabox URL)</i>

- /ziptera <i>&lt;Terabox URL&gt;</i>
- /ziptera <i>(as reply to a Terabox URL)</i>

- /filetera <i>&lt;Terabox URL&gt;</i> - Sends videos as files
- /filetera <i>(as reply to a Terabox URL)</i> - Sends videos as files"""
        )
        return

    if not XAPIVERSE_KEY:
        await message.reply_text(
            "❌ Terabox API key is not configured. Set <code>XAPIVERSE_KEY</code> in .env"
        )
        return

    status_msg = await message.reply_text("🔍 Resolving Terabox link...")

    try:
        # Call xAPIverse Terabox API
        headers = {
            "Content-Type": "application/json",
            "xAPIverse-Key": XAPIVERSE_KEY,
        }
        payload = json.dumps({"url": link})

        async with session.post(
            TERABOX_API_URL, data=payload, headers=headers
        ) as resp:
            if resp.status != 200:
                await status_msg.edit_text(
                    f"❌ Terabox API returned HTTP {resp.status}"
                )
                return
            data = await resp.json()

        if data.get("status") != "success":
            error_msg = data.get("message", data.get("error", "Unknown error"))
            await status_msg.edit_text(
                f"❌ Terabox API error: {html.escape(str(error_msg))}"
            )
            return

        file_list = data.get("list", [])
        if not file_list:
            await status_msg.edit_text("❌ No files found in the Terabox link.")
            return

        # Build file info message
        info_text = f"📦 <b>Found {len(file_list)} file(s):</b>\n\n"
        for i, file_info in enumerate(file_list, 1):
            name = file_info.get("name", "Unknown")
            size = file_info.get("size_formatted", "Unknown")
            quality = file_info.get("quality", "")
            duration = file_info.get("duration", "")

            info_text += f"<b>{i}.</b> <code>{html.escape(name)}</code>\n"
            info_text += f"    📏 {html.escape(size)}"
            if quality:
                info_text += f" | 🎬 {html.escape(quality)}"
            if duration:
                info_text += f" | ⏱ {html.escape(duration)}"
            info_text += "\n"

        info_text += "\n⬇️ Starting download..."

        # Truncate if too long for Telegram
        if len(info_text) > 4000:
            info_text = info_text[:3990] + "\n..."

        await status_msg.edit_text(info_text)

        # Download each file via Aria2
        for file_info in file_list:
            download_url = file_info.get("normal_dlink") or file_info.get("zip_dlink")
            filename = file_info.get("name")

            if not download_url:
                await message.reply_text(
                    f"⚠️ No download link available for: "
                    f"<code>{html.escape(file_info.get('name', 'Unknown'))}</code>"
                )
                continue

            await initiate_directdl(
                client, message, download_url, filename, flags
            )

            # Small delay between multiple files to avoid flooding
            if len(file_list) > 1:
                await asyncio.sleep(2)

    except asyncio.TimeoutError:
        await status_msg.edit_text("❌ Terabox API request timed out.")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {html.escape(str(e))}")


help_dict["terabox"] = (
    "Terabox",
    """/tera <i>&lt;Terabox URL&gt;</i>
/tera <i>(as reply to a Terabox URL)</i>

/ziptera <i>&lt;Terabox URL&gt;</i>
/ziptera <i>(as reply to a Terabox URL)</i>

/filetera <i>&lt;Terabox URL&gt;</i> - Sends videos as files
/filetera <i>(as reply to a Terabox URL)</i> - Sends videos as files

Downloads files from Terabox links and uploads to Telegram.""",
)
