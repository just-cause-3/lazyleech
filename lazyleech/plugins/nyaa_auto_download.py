### if you wish to disable this just fork repo and delete this plugin/file
### if you want different uploader, just replace rsslink

import os
import requests
import re
import asyncio
from bs4 import BeautifulSoup as bs
from pyrogram import Client, filters
from motor.motor_asyncio import AsyncIOMotorClient
from motor.core import AgnosticClient, AgnosticDatabase, AgnosticCollection
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .. import app, ADMIN_CHATS, ForceDocumentFlag
from ..utils.rss_control import RSSControlStore
from .leech import initiate_torrent

rsslink = list(
    filter(
        lambda x: x,
        map(
            str,
            os.environ.get(
                "NYAA_RSS_LINKS",
                "https://nyaa.si/?page=rss&c=0_0&f=0&u=AkihitoSubsWeeklies",
            ).split(" "),
        ),
    )
)

if os.environ.get("DB_URL"):
    DB_URL = os.environ.get("DB_URL")
    _MGCLIENT: AgnosticClient = AsyncIOMotorClient(DB_URL)
    _DATABASE: AgnosticDatabase = _MGCLIENT["ASWFeed"]

    def get_collection(name: str) -> AgnosticCollection:
        """Create or Get Collection from your database"""
        return _DATABASE[name]

    def _close_db() -> None:
        _MGCLIENT.close()

    A = get_collection("ASW_TITLE")
    FEEDS_DB = get_collection("RSS_FEEDS")
    RSS_CONTROL = RSSControlStore(get_collection("RSS_CONTROL"))

    @Client.on_message(filters.command("pauserss") & filters.chat(ADMIN_CHATS))
    async def pause_rss(client, message):
        await RSS_CONTROL.set_paused(True, updated_by=message.from_user.id)
        try:
            scheduler.pause_job("rss_parser")
        except Exception:
            pass
        await message.reply_text(
            "RSS auto-download is persistently paused. Future feed scans are "
            "disabled; downloads already started will continue."
        )

    @Client.on_message(filters.command("resumerss") & filters.chat(ADMIN_CHATS))
    async def resume_rss(client, message):
        await RSS_CONTROL.set_paused(False, updated_by=message.from_user.id)
        try:
            scheduler.resume_job("rss_parser")
        except Exception:
            pass
        await message.reply_text(
            "RSS auto-download resumed. Feeds will be checked at the next "
            "scheduled interval."
        )

    @Client.on_message(filters.command("rssstatus") & filters.chat(ADMIN_CHATS))
    async def rss_status(client, message):
        state = await RSS_CONTROL.get_state()
        db_feed_count = await FEEDS_DB.count_documents({})
        status = "Paused" if state.get("paused") else "Running"
        await message.reply_text(
            f"<b>RSS auto-download:</b> {status}\n"
            f"<b>Environment feeds:</b> {len(rsslink)}\n"
            f"<b>Database feeds:</b> {db_feed_count}\n"
            f"<b>Check interval:</b> "
            f"{int(os.environ.get('RSS_RECHECK_INTERVAL', 5))} minutes"
        )

    @Client.on_message(filters.command("listrss") & filters.chat(ADMIN_CHATS))
    async def list_rss(client, message):
        paused = await RSS_CONTROL.is_paused()
        db_feeds_cursor = FEEDS_DB.find({})
        db_feeds = [doc["url"] async for doc in db_feeds_cursor]

        text = (
            f"<b>RSS auto-download:</b> "
            f"{'Paused' if paused else 'Running'}\n\n"
            "<b>Base RSS Feeds (Env):</b>\n"
        )
        for idx, url in enumerate(rsslink, 1):
            text += f"{idx}. {url}\n"

        text += "\n<b>Database RSS Feeds:</b>\n"
        if db_feeds:
            for idx, url in enumerate(db_feeds, 1):
                text += f"{idx}. {url}\n"
        else:
            text += "None\n"

        await message.reply_text(text, disable_web_page_preview=True)

    @Client.on_message(filters.command("addrss") & filters.chat(ADMIN_CHATS))
    async def add_rss(client, message):
        if len(message.command) < 2:
            await message.reply_text("Usage: /addrss &lt;rss_url&gt;")
            return
        url = message.command[1]

        if await FEEDS_DB.find_one({"url": url}):
            await message.reply_text("This RSS feed is already in the database.")
            return
        if url in rsslink:
            await message.reply_text(
                "This RSS feed is already in the base environment variables."
            )
            return

        await FEEDS_DB.insert_one({"url": url})
        await message.reply_text(
            f"Added RSS feed:\n{url}", disable_web_page_preview=True
        )

    @Client.on_message(
        filters.command(["delrss", "removerss"]) & filters.chat(ADMIN_CHATS)
    )
    async def del_rss(client, message):
        if len(message.command) < 2:
            await message.reply_text("Usage: /delrss &lt;rss_url&gt;")
            return
        url = message.command[1]
        result = await FEEDS_DB.delete_one({"url": url})

        if result.deleted_count > 0:
            await message.reply_text(
                f"Removed RSS feed:\n{url}", disable_web_page_preview=True
            )
            # Remove its tracking from the A collection so it doesn't leave garbage
            await A.delete_many({"site": url})
        else:
            await message.reply_text(
                "RSS feed not found in the database. (Cannot remove Base env feeds from Telegram)"
            )

    async def fetch_url(url):
        return await asyncio.get_event_loop().run_in_executor(None, requests.get, url)

    async def rss_parser():
        if await RSS_CONTROL.is_paused():
            return
        cr = []
        db_feeds_cursor = FEEDS_DB.find({})
        db_feeds = [doc["url"] async for doc in db_feeds_cursor]

        all_links = list(set(rsslink + db_feeds))

        for i in all_links:
            try:
                resp = await fetch_url(i)
                da = bs(resp.text, features="html.parser")

                item = da.find("item")
                if not item:
                    continue
                latest_title = str(item.find("title"))

                if (await A.find_one({"site": i})) is None:
                    await A.insert_one({"_id": latest_title, "site": i})
                    continue

                count_a = 0
                for ii in da.findAll("item"):
                    if (await A.find_one({"site": i}))["_id"] == str(ii.find("title")):
                        break
                    cr.append(
                        [
                            str(ii.find("title")),
                            (
                                re.sub(r"<.*?>(.*)<.*?>", r"\1", str(ii.find("guid")))
                            ).replace("view", "download")
                            + ".torrent",
                        ]
                    )
                    count_a += 1

                if count_a != 0:
                    await A.find_one_and_delete({"site": i})
                    await A.insert_one({"_id": latest_title, "site": i})
            except Exception:
                pass

        for i in cr:
            for ii in ADMIN_CHATS:
                try:
                    msg = await app.send_message(
                        ii, f"New anime uploaded\n\n{i[0]}\n{i[1]}"
                    )
                    flags = (ForceDocumentFlag,)
                    await initiate_torrent(app, msg, i[1], flags)
                except:
                    pass

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        rss_parser,
        "interval",
        id="rss_parser",
        minutes=int(os.environ.get("RSS_RECHECK_INTERVAL", 5)),
        max_instances=5,
    )
    scheduler.start()
