import asyncio
import logging
import os
from io import BytesIO, StringIO

import aiohttp
from pyrogram import Client
from pyrogram.enums import ParseMode

API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
TESTMODE = os.environ.get("TESTMODE")
TESTMODE = TESTMODE and TESTMODE != "0"

EVERYONE_CHATS = os.environ.get("EVERYONE_CHATS")
EVERYONE_CHATS = (
    list(map(int, EVERYONE_CHATS.split(" "))) if EVERYONE_CHATS else [-1001378211961]
)
ADMIN_CHATS = os.environ.get("ADMIN_CHATS")
ADMIN_CHATS = list(map(int, ADMIN_CHATS.split(" "))) if ADMIN_CHATS else [441422215]
ALL_CHATS = EVERYONE_CHATS + ADMIN_CHATS
# LICHER_* variables are for @animebatchstash and similar, not required
LICHER_CHAT = os.environ.get("LICHER_CHAT", "")
try:
    LICHER_CHAT = int(LICHER_CHAT)
except ValueError:
    pass
LICHER_STICKER = os.environ.get("LICHER_STICKER")
LICHER_FOOTER = os.environ.get("LICHER_FOOTER", "").encode().decode("unicode_escape")
LICHER_PARSE_EPISODE = os.environ.get("LICHER_PARSE_EPISODE")
LICHER_PARSE_EPISODE = LICHER_PARSE_EPISODE and LICHER_PARSE_EPISODE != "0"

PROGRESS_UPDATE_DELAY = int(os.environ.get("PROGRESS_UPDATE_DELAY", 5))
MAGNET_TIMEOUT = int(os.environ.get("LEECH_TIMEOUT", 60))
LEECH_TIMEOUT = int(os.environ.get("LEECH_TIMEOUT", 300))
ARIA2_SECRET = os.environ.get("ARIA2_SECRET", "")
IGNORE_PADDING_FILE = os.environ.get("IGNORE_PADDING_FILE", "1")
IGNORE_PADDING_FILE = IGNORE_PADDING_FILE and IGNORE_PADDING_FILE != "0"

logging.basicConfig(level=logging.INFO)

# Session file path - use /app/session for persistence
SESSION_PATH = "/app/session" if os.path.exists("/app/session") else os.getcwd()


def get_time_sync_offset():
    """Get time sync offset file path"""
    return os.path.join(SESSION_PATH, ".time_sync")


app = Client(
    "lazyleech",
    API_ID,
    API_HASH,
    workdir=SESSION_PATH,
    plugins={"root": os.path.join(__package__, "plugins")},
    bot_token=BOT_TOKEN,
    test_mode=TESTMODE,
    parse_mode=ParseMode.HTML,
    sleep_threshold=30,
    max_concurrent_transmissions=int(os.environ.get("MAX_CONCURRENT_UPLOADS", 3)),
)


# Lazy session - will be initialized when first accessed
class LazySession:
    _session = None

    def __getattr__(self, name):
        if self._session is None:
            # Create session in a thread-safe way
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            self._session = aiohttp.ClientSession(loop=loop)
        return getattr(self._session, name)


session = LazySession()
help_dict = dict()
preserved_logs = []


class SendAsZipFlag:
    pass


class ForceDocumentFlag:
    pass


class SelectFilesFlag:
    pass


def memory_file(name=None, contents=None, *, bytes=True):
    if isinstance(contents, str) and bytes:
        contents = contents.encode()
    file = BytesIO() if bytes else StringIO()
    if name:
        file.name = name
    if contents:
        file.write(contents)
        file.seek(0)
    return file
