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
import logging
import os
import traceback

from pyrogram import idle

from . import ADMIN_CHATS, app, preserved_logs, session
from .utils.bunkr_sessions import bunkr_session_store
from .utils.status import status_worker
from .utils.upload_worker import upload_worker


async def main():
    async def _autorestart_worker():
        while True:
            try:
                await upload_worker()
            except Exception as ex:
                preserved_logs.append(ex)
                logging.exception("upload worker commited suicide")
                tb = traceback.format_exc()
                for i in ADMIN_CHATS:
                    try:
                        await app.send_message(i, "upload worker commited suicide")
                        await app.send_message(i, tb, parse_mode=None)
                    except Exception:
                        logging.exception("failed %s", i)
                        tb = traceback.format_exc()

    max_workers = int(os.environ.get("MAX_CONCURRENT_UPLOADS", 3))
    for _ in range(max_workers):
        asyncio.create_task(_autorestart_worker())

    asyncio.create_task(status_worker(app))
    await app.start()
    await idle()
    await app.stop()
    if os.environ.get("DB_URL"):
        from .plugins.nyaa_auto_download import _close_db

        _close_db()
    bunkr_session_store.close()
    if session._session is not None:
        await session._session.close()


app.loop.run_until_complete(main())
