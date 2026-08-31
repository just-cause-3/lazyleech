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
import base64
import json
import os
import random
import tempfile
import time

from .. import ARIA2_SECRET

HEX_CHARACTERS = "abcdef"
HEXNUMERIC_CHARACTERS = HEX_CHARACTERS + "0123456789"


class Aria2Error(Exception):
    def __init__(self, message):
        self.error_code = message.get("code")
        self.error_message = message.get("message")
        return super().__init__(str(message))


def _raise_or_return(data):
    if "error" in data:
        raise Aria2Error(data["error"])
    return data["result"]


async def aria2_request(session, method, params=None):
    if params is None:
        params = []
    if ARIA2_SECRET:
        params.insert(0, "token:" + ARIA2_SECRET)
    data = {
        "jsonrpc": "2.0",
        "id": str(time.time()),
        "method": method,
        "params": params,
    }
    async with session.post(
        "http://127.0.0.1:6800/jsonrpc", data=json.dumps(data)
    ) as resp:
        return await resp.json(encoding="utf-8")


async def aria2_tell_active(session):
    return _raise_or_return(await aria2_request(session, "aria2.tellActive"))


async def aria2_tell_waiting(session, offset=0, num=1000):
    # Downloads queued behind the -j concurrency limit live here, not in tellActive
    return _raise_or_return(
        await aria2_request(session, "aria2.tellWaiting", [offset, num])
    )


async def aria2_force_pause_all(session):
    return _raise_or_return(await aria2_request(session, "aria2.forcePauseAll"))


async def aria2_pause(session, gid):
    return _raise_or_return(await aria2_request(session, "aria2.pause", [gid]))


async def aria2_tell_status(session, gid):
    return _raise_or_return(await aria2_request(session, "aria2.tellStatus", [gid]))


async def aria2_change_option(session, gid, options):
    return _raise_or_return(
        await aria2_request(session, "aria2.changeOption", [gid, options])
    )


async def aria2_remove(session, gid):
    return _raise_or_return(await aria2_request(session, "aria2.remove", [gid]))


async def aria2_unpause(session, gid):
    return _raise_or_return(await aria2_request(session, "aria2.unpause", [gid]))


async def generate_gid(session, user_id):
    def _generate_gid():
        gid = str(user_id)
        gid += random.choice(HEX_CHARACTERS)
        while len(gid) < 16:
            gid += random.choice(HEXNUMERIC_CHARACTERS)
        return gid

    while True:
        gid = _generate_gid()
        try:
            await aria2_tell_status(session, gid)
        except Aria2Error as ex:
            if not (
                ex.error_code == 1 and ex.error_message == f"GID {gid} is not found"
            ):
                raise
            return gid


def is_gid_owner(user_id, gid):
    prefix = str(user_id)
    if not gid.startswith(prefix):
        return False
    rest = gid[len(prefix):]
    return bool(rest) and rest[0] in HEX_CHARACTERS


async def aria2_add_torrent(session, user_id, link, timeout=0, pause=False):
    if os.path.isfile(link):
        with open(link, "rb") as file:
            torrent = file.read()
    else:
        # Some trackers enforce strict payload parsing checks or Cloudflare. We fetch using
        # standard aiohttp and fallback to addUri if we aren't handling a raw file path.
        # But we must use memory stream directly for .torrent files, otherwise aria returns the raw file instead of following it.
        async with session.get(link) as resp:
            torrent = await resp.read()

    # For local .torrent files only
    torrent = base64.b64encode(torrent).decode()
    dir = os.path.join(os.getcwd(), str(user_id), str(time.time()))
    options = {
        "gid": await generate_gid(session, user_id),
        "dir": dir,
        "seed-time": "0",
        "bt-stop-timeout": str(timeout),
    }
    if pause:
        options["pause"] = "true"

    return _raise_or_return(
        await aria2_request(
            session,
            "aria2.addTorrent",
            [
                torrent,
                [],
                options,
            ],
        )
    )


async def aria2_add_magnet(session, user_id, link, timeout=0, pause=False):
    with tempfile.TemporaryDirectory() as tempdir:
        gid = _raise_or_return(
            await aria2_request(
                session,
                "aria2.addUri",
                [
                    [link],
                    {
                        "dir": tempdir,
                        "bt-save-metadata": "true",
                        "bt-metadata-only": "true",
                        "follow-torrent": "false",
                    },
                ],
            )
        )
        try:
            info = await aria2_tell_status(session, gid)
            while info["status"] == "active":
                await asyncio.sleep(0.5)
                info = await aria2_tell_status(session, gid)
            filename = os.path.join(tempdir, info["infoHash"] + ".torrent")
            return await aria2_add_torrent(
                session, user_id, filename, timeout, pause=pause
            )
        finally:
            try:
                await aria2_remove(session, gid)
            except Aria2Error as ex:
                if not (
                    ex.error_code == 1
                    and ex.error_message == f"Active Download not found for GID#{gid}"
                ):
                    raise


async def aria2_add_directdl(
    session,
    user_id,
    link,
    filename=None,
    timeout=60,
    headers=None,
    max_connections=8,
    download_dir=None,
    resume=False,
):
    dir = download_dir or os.path.join(os.getcwd(), str(user_id), str(time.time()))
    max_conn = str(max(1, min(int(max_connections), 16)))

    options = {
        "gid": await generate_gid(session, user_id),
        "dir": dir,
        "timeout": str(timeout),
        "follow-torrent": "false",
        "max-connection-per-server": max_conn,
        "split": max_conn,
        "min-split-size": "10M",
        "max-tries": "20",
        "retry-wait": "3",
    }
    if resume:
        options["continue"] = "true"
        options["always-resume"] = "true"

    # Always include User-Agent; append any extra headers (e.g. Referer for Bunkr)
    default_ua = (
        "User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) "
        "Gecko/20100101 Firefox/136.0"
    )
    if headers:
        if isinstance(headers, list):
            header_list = [default_ua] + headers
        else:
            header_list = [default_ua, headers]
        options["header"] = header_list
    else:
        options["header"] = default_ua

    if filename:
        options["out"] = filename
    return _raise_or_return(
        await aria2_request(session, "aria2.addUri", [[link], options])
    )
