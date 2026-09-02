"""Persistent runtime configuration for the TeraBox downloader."""

from __future__ import annotations

import asyncio
import copy
import os
from datetime import datetime, timezone


TERABOX_CONFIG_ID = "global"


def utcnow():
    return datetime.now(timezone.utc)


class TeraboxConfigStore:
    """Store a runtime cookie override in MongoDB with an in-memory fallback."""

    def __init__(self, collection=None, db_url=None, database_name=None, env_cookie=None):
        self.client = None
        if collection is None:
            if db_url is None:
                db_url = os.environ.get("DB_URL", "")
            if db_url:
                from motor.motor_asyncio import AsyncIOMotorClient

                self.client = AsyncIOMotorClient(db_url)
                database = self.client[
                    database_name
                    or os.environ.get("LAZYLEECH_DB_NAME", "ASWFeed")
                ]
                collection = database["TERABOX_CONFIG"]
        self.collection = collection
        self.env_cookie = (
            os.environ.get("TERABOX_COOKIE", "")
            if env_cookie is None
            else env_cookie
        )
        self._memory_override = None
        self._memory_lock = asyncio.Lock()

    @property
    def persistent(self):
        return self.collection is not None

    async def get_state(self):
        if self.persistent:
            document = await self.collection.find_one({"_id": TERABOX_CONFIG_ID})
            if document is not None:
                return document
            return {"_id": TERABOX_CONFIG_ID, "cookie": None}
        async with self._memory_lock:
            return {
                "_id": TERABOX_CONFIG_ID,
                "cookie": self._memory_override,
            }

    async def get_cookie(self):
        state = await self.get_state()
        override = state.get("cookie")
        return override if isinstance(override, str) and override else self.env_cookie

    async def set_cookie(self, cookie, updated_by=None):
        cookie = str(cookie or "").strip().removeprefix("ndus=")
        fields = {
            "cookie": cookie,
            "updated_at": utcnow(),
            "updated_by": int(updated_by) if updated_by is not None else None,
        }
        if self.persistent:
            await self.collection.update_one(
                {"_id": TERABOX_CONFIG_ID}, {"$set": fields}, upsert=True
            )
            return {"_id": TERABOX_CONFIG_ID, **fields}
        async with self._memory_lock:
            self._memory_override = cookie
            return copy.deepcopy({"_id": TERABOX_CONFIG_ID, **fields})

    async def clear_cookie(self):
        if self.persistent:
            await self.collection.delete_one({"_id": TERABOX_CONFIG_ID})
            return
        async with self._memory_lock:
            self._memory_override = None
