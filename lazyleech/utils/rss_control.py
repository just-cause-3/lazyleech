"""Persistent pause state for the RSS auto-download scheduler."""

import asyncio
import copy
from datetime import datetime, timezone


RSS_CONTROL_ID = "auto_download"


def utcnow():
    return datetime.now(timezone.utc)


class RSSControlStore:
    """Store RSS scheduler state in MongoDB, with a test-friendly fallback."""

    def __init__(self, collection=None):
        self.collection = collection
        self._memory_state = {
            "_id": RSS_CONTROL_ID,
            "paused": False,
            "updated_at": None,
            "updated_by": None,
        }
        self._memory_lock = asyncio.Lock()

    @property
    def persistent(self):
        return self.collection is not None

    async def get_state(self):
        if self.persistent:
            doc = await self.collection.find_one({"_id": RSS_CONTROL_ID})
            if doc is not None:
                return doc
            return copy.deepcopy(self._memory_state)
        async with self._memory_lock:
            return copy.deepcopy(self._memory_state)

    async def is_paused(self):
        return bool((await self.get_state()).get("paused", False))

    async def set_paused(self, paused, updated_by=None):
        fields = {
            "paused": bool(paused),
            "updated_at": utcnow(),
            "updated_by": int(updated_by) if updated_by is not None else None,
        }
        if self.persistent:
            await self.collection.update_one(
                {"_id": RSS_CONTROL_ID}, {"$set": fields}, upsert=True
            )
            return {"_id": RSS_CONTROL_ID, **fields}
        async with self._memory_lock:
            self._memory_state.update(fields)
            return copy.deepcopy(self._memory_state)
