"""Persistent state for resumable Bunkr download sessions."""

import asyncio
import copy
import os
from datetime import datetime, timezone
from uuid import uuid4

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, ReturnDocument


SESSION_RUNNING = "running"
SESSION_PAUSED = "paused"
SESSION_CANCELLED = "cancelled"
SESSION_COMPLETED = "completed"
SESSION_FAILED = "failed"

FILE_PENDING = "pending"
FILE_RESOLVING = "resolving"
FILE_DOWNLOADING = "downloading"
FILE_DOWNLOADED = "downloaded"
FILE_FAILED = "failed"
FILE_CANCELLED = "cancelled"

UNFINISHED_FILE_STATES = (
    FILE_PENDING,
    FILE_RESOLVING,
    FILE_DOWNLOADING,
    FILE_FAILED,
    FILE_CANCELLED,
)


def utcnow():
    return datetime.now(timezone.utc)


def new_session_id():
    return uuid4().hex[:12]


class BunkrSessionStore:
    """Store Bunkr sessions in MongoDB, with an in-memory fallback.

    The fallback preserves the feature for installations without ``DB_URL``,
    but those sessions intentionally do not survive a process restart.
    """

    def __init__(self, db_url=None, database_name=None):
        if db_url is None:
            db_url = os.environ.get("DB_URL", "")
        self.db_url = db_url
        self.database_name = database_name or os.environ.get(
            "LAZYLEECH_DB_NAME", "ASWFeed"
        )
        self.client = AsyncIOMotorClient(db_url) if db_url else None
        self.database = (
            self.client[self.database_name] if self.client is not None else None
        )
        self.sessions = (
            self.database["BUNKR_SESSIONS"] if self.database is not None else None
        )
        self.files = (
            self.database["BUNKR_SESSION_FILES"]
            if self.database is not None
            else None
        )
        self._memory_sessions = {}
        self._memory_files = {}
        self._memory_lock = asyncio.Lock()
        self._indexes_ready = False
        self._index_lock = asyncio.Lock()

    @property
    def persistent(self):
        return self.client is not None

    async def _ensure_indexes(self):
        if not self.persistent or self._indexes_ready:
            return
        async with self._index_lock:
            if self._indexes_ready:
                return
            await self.sessions.create_index(
                [("owner_id", ASCENDING), ("updated_at", ASCENDING)]
            )
            await self.files.create_index(
                [("session_id", ASCENDING), ("position", ASCENDING)], unique=True
            )
            await self.files.create_index("gid", sparse=True)
            self._indexes_ready = True

    async def create_session(
        self,
        *,
        owner_id,
        chat_id,
        source_message_id,
        source_url,
        title,
        mode,
        custom_filename,
        files,
    ):
        if not files:
            raise ValueError("A Bunkr session must contain at least one file")
        session_id = new_session_id()
        now = utcnow()
        session_doc = {
            "_id": session_id,
            "owner_id": int(owner_id),
            "chat_id": int(chat_id),
            "source_message_id": int(source_message_id),
            "source_url": source_url,
            "title": title,
            "mode": mode,
            "custom_filename": custom_filename,
            "state": SESSION_RUNNING,
            "total_files": len(files),
            "created_at": now,
            "updated_at": now,
        }
        file_docs = [
            {
                "_id": f"{session_id}:{position}",
                "session_id": session_id,
                "position": position,
                "page_url": page_url,
                "filename": filename or page_url,
                "status": FILE_PENDING,
                "gid": None,
                "error": None,
                "attempts": 0,
                "updated_at": now,
            }
            for position, (page_url, filename) in enumerate(files, 1)
        ]
        if self.persistent:
            await self._ensure_indexes()
            await self.sessions.insert_one(session_doc)
            try:
                await self.files.insert_many(file_docs, ordered=True)
            except Exception:
                await self.sessions.delete_one({"_id": session_id})
                raise
        else:
            async with self._memory_lock:
                self._memory_sessions[session_id] = session_doc
                for file_doc in file_docs:
                    self._memory_files[file_doc["_id"]] = file_doc
        return copy.deepcopy(session_doc)

    async def get_session(self, session_id, owner_id=None):
        query = {"_id": session_id}
        if owner_id is not None:
            query["owner_id"] = int(owner_id)
        if self.persistent:
            await self._ensure_indexes()
            return await self.sessions.find_one(query)
        async with self._memory_lock:
            doc = self._memory_sessions.get(session_id)
            if doc is None or (
                owner_id is not None and doc["owner_id"] != int(owner_id)
            ):
                return None
            return copy.deepcopy(doc)

    async def list_sessions(self, owner_id=None, states=None, limit=20):
        query = {}
        if owner_id is not None:
            query["owner_id"] = int(owner_id)
        if states:
            query["state"] = {"$in": list(states)}
        if self.persistent:
            await self._ensure_indexes()
            cursor = self.sessions.find(query).sort("updated_at", -1)
            if limit:
                cursor = cursor.limit(limit)
            return [doc async for doc in cursor]
        async with self._memory_lock:
            docs = list(self._memory_sessions.values())
            if owner_id is not None:
                docs = [doc for doc in docs if doc["owner_id"] == int(owner_id)]
            if states:
                docs = [doc for doc in docs if doc["state"] in states]
            docs.sort(key=lambda doc: doc["updated_at"], reverse=True)
            return copy.deepcopy(docs[:limit] if limit else docs)

    async def list_files(self, session_id):
        if self.persistent:
            await self._ensure_indexes()
            cursor = self.files.find({"session_id": session_id}).sort("position", 1)
            return [doc async for doc in cursor]
        async with self._memory_lock:
            docs = [
                doc
                for doc in self._memory_files.values()
                if doc["session_id"] == session_id
            ]
            docs.sort(key=lambda doc: doc["position"])
            return copy.deepcopy(docs)

    async def get_file(self, file_id):
        if self.persistent:
            await self._ensure_indexes()
            return await self.files.find_one({"_id": file_id})
        async with self._memory_lock:
            doc = self._memory_files.get(file_id)
            return copy.deepcopy(doc) if doc is not None else None

    async def counts(self, session_id):
        result = {
            FILE_PENDING: 0,
            FILE_RESOLVING: 0,
            FILE_DOWNLOADING: 0,
            FILE_DOWNLOADED: 0,
            FILE_FAILED: 0,
            FILE_CANCELLED: 0,
        }
        if self.persistent:
            await self._ensure_indexes()
            pipeline = [
                {"$match": {"session_id": session_id}},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            ]
            async for item in self.files.aggregate(pipeline):
                result[item["_id"]] = item["count"]
            return result
        for file_doc in await self.list_files(session_id):
            result[file_doc["status"]] = result.get(file_doc["status"], 0) + 1
        return result

    async def set_state(self, session_id, state, **fields):
        fields.update({"state": state, "updated_at": utcnow()})
        if self.persistent:
            await self._ensure_indexes()
            return await self.sessions.find_one_and_update(
                {"_id": session_id},
                {"$set": fields},
                return_document=ReturnDocument.AFTER,
            )
        async with self._memory_lock:
            doc = self._memory_sessions.get(session_id)
            if doc is None:
                return None
            doc.update(fields)
            return copy.deepcopy(doc)

    async def claim_next_file(self, session_id):
        now = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            return await self.files.find_one_and_update(
                {"session_id": session_id, "status": FILE_PENDING},
                {
                    "$set": {
                        "status": FILE_RESOLVING,
                        "gid": None,
                        "error": None,
                        "updated_at": now,
                    },
                    "$inc": {"attempts": 1},
                },
                sort=[("position", ASCENDING)],
                return_document=ReturnDocument.AFTER,
            )
        async with self._memory_lock:
            candidates = sorted(
                (
                    doc
                    for doc in self._memory_files.values()
                    if doc["session_id"] == session_id
                    and doc["status"] == FILE_PENDING
                ),
                key=lambda doc: doc["position"],
            )
            if not candidates:
                return None
            doc = candidates[0]
            doc.update(
                {
                    "status": FILE_RESOLVING,
                    "gid": None,
                    "error": None,
                    "attempts": doc["attempts"] + 1,
                    "updated_at": now,
                }
            )
            return copy.deepcopy(doc)

    async def update_file(self, file_id, status=None, **fields):
        if status is not None:
            fields["status"] = status
        fields["updated_at"] = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            return await self.files.find_one_and_update(
                {"_id": file_id},
                {"$set": fields},
                return_document=ReturnDocument.AFTER,
            )
        async with self._memory_lock:
            doc = self._memory_files.get(file_id)
            if doc is None:
                return None
            doc.update(fields)
            return copy.deepcopy(doc)

    async def find_file_by_gid(self, gid):
        if not gid:
            return None
        if self.persistent:
            await self._ensure_indexes()
            return await self.files.find_one({"gid": gid})
        async with self._memory_lock:
            for doc in self._memory_files.values():
                if doc.get("gid") == gid:
                    return copy.deepcopy(doc)
        return None

    async def active_files(self, session_id):
        active_states = (FILE_RESOLVING, FILE_DOWNLOADING)
        if self.persistent:
            await self._ensure_indexes()
            cursor = self.files.find(
                {"session_id": session_id, "status": {"$in": active_states}}
            )
            return [doc async for doc in cursor]
        files = await self.list_files(session_id)
        return [doc for doc in files if doc["status"] in active_states]

    async def prepare_continue(self, session_id, chat_id, source_message_id):
        now = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            await self.files.update_many(
                {
                    "session_id": session_id,
                    "status": {"$in": list(UNFINISHED_FILE_STATES)},
                },
                {
                    "$set": {
                        "status": FILE_PENDING,
                        "gid": None,
                        "error": None,
                        "updated_at": now,
                    }
                },
            )
        else:
            async with self._memory_lock:
                for doc in self._memory_files.values():
                    if (
                        doc["session_id"] == session_id
                        and doc["status"] in UNFINISHED_FILE_STATES
                    ):
                        doc.update(
                            {
                                "status": FILE_PENDING,
                                "gid": None,
                                "error": None,
                                "updated_at": now,
                            }
                        )
        return await self.set_state(
            session_id,
            SESSION_RUNNING,
            chat_id=int(chat_id),
            source_message_id=int(source_message_id),
        )

    async def cancel_session(self, session_id):
        now = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            await self.files.update_many(
                {
                    "session_id": session_id,
                    "status": {"$in": list(UNFINISHED_FILE_STATES)},
                },
                {
                    "$set": {
                        "status": FILE_CANCELLED,
                        "updated_at": now,
                    }
                },
            )
        else:
            async with self._memory_lock:
                for doc in self._memory_files.values():
                    if (
                        doc["session_id"] == session_id
                        and doc["status"] in UNFINISHED_FILE_STATES
                    ):
                        doc.update({"status": FILE_CANCELLED, "updated_at": now})
        return await self.set_state(session_id, SESSION_CANCELLED)

    async def delete_session(self, session_id):
        if self.persistent:
            await self._ensure_indexes()
            session_result = await self.sessions.delete_one({"_id": session_id})
            await self.files.delete_many({"session_id": session_id})
            return bool(session_result.deleted_count)
        async with self._memory_lock:
            existed = self._memory_sessions.pop(session_id, None) is not None
            for file_id in [
                file_id
                for file_id, doc in self._memory_files.items()
                if doc["session_id"] == session_id
            ]:
                self._memory_files.pop(file_id, None)
            return existed

    def close(self):
        if self.client is not None:
            self.client.close()


bunkr_session_store = BunkrSessionStore()
