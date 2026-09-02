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

    def __init__(
        self,
        db_url=None,
        database_name=None,
        collection_prefix="BUNKR",
    ):
        if db_url is None:
            db_url = os.environ.get("DB_URL", "")
        self.db_url = db_url
        self.database_name = database_name or os.environ.get(
            "LAZYLEECH_DB_NAME", "ASWFeed"
        )
        self.collection_prefix = str(collection_prefix or "BUNKR").upper()
        self.client = AsyncIOMotorClient(db_url) if db_url else None
        self.database = (
            self.client[self.database_name] if self.client is not None else None
        )
        self.sessions = (
            self.database[f"{self.collection_prefix}_SESSIONS"]
            if self.database is not None
            else None
        )
        self.files = (
            self.database[f"{self.collection_prefix}_SESSION_FILES"]
            if self.database is not None
            else None
        )
        self.cdn_health = (
            self.database[f"{self.collection_prefix}_CDN_HEALTH"]
            if self.database is not None
            else None
        )
        self._memory_sessions = {}
        self._memory_files = {}
        self._memory_cdn_health = {}
        self._memory_lock = asyncio.Lock()
        self._cdn_health_lock = asyncio.Lock()
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
            await self.files.create_index(
                [
                    ("session_id", ASCENDING),
                    ("status", ASCENDING),
                    ("cdn_host", ASCENDING),
                ]
            )
            await self.files.create_index("gid", sparse=True)
            await self.cdn_health.create_index("cooldown_until")
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
        source_urls=None,
        initial_state=SESSION_RUNNING,
        session_fields=None,
    ):
        if not files:
            raise ValueError("A Bunkr session must contain at least one file")
        if initial_state not in (SESSION_RUNNING, SESSION_PAUSED):
            raise ValueError("A new Bunkr session must be running or paused")
        session_id = new_session_id()
        now = utcnow()
        source_urls = list(source_urls or [source_url])
        session_doc = {
            "_id": session_id,
            "owner_id": int(owner_id),
            "chat_id": int(chat_id),
            "source_message_id": int(source_message_id),
            "source_url": source_url,
            "source_urls": source_urls,
            "title": title,
            "mode": mode,
            "custom_filename": custom_filename,
            "state": initial_state,
            "total_files": len(files),
            "cdn_cooldowns": [],
            "created_at": now,
            "updated_at": now,
        }
        reserved_session_fields = set(session_doc)
        for key, value in (session_fields or {}).items():
            if key not in reserved_session_fields:
                session_doc[key] = copy.deepcopy(value)

        file_docs = []
        for position, file_info in enumerate(files, 1):
            if isinstance(file_info, dict):
                page_url = file_info.get("page_url") or source_url
                filename = file_info.get("filename") or page_url
                extra_fields = {
                    key: copy.deepcopy(value)
                    for key, value in file_info.items()
                    if key not in {"page_url", "filename"}
                }
            else:
                page_url, filename = file_info
                filename = filename or page_url
                extra_fields = {}
            file_doc = {
                "_id": f"{session_id}:{position}",
                "session_id": session_id,
                "position": position,
                "page_url": page_url,
                "filename": filename,
                "cdn_host": None,
                "status": FILE_PENDING,
                "gid": None,
                "error": None,
                "attempts": 0,
                "defer_count": 0,
                "auto_defer_count": 0,
                "last_deferred_at": None,
                "last_deferred_reason": None,
                "host_defer_count": 0,
                "last_host_deferred_at": None,
                "last_host_deferred_reason": None,
                "updated_at": now,
            }
            reserved_file_fields = set(file_doc)
            for key, value in extra_fields.items():
                if key not in reserved_file_fields:
                    file_doc[key] = value
            file_docs.append(file_doc)
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

    def _session_query(self, owner_id=None, states=None):
        query = {}
        if owner_id is not None:
            query["owner_id"] = int(owner_id)
        if states:
            query["state"] = {"$in": list(states)}
        return query

    async def count_sessions(self, owner_id=None, states=None):
        query = self._session_query(owner_id, states)
        if self.persistent:
            await self._ensure_indexes()
            return await self.sessions.count_documents(query)
        async with self._memory_lock:
            return sum(
                1
                for doc in self._memory_sessions.values()
                if (owner_id is None or doc["owner_id"] == int(owner_id))
                and (not states or doc["state"] in states)
            )

    async def list_sessions(self, owner_id=None, states=None, limit=20, skip=0):
        query = self._session_query(owner_id, states)
        skip = max(0, int(skip))
        if self.persistent:
            await self._ensure_indexes()
            cursor = self.sessions.find(query).sort("updated_at", -1)
            if skip:
                cursor = cursor.skip(skip)
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
            docs = docs[skip:]
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

    async def list_chain(self, chain_id):
        """Return every session in a split chain in part order."""
        if self.persistent:
            await self._ensure_indexes()
            cursor = self.sessions.find({"chain_id": chain_id}).sort("part_index", 1)
            return [doc async for doc in cursor]
        async with self._memory_lock:
            docs = [
                doc
                for doc in self._memory_sessions.values()
                if doc.get("chain_id") == chain_id
            ]
            docs.sort(key=lambda doc: doc.get("part_index", 0))
            return copy.deepcopy(docs)

    async def activate_next_chain_part(self, session_id):
        """Atomically activate the queued part immediately after a completed one."""
        current = await self.get_session(session_id)
        if current is None or current.get("state") != SESSION_COMPLETED:
            return None
        chain_id = current.get("chain_id")
        part_index = current.get("part_index")
        if not chain_id or part_index is None:
            return None
        query = {
            "chain_id": chain_id,
            "part_index": int(part_index) + 1,
            "state": SESSION_PAUSED,
        }
        now = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            return await self.sessions.find_one_and_update(
                query,
                {"$set": {"state": SESSION_RUNNING, "updated_at": now}},
                return_document=ReturnDocument.AFTER,
            )
        async with self._memory_lock:
            for doc in self._memory_sessions.values():
                if all(doc.get(key) == value for key, value in query.items()):
                    doc.update({"state": SESSION_RUNNING, "updated_at": now})
                    return copy.deepcopy(doc)
        return None

    async def claim_next_file(
        self, session_id, excluded_hosts=None, *, allow_excluded_fallback=True
    ):
        """Claim the next pending file, preferring hosts outside a cooldown.

        Files without a known host remain eligible so their real CDN can be
        discovered lazily. Callers may disable the normal excluded-host fallback
        when they intend to wait for a CDN circuit breaker to expire.
        """
        now = utcnow()
        excluded_hosts = {
            str(host).lower() for host in (excluded_hosts or ()) if host
        }
        if self.persistent:
            await self._ensure_indexes()
            base_query = {"session_id": session_id, "status": FILE_PENDING}
            update = {
                "$set": {
                    "status": FILE_RESOLVING,
                    "gid": None,
                    "error": None,
                    "updated_at": now,
                },
                "$inc": {"attempts": 1},
            }
            if excluded_hosts:
                preferred_query = dict(base_query)
                preferred_query["cdn_host"] = {"$nin": list(excluded_hosts)}
                preferred = await self.files.find_one_and_update(
                    preferred_query,
                    update,
                    sort=[("position", ASCENDING)],
                    return_document=ReturnDocument.AFTER,
                )
                if preferred is not None:
                    return preferred
                if not allow_excluded_fallback:
                    return None
            return await self.files.find_one_and_update(
                base_query,
                update,
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
            preferred = [
                doc
                for doc in candidates
                if not doc.get("cdn_host")
                or str(doc["cdn_host"]).lower() not in excluded_hosts
            ]
            if excluded_hosts and not preferred and not allow_excluded_fallback:
                return None
            doc = (preferred or candidates)[0]
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

    async def has_pending_outside_hosts(self, session_id, excluded_hosts):
        """Return whether another pending file may use a non-cooled CDN."""
        excluded_hosts = {
            str(host).lower() for host in (excluded_hosts or ()) if host
        }
        query = {"session_id": session_id, "status": FILE_PENDING}
        if excluded_hosts:
            query["cdn_host"] = {"$nin": list(excluded_hosts)}
        if self.persistent:
            await self._ensure_indexes()
            candidate = await self.files.find_one(query, projection={"_id": 1})
            return candidate is not None
        async with self._memory_lock:
            return any(
                doc["session_id"] == session_id
                and doc["status"] == FILE_PENDING
                and (
                    not excluded_hosts
                    or not doc.get("cdn_host")
                    or str(doc["cdn_host"]).lower() not in excluded_hosts
                )
                for doc in self._memory_files.values()
            )

    async def set_host_cooldown(self, session_id, cdn_host, seconds, now=None):
        """Persist a per-session CDN cooldown and return its expiry epoch."""
        cdn_host = str(cdn_host or "").lower()
        if not cdn_host:
            return None
        now_epoch = float(now if now is not None else utcnow().timestamp())
        until_epoch = now_epoch + max(0, int(seconds))
        entry = {"host": cdn_host, "until": until_epoch}
        if self.persistent:
            await self._ensure_indexes()
            session_doc = await self.sessions.find_one(
                {"_id": session_id}, projection={"cdn_cooldowns": 1}
            )
            if session_doc is None:
                return None
            cooldowns = [
                item
                for item in session_doc.get("cdn_cooldowns", [])
                if item.get("host") != cdn_host
                and float(item.get("until", 0)) > now_epoch
            ]
            cooldowns.append(entry)
            updated = await self.sessions.update_one(
                {"_id": session_id},
                {"$set": {"cdn_cooldowns": cooldowns, "updated_at": utcnow()}},
            )
            return until_epoch if updated.matched_count else None
        async with self._memory_lock:
            session_doc = self._memory_sessions.get(session_id)
            if session_doc is None:
                return None
            cooldowns = [
                item
                for item in session_doc.get("cdn_cooldowns", [])
                if item.get("host") != cdn_host
                and float(item.get("until", 0)) > now_epoch
            ]
            cooldowns.append(entry)
            session_doc["cdn_cooldowns"] = cooldowns
            session_doc["updated_at"] = utcnow()
            return until_epoch

    async def active_host_cooldowns(self, session_id, now=None):
        """Return the set of CDN hosts whose session cooldown is still active."""
        now_epoch = float(now if now is not None else utcnow().timestamp())
        session_doc = await self.get_session(session_id)
        if session_doc is None:
            return set()
        return {
            str(item.get("host", "")).lower()
            for item in session_doc.get("cdn_cooldowns", [])
            if item.get("host") and float(item.get("until", 0)) > now_epoch
        }

    async def get_cdn_health(self, cdn_host):
        """Return shared adaptive health for a CDN host."""
        cdn_host = str(cdn_host or "").lower()
        if not cdn_host:
            return None
        if self.persistent:
            await self._ensure_indexes()
            health = await self.cdn_health.find_one({"_id": cdn_host})
        else:
            async with self._memory_lock:
                health = self._memory_cdn_health.get(cdn_host)
                health = copy.deepcopy(health) if health is not None else None
        if health is not None:
            return health
        return {
            "_id": cdn_host,
            "slow_strikes": 0,
            "cooldown_until": 0.0,
            "last_slow_at": None,
            "last_success_at": None,
        }

    async def active_global_host_cooldowns(self, now=None):
        """Return CDN hosts whose process-wide/database circuit breaker is open."""
        now_epoch = float(now if now is not None else utcnow().timestamp())
        if self.persistent:
            await self._ensure_indexes()
            return {
                doc["_id"]
                async for doc in self.cdn_health.find(
                    {"cooldown_until": {"$gt": now_epoch}}, projection={"_id": 1}
                )
            }
        async with self._memory_lock:
            return {
                host
                for host, health in self._memory_cdn_health.items()
                if float(health.get("cooldown_until", 0)) > now_epoch
            }

    async def session_cdn_hosts(self, session_id):
        """Return the resolved CDN hosts currently known for one session."""
        if self.persistent:
            await self._ensure_indexes()
            hosts = await self.files.distinct(
                "cdn_host", {"session_id": session_id, "cdn_host": {"$ne": None}}
            )
        else:
            async with self._memory_lock:
                hosts = [
                    doc.get("cdn_host")
                    for doc in self._memory_files.values()
                    if doc["session_id"] == session_id and doc.get("cdn_host")
                ]
        return {str(host).lower() for host in hosts if host}

    async def record_cdn_slowdown(
        self, cdn_host, base_cooldown_seconds, max_cooldown_seconds, now=None
    ):
        """Open a shared CDN circuit breaker with exponential backoff."""
        cdn_host = str(cdn_host or "").lower()
        if not cdn_host:
            return None
        now_epoch = float(now if now is not None else utcnow().timestamp())
        base_seconds = max(1, int(base_cooldown_seconds))
        max_seconds = max(base_seconds, int(max_cooldown_seconds))
        async with self._cdn_health_lock:
            current = await self.get_cdn_health(cdn_host)
            strikes = max(0, int(current.get("slow_strikes", 0))) + 1
            cooldown_seconds = base_seconds
            for _ in range(strikes - 1):
                if cooldown_seconds >= max_seconds:
                    break
                cooldown_seconds = min(max_seconds, cooldown_seconds * 2)
            health = {
                "_id": cdn_host,
                "slow_strikes": strikes,
                "cooldown_until": now_epoch + cooldown_seconds,
                "cooldown_seconds": cooldown_seconds,
                "last_slow_at": now_epoch,
                "last_success_at": current.get("last_success_at"),
                "updated_at": utcnow(),
            }
            if self.persistent:
                await self._ensure_indexes()
                await self.cdn_health.replace_one(
                    {"_id": cdn_host}, health, upsert=True
                )
            else:
                async with self._memory_lock:
                    self._memory_cdn_health[cdn_host] = health
            return copy.deepcopy(health)

    async def record_cdn_success(self, cdn_host, now=None):
        """Recover one adaptive level after a healthy completed download."""
        cdn_host = str(cdn_host or "").lower()
        if not cdn_host:
            return None
        now_epoch = float(now if now is not None else utcnow().timestamp())
        async with self._cdn_health_lock:
            current = await self.get_cdn_health(cdn_host)
            strikes = max(0, int(current.get("slow_strikes", 0)) - 1)
            health = {
                "_id": cdn_host,
                "slow_strikes": strikes,
                "cooldown_until": 0.0,
                "cooldown_seconds": 0,
                "last_slow_at": current.get("last_slow_at"),
                "last_success_at": now_epoch,
                "updated_at": utcnow(),
            }
            if self.persistent:
                await self._ensure_indexes()
                await self.cdn_health.replace_one(
                    {"_id": cdn_host}, health, upsert=True
                )
            else:
                async with self._memory_lock:
                    self._memory_cdn_health[cdn_host] = health
            return copy.deepcopy(health)

    async def route_file_to_bottom(self, file_id, reason):
        """Route a resolving file behind the queue without marking a slow retry."""
        now = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            file_doc = await self.files.find_one({"_id": file_id})
            if file_doc is None or file_doc["status"] != FILE_RESOLVING:
                return None
            has_next = await self.files.find_one(
                {
                    "session_id": file_doc["session_id"],
                    "_id": {"$ne": file_id},
                    "status": FILE_PENDING,
                },
                projection={"_id": 1},
            )
            if has_next is None:
                return None
            last_file = await self.files.find_one(
                {"session_id": file_doc["session_id"]},
                sort=[("position", -1)],
                projection={"position": 1},
            )
            updated = await self.files.find_one_and_update(
                {"_id": file_id, "status": FILE_RESOLVING},
                {
                    "$set": {
                        "status": FILE_PENDING,
                        "position": int(last_file["position"]) + 1,
                        "gid": None,
                        "error": reason,
                        "last_host_deferred_at": now,
                        "last_host_deferred_reason": reason,
                        "updated_at": now,
                    },
                    "$inc": {"host_defer_count": 1},
                },
                return_document=ReturnDocument.AFTER,
            )
            return updated
        async with self._memory_lock:
            file_doc = self._memory_files.get(file_id)
            if file_doc is None or file_doc["status"] != FILE_RESOLVING:
                return None
            session_files = [
                doc
                for doc in self._memory_files.values()
                if doc["session_id"] == file_doc["session_id"]
            ]
            if not any(
                doc["_id"] != file_id and doc["status"] == FILE_PENDING
                for doc in session_files
            ):
                return None
            file_doc.update(
                {
                    "status": FILE_PENDING,
                    "position": max(doc["position"] for doc in session_files) + 1,
                    "gid": None,
                    "error": reason,
                    "host_defer_count": file_doc.get("host_defer_count", 0) + 1,
                    "last_host_deferred_at": now,
                    "last_host_deferred_reason": reason,
                    "updated_at": now,
                }
            )
            return copy.deepcopy(file_doc)

    async def move_pending_host_to_bottom(self, session_id, cdn_host):
        """Group all known pending files for one CDN behind other hosts."""
        cdn_host = str(cdn_host or "").lower()
        if not cdn_host:
            return 0
        now = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            pending = [
                doc
                async for doc in self.files.find(
                    {
                        "session_id": session_id,
                        "status": FILE_PENDING,
                        "cdn_host": cdn_host,
                    }
                ).sort("position", ASCENDING)
            ]
            if not pending:
                return 0
            last_file = await self.files.find_one(
                {"session_id": session_id},
                sort=[("position", -1)],
                projection={"position": 1},
            )
            next_position = int(last_file["position"]) + 1
            moved = 0
            for file_doc in pending:
                result = await self.files.update_one(
                    {"_id": file_doc["_id"], "status": FILE_PENDING},
                    {
                        "$set": {
                            "position": next_position,
                            "updated_at": now,
                        }
                    },
                )
                if result.modified_count:
                    moved += 1
                    next_position += 1
            if moved:
                await self.sessions.update_one(
                    {"_id": session_id}, {"$set": {"updated_at": now}}
                )
            return moved
        async with self._memory_lock:
            session_files = [
                doc
                for doc in self._memory_files.values()
                if doc["session_id"] == session_id
            ]
            pending = sorted(
                (
                    doc
                    for doc in session_files
                    if doc["status"] == FILE_PENDING
                    and str(doc.get("cdn_host") or "").lower() == cdn_host
                ),
                key=lambda doc: doc["position"],
            )
            next_position = max(
                (doc["position"] for doc in session_files), default=0
            ) + 1
            for file_doc in pending:
                file_doc["position"] = next_position
                file_doc["updated_at"] = now
                next_position += 1
            if pending:
                session_doc = self._memory_sessions.get(session_id)
                if session_doc is not None:
                    session_doc["updated_at"] = now
            return len(pending)

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

    async def update_file_if_status(
        self, file_id, expected_statuses, status=None, **fields
    ):
        """Update a file only while it remains in one of the expected states."""
        if isinstance(expected_statuses, str):
            expected_statuses = (expected_statuses,)
        expected_statuses = tuple(expected_statuses)
        if status is not None:
            fields["status"] = status
        fields["updated_at"] = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            return await self.files.find_one_and_update(
                {"_id": file_id, "status": {"$in": list(expected_statuses)}},
                {"$set": fields},
                return_document=ReturnDocument.AFTER,
            )
        async with self._memory_lock:
            doc = self._memory_files.get(file_id)
            if doc is None or doc["status"] not in expected_statuses:
                return None
            doc.update(fields)
            return copy.deepcopy(doc)

    async def park_file_for_cooldown(
        self, file_id, reason, *, max_auto_defers=None
    ):
        """Return an active file to pending without requiring another queue item.

        This is used when the only available CDN is cooling down. The file keeps
        its stable download directory and queue position so aria2 can resume its
        partial data after a freshly signed URL is obtained.
        """
        active_states = (FILE_RESOLVING, FILE_DOWNLOADING)
        now = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            query = {"_id": file_id, "status": {"$in": list(active_states)}}
            if max_auto_defers is not None:
                query["auto_defer_count"] = {"$lt": int(max_auto_defers)}
            updated = await self.files.find_one_and_update(
                query,
                {
                    "$set": {
                        "status": FILE_PENDING,
                        "gid": None,
                        "error": reason,
                        "last_deferred_at": now,
                        "last_deferred_reason": reason,
                        "updated_at": now,
                    },
                    "$inc": {"defer_count": 1, "auto_defer_count": 1},
                },
                return_document=ReturnDocument.AFTER,
            )
            if updated is not None:
                await self.sessions.update_one(
                    {"_id": updated["session_id"]},
                    {"$set": {"updated_at": now}},
                )
            return updated

        async with self._memory_lock:
            file_doc = self._memory_files.get(file_id)
            if file_doc is None or file_doc["status"] not in active_states:
                return None
            if (
                max_auto_defers is not None
                and file_doc.get("auto_defer_count", 0) >= max_auto_defers
            ):
                return None
            file_doc.update(
                {
                    "status": FILE_PENDING,
                    "gid": None,
                    "error": reason,
                    "defer_count": file_doc.get("defer_count", 0) + 1,
                    "auto_defer_count": file_doc.get("auto_defer_count", 0) + 1,
                    "last_deferred_at": now,
                    "last_deferred_reason": reason,
                    "updated_at": now,
                }
            )
            session_doc = self._memory_sessions.get(file_doc["session_id"])
            if session_doc is not None:
                session_doc["updated_at"] = now
            return copy.deepcopy(file_doc)

    async def defer_file_to_bottom(
        self,
        file_id,
        reason,
        *,
        automatic=False,
        max_auto_defers=None,
    ):
        """Move an active file behind every other file in its session.

        A file is only deferred when another pending file can run next. The
        conditional update prevents simultaneous manual and automatic skips
        from moving the same file twice.
        """
        active_states = (FILE_RESOLVING, FILE_DOWNLOADING)
        now = utcnow()
        if self.persistent:
            await self._ensure_indexes()
            file_doc = await self.files.find_one({"_id": file_id})
            if file_doc is None or file_doc["status"] not in active_states:
                return None
            if (
                automatic
                and max_auto_defers is not None
                and file_doc.get("auto_defer_count", 0) >= max_auto_defers
            ):
                return None
            has_next = await self.files.find_one(
                {
                    "session_id": file_doc["session_id"],
                    "_id": {"$ne": file_id},
                    "status": FILE_PENDING,
                },
                projection={"_id": 1},
            )
            if has_next is None:
                return None
            last_file = await self.files.find_one(
                {"session_id": file_doc["session_id"]},
                sort=[("position", -1)],
                projection={"position": 1},
            )
            new_position = int(last_file["position"]) + 1
            updated = await self.files.find_one_and_update(
                {"_id": file_id, "status": {"$in": list(active_states)}},
                {
                    "$set": {
                        "status": FILE_PENDING,
                        "position": new_position,
                        "gid": None,
                        "error": reason,
                        "last_deferred_at": now,
                        "last_deferred_reason": reason,
                        "updated_at": now,
                    },
                    "$inc": {
                        "defer_count": 1,
                        "auto_defer_count": 1 if automatic else 0,
                    },
                },
                return_document=ReturnDocument.AFTER,
            )
            if updated is not None:
                await self.sessions.update_one(
                    {"_id": file_doc["session_id"]},
                    {"$set": {"updated_at": now}},
                )
            return updated

        async with self._memory_lock:
            file_doc = self._memory_files.get(file_id)
            if file_doc is None or file_doc["status"] not in active_states:
                return None
            if (
                automatic
                and max_auto_defers is not None
                and file_doc.get("auto_defer_count", 0) >= max_auto_defers
            ):
                return None
            session_files = [
                doc
                for doc in self._memory_files.values()
                if doc["session_id"] == file_doc["session_id"]
            ]
            if not any(
                doc["_id"] != file_id and doc["status"] == FILE_PENDING
                for doc in session_files
            ):
                return None
            file_doc.update(
                {
                    "status": FILE_PENDING,
                    "position": max(doc["position"] for doc in session_files) + 1,
                    "gid": None,
                    "error": reason,
                    "defer_count": file_doc.get("defer_count", 0) + 1,
                    "auto_defer_count": file_doc.get("auto_defer_count", 0)
                    + (1 if automatic else 0),
                    "last_deferred_at": now,
                    "last_deferred_reason": reason,
                    "updated_at": now,
                }
            )
            session_doc = self._memory_sessions.get(file_doc["session_id"])
            if session_doc is not None:
                session_doc["updated_at"] = now
            return copy.deepcopy(file_doc)

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
