"""Persistent download sessions for size-split TeraBox shares."""

import asyncio
import copy
import re

from pymongo import ASCENDING, DESCENDING, UpdateOne

from .bunkr_sessions import (
    FILE_CANCELLED,
    FILE_DOWNLOADED,
    FILE_DOWNLOADING,
    FILE_FAILED,
    FILE_PENDING,
    FILE_RESOLVING,
    SESSION_CANCELLED,
    SESSION_COMPLETED,
    SESSION_FAILED,
    SESSION_PAUSED,
    SESSION_RUNNING,
    BunkrSessionStore,
    new_session_id,
    utcnow,
)


FILE_UPLOADED = "uploaded"


class TeraboxSessionStore(BunkrSessionStore):
    """Use the proven session state machine with isolated TeraBox collections."""

    def __init__(self, db_url=None, database_name=None):
        super().__init__(
            db_url=db_url,
            database_name=database_name,
            collection_prefix="TERABOX",
        )
        self.chains = (
            self.database["TERABOX_CHAINS"]
            if self.database is not None
            else None
        )
        self._memory_chains = {}
        self._chain_indexes_ready = False
        self._chain_index_lock = asyncio.Lock()

    async def _ensure_indexes(self):
        await super()._ensure_indexes()
        if not self.persistent or self._chain_indexes_ready:
            return
        async with self._chain_index_lock:
            if self._chain_indexes_ready:
                return
            await self.chains.create_index(
                [("owner_id", ASCENDING), ("updated_at", ASCENDING)]
            )
            await self.chains.create_index("source_url")
            self._chain_indexes_ready = True

    async def create_chain(
        self,
        *,
        chain_id,
        owner_id,
        chat_id,
        source_message_id,
        source_url,
        name,
        mode,
        total_parts,
        total_files,
        session_ids,
        chain_fields=None,
    ):
        """Persist a parent record for a group of sequential sessions."""
        now = utcnow()
        chain_doc = {
            "_id": str(chain_id),
            "owner_id": int(owner_id),
            "chat_id": int(chat_id),
            "source_message_id": int(source_message_id),
            "source_url": source_url,
            "name": str(name or "TeraBox").strip() or "TeraBox",
            "mode": mode,
            "total_parts": int(total_parts),
            "total_files": int(total_files),
            "session_ids": [str(value) for value in session_ids],
            "created_at": now,
            "updated_at": now,
            "schema_version": 1,
        }
        reserved_fields = set(chain_doc)
        for key, value in (chain_fields or {}).items():
            if key not in reserved_fields:
                chain_doc[key] = copy.deepcopy(value)
        if self.persistent:
            await self._ensure_indexes()
            await self.chains.insert_one(chain_doc)
        else:
            async with self._memory_lock:
                self._memory_chains[chain_doc["_id"]] = chain_doc
        return copy.deepcopy(chain_doc)

    async def _backfill_chain_records(self, owner_id=None):
        """Materialize parent records for chains created by older releases."""
        sessions = await super().list_sessions(
            owner_id=owner_id, limit=0
        )
        grouped = {}
        for session_doc in sessions:
            chain_id = str(session_doc.get("chain_id") or session_doc["_id"])
            grouped.setdefault(chain_id, []).append(session_doc)

        if self.persistent and grouped:
            query = {"_id": {"$in": list(grouped)}}
            existing_chains = {
                doc["_id"]: doc
                async for doc in self.chains.find(query)
            }
        elif self.persistent:
            existing_chains = {}
        else:
            async with self._memory_lock:
                existing_chains = copy.deepcopy(self._memory_chains)

        for chain_id, chain_sessions in grouped.items():
            chain_sessions.sort(key=lambda doc: int(doc.get("part_index") or 1))
            first = chain_sessions[0]
            existing = existing_chains.get(chain_id)
            existing_name = str((existing or {}).get("name") or "").strip()
            missing_session_name = any(
                not str(doc.get("name") or "").strip()
                for doc in chain_sessions
            )
            if existing_name and not missing_session_name:
                continue
            fallback = re.sub(
                r"\s*\(part\s+\d+/\d+\)\s*$",
                "",
                str(first.get("title") or "TeraBox"),
                flags=re.IGNORECASE,
            ).strip()
            name = existing_name or str(first.get("name") or "").strip()
            if not name:
                files = []
                for session_doc in chain_sessions:
                    files.extend(await self.list_files(session_doc["_id"]))
                name = infer_shared_folder_name(
                    files, fallback=fallback or "TeraBox"
                )
            created_values = [
                doc.get("created_at") for doc in chain_sessions if doc.get("created_at")
            ]
            updated_values = [
                doc.get("updated_at") for doc in chain_sessions if doc.get("updated_at")
            ]
            chain_doc = {
                "_id": chain_id,
                "owner_id": int(first["owner_id"]),
                "chat_id": int(first["chat_id"]),
                "source_message_id": int(first["source_message_id"]),
                "source_url": first["source_url"],
                "name": name,
                "mode": first.get("mode", "normal"),
                "planning_mode": first.get("planning_mode", "source_size"),
                "max_bytes": int(first.get("max_bytes") or 0),
                "total_parts": len(chain_sessions),
                "total_files": sum(
                    int(doc.get("total_files") or 0) for doc in chain_sessions
                ),
                "session_ids": [doc["_id"] for doc in chain_sessions],
                "created_at": min(created_values) if created_values else utcnow(),
                "updated_at": max(updated_values) if updated_values else utcnow(),
                "schema_version": 1,
            }
            if self.persistent:
                await self.chains.update_one(
                    {"_id": chain_id}, {"$setOnInsert": chain_doc}, upsert=True
                )
                await self.sessions.update_many(
                    {
                        "chain_id": chain_id,
                        "$or": [
                            {"name": {"$exists": False}},
                            {"name": ""},
                        ],
                    },
                    {"$set": {"name": name}},
                )
            else:
                async with self._memory_lock:
                    self._memory_chains.setdefault(
                        chain_id, copy.deepcopy(chain_doc)
                    )
                    for session_doc in self._memory_sessions.values():
                        if (
                            session_doc.get("chain_id") == chain_id
                            and not session_doc.get("name")
                        ):
                            session_doc["name"] = name

    async def get_chain(self, chain_id, owner_id=None):
        await self._ensure_indexes()
        await self._backfill_chain_records(owner_id=owner_id)
        query = {"_id": str(chain_id)}
        if owner_id is not None:
            query["owner_id"] = int(owner_id)
        if self.persistent:
            return await self.chains.find_one(query)
        async with self._memory_lock:
            doc = self._memory_chains.get(str(chain_id))
            if doc is None or (
                owner_id is not None and doc["owner_id"] != int(owner_id)
            ):
                return None
            return copy.deepcopy(doc)

    async def count_chains(self, owner_id=None):
        await self._ensure_indexes()
        await self._backfill_chain_records(owner_id=owner_id)
        query = {"owner_id": int(owner_id)} if owner_id is not None else {}
        if self.persistent:
            return await self.chains.count_documents(query)
        async with self._memory_lock:
            return sum(
                1
                for doc in self._memory_chains.values()
                if owner_id is None or doc["owner_id"] == int(owner_id)
            )

    async def list_chains(self, owner_id=None, limit=20, skip=0):
        await self._ensure_indexes()
        await self._backfill_chain_records(owner_id=owner_id)
        query = {"owner_id": int(owner_id)} if owner_id is not None else {}
        skip = max(0, int(skip))
        if self.persistent:
            cursor = self.chains.find(query).sort(
                [
                    ("updated_at", DESCENDING),
                    ("created_at", DESCENDING),
                    ("_id", DESCENDING),
                ]
            )
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
            return [doc async for doc in cursor]
        async with self._memory_lock:
            docs = [
                doc
                for doc in self._memory_chains.values()
                if owner_id is None or doc["owner_id"] == int(owner_id)
            ]
            docs.sort(
                key=lambda doc: (
                    doc["updated_at"],
                    doc.get("created_at") or doc["updated_at"],
                    str(doc["_id"]),
                ),
                reverse=True,
            )
            docs = docs[skip:]
            return copy.deepcopy(docs[:limit] if limit else docs)

    async def delete_chain(self, chain_id, owner_id=None):
        query = {"_id": str(chain_id)}
        if owner_id is not None:
            query["owner_id"] = int(owner_id)
        if self.persistent:
            await self._ensure_indexes()
            result = await self.chains.delete_one(query)
            return bool(result.deleted_count)
        async with self._memory_lock:
            doc = self._memory_chains.get(str(chain_id))
            if doc is None or (
                owner_id is not None and doc["owner_id"] != int(owner_id)
            ):
                return False
            self._memory_chains.pop(str(chain_id), None)
            return True

    async def _reindex_chain(self, chain_id, owner_id):
        """Close part-number gaps after deleting one child session."""
        sessions = await self.list_chain(str(chain_id))
        sessions = [
            doc for doc in sessions if doc["owner_id"] == int(owner_id)
        ]
        if not sessions:
            await self.delete_chain(chain_id, owner_id=owner_id)
            return []

        now = utcnow()
        total_parts = len(sessions)
        for part_index, session_doc in enumerate(sessions, 1):
            title_base = re.sub(
                r"\s*\(part\s+\d+/\d+\)\s*$",
                "",
                str(session_doc.get("title") or session_doc.get("name") or "TeraBox"),
                flags=re.IGNORECASE,
            ).strip()
            fields = {
                "part_index": part_index,
                "total_parts": total_parts,
                "title": f"{title_base or 'TeraBox'} (part {part_index}/{total_parts})",
                "updated_at": now,
            }
            if self.persistent:
                await self.sessions.update_one(
                    {"_id": session_doc["_id"], "owner_id": int(owner_id)},
                    {"$set": fields},
                )
            else:
                async with self._memory_lock:
                    current = self._memory_sessions.get(session_doc["_id"])
                    if current is not None and current["owner_id"] == int(owner_id):
                        current.update(copy.deepcopy(fields))
            session_doc.update(copy.deepcopy(fields))

        chain_fields = {
            "session_ids": [doc["_id"] for doc in sessions],
            "total_parts": total_parts,
            "total_files": sum(int(doc.get("total_files") or 0) for doc in sessions),
            "updated_at": now,
        }
        if self.persistent:
            await self.chains.update_one(
                {"_id": str(chain_id), "owner_id": int(owner_id)},
                {"$set": chain_fields},
            )
        else:
            async with self._memory_lock:
                chain_doc = self._memory_chains.get(str(chain_id))
                if chain_doc is not None and chain_doc["owner_id"] == int(owner_id):
                    chain_doc.update(copy.deepcopy(chain_fields))
        return copy.deepcopy(sessions)

    async def delete_session_from_chain(self, session_id, owner_id):
        """Delete an owned child session and keep its parent chain usable."""
        owner_id = int(owner_id)
        await self._ensure_indexes()
        await self._backfill_chain_records(owner_id=owner_id)
        session_doc = await self.get_session(str(session_id), owner_id=owner_id)
        if session_doc is None:
            return None
        chain_id = str(session_doc.get("chain_id") or session_doc["_id"])
        await super().delete_session(session_doc["_id"])
        remaining = await self._reindex_chain(chain_id, owner_id)
        return {
            "session": session_doc,
            "chain_id": chain_id,
            "remaining_sessions": len(remaining),
        }

    async def delete_chain_tree(self, chain_id, owner_id):
        """Delete an owned parent chain together with all child/file records."""
        owner_id = int(owner_id)
        chain_doc = await self.get_chain(str(chain_id), owner_id=owner_id)
        if chain_doc is None:
            return None
        sessions = [
            doc
            for doc in await self.list_chain(str(chain_id))
            if doc["owner_id"] == owner_id
        ]
        session_ids = [doc["_id"] for doc in sessions]
        if self.persistent:
            await self._ensure_indexes()
            if session_ids:
                await self.files.delete_many({"session_id": {"$in": session_ids}})
                await self.sessions.delete_many(
                    {"_id": {"$in": session_ids}, "owner_id": owner_id}
                )
            await self.chains.delete_one(
                {"_id": str(chain_id), "owner_id": owner_id}
            )
        else:
            async with self._memory_lock:
                for session_id in session_ids:
                    self._memory_sessions.pop(session_id, None)
                for file_id in [
                    file_id
                    for file_id, doc in self._memory_files.items()
                    if doc["session_id"] in session_ids
                ]:
                    self._memory_files.pop(file_id, None)
                self._memory_chains.pop(str(chain_id), None)
        return {
            "chain": chain_doc,
            "sessions": sessions,
            "deleted_sessions": len(session_ids),
        }

    async def delete_all_for_owner(self, owner_id):
        """Delete all owned TeraBox chains, sessions, and file records."""
        owner_id = int(owner_id)
        sessions = await super().list_sessions(owner_id=owner_id, limit=0)
        session_ids = [doc["_id"] for doc in sessions]
        chains = await self.list_chains(owner_id=owner_id, limit=0)
        if self.persistent:
            await self._ensure_indexes()
            if session_ids:
                await self.files.delete_many({"session_id": {"$in": session_ids}})
            await self.sessions.delete_many({"owner_id": owner_id})
            await self.chains.delete_many({"owner_id": owner_id})
        else:
            async with self._memory_lock:
                for session_id in session_ids:
                    self._memory_sessions.pop(session_id, None)
                for file_id in [
                    file_id
                    for file_id, doc in self._memory_files.items()
                    if doc["session_id"] in session_ids
                ]:
                    self._memory_files.pop(file_id, None)
                for chain_id in [
                    chain_id
                    for chain_id, doc in self._memory_chains.items()
                    if doc["owner_id"] == owner_id
                ]:
                    self._memory_chains.pop(chain_id, None)
        return {
            "deleted_sessions": len(session_ids),
            "deleted_chains": len(chains),
        }

    async def list_chain_files(self, chain_id):
        """Return every file record in a chain, preserving source order."""
        sessions = await self.list_chain(str(chain_id))
        files = []
        for session_doc in sessions:
            files.extend(await self.list_files(session_doc["_id"]))
        files.sort(
            key=lambda doc: (
                int(doc.get("source_position") or 0),
                int(doc.get("position") or 0),
                str(doc.get("_id") or ""),
            )
        )
        return files

    async def replace_chain_file_metadata(self, chain_id, refreshed_files):
        """Replace durable share metadata across every child session.

        Stable ``fs_id`` values are the authoritative join key. Records made by
        older releases, which did not persist an fs_id, receive a one-time
        migration by unique relative path; all subsequent refreshes use fs_id.
        """
        chain_id = str(chain_id)
        refreshed_files = [copy.deepcopy(doc) for doc in refreshed_files]
        by_fs_id = {}
        by_path = {}
        for metadata in refreshed_files:
            fs_id = str(metadata.get("fs_id") or "").strip()
            source_dlink = str(metadata.get("source_dlink") or "").strip()
            if not fs_id or not source_dlink:
                continue
            if fs_id in by_fs_id:
                raise ValueError(f"Duplicate TeraBox fs_id in refreshed share: {fs_id}")
            by_fs_id[fs_id] = metadata
            relative_path = str(metadata.get("relative_path") or "")
            by_path.setdefault(relative_path, []).append(metadata)

        stored_files = await self.list_chain_files(chain_id)
        chain_doc = await self.get_chain(chain_id)
        next_revision = int((chain_doc or {}).get("metadata_revision") or 0) + 1
        now = utcnow()
        matched = {}
        unmatched = []
        mongo_updates = []
        for file_doc in stored_files:
            fs_id = str(file_doc.get("fs_id") or "").strip()
            metadata = by_fs_id.get(fs_id) if fs_id else None
            if metadata is None and not fs_id:
                legacy_matches = by_path.get(
                    str(file_doc.get("relative_path") or file_doc.get("filename") or ""),
                    [],
                )
                if len(legacy_matches) == 1:
                    metadata = legacy_matches[0]
            if metadata is None:
                unmatched.append(file_doc["_id"])
                continue

            fields = {
                "fs_id": str(metadata["fs_id"]),
                "source_dlink": str(metadata["source_dlink"]),
                "relative_path": str(
                    metadata.get("relative_path") or file_doc.get("relative_path") or ""
                ),
                "filename": str(
                    metadata.get("filename") or file_doc.get("filename") or ""
                ),
                "size_bytes": max(
                    0,
                    int(metadata.get("size_bytes") or file_doc.get("size_bytes") or 0),
                ),
                "source_position": int(
                    metadata.get("source_position")
                    or file_doc.get("source_position")
                    or file_doc.get("position")
                    or 0
                ),
                "metadata_revision": next_revision,
                "metadata_updated_at": now,
                "updated_at": now,
            }
            if self.persistent:
                mongo_updates.append(
                    UpdateOne({"_id": file_doc["_id"]}, {"$set": fields})
                )
            else:
                async with self._memory_lock:
                    current = self._memory_files.get(file_doc["_id"])
                    if current is not None:
                        current.update(copy.deepcopy(fields))
            matched[file_doc["_id"]] = fields

        if self.persistent and mongo_updates:
            await self.files.bulk_write(mongo_updates, ordered=False)

        chain_fields = {
            "metadata_revision": next_revision,
            "metadata_refreshed_at": now,
            "metadata_file_count": len(refreshed_files),
            "updated_at": now,
        }
        if self.persistent:
            await self.chains.update_one(
                {"_id": chain_id}, {"$set": chain_fields}
            )
        else:
            async with self._memory_lock:
                current_chain = self._memory_chains.get(chain_id)
                if current_chain is not None:
                    current_chain.update(copy.deepcopy(chain_fields))

        return {
            "revision": next_revision,
            "matched": matched,
            "unmatched_file_ids": unmatched,
            "files": await self.list_chain_files(chain_id),
        }

    async def set_state(self, session_id, state, **fields):
        session_doc = await super().set_state(session_id, state, **fields)
        if session_doc and session_doc.get("chain_id"):
            now = utcnow()
            if self.persistent:
                await self._ensure_indexes()
                await self.chains.update_one(
                    {"_id": session_doc["chain_id"]},
                    {"$set": {"updated_at": now}},
                )
            else:
                async with self._memory_lock:
                    chain_doc = self._memory_chains.get(session_doc["chain_id"])
                    if chain_doc is not None:
                        chain_doc["updated_at"] = now
        return session_doc

    async def prepare_continue(self, session_id, chat_id, source_message_id):
        """Redownload files whose queued upload was lost after a restart."""
        now = utcnow()
        reset = {
            "status": FILE_PENDING,
            "gid": None,
            "error": None,
            "telegram_files": [],
            "updated_at": now,
        }
        if self.persistent:
            await self._ensure_indexes()
            await self.files.update_many(
                {"session_id": session_id, "status": FILE_DOWNLOADED},
                {"$set": reset},
            )
        else:
            async with self._memory_lock:
                for doc in self._memory_files.values():
                    if (
                        doc["session_id"] == session_id
                        and doc["status"] == FILE_DOWNLOADED
                    ):
                        doc.update(copy.deepcopy(reset))
        return await super().prepare_continue(
            session_id, chat_id, source_message_id
        )


terabox_session_store = TeraboxSessionStore()


def infer_shared_folder_name(items, fallback="TeraBox"):
    """Infer the shared top-level directory from persisted relative paths."""
    paths = []
    for item in items:
        value = str(
            item.get("relative_path") or item.get("path") or item.get("filename") or ""
        ).replace("\\", "/").strip("/")
        if value:
            paths.append([part for part in value.split("/") if part])
    if paths and all(len(parts) > 1 for parts in paths):
        root = paths[0][0]
        if root and all(parts[0] == root for parts in paths):
            return root
    return str(fallback or "TeraBox").strip() or "TeraBox"


_SIZE_RE = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*(B|K(?:I?B)?|M(?:I?B)?|G(?:I?B)?|T(?:I?B)?)\s*$",
    re.IGNORECASE,
)
_SIZE_POWERS = {
    "B": 0,
    "K": 1,
    "KB": 1,
    "KIB": 1,
    "M": 2,
    "MB": 2,
    "MIB": 2,
    "G": 3,
    "GB": 3,
    "GIB": 3,
    "T": 4,
    "TB": 4,
    "TIB": 4,
}


def parse_size_limit(value):
    """Parse a user-facing size using binary multiples (GB == GiB here)."""
    match = _SIZE_RE.fullmatch(str(value or ""))
    if not match:
        raise ValueError("Use a size such as 40GB, 800MB, or 1.5TB")
    amount = float(match.group(1))
    size_bytes = int(amount * (1024 ** _SIZE_POWERS[match.group(2).upper()]))
    if size_bytes < 1024 * 1024:
        raise ValueError("The per-session size must be at least 1MB")
    return size_bytes


def split_by_cumulative_size(items, max_bytes, size_getter=None):
    """Greedily preserve order while limiting each multi-file part by size.

    A file larger than the requested limit is kept intact in its own part.
    """
    max_bytes = int(max_bytes)
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    size_getter = size_getter or (lambda item: item["size_bytes"])
    groups = []
    current = []
    current_size = 0
    for item in items:
        item_size = max(0, int(size_getter(item) or 0))
        if current and current_size + item_size > max_bytes:
            groups.append(current)
            current = []
            current_size = 0
        current.append(item)
        current_size += item_size
    if current:
        groups.append(current)
    return groups


__all__ = [
    "FILE_CANCELLED",
    "FILE_DOWNLOADED",
    "FILE_DOWNLOADING",
    "FILE_FAILED",
    "FILE_PENDING",
    "FILE_RESOLVING",
    "FILE_UPLOADED",
    "SESSION_CANCELLED",
    "SESSION_COMPLETED",
    "SESSION_FAILED",
    "SESSION_PAUSED",
    "SESSION_RUNNING",
    "TeraboxSessionStore",
    "infer_shared_folder_name",
    "new_session_id",
    "parse_size_limit",
    "split_by_cumulative_size",
    "terabox_session_store",
]
