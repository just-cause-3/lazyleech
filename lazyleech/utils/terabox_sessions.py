"""Persistent download sessions for size-split TeraBox shares."""

import re

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
)


class TeraboxSessionStore(BunkrSessionStore):
    """Use the proven session state machine with isolated TeraBox collections."""

    def __init__(self, db_url=None, database_name=None):
        super().__init__(
            db_url=db_url,
            database_name=database_name,
            collection_prefix="TERABOX",
        )


terabox_session_store = TeraboxSessionStore()


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
    "SESSION_CANCELLED",
    "SESSION_COMPLETED",
    "SESSION_FAILED",
    "SESSION_PAUSED",
    "SESSION_RUNNING",
    "TeraboxSessionStore",
    "new_session_id",
    "parse_size_limit",
    "split_by_cumulative_size",
    "terabox_session_store",
]
