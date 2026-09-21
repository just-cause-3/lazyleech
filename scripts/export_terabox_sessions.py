#!/usr/bin/env python3
"""Export all persistent TeraBox session history to one UTF-8 text file.

This utility is intentionally independent from LazyLeech.  It imports no bot
modules, does not start Pyrogram, and only performs read operations in MongoDB.
It understands the collection/schema names used by the bot so that its output
resembles the ``/terasessions`` listing without Telegram pagination.
"""

from __future__ import annotations

import argparse
import getpass
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # The script also works without python-dotenv.
    load_dotenv = None

try:
    from pymongo import DESCENDING, MongoClient
except ImportError as error:  # Give a useful standalone installation hint.
    raise SystemExit(
        "Missing dependency: install it with `python3 -m pip install pymongo dnspython`."
    ) from error


DEFAULT_OWNER_ID = 804248372
DEFAULT_DATABASE = "ASWFeed"
CHAINS_COLLECTION = "TERABOX_CHAINS"
SESSIONS_COLLECTION = "TERABOX_SESSIONS"


def _safe_text(value: Any, fallback: str = "") -> str:
    """Return one printable line without allowing record fields to break it."""
    text = str(value if value is not None else fallback)
    return " ".join(text.replace("\x00", "").splitlines()).strip() or fallback


def _human_size(size: Any) -> str:
    try:
        value = float(size or 0)
    except (TypeError, ValueError):
        value = 0.0
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{int(value)} B" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024
    return "0 B"


def _format_time(value: Any) -> str:
    if not isinstance(value, datetime):
        return "unknown"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _chain_key(session: dict[str, Any]) -> str:
    return str(session.get("chain_id") or session.get("_id") or "unknown")


def _session_sort_key(session: dict[str, Any]) -> tuple[int, str]:
    try:
        part = int(session.get("part_index") or 1)
    except (TypeError, ValueError):
        part = 1
    return part, str(session.get("_id") or "")


def _history_sort_key(chain: dict[str, Any]) -> tuple[datetime, datetime, str]:
    minimum = datetime.min.replace(tzinfo=timezone.utc)

    def normalized(value: Any) -> datetime:
        if not isinstance(value, datetime):
            return minimum
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value

    return (
        normalized(chain.get("updated_at")),
        normalized(chain.get("created_at")),
        str(chain.get("_id") or ""),
    )


def _fallback_name(session: dict[str, Any]) -> str:
    name = _safe_text(session.get("name"))
    if name:
        return name
    title = _safe_text(session.get("title"), "TeraBox")
    return re.sub(
        r"\s*\(part\s+\d+/\d+\)\s*$", "", title, flags=re.IGNORECASE
    ).strip() or "TeraBox"


def _synthetic_chain(chain_id: str, sessions: list[dict[str, Any]]) -> dict[str, Any]:
    """Represent old/orphaned session records without modifying MongoDB."""
    first = sessions[0]
    created = [item.get("created_at") for item in sessions if item.get("created_at")]
    updated = [item.get("updated_at") for item in sessions if item.get("updated_at")]
    return {
        "_id": chain_id,
        "owner_id": first.get("owner_id"),
        "name": _fallback_name(first),
        "created_at": min(created) if created else None,
        "updated_at": max(updated) if updated else None,
        "chain_queue_id": first.get("chain_queue_id"),
        "chain_queue_index": first.get("chain_queue_index"),
        "chain_queue_total": first.get("chain_queue_total"),
    }


def render_history(
    owner_id: int,
    chains: list[dict[str, Any]],
    sessions: list[dict[str, Any]],
    *,
    generated_at: datetime | None = None,
) -> str:
    """Render every chain/session into a single plain-text report."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for session in sessions:
        grouped[_chain_key(session)].append(session)
    for chain_sessions in grouped.values():
        chain_sessions.sort(key=_session_sort_key)

    known = {str(chain.get("_id")) for chain in chains}
    complete_chains = list(chains)
    for chain_id, chain_sessions in grouped.items():
        if chain_id not in known:
            complete_chains.append(_synthetic_chain(chain_id, chain_sessions))
    complete_chains.sort(key=_history_sort_key, reverse=True)

    generated_at = generated_at or datetime.now(timezone.utc)
    lines = [
        "Your TeraBox chains — complete session history",
        f"Owner: {owner_id}",
        f"Generated: {_format_time(generated_at)}",
        f"Chains: {len(complete_chains)}",
        f"Sessions: {len(sessions)}",
        "",
    ]

    for chain_number, chain in enumerate(complete_chains, 1):
        chain_id = str(chain.get("_id") or "unknown")
        chain_sessions = grouped.get(chain_id, [])
        name = _safe_text(chain.get("name"), "TeraBox")
        completed = sum(
            1 for session in chain_sessions if session.get("state") == "completed"
        )
        lines.extend(
            [
                "=" * 78,
                f"{chain_number}. 📁 {name}",
                f"Chain: {chain_id}",
                f"Progress: {completed}/{len(chain_sessions)} session(s) completed",
            ]
        )
        queue_id = chain.get("chain_queue_id")
        if queue_id:
            lines.append(
                f"Queue: {_safe_text(queue_id)} · "
                f"{int(chain.get('chain_queue_index') or 1)}/"
                f"{int(chain.get('chain_queue_total') or 1)}"
            )
        lines.extend(
            [
                f"Created: {_format_time(chain.get('created_at'))}",
                f"Updated: {_format_time(chain.get('updated_at'))}",
            ]
        )

        if not chain_sessions:
            lines.append("└── No sessions")
        for index, session in enumerate(chain_sessions):
            branch = "└──" if index == len(chain_sessions) - 1 else "├──"
            part_index = int(session.get("part_index") or 1)
            total_parts = int(session.get("total_parts") or len(chain_sessions) or 1)
            state = _safe_text(session.get("state"), "unknown")
            if session.get("skipped_by_user"):
                state = "skipping (uploads finishing)" if state == "running" else "skipped"
            lines.append(
                f"{branch} Part {part_index}/{total_parts} · {state} · "
                f"{int(session.get('total_files') or 0)} file(s) · "
                f"{_human_size(session.get('part_bytes'))} · "
                f"{_safe_text(session.get('_id'), 'unknown')}"
            )
        lines.append("")

    if not complete_chains:
        lines.append("No persistent TeraBox sessions were found for this owner.")
    return "\n".join(lines).rstrip() + "\n"


def fetch_history(
    mongo_uri: str,
    database_name: str,
    owner_id: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read TeraBox chains and sessions without mutating the database."""
    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=20_000,
        connectTimeoutMS=20_000,
    )
    try:
        client.admin.command("ping")
        database = client[database_name]
        query = {"owner_id": int(owner_id)}
        chains = list(
            database[CHAINS_COLLECTION]
            .find(query)
            .sort(
                [
                    ("updated_at", DESCENDING),
                    ("created_at", DESCENDING),
                    ("_id", DESCENDING),
                ]
            )
        )
        sessions = list(database[SESSIONS_COLLECTION].find(query))
        return chains, sessions
    finally:
        client.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export all TeraBox chain/session history from MongoDB."
    )
    parser.add_argument(
        "--owner-id",
        type=int,
        default=int(os.environ.get("TERABOX_EXPORT_OWNER_ID", DEFAULT_OWNER_ID)),
        help=f"Telegram owner ID (default: {DEFAULT_OWNER_ID})",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("LAZYLEECH_DB_NAME", DEFAULT_DATABASE),
        help=f"MongoDB database name (default: {DEFAULT_DATABASE})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output path (default: terabox_sessions_<owner-id>.txt)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    if load_dotenv is not None:
        load_dotenv()
    args = parse_args(argv)
    mongo_uri = os.environ.get("DB_URL", "").strip()
    if not mongo_uri:
        mongo_uri = getpass.getpass("MongoDB URL (input hidden): ").strip()
    if not mongo_uri:
        print("DB_URL is required.", file=sys.stderr)
        return 2

    output = args.output or Path(f"terabox_sessions_{args.owner_id}.txt")
    try:
        chains, sessions = fetch_history(mongo_uri, args.database, args.owner_id)
        report = render_history(args.owner_id, chains, sessions)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8", newline="\n")
    except Exception as error:
        print(f"Export failed: {error}", file=sys.stderr)
        return 1

    print(
        f"Exported {len(chains)} chain record(s) and {len(sessions)} session "
        f"record(s) to {output.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
