#!/usr/bin/env python3
"""Resolve and download files from a TeraBox share using aria2c."""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from terabox_resolver import (  # noqa: E402
    DEFAULT_TERABOX_ENDPOINT,
    TERABOX_USER_AGENT,
    TeraboxError,
    TeraboxResolver,
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("share_url", nargs="?", help="TeraBox sharing URL or surl code")
    parser.add_argument("-d", "--output-dir", default="downloads/terabox")
    parser.add_argument("-x", "--connections", type=int, default=8)
    parser.add_argument("--resolve-only", action="store_true")
    parser.add_argument("--validate-cookie", action="store_true")
    return parser.parse_args()


async def run() -> int:
    args = arguments()
    load_dotenv()
    cookie = os.environ.get("TERABOX_COOKIE", "")
    endpoint = os.environ.get("TERABOX_BASE_URL", DEFAULT_TERABOX_ENDPOINT)
    if not cookie:
        print("TERABOX_COOKIE is not configured in .env", file=sys.stderr)
        return 2

    async with aiohttp.ClientSession() as session:
        resolver = TeraboxResolver(session, cookie, endpoint)
        if args.validate_cookie:
            valid = await resolver.validate_cookie()
            print(f"TeraBox cookie valid: {'yes' if valid else 'no'}")
            return 0 if valid else 1
        if not args.share_url:
            print("A TeraBox share URL is required", file=sys.stderr)
            return 2
        files = await resolver.resolve(args.share_url)
        if not args.resolve_only:
            files = [
                replace(
                    item,
                    download_url=await resolver.authorize_download_url(
                        item.download_url
                    ),
                )
                for item in files
            ]

    total = sum(item.size for item in files)
    print(f"Resolved {len(files)} file(s), {total} bytes total")
    for item in files:
        print(f"- {item.relative_path} ({item.size} bytes)")
    if args.resolve_only:
        return 0

    aria2 = shutil.which("aria2c")
    if not aria2:
        print("aria2c is required for downloading", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    connections = max(1, min(args.connections, 16))
    payload: list[str] = []
    for item in files:
        relative = Path(*Path(item.relative_path).parts)
        target_dir = output_dir / relative.parent
        target_dir.mkdir(parents=True, exist_ok=True)
        payload.extend(
            [
                item.download_url,
                f"  dir={target_dir}",
                f"  out={relative.name}",
                f"  user-agent={TERABOX_USER_AGENT}",
                f"  referer={endpoint}",
                "  continue=true",
            ]
        )

    process = await asyncio.create_subprocess_exec(
        aria2,
        "--input-file=-",
        f"--max-connection-per-server={connections}",
        f"--split={connections}",
        "--min-split-size=1M",
        "--max-tries=10",
        "--retry-wait=3",
        "--auto-file-renaming=false",
        stdin=asyncio.subprocess.PIPE,
    )
    await process.communicate(("\n".join(payload) + "\n").encode())
    return int(process.returncode or 0)


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(run()))
    except (TeraboxError, aiohttp.ClientError, asyncio.TimeoutError) as error:
        print(f"TeraBox download failed: {error}", file=sys.stderr)
        raise SystemExit(1)
