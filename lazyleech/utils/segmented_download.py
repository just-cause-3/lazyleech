"""Validated multi-range HTTP downloads exposed through an Aria-like status API.

This is intentionally narrow: callers must provide a size learned from a
successful byte-range preflight.  It exists for authenticated streaming
endpoints whose parallel range behavior is compatible with browsers/IDM but
not with Aria2's open-ended first request.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import os
import re
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

import aiohttp


# TeraBox's batch endpoint currently omits the RFC 7233 ``bytes`` unit and
# returns ``start-end/total``.  IDM accepts that shape while Aria2 abandons the
# extra connections.  Accept only these two tightly validated forms.
_CONTENT_RANGE_RE = re.compile(
    r"^(?:bytes\s+)?(\d+)-(\d+)/(\d+)$", re.IGNORECASE
)
_DOWNLOADS = {}
_RANGE_SIZE = 8 * 1024 * 1024
_READ_SIZE = 512 * 1024
_MAX_RETRIES = 8


def _headers_dict(headers):
    output = {}
    for raw_header in headers or []:
        key, separator, value = str(raw_header).partition(":")
        if separator and key.strip():
            output[key.strip()] = value.strip()
    output.setdefault("Accept", "*/*")
    output["Accept-Encoding"] = "identity"
    return output


def _safe_output_path(download_dir, filename):
    directory = os.path.abspath(download_dir)
    safe_filename = os.path.basename(str(filename or "download.bin"))
    if not safe_filename or safe_filename in {".", ".."}:
        safe_filename = "download.bin"
    output = os.path.abspath(os.path.join(directory, safe_filename))
    if os.path.commonpath((directory, output)) != directory:
        raise ValueError("Segmented download path escapes its target directory")
    return directory, output


def _segments(total_length):
    """Return IDM-sized bounded ranges covering the file exactly once."""
    return [
        (start, min(start + _RANGE_SIZE, total_length) - 1)
        for start in range(0, total_length, _RANGE_SIZE)
    ]


@dataclass
class _SegmentedDownload:
    session: aiohttp.ClientSession
    gid: str
    url: str
    filename: str
    download_dir: str
    total_length: int
    headers: list[str]
    connections_requested: int
    timeout: int
    output_path: str = ""
    status: str = "waiting"
    completed_length: int = 0
    download_speed: int = 0
    active_connections: int = 0
    error_code: str = "0"
    error_message: str = ""
    task: asyncio.Task | None = None
    started_at: float = field(default_factory=time.monotonic)
    _last_sample_at: float = field(default_factory=time.monotonic)
    _last_sample_bytes: int = 0

    def snapshot(self):
        return {
            "gid": self.gid,
            "status": self.status,
            "totalLength": str(self.total_length),
            "completedLength": str(self.completed_length),
            "uploadLength": "0",
            "downloadSpeed": str(max(0, int(self.download_speed))),
            "uploadSpeed": "0",
            "connections": str(max(0, self.active_connections)),
            "dir": self.download_dir,
            "errorCode": self.error_code,
            "errorMessage": self.error_message,
            "files": [
                {
                    "index": "1",
                    "path": self.output_path,
                    "length": str(self.total_length),
                    "completedLength": str(self.completed_length),
                    "selected": "true",
                    # Do not expose a signed URL or authentication material in
                    # status output.  The local path is sufficient downstream.
                    "uris": [],
                }
            ],
        }

    def record_bytes(self, length):
        self.completed_length += length
        now = time.monotonic()
        elapsed = now - self._last_sample_at
        if elapsed >= 0.5:
            current_speed = (self.completed_length - self._last_sample_bytes) / elapsed
            if self.download_speed:
                self.download_speed = int(self.download_speed * 0.35 + current_speed * 0.65)
            else:
                self.download_speed = int(current_speed)
            self._last_sample_at = now
            self._last_sample_bytes = self.completed_length

    async def run(self):
        self.status = "active"
        workers = []
        try:
            os.makedirs(self.download_dir, exist_ok=True)
            # A new signed batch URL describes a newly generated archive.
            # Never mix it with an unverified partial file from an older URL.
            with open(self.output_path, "wb") as output:
                output.truncate(self.total_length)

            ranges = _segments(self.total_length)
            worker_count = min(self.connections_requested, len(ranges))
            logging.info(
                "Starting validated segmented download %s: host=%s, size=%s, "
                "workers=%s, ranges=%s",
                self.gid,
                urlparse(self.url).hostname,
                self.total_length,
                worker_count,
                len(ranges),
            )
            queue = asyncio.Queue()
            for byte_range in ranges:
                queue.put_nowait(byte_range)

            async def range_worker():
                while True:
                    try:
                        start, end = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        await self._download_range(start, end)
                    finally:
                        queue.task_done()

            workers = [
                asyncio.create_task(range_worker())
                for _index in range(worker_count)
            ]
            await asyncio.gather(*workers)
            if self.completed_length != self.total_length:
                raise IOError(
                    "Segmented download finished with an incomplete byte count "
                    f"({self.completed_length}/{self.total_length})"
                )
            if os.path.getsize(self.output_path) != self.total_length:
                raise IOError("Segmented download output size is invalid")
            self.status = "complete"
            self.download_speed = 0
            self.active_connections = 0
            logging.info("Validated segmented download %s completed", self.gid)
        except asyncio.CancelledError:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self.status = "removed"
            self.download_speed = 0
            self.active_connections = 0
            raise
        except Exception as error:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self.status = "error"
            self.error_code = "24"
            self.error_message = str(error)
            self.download_speed = 0
            self.active_connections = 0

    async def _download_range(self, start, end):
        current = start
        retry = 0
        request_timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=min(max(10, self.timeout), 60),
            sock_read=max(10, self.timeout),
        )
        with open(self.output_path, "r+b", buffering=0) as output:
            output.seek(start)
            while current <= end:
                headers = _headers_dict(self.headers)
                headers["Range"] = f"bytes={current}-{end}"
                self.active_connections += 1
                try:
                    async with self.session.get(
                        self.url,
                        headers=headers,
                        allow_redirects=False,
                        timeout=request_timeout,
                    ) as response:
                        content_range = response.headers.get("Content-Range", "")
                        match = _CONTENT_RANGE_RE.fullmatch(content_range.strip())
                        # Some TeraBox batch responses ignore Range for small
                        # files and return the complete body with HTTP 200.  It
                        # is safe to accept that only when this task consists
                        # of one range covering the complete known file and
                        # the declared body length is an exact match.  Never
                        # treat HTTP 200 as a segment of a larger download.
                        content_encoding = response.headers.get(
                            "Content-Encoding", ""
                        ).strip().lower()
                        try:
                            content_length = int(
                                response.headers.get("Content-Length", "")
                            )
                        except (TypeError, ValueError):
                            content_length = -1
                        whole_body_response = (
                            response.status == 200
                            and not content_range.strip()
                            and start == 0
                            and end == self.total_length - 1
                            and content_length == self.total_length
                            and content_encoding in {"", "identity"}
                        )
                        if response.status == 206 and match:
                            response_start, response_end, response_total = map(
                                int, match.groups()
                            )
                            if (
                                response_start != current
                                or response_end != end
                                or response_total != self.total_length
                            ):
                                raise IOError(
                                    "Range server returned bytes outside the requested segment"
                                )
                        elif whole_body_response:
                            # If a previous whole-body attempt disconnected,
                            # the next HTTP 200 starts again at byte zero. Undo
                            # its partial accounting and safely overwrite it.
                            previously_written = current - start
                            if previously_written:
                                self.completed_length = max(
                                    0, self.completed_length - previously_written
                                )
                                self._last_sample_bytes = min(
                                    self._last_sample_bytes, self.completed_length
                                )
                                self._last_sample_at = time.monotonic()
                                self.download_speed = 0
                                current = start
                                output.seek(start)
                        else:
                            raise IOError(
                                "Range server returned HTTP "
                                f"{response.status} without a valid Content-Range "
                                "or exact whole-file response"
                            )
                        before = current
                        async for block in response.content.iter_chunked(_READ_SIZE):
                            if not block:
                                continue
                            remaining = end - current + 1
                            if len(block) > remaining:
                                raise IOError("Range server returned too many bytes")
                            written = output.write(block)
                            if written != len(block):
                                raise IOError("Could not write the complete range block")
                            current += len(block)
                            self.record_bytes(len(block))
                        if current <= end:
                            raise IOError(
                                "Range connection ended before its segment completed"
                            )
                        if current == before:
                            raise IOError("Range server returned an empty segment")
                        retry = 0
                except asyncio.CancelledError:
                    raise
                except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as error:
                    retry += 1
                    if retry > _MAX_RETRIES:
                        raise IOError(
                            f"Range {start}-{end} failed after {_MAX_RETRIES} retries: {error}"
                        ) from error
                    await asyncio.sleep(min(2 ** (retry - 1), 15))
                finally:
                    self.active_connections = max(0, self.active_connections - 1)


async def add_segmented_download(
    session,
    gid,
    url,
    filename,
    *,
    total_length,
    headers=None,
    connections=8,
    download_dir,
    timeout=300,
):
    parsed = urlparse(str(url or ""))
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("Segmented downloads require a valid HTTPS URL")
    total_length = int(total_length)
    if total_length <= 0:
        raise ValueError("Segmented downloads require a positive content length")
    directory, output_path = _safe_output_path(download_dir, filename)
    download = _SegmentedDownload(
        session=session,
        gid=str(gid),
        url=str(url),
        filename=os.path.basename(output_path),
        download_dir=directory,
        total_length=total_length,
        headers=list(headers or []),
        connections_requested=max(1, min(int(connections), 16)),
        timeout=max(10, int(timeout)),
        output_path=output_path,
    )
    _DOWNLOADS[download.gid] = download
    download.task = asyncio.create_task(download.run())
    return download.gid


async def segmented_tell_status(gid):
    download = _DOWNLOADS.get(str(gid))
    return copy.deepcopy(download.snapshot()) if download else None


async def segmented_tell_active():
    return [
        copy.deepcopy(download.snapshot())
        for download in list(_DOWNLOADS.values())
        if download.status in {"active", "waiting", "paused"}
    ]


async def segmented_remove(gid):
    gid = str(gid)
    download = _DOWNLOADS.get(gid)
    if download is None:
        return False
    if download.status in {"active", "waiting", "paused"}:
        download.status = "removed"
        download.download_speed = 0
        download.active_connections = 0
        if download.task and not download.task.done():
            download.task.cancel()
            await asyncio.gather(download.task, return_exceptions=True)
        return True
    _DOWNLOADS.pop(gid, None)
    return True


async def clear_segmented_downloads():
    downloads = list(_DOWNLOADS.values())
    for download in downloads:
        if download.task and not download.task.done():
            download.task.cancel()
    if downloads:
        await asyncio.gather(
            *(download.task for download in downloads if download.task),
            return_exceptions=True,
        )
    _DOWNLOADS.clear()


__all__ = [
    "add_segmented_download",
    "clear_segmented_downloads",
    "segmented_remove",
    "segmented_tell_active",
    "segmented_tell_status",
]
