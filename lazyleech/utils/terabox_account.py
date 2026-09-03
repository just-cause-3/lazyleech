"""Authenticated TeraBox "My Cloud" listing and batch-download helpers."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urljoin, urlparse

import aiohttp

from .terabox import (
    DEFAULT_TERABOX_ENDPOINT,
    TERABOX_APP_PARAMS,
    TERABOX_USER_AGENT,
    TeraboxError,
    TeraboxResolver,
    _cookie_site,
    _needs_verification,
    _safe_name,
)


TERABOX_ACCOUNT_SOURCE_PREFIX = "terabox-account:"
TERABOX_ACCOUNT_LIST_PAGE_SIZE = 1000
TERABOX_ACCOUNT_MAX_PAGES = 1000


@dataclass(frozen=True)
class TeraboxBatchDownload:
    url: str
    headers: list[str]
    max_connections: int = 1


def normalize_account_path(value: str) -> str:
    """Normalize a user-supplied account path without permitting traversal."""
    raw = str(value or "").strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {'"', "'"}:
        raw = raw[1:-1].strip()
    raw = raw.replace("\\", "/")
    if not raw:
        raise TeraboxError("A TeraBox account folder path is required")
    if any(ord(character) < 32 for character in raw):
        raise TeraboxError("The TeraBox account path contains control characters")
    parts = [part for part in raw.split("/") if part]
    if any(part in {".", ".."} for part in parts):
        raise TeraboxError("Relative path traversal is not allowed")
    normalized = "/" + "/".join(parts)
    if len(normalized.encode("utf-8")) > 4096:
        raise TeraboxError("The TeraBox account path is too long")
    return normalized if parts else "/"


def account_source_url(path: str) -> str:
    return TERABOX_ACCOUNT_SOURCE_PREFIX + normalize_account_path(path)


def is_account_directory(item: dict) -> bool:
    value = item.get("isdir")
    return value is True or str(value).strip().lower() in {"1", "true"}


def _rc4_signature(key: str, value: str) -> str:
    """Generate the signature used by TeraBox's authenticated download API."""
    key_bytes = str(key or "").encode("utf-8")
    value_bytes = str(value or "").encode("utf-8")
    if not key_bytes or not value_bytes:
        raise TeraboxError("TeraBox did not provide complete download signing data")

    state = list(range(256))
    offset = 0
    for index in range(256):
        offset = (offset + state[index] + key_bytes[index % len(key_bytes)]) % 256
        state[index], state[offset] = state[offset], state[index]

    left = right = 0
    output = bytearray()
    for value_byte in value_bytes:
        left = (left + 1) % 256
        right = (right + state[left]) % 256
        state[left], state[right] = state[right], state[left]
        output.append(value_byte ^ state[(state[left] + state[right]) % 256])
    return base64.b64encode(output).decode("ascii")


class TeraboxAccountClient:
    """Use a configured ``ndus`` cookie for the owner's private drive."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        cookie: str,
        endpoint: str = DEFAULT_TERABOX_ENDPOINT,
        timeout: int = 30,
    ) -> None:
        self.resolver = TeraboxResolver(session, cookie, endpoint, timeout=timeout)
        # The account file APIs are bootstrapped by the My Cloud page. The
        # legacy /ai/index entrypoint currently bounces between the dm and www
        # hosts, while /main supplies the account jsToken used below.
        if getattr(self.resolver, "bootstrap_path", "").startswith("/ai/"):
            self.resolver.bootstrap_path = "/main"
        self.cookie = self.resolver.ndus
        self.timeout = self.resolver.timeout

    @property
    def origin(self) -> str:
        return self.resolver.origin

    @property
    def download_headers(self) -> list[str]:
        """Headers needed by the same-site streaming batch endpoint."""
        return [
            f"User-Agent: {TERABOX_USER_AGENT}",
            f"Referer: {self.origin}/main",
            f"Cookie: lang=en; ndus={self.cookie}",
        ]

    async def _ensure_ready(self, *, refresh: bool = False) -> None:
        self.resolver._share_authenticated = True
        if refresh:
            self.resolver.js_token = ""
        if not self.resolver.js_token:
            await self.resolver._bootstrap()

    async def _account_get(
        self,
        path: str,
        params: dict,
        *,
        retry_verification: bool = True,
    ) -> dict:
        await self._ensure_ready()
        request_params = {
            **TERABOX_APP_PARAMS,
            "jsToken": self.resolver.js_token,
            **params,
        }
        data = await self.resolver._json_get(
            path,
            request_params,
            authenticated=True,
        )
        if _needs_verification(data) and retry_verification:
            await self._ensure_ready(refresh=True)
            request_params["jsToken"] = self.resolver.js_token
            data = await self.resolver._json_get(
                path,
                request_params,
                authenticated=True,
            )
        if data.get("errno") not in (0, "0", None):
            message = data.get("show_msg") or data.get("errmsg") or "request failed"
            if _needs_verification(data):
                raise TeraboxError(
                    "TeraBox requires account verification. Complete it in the "
                    "official website, save a fresh ndus cookie, and retry"
                )
            raise TeraboxError(f"TeraBox account request failed: {message}")
        return data

    async def list_directory(self, path: str) -> list[dict]:
        """Return every immediate child of an account directory."""
        directory = normalize_account_path(path)
        output = []
        seen = set()
        for page in range(1, TERABOX_ACCOUNT_MAX_PAGES + 1):
            data = await self._account_get(
                "/api/list",
                {
                    "dir": directory,
                    "num": str(TERABOX_ACCOUNT_LIST_PAGE_SIZE),
                    "page": str(page),
                    "order": "name",
                    "desc": "0",
                    "showempty": "0",
                },
            )
            items = data.get("list") or []
            for item in items:
                identity = str(item.get("fs_id") or item.get("path") or "")
                if not identity or identity in seen:
                    continue
                seen.add(identity)
                output.append(item)
            has_more = data.get("has_more")
            if not items or (
                len(items) < TERABOX_ACCOUNT_LIST_PAGE_SIZE
                and str(has_more).lower() not in {"1", "true"}
            ):
                break
        else:
            raise TeraboxError("TeraBox directory pagination exceeded the safety limit")
        return output

    async def get_directory(self, path: str) -> dict:
        """Resolve a named account folder and return its stable ID."""
        directory = normalize_account_path(path)
        if directory == "/":
            return {
                "server_filename": "TeraBox Root",
                "path": "/",
                "fs_id": None,
                "isdir": 1,
            }
        pure_path = PurePosixPath(directory)
        parent = str(pure_path.parent)
        if parent == ".":
            parent = "/"
        for item in await self.list_directory(parent):
            raw_item_path = str(item.get("path") or "").strip()
            if not raw_item_path:
                continue
            try:
                item_path = normalize_account_path(raw_item_path)
            except TeraboxError:
                continue
            if item_path == directory and is_account_directory(item):
                return item
        raise TeraboxError(f'TeraBox account folder not found: "{directory}"')

    async def home_info(self) -> dict:
        data = await self._account_get("/api/home/info", {})
        info = data.get("data") or {}
        if not info.get("uk"):
            raise TeraboxError("TeraBox account metadata did not include a user ID")
        return info

    async def authorize_batch_download(
        self,
        fs_ids: list[int | str],
        archive_name: str,
        preferred_connections: int = 16,
    ) -> TeraboxBatchDownload:
        """Return a fresh, preflighted URL for one server-generated ZIP archive."""
        safe_archive_name(archive_name)
        preferred_connections = max(1, min(int(preferred_connections), 16))
        normalized_ids = []
        for value in fs_ids:
            try:
                normalized_ids.append(int(value))
            except (TypeError, ValueError) as error:
                raise TeraboxError("A TeraBox batch item has an invalid file ID") from error
        if not normalized_ids:
            raise TeraboxError("A TeraBox batch download must contain at least one item")

        info = await self.home_info()
        signature = _rc4_signature(info.get("sign3"), info.get("sign1"))
        data = await self._account_get(
            "/api/download",
            {
                "type": "batch",
                "fidlist": json.dumps(normalized_ids, separators=(",", ":")),
                "sign": signature,
                "timestamp": str(info.get("timestamp") or info.get("task_time") or ""),
                "vip": "2",
                "need_speed": "0",
                "bdstoken": "",
            },
        )
        dlink = data.get("dlink")
        if isinstance(dlink, list) and dlink:
            first = dlink[0]
            dlink = first.get("dlink") if isinstance(first, dict) else first
        dlink = str(dlink or "").strip()
        if not dlink:
            raise TeraboxError("TeraBox did not return a batch download URL")

        parsed = urlparse(dlink)
        origin_host = urlparse(self.origin).hostname or ""
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.port not in (None, 443)
            or _cookie_site(parsed.hostname) != _cookie_site(origin_host)
        ):
            raise TeraboxError("TeraBox returned an unsafe batch download URL")
        current_url = dlink
        visited_urls = set()
        for _redirect in range(3):
            if current_url in visited_urls:
                raise TeraboxError("TeraBox returned a batch download redirect loop")
            visited_urls.add(current_url)
            current_host = urlparse(current_url).hostname or ""
            same_cookie_site = _cookie_site(current_host) == _cookie_site(origin_host)
            request_headers = self.resolver._request_headers(
                authenticated=same_cookie_site
            )
            request_headers["Referer"] = f"{self.origin}/main"
            request_headers["Range"] = "bytes=0-0"
            async with self.resolver.session.get(
                current_url,
                headers=request_headers,
                timeout=self.timeout,
                allow_redirects=False,
            ) as response:
                body = await response.content.read(512)
                if response.status in {301, 302, 303, 307, 308}:
                    target = urljoin(
                        str(response.url), response.headers.get("Location", "")
                    )
                    target_parts = urlparse(target)
                    if (
                        target_parts.scheme != "https"
                        or not target_parts.hostname
                        or target_parts.username
                        or target_parts.password
                        or target_parts.port not in (None, 443)
                    ):
                        raise TeraboxError("TeraBox returned an unsafe batch redirect")
                    current_url = target
                    continue
                if response.status in {200, 206}:
                    if response.headers.get("Content-Type", "").lower().startswith(
                        "application/json"
                    ):
                        try:
                            error_data = json.loads(body)
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            error_data = {}
                        message = (
                            error_data.get("errmsg")
                            or error_data.get("show_msg")
                            or "batch stream was rejected"
                        )
                        raise TeraboxError(
                            f"TeraBox batch download failed: {message}"
                        )
                    download_headers = [
                        f"User-Agent: {TERABOX_USER_AGENT}",
                        f"Referer: {self.origin}/main",
                    ]
                    if same_cookie_site:
                        download_headers.append(f"Cookie: lang=en; ndus={self.cookie}")
                    return TeraboxBatchDownload(
                        current_url, download_headers, preferred_connections
                    )
                raise TeraboxError(
                    "TeraBox batch download authorization returned HTTP "
                    f"{response.status}"
                )
        raise TeraboxError("TeraBox batch download exceeded the redirect limit")


def safe_archive_name(value: str, fallback: str = "terabox-batch.zip") -> str:
    base = _safe_name(value, fallback=fallback)
    base = re.sub(r"(?i)\.zip$", "", base).strip(" .") or "terabox-batch"
    return base + ".zip"


__all__ = [
    "TERABOX_ACCOUNT_SOURCE_PREFIX",
    "TeraboxAccountClient",
    "TeraboxBatchDownload",
    "account_source_url",
    "is_account_directory",
    "normalize_account_path",
    "safe_archive_name",
]
