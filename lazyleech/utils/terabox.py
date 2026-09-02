"""TeraBox share-link resolution helpers.

The request flow is based on the MIT-licensed ``terabox-node`` and
``terabox-api`` projects by Seiya Dev.  Only the read-only share listing path
is implemented here; account file management and upload APIs are deliberately
out of scope.
"""

from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import aiohttp


TERABOX_APP_PARAMS = {
    "app_id": "250528",
    "web": "1",
    "channel": "dubox",
    "clienttype": "0",
}
TERABOX_USER_AGENT = (
    "terabox;1.40.0.132;PC;PC-Windows;10.0.26100;WindowsTeraBox"
)
DEFAULT_TERABOX_ENDPOINT = "https://dm.1024terabox.com/ai/index"
_SURL_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_TEMPLATE_DATA_RE = re.compile(
    r"<script>\s*var\s+templateData\s*=\s*(\{.*?\})\s*;?\s*</script>",
    re.DOTALL,
)
_JS_TOKEN_RE = re.compile(r"window\.jsToken%20%3D%20a%7D%3Bfn%28%22(.*?)%22%29")
_COOKIE_SITE_SUFFIXES = (
    "1024terabox.com",
    "1024tera.com",
    "terabox.com",
    "terabox.app",
)


class TeraboxError(RuntimeError):
    """Raised when a TeraBox share cannot be resolved safely."""


@dataclass(frozen=True)
class TeraboxFile:
    name: str
    relative_path: str
    size: int
    download_url: str
    fs_id: str = ""


def extract_surl(value: str) -> str:
    """Extract and validate a TeraBox share code without following the URL."""

    value = (value or "").strip()
    if _SURL_RE.fullmatch(value):
        return value[1:] if value.startswith("1") else value

    parsed = urlparse(value)
    query = parse_qs(parsed.query)
    for key in ("surl", "shorturl", "key"):
        candidate = (query.get(key) or [""])[0]
        if _SURL_RE.fullmatch(candidate):
            return candidate[1:] if candidate.startswith("1") else candidate

    match = re.search(r"/s/1?([A-Za-z0-9_-]+)(?:/|$)", parsed.path)
    if match:
        return match.group(1)

    raise TeraboxError("Invalid or unsupported TeraBox share URL")


def normalize_endpoint(value: str) -> tuple[str, str]:
    """Return the HTTPS origin and optional bootstrap path for an endpoint."""

    parsed = urlparse((value or DEFAULT_TERABOX_ENDPOINT).strip())
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise TeraboxError("TERABOX_BASE_URL must be an HTTPS URL")
    if parsed.username or parsed.password or parsed.port not in (None, 443):
        raise TeraboxError("TERABOX_BASE_URL must not contain credentials or a custom port")
    origin = f"https://{parsed.hostname.lower()}"
    bootstrap_path = parsed.path if parsed.path and parsed.path != "/" else "/main"
    return origin, bootstrap_path


def _safe_name(value: str, fallback: str = "terabox-download") -> str:
    name = PurePosixPath((value or "").replace("\\", "/")).name
    name = re.sub(r'[\x00-\x1f<>:"/\\|?*]', "_", name).strip(" .")
    return name or fallback


def _download_url(value: str) -> str:
    parsed = urlparse(value or "")
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise TeraboxError("TeraBox returned an invalid download URL")
    query = parse_qs(parsed.query, keep_blank_values=True)
    for key in ("chkv", "chkbd", "chkpc", "dp-logid", "dp-callid", "r", "sh"):
        query.pop(key, None)
    query["origin"] = ["dlna"]
    flat_query = [(key, item) for key, values in query.items() for item in values]
    return urlunparse(parsed._replace(query=urlencode(flat_query)))


def _cookie_site(hostname: str) -> str:
    hostname = (hostname or "").lower().rstrip(".")
    for suffix in _COOKIE_SITE_SUFFIXES:
        if hostname == suffix or hostname.endswith("." + suffix):
            return suffix
    return hostname


class TeraboxResolver:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        cookie: str,
        endpoint: str = DEFAULT_TERABOX_ENDPOINT,
        timeout: int = 25,
    ) -> None:
        cookie = (cookie or "").strip()
        if not cookie:
            raise TeraboxError("TERABOX_COOKIE is not configured")
        if cookie.startswith("ndus="):
            cookie = cookie[5:]
        if any(character in cookie for character in "\r\n;"):
            raise TeraboxError("TERABOX_COOKIE must contain only the ndus value")

        self.session = session
        self.ndus = cookie
        self.origin, self.bootstrap_path = normalize_endpoint(endpoint)
        self.timeout = aiohttp.ClientTimeout(total=max(5, int(timeout)))
        self.js_token = ""

    @property
    def headers(self) -> dict[str, str]:
        return {
            "User-Agent": TERABOX_USER_AGENT,
            "Cookie": f"lang=en; ndus={self.ndus}",
            "Referer": self.origin + self.bootstrap_path,
            "Accept": "application/json, text/plain, */*",
        }

    async def _json_get(
        self,
        path: str,
        params: dict[str, Any],
        *,
        browser_id: bool = False,
    ) -> dict[str, Any]:
        headers = self.headers
        if browser_id:
            headers["Cookie"] += "; browserid=" + base64.b64encode(os.urandom(44)).decode()
        async with self.session.get(
            self.origin + path,
            params=params,
            headers=headers,
            timeout=self.timeout,
            allow_redirects=False,
        ) as response:
            if response.status != 200:
                raise TeraboxError(f"TeraBox returned HTTP {response.status} for {path}")
            try:
                data = await response.json(content_type=None)
            except (json.JSONDecodeError, aiohttp.ContentTypeError) as error:
                raise TeraboxError(f"TeraBox returned invalid JSON for {path}") from error
        if not isinstance(data, dict):
            raise TeraboxError(f"TeraBox returned an invalid response for {path}")
        return data

    async def _bootstrap(self) -> None:
        async with self.session.get(
            self.origin + self.bootstrap_path,
            headers=self.headers,
            timeout=self.timeout,
            allow_redirects=False,
        ) as response:
            if response.status in {301, 302, 303, 307, 308}:
                location = response.headers.get("Location", "")
                redirect = urlparse(location)
                if (
                    redirect.hostname
                    and redirect.hostname.lower() != urlparse(self.origin).hostname
                ):
                    raise TeraboxError(
                        "TeraBox attempted to redirect authentication to another host"
                    )
                raise TeraboxError(f"TeraBox bootstrap redirected to {location or 'another page'}")
            if response.status != 200:
                raise TeraboxError(f"TeraBox bootstrap returned HTTP {response.status}")
            body = await response.text()

        match = _TEMPLATE_DATA_RE.search(body)
        if match:
            try:
                data = json.loads(match.group(1))
            except json.JSONDecodeError:
                data = {}
            token = data.get("jsToken", "") if isinstance(data, dict) else ""
            encoded_match = re.search(r"%28%22(.*?)%22%29", str(token))
            self.js_token = encoded_match.group(1) if encoded_match else str(token)
        if not self.js_token:
            match = _JS_TOKEN_RE.search(body)
            self.js_token = match.group(1) if match else ""

    async def _share_list(self, surl: str, remote_dir: str = "") -> dict[str, Any]:
        if not self.js_token:
            await self._bootstrap()
        params: dict[str, Any] = {
            **TERABOX_APP_PARAMS,
            "jsToken": self.js_token,
            "shorturl": surl,
            "by": "name",
            "order": "asc",
            "num": "20000",
            "dir": remote_dir,
            "page": "1",
        }
        if not remote_dir:
            params["root"] = "1"
        data = await self._json_get("/share/list", params)
        if data.get("errno") == 4000020:
            self.js_token = ""
            await self._bootstrap()
            params["jsToken"] = self.js_token
            data = await self._json_get("/share/list", params)
        return data

    async def resolve(self, share_url: str) -> list[TeraboxFile]:
        surl = extract_surl(share_url)
        info = await self._json_get(
            "/api/shorturlinfo",
            {"shorturl": "1" + surl, "root": "1"},
            browser_id=True,
        )
        if info.get("errno") != 0:
            message = info.get("show_msg") or info.get("errmsg") or "share lookup failed"
            raise TeraboxError(f"TeraBox rejected the share: {message}")

        files: list[TeraboxFile] = []
        await self._walk(surl, "", "", files)
        if not files:
            raise TeraboxError("No downloadable files were found in this share")
        return files

    async def validate_cookie(self) -> bool:
        data = await self._json_get("/api/check/login", {})
        return data.get("errno") == 0

    async def authorize_download_url(self, download_url: str) -> str:
        """Exchange an authenticated API dlink for a cookie-free CDN URL.

        TeraBox's regional ``*-d`` host requires the account cookie and then
        redirects to a short-lived storage URL.  Redirects are intentionally
        handled manually so the cookie is never forwarded to that storage
        host.
        """

        parsed = urlparse(download_url)
        origin_host = urlparse(self.origin).hostname or ""
        if parsed.scheme != "https" or not parsed.hostname:
            raise TeraboxError("TeraBox returned an insecure download URL")
        if _cookie_site(parsed.hostname) != _cookie_site(origin_host):
            raise TeraboxError(
                "Refusing to send the TeraBox cookie to a different site"
            )

        headers = self.headers
        headers["Range"] = "bytes=0-0"
        async with self.session.get(
            download_url,
            headers=headers,
            timeout=self.timeout,
            allow_redirects=False,
        ) as response:
            await response.content.read(512)
            if response.status in {301, 302, 303, 307, 308}:
                location = response.headers.get("Location", "")
                target = urljoin(str(response.url), location)
                target_parts = urlparse(target)
                if target_parts.scheme != "https" or not target_parts.hostname:
                    raise TeraboxError("TeraBox returned an insecure CDN redirect")
                return target
            if response.status in {200, 206}:
                raise TeraboxError(
                    "TeraBox did not provide a cookie-free CDN redirect"
                )
            raise TeraboxError(
                f"TeraBox download authorization returned HTTP {response.status}"
            )

    async def _walk(
        self,
        surl: str,
        remote_dir: str,
        relative_dir: str,
        output: list[TeraboxFile],
    ) -> None:
        data = await self._share_list(surl, remote_dir)
        if data.get("errno") != 0:
            message = data.get("show_msg") or data.get("errmsg") or "file listing failed"
            raise TeraboxError(f"TeraBox could not list the share: {message}")

        for item in data.get("list") or []:
            name = _safe_name(str(item.get("server_filename") or ""))
            if str(item.get("isdir")) == "1":
                subdir = str(item.get("path") or "")
                if subdir:
                    child_relative = "/".join(part for part in (relative_dir, name) if part)
                    await self._walk(surl, subdir, child_relative, output)
                continue
            dlink = str(item.get("dlink") or "")
            if not dlink:
                continue
            relative_path = "/".join(part for part in (relative_dir, name) if part)
            output.append(
                TeraboxFile(
                    name=name,
                    relative_path=relative_path,
                    size=int(item.get("size") or 0),
                    download_url=_download_url(dlink),
                    fs_id=str(item.get("fs_id") or ""),
                )
            )


def configured_resolver(
    session: aiohttp.ClientSession,
    *,
    timeout: int = 25,
) -> TeraboxResolver:
    return TeraboxResolver(
        session,
        os.environ.get("TERABOX_COOKIE", ""),
        os.environ.get("TERABOX_BASE_URL", DEFAULT_TERABOX_ENDPOINT),
        timeout,
    )
