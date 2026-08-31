"""Bunkr URL resolver and album crawler.

Resolves Bunkr file/album URLs to signed direct download links using:
1. CDN variable extraction from inline JavaScript (jsCDN)
2. Signed URL tokens from the Bunkr signing API
3. Fallback download API for assets without landing pages

Ported from BunkrDownloader (https://github.com/Lysagxra/BunkrDownloader)
and adapted for lazyleech's async aiohttp-based architecture.
"""

import asyncio
import json
import logging
import random
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import PurePosixPath
from typing import Dict, List, Optional, Tuple
from urllib.parse import (
    parse_qsl,
    urlencode,
    urlparse,
    urlsplit,
    urlunparse,
    urlunsplit,
)

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ============================
# Constants
# ============================
USER_AGENT = (
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) "
    "Gecko/20100101 Firefox/136.0"
)

# API endpoints (from BunkrDownloader)
BUNKR_SIGN_API = "https://glb-apisign.cdn.cr/sign"
DOWNLOAD_API = "https://dl.bunkr.cr/api/_001_v2"
DOWNLOAD_REFERER = "https://dl.bunkr.cr/"
STATUS_PAGE = "https://status.bunkr.ru/"
FALLBACK_DOMAIN = "bunkr.cr"

# Regex patterns
JS_VARS_REGEX = re.compile(r'var\s+(\w+)\s*=\s*(".*?"|\'.*?\'|[^;]+);', re.DOTALL)

# Retry / timeout configuration
MAX_RETRIES = 5
BASE_DELAY = 2.0
DEFAULT_TIMEOUT = 30
PAGE_FETCH_TIMEOUT = 40
# Pause between album page fetches when crawling a paginated album, so a
# large album doesn't trip Bunkr's rate limiter.
PAGE_CRAWL_DELAY = 1.5
REQUEST_INTERVAL = 0.5
MAX_RATE_LIMIT_DELAY = 300.0
# Guard against a misparsed pagination nav sending us on a huge crawl.
MAX_ALBUM_PAGES = 100

# URL type mapping: True = album, False = single file/media
URL_TYPE_MAPPING = {"a": True, "f": False, "i": False, "v": False}

# Cached server status (populated once per bot lifetime)
_bunkr_status: Dict[str, str] = {}
_status_fetched = False


def _retry_after_seconds(value, now=None):
    """Parse a Retry-After delta or HTTP date, capped to a safe wait."""
    if not value:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            now = now or datetime.now(timezone.utc)
            seconds = (retry_at - now).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None
    return min(MAX_RATE_LIMIT_DELAY, max(0.0, seconds))


class _BunkrRequestThrottle:
    """Pace all Bunkr metadata calls and share server cooldowns."""

    def __init__(self, interval=REQUEST_INTERVAL):
        self.interval = interval
        self.next_request_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            loop = asyncio.get_running_loop()
            delay = self.next_request_at - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self.next_request_at = loop.time() + self.interval

    async def penalize(self, retry_after, attempt):
        server_delay = _retry_after_seconds(retry_after) or 0.0
        adaptive_delay = min(120.0, 10.0 * (2 ** max(0, attempt - 1)))
        delay = max(server_delay, adaptive_delay + random.uniform(0.5, 2.0))
        async with self.lock:
            loop = asyncio.get_running_loop()
            self.next_request_at = max(self.next_request_at, loop.time() + delay)
        return delay


_bunkr_request_throttle = _BunkrRequestThrottle()


# ============================
# HTTP Helpers
# ============================
def _random_headers(referer: Optional[str] = None) -> dict:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Referer": referer or DOWNLOAD_REFERER,
    }


def _replace_domain_with_fallback(url: str) -> str:
    """Replace the domain of a URL with the configured fallback domain."""
    parsed = urlparse(url)
    return urlunparse(parsed._replace(netloc=FALLBACK_DOMAIN))


def _album_page_url(url: str, page: int = 1, advanced: bool = True) -> str:
    """Build the URL for one page of an album.

    Any ``page``/``advanced`` already on *url* is replaced rather than appended,
    so a user-supplied ``?page=10`` can't produce ``...?page=10?page=2``. The
    caller's page choice is intentionally discarded: we always crawl the whole
    album starting from page 1.
    """
    parts = urlsplit(url)
    query = [
        (k, v)
        for k, v in parse_qsl(parts.query)
        if k not in ("page", "advanced")
    ]
    if advanced:
        query.append(("advanced", "1"))
    if page > 1:
        query.append(("page", str(page)))
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
    )


async def _fetch_page(
    url: str,
    session: aiohttp.ClientSession,
    retries: int = MAX_RETRIES,
) -> Optional[BeautifulSoup]:
    """Fetch and parse an HTML page with retry + domain fallback on 403."""
    tried_fallback = False
    for attempt in range(retries):
        try:
            headers = _random_headers()
            timeout = aiohttp.ClientTimeout(total=PAGE_FETCH_TIMEOUT)
            await _bunkr_request_throttle.wait()
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 429:
                    wait = await _bunkr_request_throttle.penalize(
                        resp.headers.get("Retry-After"), attempt + 1
                    )
                    logger.warning("Rate-limited (429) on %s, waiting %.1fs", url, wait)
                    resp.release()
                    await _bunkr_request_throttle.wait()
                    continue
                if resp.status == 403 and not tried_fallback:
                    tried_fallback = True
                    url = _replace_domain_with_fallback(url)
                    logger.info("Got 403, retrying with fallback domain: %s", url)
                    continue
                resp.raise_for_status()
                content = await resp.read()
                return BeautifulSoup(content, "html.parser")
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning(
                "Fetch attempt %d/%d failed for %s: %s",
                attempt + 1, retries, url, exc,
            )
            if attempt < retries - 1:
                delay = 2 ** (attempt + 1) + random.uniform(1, 2)
                await asyncio.sleep(delay)
    return None


# ============================
# Server Status
# ============================
async def _ensure_bunkr_status(session: aiohttp.ClientSession) -> None:
    """Fetch Bunkr CDN server status (cached for bot lifetime)."""
    global _bunkr_status, _status_fetched
    if _status_fetched:
        return
    try:
        headers = _random_headers()
        timeout = aiohttp.ClientTimeout(total=10)
        await _bunkr_request_throttle.wait()
        async with session.get(STATUS_PAGE, headers=headers, timeout=timeout) as resp:
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                items = soup.find_all(
                    "div",
                    {
                        "class": (
                            "flex items-center gap-4 py-4 "
                            "border-b border-soft last:border-b-0"
                        )
                    },
                )
                for item in items:
                    p_tag = item.find("p")
                    span_tag = item.find("span")
                    if p_tag and span_tag:
                        name = p_tag.get_text(strip=True)
                        status = span_tag.get_text(strip=True)
                        _bunkr_status[name] = status
                logger.info("Bunkr status loaded: %d servers", len(_bunkr_status))
    except Exception as exc:
        logger.warning("Failed to fetch Bunkr status page: %s", exc)
    _status_fetched = True


def _subdomain_is_offline(download_url: str) -> bool:
    """Check if a CDN subdomain is known to be offline."""
    if not _bunkr_status:
        return False
    netloc = urlparse(download_url).netloc
    subdomain = netloc.split(".")[0]
    status = _bunkr_status.get(subdomain)
    return status is not None and status != "Operational"


# ============================
# CDN Variable Extraction
# ============================
def _unescape_js_path(value: str) -> str:
    """Normalize JavaScript-escaped URL fragments."""
    return value.replace(r"\/", "/").replace(r"\\", "\\")


def _extract_page_vars(soup: BeautifulSoup) -> dict:
    """Extract CDN/runtime variables from inline script tags."""
    for script in soup.find_all("script"):
        text = script.string
        if text and "var jsCDN" in text:
            matches = JS_VARS_REGEX.findall(text)
            return {
                key: _unescape_js_path(value).strip("'\"")
                for key, value in matches
            }
    return {}


def _extract_file_id(soup: BeautifulSoup) -> Optional[str]:
    """Extract file identifier from HTML script metadata."""
    script = soup.find("script")
    if not script:
        return None
    return script.get("data-file-id")


# ============================
# Download API (fallback for assets without jsCDN)
# ============================
async def _get_download_response(
    session: aiohttp.ClientSession,
    file_id: str,
) -> Optional[str]:
    """Fetch unsigned download URL for non-landing page assets.

    Used for file types that don't expose CDN variables (e.g. archives, videos).
    Returns None instead of raising on failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)
            await _bunkr_request_throttle.wait()
            async with session.post(
                DOWNLOAD_API,
                json={"id": file_id},
                headers={
                    "User-Agent": USER_AGENT,
                    "Referer": DOWNLOAD_REFERER,
                    "Origin": DOWNLOAD_REFERER.rstrip("/"),
                },
                timeout=timeout,
            ) as resp:
                if resp.status == 429:
                    wait = await _bunkr_request_throttle.penalize(
                        resp.headers.get("Retry-After"), attempt
                    )
                    logger.warning("Rate-limited (429) on download API, waiting %.1fs", wait)
                    resp.release()
                    await _bunkr_request_throttle.wait()
                    continue
                resp.raise_for_status()
                data = await resp.json()

            base_url = data.get("mediafiles")
            path = data.get("path")

            if not base_url or not path:
                logger.warning(
                    "Download API returned unexpected response for file_id=%s: %s",
                    file_id, data,
                )
                return None

            parsed = urlparse(base_url)
            return urlunparse(parsed._replace(path=path))

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning(
                "Download API attempt %d/%d failed for file_id=%s: %s",
                attempt, MAX_RETRIES, file_id, exc,
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(BASE_DELAY * (2 ** (attempt - 1)))

    return None


# ============================
# Signing API
# ============================
async def _get_signed_url(
    session: aiohttp.ClientSession,
    item_url: str,
    soup: Optional[BeautifulSoup] = None,
) -> Optional[str]:
    """Resolve and sign a Bunkr media URL using CDN extraction + signing API.

    Resolution strategy:
        1. Extract CDN base URL from inline JavaScript (jsCDN)
        2. If missing, fallback to the download API with data-file-id
        3. Build media path from available source
        4. Request signed URL token from signing API
    """
    page_vars = _extract_page_vars(soup) if soup else {}
    cdn_url = page_vars.get("jsCDN")

    # Only use the download API when no JS CDN vars are found,
    # indicating an asset type without a standard landing page.
    file_id = _extract_file_id(soup) if soup and not page_vars else None
    unsigned_url = (
        await _get_download_response(session, file_id) if file_id else None
    )

    if not cdn_url and not unsigned_url:
        logger.warning("No CDN URL or unsigned URL found for %s", item_url)
        return None

    # Build the media path for the signing API
    media_slug = PurePosixPath(urlparse(unsigned_url or item_url).path).name
    media_path = (
        urlparse(cdn_url).path if cdn_url else f"/storage/media/{media_slug}"
    )

    # Request signed URL from signing API with retry
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)
            await _bunkr_request_throttle.wait()
            async with session.get(
                BUNKR_SIGN_API,
                params={"path": media_path},
                headers={
                    "User-Agent": USER_AGENT,
                    "Referer": DOWNLOAD_REFERER,
                    "Origin": DOWNLOAD_REFERER.rstrip("/"),
                },
                timeout=timeout,
            ) as resp:
                if resp.status == 429:
                    wait = await _bunkr_request_throttle.penalize(
                        resp.headers.get("Retry-After"), attempt
                    )
                    logger.warning("Rate-limited (429) on signing API, waiting %.1fs", wait)
                    resp.release()
                    await _bunkr_request_throttle.wait()
                    continue
                resp.raise_for_status()
                data = await resp.json()

            token = data.get("token")
            expires_at = data.get("ex")
            base_url = cdn_url or unsigned_url

            if token and expires_at and base_url:
                signed = f"{base_url}?token={token}&ex={expires_at}"
                logger.info("Signed URL obtained for %s", media_slug)
                return signed

            # API responded but returned no token → return plain CDN URL
            logger.warning("Signing API returned no token, using plain CDN URL")
            return cdn_url

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning(
                "Signing API attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc,
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(BASE_DELAY * (2 ** (attempt - 1)))

    return None


# ============================
# Cloudflare Email Decryption
# ============================
def _decrypt_cf_email(cf_email_hex: str) -> str:
    """Decrypt a Cloudflare-obfuscated email address."""
    raw_bytes = bytes.fromhex(cf_email_hex)
    key = raw_bytes[0]
    decrypted_bytes = bytes(byte ^ key for byte in raw_bytes[1:])
    return decrypted_bytes.decode("utf-8")


# ============================
# Filename Extraction
# ============================
def _get_item_filename(soup: BeautifulSoup) -> Optional[str]:
    """Extract the filename from a Bunkr file page."""
    # Try the specific Bunkr class first
    container = soup.find(
        "h1",
        {"class": "text-subs font-semibold text-base sm:text-lg truncate"},
    )
    # Fallback to any <h1>
    if not container:
        container = soup.find("h1")
    if not container:
        return None

    # Handle Cloudflare email protection in filenames
    cf_email_tag = container.find(class_="__cf_email__")
    if cf_email_tag:
        cf_email_hex = cf_email_tag.get("data-cfemail")
        if cf_email_hex:
            decrypted_email = _decrypt_cf_email(cf_email_hex)
            cf_email_tag.replace_with(decrypted_email)

    filename = container.get_text().strip()

    # Fix mojibake (UTF-8 bytes mis-decoded as Latin-1)
    try:
        filename = filename.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    return filename or None


# ============================
# Album Crawling — HTML-based with Pagination
# ============================
def _extract_item_pages(
    soup: BeautifulSoup,
    host_page: str,
) -> List[str]:
    """Extract individual item page URLs from album HTML."""
    try:
        items = soup.find_all(
            "a",
            {
                "class": "after:absolute after:z-10 after:inset-0",
                "href": True,
            },
        )
        return [f"{host_page}{item.get('href')}" for item in items]
    except AttributeError:
        return []


def _get_album_page_count(soup: BeautifulSoup) -> int:
    """Highest page number in the album's pagination nav (1 when unpaginated)."""
    pagination_nav = soup.find("nav", {"class": "pagination"})
    if pagination_nav is None:
        return 1

    # get_text() with no separator welds adjacent link labels together, so
    # <a>1</a><a>2</a><a>3</a> reads as "123". Force a separator, and also
    # take page numbers straight from the ?page= hrefs, which are unambiguous.
    page_ids = re.findall(r"\d+", pagination_nav.get_text(" "))
    page_ids += re.findall(r"[?&]page=(\d+)", str(pagination_nav))
    if not page_ids:
        return 1

    num_pages = max(int(pid) for pid in page_ids)
    if num_pages > MAX_ALBUM_PAGES:
        logger.warning(
            "Album reports %d pages, clamping to %d", num_pages, MAX_ALBUM_PAGES
        )
        return MAX_ALBUM_PAGES
    return num_pages


def _extract_next_album_pages(
    soup: BeautifulSoup,
    url: str,
) -> Optional[List[str]]:
    """Extract pagination links for subsequent album pages."""
    num_pages = _get_album_page_count(soup)
    if num_pages <= 1:
        return None

    return [
        _album_page_url(url, page, advanced=False)
        for page in range(2, num_pages + 1)
    ]


def _normalize_album_json(raw: str) -> str:
    """Normalize quasi-JSON from window.albumFiles to valid JSON."""
    out = re.sub(r"(?m)^(\s*)([A-Za-z0-9_]+):", r'\1"\2":', raw)
    out = re.sub(r",\s*([}\]])", r"\1", out)
    out = out.replace("\\'", "'")
    out = re.sub(r"\\(?![\\\"\/bfnrtu])", r"\\\\", out)
    return out


def _parse_album_files(
    soup: BeautifulSoup,
    scheme: str,
    netloc: str,
) -> Optional[List[Tuple[str, str]]]:
    """Pull (file_url, filename) tuples out of a page's window.albumFiles var.

    Returns None when the variable is absent or yields nothing usable, which
    tells the caller to fall back to HTML crawling.
    """
    for script in soup.find_all("script"):
        text = script.string or script.get_text()
        if not text or "window.albumFiles" not in text:
            continue
        m = re.search(r"window\.albumFiles\s*=\s*(\[.*?]);", text, re.S)
        if not m:
            continue
        normalized = _normalize_album_json(m.group(1))
        try:
            album_files = json.loads(normalized)
        except json.JSONDecodeError:
            continue
        results = []
        for f in album_files:
            slug = f.get("slug")
            if not slug:
                continue
            file_url = f"{scheme}://{netloc}/f/{slug}"
            filename = f.get("original") or f.get("name") or slug
            results.append((file_url, filename))
        if results:
            return results

    return None


async def _extract_album_via_js(
    url: str,
    session: aiohttp.ClientSession,
) -> Optional[List[Tuple[str, str]]]:
    """Extract album files from the window.albumFiles JS variable.

    This is the legacy approach that provides filenames directly. Albums larger
    than one page are paginated, and window.albumFiles only ever describes the
    page it was served on, so every page is crawled and the results merged.

    Returns None if the JS variable is not found on the first page.
    """
    parts = urlsplit(url)

    soup = await _fetch_page(_album_page_url(url, 1), session)
    if not soup:
        return None

    results = _parse_album_files(soup, parts.scheme, parts.netloc)
    if results is None:
        return None

    num_pages = _get_album_page_count(soup)
    for page in range(2, num_pages + 1):
        await asyncio.sleep(PAGE_CRAWL_DELAY)
        page_soup = await _fetch_page(_album_page_url(url, page), session)
        if not page_soup:
            logger.warning(
                "Failed to fetch album page %d/%d for %s", page, num_pages, url
            )
            continue
        page_results = _parse_album_files(page_soup, parts.scheme, parts.netloc)
        if not page_results:
            logger.warning(
                "No albumFiles found on page %d/%d for %s", page, num_pages, url
            )
            continue
        results.extend(page_results)

    # A file can repeat across pages if the album reflows between fetches.
    seen = set()
    deduped = []
    for file_url, filename in results:
        if file_url in seen:
            continue
        seen.add(file_url)
        deduped.append((file_url, filename))

    logger.info(
        "Extracted %d files from album JS across %d page(s) for %s",
        len(deduped), num_pages, url,
    )
    return deduped


async def _extract_album_via_html(
    url: str,
    session: aiohttp.ClientSession,
) -> List[Tuple[str, str]]:
    """Extract album files by crawling HTML item links (with pagination).

    Fallback when window.albumFiles is not available.
    """
    soup = await _fetch_page(url, session)
    if not soup:
        raise Exception(f"Failed to fetch album page: {url}")

    host_page = f"https://{urlparse(url).netloc}"
    item_pages = _extract_item_pages(soup, host_page)

    if not item_pages:
        raise Exception(f"No items found in album: {url}")

    # Handle pagination
    next_pages = _extract_next_album_pages(soup, url)
    if next_pages:
        for next_page_url in next_pages:
            next_soup = await _fetch_page(next_page_url, session)
            if next_soup:
                more_pages = _extract_item_pages(next_soup, host_page)
                item_pages.extend(more_pages)

    # Build result tuples with URL slug as filename placeholder
    results = []
    for page_url in item_pages:
        slug = page_url.rstrip("/").split("/")[-1]
        results.append((page_url, slug))

    logger.info("Extracted %d items from album HTML for %s", len(results), url)
    return results


# ============================
# Public API
# ============================
async def resolve_bunkr_file(
    url: str,
    session: aiohttp.ClientSession,
) -> Tuple[str, str, str]:
    """Resolve a Bunkr file URL to a signed direct download URL.

    Returns:
        (direct_url, filename, referer) — ready for aria2.
    """
    await _ensure_bunkr_status(session)

    soup = await _fetch_page(url, session)
    if soup is None:
        raise Exception(f"Failed to fetch bunkr page: {url}")

    # Extract filename from the page HTML
    filename = _get_item_filename(soup) or "unknown_file"

    # Resolve to a signed download URL
    signed_url = await _get_signed_url(session, url, soup)
    if not signed_url:
        raise Exception(f"Could not resolve download URL for: {filename} ({url})")

    # Check if the CDN subdomain is known to be offline
    if _subdomain_is_offline(signed_url):
        logger.warning("CDN subdomain offline for %s, attempting anyway", filename)

    return signed_url, filename, DOWNLOAD_REFERER


async def extract_album_urls(
    url: str,
    session: aiohttp.ClientSession,
) -> List[Tuple[str, str]]:
    """Extract all file URLs and filenames from a Bunkr album.

    Tries the JS-based approach first (provides proper filenames), then falls
    back to HTML crawling. Both paths walk every page of a paginated album.

    Any ``?page=N`` on *url* is ignored — the whole album is always crawled.

    Returns:
        List of (file_page_url, filename) tuples.
    """
    await _ensure_bunkr_status(session)

    # Primary: try window.albumFiles (gives proper filenames for filtering)
    try:
        js_result = await _extract_album_via_js(url, session)
        if js_result:
            return js_result
    except Exception as exc:
        logger.warning("JS album extraction failed for %s: %s", url, exc)

    # Fallback: HTML crawling with pagination support
    return await _extract_album_via_html(url, session)


def is_bunkr_url(url: str) -> bool:
    """Check if a URL belongs to the Bunkr domain."""
    parsed = urlsplit(url)
    return "bunkr" in parsed.netloc
