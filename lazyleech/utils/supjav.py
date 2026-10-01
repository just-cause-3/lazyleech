# lazyleech - SupJav video resolver
# Resolves streaming links and direct MP4 downloads from SupJav

import html
import logging
import os
import re
import unicodedata
from urllib.parse import unquote, urlsplit

try:
    from curl_cffi import requests as cffi_requests
    _HAS_CURL_CFFI = True
except ImportError:
    _HAS_CURL_CFFI = False

try:
    import cloudscraper
    _HAS_CLOUDSCRAPER = True
except ImportError:
    _HAS_CLOUDSCRAPER = False

import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)

SUPREMEJAV = "https://lk1.supremejav.com/supjav.php?c={}"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

SUPJAV_URL_REGEX = re.compile(
    r"https?://(?:www\.)?supjav\.com/(?:(?:zh|ja|en)/)?(?P<id>\d+)\.html",
    re.IGNORECASE,
)


class SupJavError(Exception):
    """Raised when SupJav extraction or resolution fails."""
    pass


def _make_scraper():
    """Create a Cloudflare-capable HTTP session."""
    if _HAS_CURL_CFFI:
        return cffi_requests.Session(impersonate="chrome")
    if _HAS_CLOUDSCRAPER:
        return cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    return session


def is_supjav_url(url: str) -> bool:
    """Check if the given string is a valid SupJav video URL."""
    if not url:
        return False
    clean = str(url).strip()
    return bool(SUPJAV_URL_REGEX.search(clean))


def extract_supjav_url(text: str) -> str | None:
    """Extract a SupJav URL from text or message."""
    if not text:
        return None
    match = SUPJAV_URL_REGEX.search(text.strip())
    if match:
        return match.group(0)
    # Also support general supjav.com links
    match_general = re.search(r"https?://(?:www\.)?supjav\.com/[^\s'\"]+", text.strip(), re.IGNORECASE)
    if match_general:
        return match_general.group(0)
    return None


def sanitize_filename(name: str, max_bytes: int = 200) -> str:
    """Sanitize title into a safe filesystem filename."""
    cleaned = re.sub(r'[\\/*?:"<>|]', " ", str(name or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    normalized = unicodedata.normalize("NFC", cleaned)
    encoded = normalized.encode("utf-8")
    if len(encoded) > max_bytes:
        normalized = encoded[:max_bytes].decode("utf-8", errors="ignore").strip()
    return normalized or "supjav_video"


def _unpack_dean_edwards(packed: str) -> str | None:
    """Unpack Dean Edwards packed JavaScript."""
    match = re.search(
        r"\}\s*\(\s*(['\"].*?['\"])\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(['\"].*?['\"])\.split\((['\"].*?['\"])\)",
        packed,
        re.DOTALL,
    )
    if not match:
        return None
    p_raw = match.group(1)[1:-1]
    a = int(match.group(2))
    c = int(match.group(3))
    k_raw = match.group(4)[1:-1]
    d_raw = match.group(5)[1:-1]
    k = k_raw.split(d_raw)

    def base_n(num, b):
        digits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        if num == 0:
            return "0"
        res = []
        while num:
            res.append(digits[num % b])
            num //= b
        return "".join(reversed(res))

    lookup = {}
    for i in range(c):
        key = base_n(i, a)
        val = k[i] if i < len(k) and k[i] else key
        lookup[key] = val

    def replace_word(m):
        w = m.group(0)
        return lookup.get(w, w)

    return re.sub(r"\b\w+\b", replace_word, p_raw)


def _streamtape_direct_url(html_text: str) -> str | None:
    """Extract progressive MP4 URL from Streamtape embed HTML."""
    match = re.search(
        r"getElementById\(\s*['\"]robotlink['\"]\s*\)\.innerHTML\s*=\s*"
        r"['\"]([^'\"]*)['\"]\s*\+\s*(?:['\"]{2}\s*\+\s*)?"
        r"\(\s*['\"]([^'\"]*)['\"]\s*\)((?:\.substring\(\s*\d+\s*\))+)",
        html_text,
    )
    if not match:
        return None
    prefix, suffix, subs = match.group(1), match.group(2), match.group(3)
    s_val = suffix
    for off in re.findall(r"substring\(\s*(\d+)\s*\)", subs):
        s_val = s_val[int(off) :]
    link = (prefix + s_val).lstrip("/")
    if "get_video" not in link:
        return None
    return "https://" + link


def _extract_server_links(html_text: str) -> dict[str, str]:
    """Extract {SERVER_NAME: data_link} from SupJav page."""
    soup = BeautifulSoup(html_text, "html.parser")
    servers = {}
    for anchor in soup.select("a.btn-server[data-link]"):
        name = anchor.get_text(strip=True).upper()
        link = anchor.get("data-link", "")
        if name and link and name not in servers:
            servers[name] = link
    return servers


def resolve_supjav(url: str, timeout: int = 30) -> dict:
    """Resolve a SupJav video page into direct/HLS streams."""
    scraper = _make_scraper()
    try:
        resp = scraper.get(url, timeout=timeout)
    except Exception as exc:
        raise SupJavError(f"Failed to fetch SupJav URL: {exc}") from exc

    if getattr(resp, "status_code", 0) != 200:
        raise SupJavError(
            f"SupJav returned HTTP {getattr(resp, 'status_code', 0)} (may be Cloudflare blocked or page not found)"
        )

    soup = BeautifulSoup(resp.text, "html.parser")
    h1 = soup.find("h1")
    raw_title = (
        h1.get_text(strip=True)
        if h1
        else (soup.title.get_text(strip=True) if soup.title else "SupJav Video")
    )
    title = html.unescape(raw_title)

    img = soup.select_one("div.post img") or soup.find("meta", property="og:image")
    thumbnail = ""
    if img:
        thumbnail = (
            img.get("data-original")
            or img.get("data-src")
            or img.get("src")
            or img.get("content")
            or ""
        )

    servers = _extract_server_links(resp.text)
    if not servers:
        raise SupJavError("No video server sources found on this SupJav page (layout changed or video removed).")

    resolved_servers = {}

    # 1. Resolve FST (fc2stream.tv HLS - original quality up to 1080p)
    if "FST" in servers:
        try:
            ep = SUPREMEJAV.format(servers["FST"][::-1])
            fst_resp = scraper.get(ep, headers={"Referer": "https://supjav.com/"}, timeout=20)
            for script in re.findall(r"<script[^>]*>(.*?)</script>", fst_resp.text, re.DOTALL):
                if "eval(function" in script and "m3u8" in script:
                    unpacked = _unpack_dean_edwards(script)
                    if unpacked:
                        m = re.search(r"https?://[^'\"\\;\s]+\.m3u8[^'\"\\;\s]*", unpacked)
                        if m:
                            parts = urlsplit(str(getattr(fst_resp, "url", "") or ""))
                            origin = (
                                f"{parts.scheme}://{parts.netloc}"
                                if parts.scheme and parts.netloc
                                else "https://fc2stream.tv/"
                            )
                            resolved_servers["fst"] = {
                                "type": "hls",
                                "url": m.group(0),
                                "headers": {
                                    "Referer": str(fst_resp.url) or "https://fc2stream.tv/",
                                    "Origin": origin,
                                    "User-Agent": DEFAULT_USER_AGENT,
                                },
                            }
                            break
        except Exception as exc:
            LOGGER.warning("Failed to resolve FST server: %s", exc)

    # 2. Resolve ST (Streamtape progressive MP4)
    if "ST" in servers:
        try:
            ep = SUPREMEJAV.format(servers["ST"][::-1])
            st_resp = scraper.get(ep, headers={"Referer": "https://supjav.com/"}, timeout=20)
            direct = _streamtape_direct_url(st_resp.text)
            if direct:
                resolved_servers["st"] = {
                    "type": "direct",
                    "url": direct,
                    "headers": {
                        "Referer": str(getattr(st_resp, "url", "") or "https://streamtape.com/"),
                        "User-Agent": DEFAULT_USER_AGENT,
                    },
                }
        except Exception as exc:
            LOGGER.warning("Failed to resolve ST server: %s", exc)

    # 3. Resolve TV (HLS fallback)
    if not resolved_servers and "TV" in servers:
        try:
            ep = SUPREMEJAV.format(servers["TV"][::-1])
            tv_resp = scraper.get(ep, headers={"Referer": "https://supjav.com/"}, timeout=20)
            match = re.search(
                r"urlPlay[\s=:\'\"]+(?P<u>https?://[^\s\'\"\\]+\.m3u8[^\s\'\"\\]*)",
                tv_resp.text,
            )
            tv_url = match.group("u") if match else None
            if not tv_url:
                m_gen = re.search(r"https?://[^\s\'\"\\]+\.m3u8[^\s\'\"\\]*", tv_resp.text)
                tv_url = m_gen.group(0) if m_gen else None
            if tv_url:
                resolved_servers["tv"] = {
                    "type": "hls",
                    "url": tv_url,
                    "headers": {
                        "Referer": "https://supjav.com/",
                        "User-Agent": DEFAULT_USER_AGENT,
                    },
                }
        except Exception as exc:
            LOGGER.warning("Failed to resolve TV server: %s", exc)

    if not resolved_servers:
        raise SupJavError(
            "Could not extract any playable stream from the available servers (FST, Streamtape, TV)."
        )

    clean_stem = sanitize_filename(title)
    return {
        "url": url,
        "title": title,
        "filename": f"{clean_stem}.mp4",
        "thumbnail": thumbnail,
        "servers": resolved_servers,
    }
