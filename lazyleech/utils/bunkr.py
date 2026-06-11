import asyncio
import base64
import json
import random
import re
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import aiohttp
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]

API_URLS = (
    "https://get.bunkrr.su/api/_001_v2",
    "https://apidl.bunkr.ru/api/_001_v2",
)


def _b64_to_bytes(b64_str: str) -> bytes:
    return base64.b64decode(b64_str)


def _xor_with_key(data: bytes, key: str) -> str:
    key_bytes = key.encode("utf-8")
    out = bytearray(len(data))
    for i, b in enumerate(data):
        out[i] = b ^ key_bytes[i % len(key_bytes)]
    return out.decode("utf-8", errors="replace")


async def fetch_html(url: str, session: aiohttp.ClientSession) -> str:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://bunkr.ac/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
    }
    async with session.get(url, headers=headers) as response:
        response.raise_for_status()
        return await response.text()


def _extract_file_id_from_html(html: str) -> str | None:
    patterns = (
        r"""data-file-id\s*=\s*["']?(\d+)["']?""",
        r"""data-id\s*=\s*["']?(\d+)["']?""",
        r"""/file/(\d+)""",
    )
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _extract_ogname_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("h1")
    if title_tag:
        return title_tag.text.strip()
    return None


async def resolve_bunkr_url(file_id: str, session: aiohttp.ClientSession) -> str:
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "User-Agent": random.choice(USER_AGENTS),
        "Origin": "https://get.bunkrr.su",
        "Referer": f"https://get.bunkrr.su/file/{file_id}",
    }

    for api_url in API_URLS:
        try:
            async with session.post(
                api_url, json={"id": file_id}, headers=headers
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(3)
                    continue
                resp.raise_for_status()
                data = await resp.json()
                if data.get("encrypted"):
                    timestamp = data["timestamp"]
                    enc_url = data["url"]
                    key = f"SECRET_KEY_{timestamp // 3600}"
                    enc_bytes = _b64_to_bytes(enc_url)
                    decrypted_url = _xor_with_key(enc_bytes, key)
                    # For some images, the API returns a url without the http scheme prefix
                    if decrypted_url.startswith("//"):
                        decrypted_url = "https:" + decrypted_url
                    return decrypted_url
        except Exception:
            continue
    raise Exception(f"Failed to resolve bunkr file_id: {file_id}")


async def resolve_bunkr_file(
    url: str, session: aiohttp.ClientSession
) -> Tuple[str, str, str]:
    """Returns (direct_url, filename, referer) for a bunkr file URL"""
    html = await fetch_html(url, session)
    file_id = _extract_file_id_from_html(html)
    if not file_id:
        raise Exception("Could not find file_id in bunkr HTML")

    filename = _extract_ogname_from_html(html) or "unknown_file"
    direct_url = await resolve_bunkr_url(file_id, session)
    referer = f"https://get.bunkrr.su/file/{file_id}"
    return direct_url, filename, referer


def normalize_album_json(raw: str) -> str:
    out = re.sub(r"(?m)^(\s*)([A-Za-z0-9_]+):", r'\1"\2":', raw)
    out = re.sub(r",\s*([}\]])", r"\1", out)
    out = out.replace("\\'", "'")
    out = re.sub(r"\\(?![\\\\\"/bfnrtu])", r"\\\\", out)
    return out


async def extract_album_urls(
    url: str, session: aiohttp.ClientSession
) -> List[Tuple[str, str]]:
    """Returns a list of (file_url, filename) from a bunkr album URL"""
    parts = urlsplit(url)
    target_url = urlunsplit(
        (parts.scheme, parts.netloc, parts.path, "advanced=1", parts.fragment)
    )
    html = await fetch_html(target_url, session)
    soup = BeautifulSoup(html, "html.parser")

    for script in soup.find_all("script"):
        text = script.string or script.get_text()
        if not text or "window.albumFiles" not in text:
            continue
        m = re.search(r"window\.albumFiles\s*=\s*(\[.*?]);", text, re.S)
        if not m:
            continue
        normalized = normalize_album_json(m.group(1))
        try:
            album_files = json.loads(normalized)
            results = []
            for f in album_files:
                slug = f.get("slug")
                if not slug:
                    continue
                file_url = f"{parts.scheme}://{parts.netloc}/f/{slug}"
                filename = f.get("original") or f.get("name") or slug
                results.append((file_url, filename))
            return results
        except json.JSONDecodeError:
            continue

    raise Exception("Could not parse album files from bunkr HTML")


def is_bunkr_url(url: str) -> bool:
    parsed = urlsplit(url)
    return "bunkr" in parsed.netloc
