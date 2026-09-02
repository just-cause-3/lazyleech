"""Standalone import surface for the TeraBox resolver.

This loader keeps the command-line downloader independent from the Telegram
bot's package initialization and its Pyrogram configuration.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_MODULE_NAME = "_lazyleech_terabox_resolver"
_MODULE_PATH = Path(__file__).parent / "lazyleech" / "utils" / "terabox.py"
_SPEC = importlib.util.spec_from_file_location(_MODULE_NAME, _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Unable to load TeraBox resolver from {_MODULE_PATH}")
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_MODULE_NAME] = _MODULE
_SPEC.loader.exec_module(_MODULE)

DEFAULT_TERABOX_ENDPOINT = _MODULE.DEFAULT_TERABOX_ENDPOINT
TERABOX_USER_AGENT = _MODULE.TERABOX_USER_AGENT
TeraboxError = _MODULE.TeraboxError
TeraboxFile = _MODULE.TeraboxFile
TeraboxResolver = _MODULE.TeraboxResolver
configured_resolver = _MODULE.configured_resolver
extract_surl = _MODULE.extract_surl
normalize_endpoint = _MODULE.normalize_endpoint
_download_url = _MODULE._download_url
_cookie_site = _MODULE._cookie_site

__all__ = [
    "DEFAULT_TERABOX_ENDPOINT",
    "TERABOX_USER_AGENT",
    "TeraboxError",
    "TeraboxFile",
    "TeraboxResolver",
    "configured_resolver",
    "extract_surl",
    "normalize_endpoint",
]
