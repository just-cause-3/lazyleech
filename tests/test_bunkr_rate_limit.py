import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import lazyleech.plugins.leech as leech
import lazyleech.utils.aria2 as aria2
from lazyleech.utils.bunkr import _retry_after_seconds


class BunkrConnectionProfileTests(unittest.TestCase):
    def test_fresh_file_uses_normal_profile(self):
        with patch.object(leech, "BUNKR_CONNECTIONS", 4):
            self.assertEqual(4, leech._bunkr_connection_count({"defer_count": 0}))

    def test_deferred_file_uses_conservative_profile(self):
        with (
            patch.object(leech, "BUNKR_CONNECTIONS", 4),
            patch.object(leech, "BUNKR_RECOVERY_CONNECTIONS", 2),
        ):
            self.assertEqual(
                2, leech._bunkr_connection_count({"auto_defer_count": 1})
            )

    def test_repeated_or_host_slowdown_uses_single_connection(self):
        with (
            patch.object(leech, "BUNKR_CONNECTIONS", 4),
            patch.object(leech, "BUNKR_RECOVERY_CONNECTIONS", 2),
        ):
            self.assertEqual(
                1,
                leech._bunkr_connection_count(
                    {"auto_defer_count": 0}, {"slow_strikes": 2}
                ),
            )

    def test_manual_skip_does_not_reduce_connections(self):
        with patch.object(leech, "BUNKR_CONNECTIONS", 4):
            self.assertEqual(
                4,
                leech._bunkr_connection_count(
                    {"defer_count": 1, "auto_defer_count": 0}
                ),
            )

    def test_session_download_directory_is_stable(self):
        first = leech._bunkr_download_dir(123, "abcdef123456", "abcdef123456:7")
        second = leech._bunkr_download_dir(123, "abcdef123456", "abcdef123456:7")
        self.assertEqual(first, second)
        self.assertTrue(first.endswith(os.path.join("abcdef123456", "7")))


class RetryAfterTests(unittest.TestCase):
    def test_parses_delta_seconds(self):
        self.assertEqual(42.0, _retry_after_seconds("42"))

    def test_parses_http_date(self):
        now = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(
            30.0,
            _retry_after_seconds("Mon, 31 Aug 2026 12:00:30 GMT", now=now),
        )

    def test_caps_unreasonable_server_wait(self):
        self.assertEqual(300.0, _retry_after_seconds("9999"))


class Aria2DirectDownloadProfileTests(unittest.IsolatedAsyncioTestCase):
    async def test_direct_download_applies_connection_and_resume_options(self):
        request = AsyncMock(return_value={"result": "123abc0000000000"})
        with (
            patch.object(aria2, "generate_gid", AsyncMock(return_value="123abc0000000000")),
            patch.object(aria2, "aria2_request", request),
        ):
            await aria2.aria2_add_directdl(
                object(),
                123,
                "https://cdn.example/file.mp4",
                "file.mp4",
                max_connections=4,
                download_dir="stable-dir",
                resume=True,
            )

        options = request.await_args.args[2][1]
        self.assertEqual("stable-dir", options["dir"])
        self.assertEqual("4", options["max-connection-per-server"])
        self.assertEqual("4", options["split"])
        self.assertEqual("true", options["continue"])
        self.assertEqual("true", options["always-resume"])

    async def test_service_user_agent_replaces_default(self):
        request = AsyncMock(return_value={"result": "123abc0000000000"})
        with (
            patch.object(aria2, "generate_gid", AsyncMock(return_value="123abc0000000000")),
            patch.object(aria2, "aria2_request", request),
        ):
            await aria2.aria2_add_directdl(
                object(),
                123,
                "https://cdn.example/file.rar",
                headers=[
                    "User-Agent: TeraBox-Test",
                    "Referer: https://example.test/",
                ],
            )

        options = request.await_args.args[2][1]
        self.assertEqual(
            ["User-Agent: TeraBox-Test", "Referer: https://example.test/"],
            options["header"],
        )


if __name__ == "__main__":
    unittest.main()
