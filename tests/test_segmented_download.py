import asyncio
import os
import tempfile
import unittest
from unittest.mock import patch

from lazyleech.utils import segmented_download


class FakeContent:
    def __init__(self, data, tracker):
        self.data = data
        self.tracker = tracker

    async def iter_chunked(self, size):
        for offset in range(0, len(self.data), size):
            await asyncio.sleep(0)
            yield self.data[offset : offset + size]


class FakeRangeResponse:
    status = 206

    def __init__(self, data, start, end, total, tracker):
        self.headers = {
            "Content-Range": f"{start}-{end}/{total}",
            "Content-Length": str(end - start + 1),
        }
        self.content = FakeContent(data[start : end + 1], tracker)
        self.tracker = tracker

    async def __aenter__(self):
        self.tracker["active"] += 1
        self.tracker["maximum"] = max(
            self.tracker["maximum"], self.tracker["active"]
        )
        return self

    async def __aexit__(self, *_args):
        self.tracker["active"] -= 1


class FakeRangeSession:
    def __init__(self, data):
        self.data = data
        self.ranges = []
        self.tracker = {"active": 0, "maximum": 0}

    def get(self, _url, *, headers, **_kwargs):
        value = headers["Range"].removeprefix("bytes=")
        start, end = map(int, value.split("-", 1))
        self.ranges.append((start, end))
        return FakeRangeResponse(
            self.data, start, end, len(self.data), self.tracker
        )


class FlakyRangeSession(FakeRangeSession):
    def get(self, _url, *, headers, **_kwargs):
        value = headers["Range"].removeprefix("bytes=")
        start, end = map(int, value.split("-", 1))
        self.ranges.append((start, end))
        response = FakeRangeResponse(
            self.data, start, end, len(self.data), self.tracker
        )
        if len(self.ranges) == 1:
            midpoint = start + (end - start + 1) // 2
            response.content = FakeContent(
                self.data[start:midpoint], self.tracker
            )
        return response


class SegmentedDownloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await segmented_download.clear_segmented_downloads()

    async def test_downloads_validated_ranges_into_one_file(self):
        data = bytes(range(251)) * 41
        session = FakeRangeSession(data)
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(segmented_download, "_RANGE_SIZE", 2048),
            patch.object(segmented_download, "_READ_SIZE", 127),
        ):
            gid = await segmented_download.add_segmented_download(
                session,
                "123abc0000000000",
                "https://storage.example/file",
                "archive.zip",
                total_length=len(data),
                connections=4,
                download_dir=directory,
            )
            for _attempt in range(100):
                status = await segmented_download.segmented_tell_status(gid)
                if status["status"] not in {"active", "waiting"}:
                    break
                await asyncio.sleep(0.01)

            self.assertEqual("complete", status["status"])
            self.assertEqual(str(len(data)), status["completedLength"])
            self.assertEqual(6, len(session.ranges))
            self.assertGreater(session.tracker["maximum"], 1)
            with open(os.path.join(directory, "archive.zip"), "rb") as output:
                self.assertEqual(data, output.read())

    async def test_active_download_can_be_removed(self):
        data = b"x" * 4096
        session = FakeRangeSession(data)
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(segmented_download, "_RANGE_SIZE", 1024),
            patch.object(segmented_download, "_READ_SIZE", 1),
        ):
            gid = await segmented_download.add_segmented_download(
                session,
                "123abc0000000001",
                "https://storage.example/file",
                "archive.zip",
                total_length=len(data),
                connections=4,
                download_dir=directory,
            )
            self.assertTrue(await segmented_download.segmented_remove(gid))
            await asyncio.sleep(0)
            status = await segmented_download.segmented_tell_status(gid)
            self.assertEqual("removed", status["status"])

    async def test_short_response_retries_from_exact_written_offset(self):
        data = bytes(range(251)) * 17
        session = FlakyRangeSession(data)
        real_sleep = asyncio.sleep
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(segmented_download, "_RANGE_SIZE", len(data)),
            patch.object(segmented_download, "_READ_SIZE", 127),
            patch.object(segmented_download.asyncio, "sleep", return_value=None),
        ):
            gid = await segmented_download.add_segmented_download(
                session,
                "123abc0000000002",
                "https://storage.example/file",
                "archive.zip",
                total_length=len(data),
                connections=1,
                download_dir=directory,
            )
            for _attempt in range(100):
                status = await segmented_download.segmented_tell_status(gid)
                if status["status"] not in {"active", "waiting"}:
                    break
                await real_sleep(0)

            self.assertEqual("complete", status["status"])
            self.assertEqual((0, len(data) - 1), session.ranges[0])
            self.assertGreater(session.ranges[1][0], 0)
            self.assertEqual(len(data) - 1, session.ranges[1][1])
            with open(os.path.join(directory, "archive.zip"), "rb") as output:
                self.assertEqual(data, output.read())

    async def test_startup_file_error_becomes_terminal_status(self):
        session = FakeRangeSession(b"data")
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                segmented_download.os,
                "makedirs",
                side_effect=OSError("No space left on device"),
            ),
        ):
            gid = await segmented_download.add_segmented_download(
                session,
                "123abc0000000003",
                "https://storage.example/file",
                "archive.zip",
                total_length=4,
                connections=1,
                download_dir=directory,
            )
            await asyncio.sleep(0)
            status = await segmented_download.segmented_tell_status(gid)

        self.assertEqual("error", status["status"])
        self.assertIn("No space left", status["errorMessage"])


if __name__ == "__main__":
    unittest.main()
