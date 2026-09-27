import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import aiohttp

from lazyleech.plugins import torrent_chain
from lazyleech.utils.aria2 import Aria2Error, aria2_request
from lazyleech.utils.bunkr_sessions import (
    FILE_DOWNLOADED,
    SESSION_PAUSED,
    SESSION_RUNNING,
)
from lazyleech.utils.terabox_sessions import FILE_UPLOADED
from lazyleech.utils.torrent_sessions import TorrentSessionStore
from lazyleech.utils.upload_worker import UploadResult


GIB = 1024**3


def torrent_file(index, name, size):
    return {
        "torrent_index": index,
        "relative_path": name,
        "filename": name.rsplit("/", 1)[-1],
        "size_bytes": size,
    }


class TorrentWorkspacePlannerTests(unittest.TestCase):
    def test_help_entry_has_display_name_and_body(self):
        display_name, body = torrent_chain.help_dict["torrent-chain"]
        self.assertEqual("Torrent Chains", display_name)
        self.assertIn("/splittorrent", body)

    def test_peak_includes_all_sources_and_largest_split_copy(self):
        files = [
            torrent_file(1, "root/small.bin", 1 * GIB),
            torrent_file(2, "root/large.bin", 3 * GIB),
        ]
        self.assertEqual(7 * GIB, torrent_chain._torrent_part_peak(files))

    def test_partition_preserves_torrent_order(self):
        files = [
            torrent_file(1, "one.bin", 1 * GIB),
            torrent_file(2, "two.bin", 3 * GIB),
            torrent_file(3, "three.bin", 1 * GIB),
        ]
        groups = torrent_chain._partition_torrent_files(files, 6 * GIB)
        self.assertEqual([[1], [2], [3]], [
            [item["torrent_index"] for item in group] for group in groups
        ])

    def test_single_file_that_cannot_fit_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "needs about"):
            torrent_chain._partition_torrent_files(
                [torrent_file(1, "large.bin", 3 * GIB)], 5 * GIB
            )

    def test_upload_mapping_uses_per_source_results_not_part_estimates(self):
        files = [
            {"_id": "s:1", **torrent_file(1, "root/video.mp4", 4 * GIB)},
            {"_id": "s:2", **torrent_file(2, "root/readme.txt", 10)},
        ]
        sent = UploadResult(
            [
                ("video-part1.mp4", "https://t.me/c/1/1"),
                ("video-part2.mp4", "https://t.me/c/1/2"),
                ("video-part3.mp4", "https://t.me/c/1/3"),
                ("readme.txt", "https://t.me/c/1/4"),
            ],
            complete=True,
            source_results=[
                {
                    "relative_name": "root/video.mp4",
                    "complete": True,
                    "uploads": [
                        ("video-part1.mp4", "https://t.me/c/1/1"),
                        ("video-part2.mp4", "https://t.me/c/1/2"),
                        ("video-part3.mp4", "https://t.me/c/1/3"),
                    ],
                },
                {
                    "relative_name": "root/readme.txt",
                    "complete": True,
                    "uploads": [("readme.txt", "https://t.me/c/1/4")],
                },
            ],
        )
        mapped = torrent_chain._map_torrent_uploads(files, sent)
        self.assertEqual(3, len(mapped["s:1"]))
        self.assertEqual("https://t.me/c/1/4", mapped["s:2"][0]["link"])

    def test_torrent_payload_validation_rejects_truncated_metadata(self):
        with self.assertRaisesRegex(ValueError, "Invalid or truncated"):
            torrent_chain._validate_torrent_payload(b"d4:infod4:name4:teste")

    def test_torrent_payload_validation_identifies_html(self):
        with self.assertRaisesRegex(ValueError, "HTML page"):
            torrent_chain._validate_torrent_payload(b"<!doctype html><html></html>")


class TorrentFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_aria2_broken_pipe_becomes_reportable_aria2_error(self):
        class BrokenRequest:
            async def __aenter__(self):
                raise aiohttp.ClientOSError(32, "Broken pipe")

            async def __aexit__(self, *_args):
                return False

        rpc_session = SimpleNamespace(post=Mock(return_value=BrokenRequest()))
        with self.assertRaisesRegex(Aria2Error, "Aria2 RPC request failed"):
            await aria2_request(rpc_session, "aria2.addTorrent", [])

    async def test_fetch_reads_every_stream_chunk_before_validation(self):
        payload = b"d8:announce14:https://test/a4:infod4:name4:testee"

        class Content:
            async def iter_chunked(self, _size):
                yield payload[:7]
                yield payload[7:29]
                yield payload[29:]

        class Response:
            status = 200
            headers = {"Content-Length": str(len(payload))}
            content = Content()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

        http_session = SimpleNamespace(get=Mock(return_value=Response()))
        with patch.object(torrent_chain, "session", http_session):
            result = await torrent_chain._fetch_torrent_bytes(
                "https://sukebei.nyaa.si/download/4520571.torrent"
            )

        self.assertEqual(payload, result)
        request_kwargs = http_session.get.call_args.kwargs
        self.assertEqual(
            "https://sukebei.nyaa.si/view/4520571",
            request_kwargs["headers"]["Referer"],
        )

    async def test_fetch_rejects_declared_oversized_metadata_before_streaming(self):
        class Content:
            async def iter_chunked(self, _size):
                raise AssertionError("oversized response should not be consumed")
                yield b""

        class Response:
            status = 200
            headers = {"Content-Length": str(torrent_chain.TORRENT_FILE_MAX_BYTES + 1)}
            content = Content()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

        with patch.object(
            torrent_chain,
            "session",
            SimpleNamespace(get=Mock(return_value=Response())),
        ):
            with self.assertRaisesRegex(ValueError, "exceeds the 4 MiB"):
                await torrent_chain._fetch_torrent_bytes("https://example.test/a.torrent")


class TorrentChainPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_runner_selects_only_part_files_and_persists_upload_links(self):
        store = TorrentSessionStore(db_url="")
        session_doc = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=77,
            source_url="telegram:example.torrent",
            title="Example (part 1/1)",
            mode="normal",
            custom_filename=None,
            files=[
                {
                    "page_url": "telegram:example.torrent",
                    "filename": "one.bin",
                    "relative_path": "Example/one.bin",
                    "torrent_index": 4,
                    "size_bytes": 10,
                },
                {
                    "page_url": "telegram:example.torrent",
                    "filename": "two.bin",
                    "relative_path": "Example/two.bin",
                    "torrent_index": 9,
                    "size_bytes": 20,
                },
            ],
            session_fields={
                "chain_id": "runnerchain",
                "name": "Example",
                "part_index": 1,
                "total_parts": 1,
                "workspace_bytes": GIB,
                "peak_workspace_bytes": 30,
            },
        )
        await store.create_chain(
            chain_id="runnerchain",
            owner_id=123,
            chat_id=-1001,
            source_message_id=77,
            source_url="telegram:example.torrent",
            name="Example",
            mode="normal",
            total_parts=1,
            total_files=2,
            session_ids=[session_doc["_id"]],
            chain_fields={"torrent_data": b"torrent"},
        )
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            reply_text=AsyncMock(),
        )

        async def finish_upload(*_args, **kwargs):
            await kwargs["on_downloaded"]()
            sent = UploadResult(
                [
                    ("one.bin", "https://t.me/c/1/1"),
                    ("two.bin", "https://t.me/c/1/2"),
                ],
                complete=True,
                source_results=[
                    {
                        "relative_name": "Example/one.bin",
                        "complete": True,
                        "uploads": [("one.bin", "https://t.me/c/1/1")],
                    },
                    {
                        "relative_name": "Example/two.bin",
                        "complete": True,
                        "uploads": [("two.bin", "https://t.me/c/1/2")],
                    },
                ],
            )
            await kwargs["on_uploaded"](sent, None)
            return "complete"

        original_store = torrent_chain.torrent_session_store
        torrent_chain.torrent_session_store = store
        torrent_chain.torrent_chain_locks.clear()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                with (
                    patch.object(torrent_chain.os, "getcwd", return_value=workdir),
                    patch.object(
                        torrent_chain,
                        "aria2_add_torrent",
                        AsyncMock(return_value="gid"),
                    ) as add_torrent,
                    patch.object(torrent_chain, "aria2_unpause", AsyncMock()),
                    patch.object(
                        torrent_chain, "handle_leech", side_effect=finish_upload
                    ),
                ):
                    await torrent_chain._run_torrent_session(
                        None, message, session_doc["_id"]
                    )
                    self.assertTrue(Path(workdir, "123").exists())
        finally:
            torrent_chain.torrent_session_store = original_store

        self.assertEqual([4, 9], add_torrent.await_args.kwargs["selected_files"])
        updated = await store.get_session(session_doc["_id"])
        self.assertEqual("completed", updated["state"])
        files = await store.list_files(session_doc["_id"])
        self.assertTrue(all(item["status"] == FILE_UPLOADED for item in files))
        self.assertEqual("https://t.me/c/1/1", files[0]["telegram_files"][0]["link"])

    async def test_creation_persists_raw_torrent_and_queues_later_parts(self):
        store = TorrentSessionStore(db_url="")
        files = [
            torrent_file(1, "Example/one.bin", 1 * GIB),
            torrent_file(2, "Example/two.bin", 3 * GIB),
        ]
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=77,
            command=["splittorrent"],
        )
        original_store = torrent_chain.torrent_session_store
        torrent_chain.torrent_session_store = store
        try:
            with patch.object(
                torrent_chain,
                "_inspect_torrent_bytes",
                AsyncMock(return_value=("Example", files)),
            ):
                chain_id, title, _files, sessions = (
                    await torrent_chain._create_torrent_chain(
                        message,
                        b"torrent-metadata",
                        "telegram:example.torrent",
                        6 * GIB,
                    )
                )
        finally:
            torrent_chain.torrent_session_store = original_store

        self.assertEqual("Example", title)
        self.assertEqual(2, len(sessions))
        self.assertEqual(
            [SESSION_RUNNING, SESSION_PAUSED],
            [item["state"] for item in sessions],
        )
        chain = await store.get_chain(chain_id, owner_id=123)
        self.assertEqual(b"torrent-metadata", chain["torrent_data"])
        second_files = await store.list_files(sessions[1]["_id"])
        self.assertEqual(2, second_files[0]["torrent_index"])

    async def test_completed_part_activates_next_persisted_part(self):
        store = TorrentSessionStore(db_url="")
        chain_id = "torrentchain"
        sessions = []
        for part in (1, 2):
            session_doc = await store.create_session(
                owner_id=123,
                chat_id=-1001,
                source_message_id=77,
                source_url="telegram:example.torrent",
                title=f"Example (part {part}/2)",
                mode="normal",
                custom_filename=None,
                files=[
                    {
                        "page_url": "telegram:example.torrent",
                        "filename": f"{part}.bin",
                        "relative_path": f"Example/{part}.bin",
                        "torrent_index": part,
                        "size_bytes": 10,
                    }
                ],
                initial_state=SESSION_RUNNING if part == 1 else SESSION_PAUSED,
                session_fields={
                    "chain_id": chain_id,
                    "name": "Example",
                    "part_index": part,
                    "total_parts": 2,
                },
            )
            sessions.append(session_doc)
        await store.create_chain(
            chain_id=chain_id,
            owner_id=123,
            chat_id=-1001,
            source_message_id=77,
            source_url="telegram:example.torrent",
            name="Example",
            mode="normal",
            total_parts=2,
            total_files=2,
            session_ids=[item["_id"] for item in sessions],
            chain_fields={"torrent_data": b"torrent"},
        )
        first_file = (await store.list_files(sessions[0]["_id"]))[0]
        await store.update_file(
            first_file["_id"], FILE_UPLOADED, telegram_files=[]
        )
        message = SimpleNamespace(reply_text=AsyncMock())

        original_store = torrent_chain.torrent_session_store
        torrent_chain.torrent_session_store = store
        torrent_chain.torrent_chain_locks.clear()
        try:
            with patch.object(torrent_chain, "_start_torrent_session") as start:
                completed = await torrent_chain._complete_torrent_session(
                    None, message, sessions[0]["_id"]
                )
        finally:
            torrent_chain.torrent_session_store = original_store

        self.assertTrue(completed)
        second = await store.get_session(sessions[1]["_id"])
        self.assertEqual(SESSION_RUNNING, second["state"])
        start.assert_called_once_with(None, message, sessions[1]["_id"])

    async def test_prepare_continue_resets_lost_upload_but_not_uploaded_file(self):
        store = TorrentSessionStore(db_url="")
        session_doc = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=77,
            source_url="telegram:example.torrent",
            title="Example",
            mode="normal",
            custom_filename=None,
            files=[
                torrent_file(1, "one.bin", 10),
                torrent_file(2, "two.bin", 10),
            ],
            session_fields={"chain_id": "chain"},
        )
        files = await store.list_files(session_doc["_id"])
        await store.update_file(files[0]["_id"], FILE_DOWNLOADED)
        await store.update_file(files[1]["_id"], FILE_UPLOADED)

        await store.prepare_continue(session_doc["_id"], -1001, 88)

        files = await store.list_files(session_doc["_id"])
        self.assertEqual("pending", files[0]["status"])
        self.assertEqual(FILE_UPLOADED, files[1]["status"])


if __name__ == "__main__":
    unittest.main()
