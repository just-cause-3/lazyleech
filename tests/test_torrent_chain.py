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
    FILE_DOWNLOADING,
    FILE_FAILED,
    SESSION_COMPLETED,
    SESSION_FAILED,
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
        groups, skipped = torrent_chain._partition_torrent_files(files, 6 * GIB)
        self.assertEqual([[1], [2], [3]], [
            [item["torrent_index"] for item in group] for group in groups
        ])
        self.assertEqual([], skipped)

    def test_single_file_that_cannot_fit_is_terminally_skipped(self):
        groups, skipped = torrent_chain._partition_torrent_files(
            [torrent_file(1, "large.bin", 3 * GIB)], 5 * GIB
        )
        self.assertEqual([], groups)
        self.assertEqual([1], [item["torrent_index"] for item in skipped])
        self.assertEqual(6 * GIB, skipped[0]["required_workspace_bytes"])
        self.assertIn("configured limit is 5.00 GB", skipped[0]["skip_reason"])

    def test_final_index_marks_workspace_skips_in_original_torrent_order(self):
        chain = [{"_id": "part1", "chain_id": "chain1", "name": "Example"}]
        files = {
            "part1": [
                {
                    "torrent_index": 2,
                    "position": 1,
                    "filename": "large.bin",
                    "relative_path": "Example/large.bin",
                    "status": FILE_FAILED,
                    "terminal_skip": True,
                    "error": "requires about 30.00 GB; configured limit is 25.00 GB",
                    "telegram_files": [],
                },
                {
                    "torrent_index": 1,
                    "position": 2,
                    "filename": "small.bin",
                    "relative_path": "Example/small.bin",
                    "status": FILE_UPLOADED,
                    "telegram_files": [
                        {"name": "small.bin", "link": "https://t.me/c/1/2"}
                    ],
                },
            ]
        }
        rendered = "\n".join(torrent_chain._torrent_index_lines(chain, files))
        self.assertIn("finished with 1 skipped file(s)", rendered)
        self.assertIn("large.bin (skipped: requires about 30.00 GB", rendered)
        self.assertLess(rendered.index("small.bin"), rendered.index("large.bin"))

    def test_creation_plan_is_one_paginated_message(self):
        chain = {
            "_id": "abc123def456",
            "owner_id": 123,
            "name": "Large Torrent",
            "workspace_bytes": 25 * GIB,
        }
        sessions = [
            {
                "_id": f"part{part:02d}",
                "part_index": part,
                "total_parts": 21,
                "total_files": 2,
                "downloadable_files": 2,
                "skipped_files": 0,
                "source_bytes": GIB,
                "peak_workspace_bytes": 2 * GIB,
                "state": SESSION_RUNNING if part == 1 else SESSION_PAUSED,
            }
            for part in range(1, 22)
        ]

        first_text, first_markup = torrent_chain._torrent_plan_page(
            chain, sessions, 1
        )
        last_text, last_markup = torrent_chain._torrent_plan_page(
            chain, sessions, 3
        )

        self.assertIn("Page:</b> 1/3", first_text)
        self.assertIn("Part 1/21", first_text)
        self.assertIn("Part 10/21", first_text)
        self.assertNotIn("Part 11/21", first_text)
        self.assertIn("Page:</b> 3/3", last_text)
        self.assertIn("Part 21/21", last_text)
        callbacks = [
            button.callback_data
            for row in first_markup.inline_keyboard
            for button in row
        ]
        self.assertIn("torrentplan_page:123:abc123def456:2", callbacks)
        last_callbacks = [
            button.callback_data
            for row in last_markup.inline_keyboard
            for button in row
        ]
        self.assertIn("torrentplan_page:123:abc123def456:2", last_callbacks)

    def test_skip_command_can_read_session_id_from_error_reply(self):
        message = SimpleNamespace(
            command=["skiptorrentsession"],
            reply_to_message=SimpleNamespace(
                empty=False,
                text=(
                    "Torrent session b6eee8f19868 needs about 25.39 GB "
                    "additional free space"
                ),
                caption=None,
            ),
        )
        self.assertEqual(
            "b6eee8f19868", torrent_chain._torrent_id_from_message(message)
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

    def test_partial_parallel_upload_keeps_each_successful_source_mapping(self):
        files = [
            {"_id": "s:1", **torrent_file(1, "root/one.bin", 10)},
            {"_id": "s:2", **torrent_file(2, "root/two.bin", 10)},
        ]
        sent = UploadResult(
            [("one.bin", "https://t.me/c/1/1")],
            complete=False,
            source_results=[
                {
                    "relative_name": "root/one.bin",
                    "complete": True,
                    "uploads": [("one.bin", "https://t.me/c/1/1")],
                },
                {
                    "relative_name": "root/two.bin",
                    "complete": False,
                    "uploads": [],
                },
            ],
        )

        mapped = torrent_chain._map_successful_torrent_uploads(files, sent)

        self.assertEqual(["s:1"], list(mapped))
        self.assertEqual("https://t.me/c/1/1", mapped["s:1"][0]["link"])

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
    async def _two_part_chain(self, store, first_state=SESSION_FAILED):
        chain_id = "abc123def456"
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
                initial_state=(SESSION_RUNNING if part == 1 else SESSION_PAUSED),
                session_fields={
                    "chain_id": chain_id,
                    "name": "Example",
                    "part_index": part,
                    "total_parts": 2,
                    "workspace_bytes": GIB,
                },
            )
            sessions.append(session_doc)
            if part == 1 and first_state != SESSION_RUNNING:
                session_doc = await store.set_state(
                    session_doc["_id"], first_state
                )
                sessions[-1] = session_doc
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
            chain_fields={
                "workspace_bytes": GIB,
                "torrent_data": b"torrent",
            },
        )
        return sessions

    async def test_skip_failed_part_cleans_marks_files_and_starts_next(self):
        store = TorrentSessionStore(db_url="")
        sessions = await self._two_part_chain(store)
        first_file = (await store.list_files(sessions[0]["_id"]))[0]
        await store.update_file(first_file["_id"], FILE_DOWNLOADING, gid="old")
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=99,
            command=["skiptorrentsession", sessions[0]["_id"]],
            reply_to_message=None,
            reply_text=AsyncMock(),
        )
        original_store = torrent_chain.torrent_session_store
        torrent_chain.torrent_session_store = store
        torrent_chain.torrent_chain_locks.clear()
        torrent_chain.torrent_pending_uploads.clear()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                session_dir = (
                    Path(workdir)
                    / "123"
                    / "torrent_sessions"
                    / sessions[0]["_id"]
                )
                session_dir.mkdir(parents=True)
                (session_dir / "partial.bin").write_bytes(b"partial")
                with (
                    patch.object(torrent_chain.os, "getcwd", return_value=workdir),
                    patch.object(
                        torrent_chain, "_stop_torrent_session", AsyncMock()
                    ),
                    patch.object(torrent_chain, "_start_torrent_session") as start,
                ):
                    await torrent_chain.skip_torrent_session_cmd(None, message)
                self.assertFalse(session_dir.exists())
        finally:
            torrent_chain.torrent_session_store = original_store
            torrent_chain.torrent_pending_uploads.clear()

        skipped_session = await store.get_session(sessions[0]["_id"])
        self.assertEqual(SESSION_COMPLETED, skipped_session["state"])
        self.assertTrue(skipped_session["skipped_by_user"])
        skipped_file = (await store.list_files(sessions[0]["_id"]))[0]
        self.assertEqual(FILE_FAILED, skipped_file["status"])
        self.assertTrue(skipped_file["terminal_skip"])
        self.assertEqual("Torrent session skipped by user", skipped_file["error"])
        second = await store.get_session(sessions[1]["_id"])
        self.assertEqual(SESSION_RUNNING, second["state"])
        start.assert_called_once_with(None, message, sessions[1]["_id"])
        index = "\n".join(
            torrent_chain._torrent_index_lines(
                await store.list_chain("abc123def456"),
                {
                    item["_id"]: await store.list_files(item["_id"])
                    for item in await store.list_chain("abc123def456")
                },
            )
        )
        self.assertIn("1.bin (skipped: Torrent session skipped by user)", index)

    async def test_skip_waits_for_already_queued_upload_before_cleanup(self):
        store = TorrentSessionStore(db_url="")
        sessions = await self._two_part_chain(store, first_state=SESSION_RUNNING)
        first_file = (await store.list_files(sessions[0]["_id"]))[0]
        await store.update_file(first_file["_id"], FILE_DOWNLOADED, gid=None)
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=99,
            command=["skiptorrentsession", sessions[0]["_id"]],
            reply_to_message=None,
            reply_text=AsyncMock(),
        )
        original_store = torrent_chain.torrent_session_store
        torrent_chain.torrent_session_store = store
        torrent_chain.torrent_chain_locks.clear()
        torrent_chain.torrent_pending_uploads.clear()
        torrent_chain.torrent_pending_uploads.add(sessions[0]["_id"])
        try:
            with tempfile.TemporaryDirectory() as workdir:
                session_dir = (
                    Path(workdir)
                    / "123"
                    / "torrent_sessions"
                    / sessions[0]["_id"]
                )
                session_dir.mkdir(parents=True)
                (session_dir / "ready.bin").write_bytes(b"ready")
                with (
                    patch.object(torrent_chain.os, "getcwd", return_value=workdir),
                    patch.object(
                        torrent_chain, "_stop_torrent_session", AsyncMock()
                    ),
                    patch.object(torrent_chain, "_start_torrent_session") as start,
                ):
                    await torrent_chain.skip_torrent_session_cmd(None, message)
                self.assertTrue(session_dir.exists())
        finally:
            torrent_chain.torrent_session_store = original_store
            torrent_chain.torrent_pending_uploads.clear()

        preserved = (await store.list_files(sessions[0]["_id"]))[0]
        self.assertEqual(FILE_DOWNLOADED, preserved["status"])
        first = await store.get_session(sessions[0]["_id"])
        second = await store.get_session(sessions[1]["_id"])
        self.assertEqual(SESSION_RUNNING, first["state"])
        self.assertEqual(SESSION_PAUSED, second["state"])
        start.assert_not_called()
        self.assertIn("queued Telegram upload", message.reply_text.await_args.args[0])

    async def test_creation_plan_callback_edits_the_existing_message(self):
        callback = SimpleNamespace(
            data="torrentplan_page:123:abc123def456:2",
            from_user=SimpleNamespace(id=123),
            message=SimpleNamespace(edit_text=AsyncMock()),
            answer=AsyncMock(),
        )
        markup = SimpleNamespace()
        with patch.object(
            torrent_chain,
            "_stored_torrent_plan_page",
            AsyncMock(return_value=("page two", markup)),
        ) as stored_page:
            await torrent_chain.torrent_chain_plan_page_callback(None, callback)

        stored_page.assert_awaited_once_with(123, "abc123def456", 2)
        callback.message.edit_text.assert_awaited_once_with(
            "page two",
            reply_markup=markup,
            disable_web_page_preview=True,
        )
        callback.answer.assert_awaited_once_with()

    async def test_delete_chain_cleans_every_child_before_database_history(self):
        store = TorrentSessionStore(db_url="")
        sessions = await self._two_part_chain(store)
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            command=["deletetorrentchain", "abc123def456"],
            reply_to_message=None,
            reply_text=AsyncMock(),
        )
        original_store = torrent_chain.torrent_session_store
        torrent_chain.torrent_session_store = store
        torrent_chain.torrent_chain_locks.clear()
        torrent_chain.torrent_pending_uploads.clear()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                session_dirs = []
                for session_doc in sessions:
                    session_dir = (
                        Path(workdir)
                        / "123"
                        / "torrent_sessions"
                        / session_doc["_id"]
                    )
                    session_dir.mkdir(parents=True)
                    (session_dir / "partial.bin").write_bytes(b"partial")
                    session_dirs.append(session_dir)
                with (
                    patch.object(torrent_chain.os, "getcwd", return_value=workdir),
                    patch.object(
                        torrent_chain, "_stop_torrent_session", AsyncMock()
                    ),
                ):
                    await torrent_chain.delete_torrent_chain_cmd(None, message)
                self.assertTrue(all(not path.exists() for path in session_dirs))
        finally:
            torrent_chain.torrent_session_store = original_store
            torrent_chain.torrent_pending_uploads.clear()

        self.assertIsNone(await store.get_chain("abc123def456", owner_id=123))
        self.assertIsNone(await store.get_session(sessions[0]["_id"]))
        self.assertIn("Deleted torrent chain", message.reply_text.await_args.args[0])

    async def test_delete_chain_retains_history_when_cleanup_fails(self):
        store = TorrentSessionStore(db_url="")
        await self._two_part_chain(store)
        chain_doc = await store.get_chain("abc123def456", owner_id=123)
        original_store = torrent_chain.torrent_session_store
        torrent_chain.torrent_session_store = store
        torrent_chain.torrent_chain_locks.clear()
        try:
            with (
                patch.object(torrent_chain, "_stop_torrent_session", AsyncMock()),
                patch.object(
                    torrent_chain,
                    "_cleanup_torrent_session_directory",
                    AsyncMock(side_effect=OSError("busy")),
                ),
            ):
                result, errors = await torrent_chain._delete_owned_torrent_chain(
                    chain_doc
                )
        finally:
            torrent_chain.torrent_session_store = original_store

        self.assertIsNone(result)
        self.assertTrue(errors)
        self.assertIsNotNone(
            await store.get_chain("abc123def456", owner_id=123)
        )

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
                {
                    "page_url": "telegram:example.torrent",
                    "filename": "too-large.bin",
                    "relative_path": "Example/too-large.bin",
                    "torrent_index": 12,
                    "size_bytes": 30,
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
            total_files=3,
            session_ids=[session_doc["_id"]],
            chain_fields={"torrent_data": b"torrent"},
        )
        created_files = await store.list_files(session_doc["_id"])
        await store.update_file(
            created_files[2]["_id"],
            FILE_FAILED,
            terminal_skip=True,
            error="workspace limit exceeded",
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
                    ) as handle_leech,
                ):
                    await torrent_chain._run_torrent_session(
                        None, message, session_doc["_id"]
                    )
                    self.assertTrue(Path(workdir, "123").exists())
        finally:
            torrent_chain.torrent_session_store = original_store

        self.assertEqual([4, 9], add_torrent.await_args.kwargs["selected_files"])
        self.assertEqual(
            3, handle_leech.await_args.kwargs["parallel_uploads"]
        )
        updated = await store.get_session(session_doc["_id"])
        self.assertEqual("completed", updated["state"])
        files = await store.list_files(session_doc["_id"])
        self.assertEqual(
            [FILE_UPLOADED, FILE_UPLOADED, FILE_FAILED],
            [item["status"] for item in files],
        )
        self.assertTrue(files[2]["terminal_skip"])
        self.assertEqual("https://t.me/c/1/1", files[0]["telegram_files"][0]["link"])

    async def test_partial_parallel_upload_retries_only_failed_source(self):
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
                    "torrent_index": 1,
                    "size_bytes": 10,
                },
                {
                    "page_url": "telegram:example.torrent",
                    "filename": "two.bin",
                    "relative_path": "Example/two.bin",
                    "torrent_index": 2,
                    "size_bytes": 10,
                },
            ],
            session_fields={
                "chain_id": "partialchain",
                "name": "Example",
                "part_index": 1,
                "total_parts": 1,
                "workspace_bytes": GIB,
                "peak_workspace_bytes": 20,
            },
        )
        await store.create_chain(
            chain_id="partialchain",
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

        async def partial_upload(*_args, **kwargs):
            await kwargs["on_downloaded"]()
            sent = UploadResult(
                [("one.bin", "https://t.me/c/1/1")],
                complete=False,
                source_results=[
                    {
                        "relative_name": "Example/one.bin",
                        "complete": True,
                        "uploads": [("one.bin", "https://t.me/c/1/1")],
                    },
                    {
                        "relative_name": "Example/two.bin",
                        "complete": False,
                        "uploads": [],
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
                    ),
                    patch.object(torrent_chain, "aria2_unpause", AsyncMock()),
                    patch.object(
                        torrent_chain, "handle_leech", side_effect=partial_upload
                    ),
                ):
                    await torrent_chain._run_torrent_session(
                        None, message, session_doc["_id"]
                    )
        finally:
            torrent_chain.torrent_session_store = original_store

        failed_session = await store.get_session(session_doc["_id"])
        self.assertEqual(SESSION_FAILED, failed_session["state"])
        files = await store.list_files(session_doc["_id"])
        self.assertEqual([FILE_UPLOADED, FILE_FAILED], [item["status"] for item in files])
        self.assertEqual("https://t.me/c/1/1", files[0]["telegram_files"][0]["link"])

        await store.prepare_continue(session_doc["_id"], -1001, 88)
        files = await store.list_files(session_doc["_id"])
        self.assertEqual([FILE_UPLOADED, "pending"], [item["status"] for item in files])

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

    async def test_creation_persists_oversized_file_as_terminal_skip(self):
        store = TorrentSessionStore(db_url="")
        files = [
            torrent_file(1, "Example/small.bin", 1 * GIB),
            torrent_file(2, "Example/large.bin", 13 * GIB),
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
                chain_id, _title, _files, sessions = (
                    await torrent_chain._create_torrent_chain(
                        message,
                        b"torrent-metadata",
                        "telegram:example.torrent",
                        25 * GIB,
                    )
                )
        finally:
            torrent_chain.torrent_session_store = original_store

        self.assertEqual(1, len(sessions))
        self.assertEqual(1, sessions[0]["downloadable_files"])
        self.assertEqual(1, sessions[0]["skipped_files"])
        stored = await store.list_files(sessions[0]["_id"])
        self.assertEqual(["pending", FILE_FAILED], [item["status"] for item in stored])
        self.assertTrue(stored[1]["terminal_skip"])
        self.assertIn("configured limit is 25.00 GB", stored[1]["error"])
        chain = await store.get_chain(chain_id, owner_id=123)
        self.assertEqual(1, chain["skipped_files"])

    async def test_all_oversized_files_complete_without_starting_aria2(self):
        store = TorrentSessionStore(db_url="")
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=77,
            command=["splittorrent"],
            reply_text=AsyncMock(),
        )
        original_store = torrent_chain.torrent_session_store
        torrent_chain.torrent_session_store = store
        torrent_chain.torrent_chain_locks.clear()
        try:
            with patch.object(
                torrent_chain,
                "_inspect_torrent_bytes",
                AsyncMock(
                    return_value=(
                        "Example",
                        [torrent_file(1, "Example/large.bin", 13 * GIB)],
                    )
                ),
            ):
                _chain_id, _title, _files, sessions = (
                    await torrent_chain._create_torrent_chain(
                        message,
                        b"torrent-metadata",
                        "telegram:example.torrent",
                        25 * GIB,
                    )
                )
            with (
                patch.object(
                    torrent_chain, "aria2_add_torrent", AsyncMock()
                ) as add_torrent,
                patch.object(
                    torrent_chain, "_send_torrent_chain_index", AsyncMock()
                ) as send_index,
            ):
                await torrent_chain._run_torrent_session(
                    None, message, sessions[0]["_id"]
                )
        finally:
            torrent_chain.torrent_session_store = original_store

        add_torrent.assert_not_awaited()
        send_index.assert_awaited_once()
        completed = await store.get_session(sessions[0]["_id"])
        self.assertEqual(SESSION_COMPLETED, completed["state"])
        self.assertEqual(1, completed["skipped_files"])

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
