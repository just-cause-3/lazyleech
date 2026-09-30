import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from lazyleech.plugins import torrent_chain as tc
from lazyleech.utils.torrent_sessions import TorrentSessionStore


GIB = 1024**3
TORRENT = b"d4:infod4:name4:test12:piece lengthi1048576eee"


class TorrentPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.workdir = self.temp.name
        self.store = TorrentSessionStore(db_url="")
        for patcher in (
            patch.object(tc, "torrent_session_store", self.store),
            patch.object(tc.os, "getcwd", return_value=self.workdir),
            patch.dict(
                os.environ,
                {"TORRENT_DOWNLOAD_ROOT": str(Path(self.workdir) / "downloads")},
            ),
            patch.object(
                tc.shutil, "disk_usage", return_value=SimpleNamespace(free=100 * GIB)
            ),
            patch.object(tc, "_remove_torrent_gid", AsyncMock()),
            patch.object(tc, "aria2_remove_result", AsyncMock()),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        tc.torrent_pipeline_lock = asyncio.Lock()
        tc.torrent_workspace_reservations.clear()
        tc.torrent_prefetch_tasks.clear()
        tc.torrent_prefetch_disabled.clear()
        tc.torrent_prefetch_contexts.clear()
        tc.torrent_pending_uploads.clear()
        tc.torrent_chain_locks.clear()
        self.parts = []
        for index, sizes in enumerate(([GIB, GIB, 3 * GIB], [GIB, GIB, 4 * GIB]), 1):
            files = [
                {
                    "page_url": "torrent:test",
                    "filename": f"{index}-{i}.bin",
                    "relative_path": f"test/{index}-{i}.bin",
                    "torrent_index": index * 10 + i,
                    "size_bytes": size,
                }
                for i, size in enumerate(sizes)
            ]
            part = await self.store.create_session(
                owner_id=123,
                chat_id=-1001,
                source_message_id=77,
                source_url="torrent:test",
                title="test",
                mode="normal",
                custom_filename=None,
                files=files,
                initial_state="running" if index == 1 else "paused",
                session_fields={
                    "chain_id": "testchain",
                    "part_index": index,
                    "total_parts": 2,
                    "workspace_bytes": 8 * GIB,
                    "peak_workspace_bytes": 8 * GIB,
                },
            )
            self.parts.append(part)
        await self.store.create_chain(
            chain_id="testchain",
            owner_id=123,
            chat_id=-1001,
            source_message_id=77,
            source_url="torrent:test",
            name="test",
            mode="normal",
            total_parts=2,
            total_files=6,
            session_ids=[part["_id"] for part in self.parts],
            chain_fields={"torrent_data": TORRENT},
        )
        self.files = await self.store.list_files(self.parts[0]["_id"])
        self.message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=77,
            reply_text=AsyncMock(),
        )
        async with tc.torrent_pipeline_lock:
            accepted, _, _ = await tc._reserve_torrent_workspace(self.parts[0], 8 * GIB)
            self.assertTrue(accepted)

    async def asyncTearDown(self):
        await tc._stop_chain_prefetch("testchain")
        await asyncio.sleep(0)
        tc.torrent_workspace_reservations.clear()
        tc.torrent_pending_uploads.clear()

    async def schedule(self, remaining, freed):
        await tc._maybe_prefetch_torrent(
            None, self.message, self.parts[0], remaining, freed
        )

    async def test_2gib_gate_and_combined_reservation_prevent_fast_download_overrun(
        self,
    ):
        gate = asyncio.Event()

        async def fast_download(*args):
            await gate.wait()

        with patch.object(
            tc, "_prefetch_torrent_files", side_effect=fast_download
        ) as worker:
            await self.schedule(self.files[1:], GIB)
            self.assertFalse(tc.torrent_prefetch_tasks)
            await self.schedule(self.files[2:], 2 * GIB)
            await asyncio.sleep(0)
            worker.assert_awaited_once()
            self.assertEqual(1, len(worker.await_args.args[-1]))
            # 6 GiB remaining upload peak leaves <2 GiB after boundary overhead.
            self.assertLessEqual(
                sum(x["budget"] for x in tc.torrent_workspace_reservations.values()),
                8 * GIB,
            )
            await self.schedule(self.files[2:], 2 * GIB)
            worker.assert_awaited_once()
            child = await self.store.get_session(self.parts[1]["_id"])
            self.assertEqual("paused", child["state"])

    async def test_completed_fast_batch_waits_then_grows_after_more_cleanup(self):
        with patch.object(tc, "_prefetch_torrent_files", AsyncMock()) as worker:
            await self.schedule(self.files[2:], 2 * GIB)
            await tc.torrent_prefetch_tasks[self.parts[1]["_id"]]
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            self.assertEqual(1, worker.await_count)
            await self.schedule([], 5 * GIB)
            await tc.torrent_prefetch_tasks[self.parts[1]["_id"]]
            self.assertEqual(2, worker.await_count)
            self.assertEqual(3, len(worker.await_args.args[-1]))
            self.assertLessEqual(
                sum(x["budget"] for x in tc.torrent_workspace_reservations.values()),
                8 * GIB,
            )

    async def test_physical_free_space_accounts_for_other_chains_unwritten_bytes(self):
        other = dict(
            self.parts[0],
            _id="otherpart",
            chain_id="otherchain",
            workspace_bytes=20 * GIB,
        )
        async with tc.torrent_pipeline_lock:
            accepted, _, _ = await tc._reserve_torrent_workspace(other, 10 * GIB)
        self.assertTrue(accepted)
        with patch.object(
            tc.shutil, "disk_usage", return_value=SimpleNamespace(free=12 * GIB)
        ):
            await self.schedule(self.files[2:], 2 * GIB)
        self.assertFalse(tc.torrent_prefetch_tasks)

    async def test_next_file_larger_than_freed_space_waits(self):
        next_files = await self.store.list_files(self.parts[1]["_id"])
        await self.store.update_file(
            next_files[0]["_id"], "pending", size_bytes=3 * GIB
        )
        await self.schedule(self.files[2:], 2 * GIB)
        self.assertFalse(tc.torrent_prefetch_tasks)

    async def test_failure_stops_writer_before_deleting_only_prefetched_child(self):
        child_dir = Path(tc._torrent_download_dir(self.parts[1]))
        parent_dir = Path(tc._torrent_download_dir(self.parts[0]))
        parent_dir.mkdir(parents=True)
        (parent_dir / "waiting-upload.bin").write_bytes(b"preserve")
        stopped = asyncio.Event()

        async def writing(*args):
            child_dir.mkdir(parents=True)
            (child_dir / "partial.bin").write_bytes(b"partial")
            try:
                await asyncio.Event().wait()
            finally:
                self.assertTrue(child_dir.exists())
                stopped.set()

        with patch.object(tc, "_prefetch_torrent_files", side_effect=writing):
            await self.schedule(self.files[2:], 2 * GIB)
            await asyncio.sleep(0)
            await tc._stop_chain_prefetch("testchain", clear=True)
        self.assertTrue(stopped.is_set())
        self.assertFalse(child_dir.exists())
        self.assertTrue((parent_dir / "waiting-upload.bin").exists())
        await self.schedule([], 5 * GIB)
        self.assertFalse(tc.torrent_prefetch_tasks)

    async def test_prefetch_completes_without_upload_or_advancing_child(self):
        chain = await self.store.get_chain("testchain")
        files = await self.store.list_files(self.parts[1]["_id"])
        with (
            patch.object(
                tc, "aria2_add_torrent", AsyncMock(return_value="prefetchgid")
            ) as add,
            patch.object(tc, "aria2_unpause", AsyncMock()),
            patch.object(
                tc, "aria2_tell_status", AsyncMock(return_value={"status": "complete"})
            ),
            patch.object(tc, "handle_leech", AsyncMock()) as upload,
        ):
            await tc._prefetch_torrent_files(
                None, self.message, self.parts[1], chain, files[:1]
            )
        upload.assert_not_awaited()
        self.assertTrue(add.await_args.kwargs["check_integrity"])
        child = await self.store.get_session(self.parts[1]["_id"])
        self.assertEqual("paused", child["state"])
        self.assertEqual("waiting for workspace", child["prefetch_phase"])
        self.assertEqual(
            "downloaded", (await self.store.list_files(child["_id"]))[0]["status"]
        )

    async def test_existing_resumed_chain_persists_each_upload_and_uses_pipeline(self):
        # No new chain fields/schema required: ordinary persisted parts work.
        callbacks = {}

        async def download(*args, **kwargs):
            callbacks.update(kwargs)
            await kwargs["on_downloaded"]()
            return "complete"

        with (
            patch.object(tc, "aria2_add_torrent", AsyncMock(return_value="gid")),
            patch.object(tc, "aria2_unpause", AsyncMock()),
            patch.object(tc, "handle_leech", side_effect=download),
            patch.object(tc, "_prefetch_torrent_files", AsyncMock()) as prefetch,
        ):
            await tc._run_torrent_session(None, self.message, self.parts[0]["_id"])
            for i in (0, 1):
                await callbacks["on_source_uploaded"](
                    {
                        "relative_name": self.files[i]["relative_path"],
                        "complete": True,
                        "source_removed": True,
                        "uploads": [
                            (self.files[i]["filename"], f"https://t.me/c/1/{i+1}")
                        ],
                    }
                )
            await tc.torrent_prefetch_tasks[self.parts[1]["_id"]]
            prefetch.assert_awaited_once()
        files = await self.store.list_files(self.parts[0]["_id"])
        self.assertEqual(
            ["uploaded", "uploaded", "downloaded"], [f["status"] for f in files]
        )
        self.assertEqual("https://t.me/c/1/1", files[0]["telegram_files"][0]["link"])

    async def test_upload_success_without_cleanup_does_not_release_workspace(self):
        callbacks = {}

        async def download(*args, **kwargs):
            callbacks.update(kwargs)
            await kwargs["on_downloaded"]()
            return "complete"

        with (
            patch.object(tc, "aria2_add_torrent", AsyncMock(return_value="gid")),
            patch.object(tc, "aria2_unpause", AsyncMock()),
            patch.object(tc, "handle_leech", side_effect=download),
        ):
            await tc._run_torrent_session(None, self.message, self.parts[0]["_id"])
            await callbacks["on_source_uploaded"](
                {
                    "relative_name": self.files[2]["relative_path"],
                    "complete": True,
                    "source_removed": False,
                    "uploads": [("large.bin", "https://t.me/c/1/99")],
                }
            )
        self.assertFalse(tc.torrent_prefetch_tasks)
        self.assertGreaterEqual(
            tc.torrent_workspace_reservations[self.parts[0]["_id"]]["budget"], 8 * GIB
        )

    async def test_handoff_joins_prefetch_and_hash_checks_stored_bytes(self):
        gate = asyncio.Event()
        child_dir = Path(tc._torrent_download_dir(self.parts[1]))

        async def download_ahead(*args):
            child_dir.mkdir(parents=True)
            (child_dir / "partial.bin").write_bytes(b"partial")
            await gate.wait()

        with patch.object(tc, "_prefetch_torrent_files", side_effect=download_ahead):
            await self.schedule(self.files[2:], 2 * GIB)
            await asyncio.sleep(0)
            prefetch_task = tc.torrent_prefetch_tasks[self.parts[1]["_id"]]
            await tc._stop_chain_prefetch("testchain")
        self.assertTrue(prefetch_task.done())
        self.assertTrue((child_dir / "partial.bin").exists())
        tc.torrent_workspace_reservations.pop(self.parts[0]["_id"], None)
        await self.store.set_state(self.parts[1]["_id"], "running")
        with (
            patch.object(tc, "aria2_add_torrent", AsyncMock(return_value="gid")) as add,
            patch.object(tc, "aria2_unpause", AsyncMock()),
            patch.object(tc, "handle_leech", AsyncMock(return_value="complete")),
        ):
            await tc._run_torrent_session(None, self.message, self.parts[1]["_id"])
        self.assertTrue(add.await_args.kwargs["check_integrity"])
        self.assertEqual(str(child_dir), add.await_args.kwargs["download_dir"])

    async def test_external_disk_pressure_pauses_and_resumes_active_prefetch(self):
        chain = await self.store.get_chain("testchain")
        files = await self.store.list_files(self.parts[1]["_id"])
        tc.torrent_workspace_reservations[self.parts[1]["_id"]] = {"budget": 2 * GIB}
        with (
            patch.object(
                tc, "aria2_add_torrent", AsyncMock(return_value="prefetchgid")
            ),
            patch.object(tc, "aria2_pause", AsyncMock()) as pause,
            patch.object(tc, "aria2_unpause", AsyncMock()) as resume,
            patch.object(
                tc,
                "aria2_tell_status",
                AsyncMock(
                    side_effect=[
                        {"status": "active"},
                        {"status": "paused"},
                        {"status": "complete"},
                    ]
                ),
            ),
            patch.object(
                tc,
                "_reserve_torrent_workspace",
                AsyncMock(
                    side_effect=[
                        (False, 3 * GIB, GIB),
                        (True, 2 * GIB, 4 * GIB),
                    ]
                ),
            ),
            patch.object(tc.asyncio, "sleep", AsyncMock()),
        ):
            await tc._prefetch_torrent_files(
                None, self.message, self.parts[1], chain, files[:1]
            )
        pause.assert_awaited_once_with(tc.session, "prefetchgid")
        self.assertEqual(2, resume.await_count)

    async def test_restart_reclaims_speculative_sibling_before_rebuilding_budget(self):
        child = self.parts[1]
        child_dir = Path(tc._torrent_download_dir(child))
        child_dir.mkdir(parents=True)
        (child_dir / "stale-prefetch.bin").write_bytes(b"partial")
        await self.store.set_state(child["_id"], "paused", prefetch_active=True)
        with (
            patch.object(tc, "aria2_add_torrent", AsyncMock(return_value="gid")),
            patch.object(tc, "aria2_unpause", AsyncMock()),
            patch.object(tc, "handle_leech", AsyncMock(return_value="complete")),
        ):
            await tc._run_torrent_session(None, self.message, self.parts[0]["_id"])
        self.assertFalse(child_dir.exists())
        self.assertFalse(
            (await self.store.get_session(child["_id"]))["prefetch_active"]
        )

    async def test_delete_does_not_race_live_uploads(self):
        tc.torrent_pending_uploads.add(self.parts[0]["_id"])
        chain = await self.store.get_chain("testchain")
        result, errors = await tc._delete_owned_torrent_chain(chain)
        self.assertIsNone(result)
        self.assertIn("uploads are still running", errors[0])
        self.assertIsNotNone(await self.store.get_chain("testchain"))


if __name__ == "__main__":
    unittest.main()
