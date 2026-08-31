import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import lazyleech.plugins.leech as leech
from lazyleech.plugins.leech import _bunkr_session_id_from_message
from lazyleech.utils.bunkr_sessions import (
    FILE_CANCELLED,
    FILE_DOWNLOADED,
    FILE_DOWNLOADING,
    FILE_FAILED,
    FILE_PENDING,
    FILE_RESOLVING,
    SESSION_CANCELLED,
    SESSION_COMPLETED,
    SESSION_RUNNING,
    BunkrSessionStore,
)


class BunkrSessionStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = BunkrSessionStore(db_url="")
        self.session = await self.store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://bunkr.example/a/album",
            title="album",
            mode="normal",
            custom_filename=None,
            files=[
                ("https://bunkr.example/f/one", "one.mp4"),
                ("https://bunkr.example/f/two", "two.mp4"),
                ("https://bunkr.example/f/three", "three.mp4"),
            ],
        )

    async def test_create_claim_and_count_files(self):
        self.assertEqual(12, len(self.session["_id"]))
        first = await self.store.claim_next_file(self.session["_id"])
        self.assertEqual(1, first["position"])
        self.assertEqual(1, first["attempts"])

        await self.store.update_file(
            first["_id"], FILE_DOWNLOADING, gid="123abc0000000000"
        )
        found = await self.store.find_file_by_gid("123abc0000000000")
        self.assertEqual(first["_id"], found["_id"])

        await self.store.update_file(first["_id"], FILE_DOWNLOADED, gid=None)
        counts = await self.store.counts(self.session["_id"])
        self.assertEqual(1, counts[FILE_DOWNLOADED])
        self.assertEqual(2, counts[FILE_PENDING])

    async def test_continue_resets_only_unfinished_files(self):
        first = await self.store.claim_next_file(self.session["_id"])
        await self.store.update_file(first["_id"], FILE_DOWNLOADED)
        second = await self.store.claim_next_file(self.session["_id"])
        await self.store.update_file(second["_id"], FILE_FAILED, error="network")

        await self.store.prepare_continue(self.session["_id"], -1002, 99)
        files = await self.store.list_files(self.session["_id"])
        self.assertEqual(FILE_DOWNLOADED, files[0]["status"])
        self.assertEqual(FILE_PENDING, files[1]["status"])
        self.assertEqual(FILE_PENDING, files[2]["status"])
        session = await self.store.get_session(self.session["_id"])
        self.assertEqual(SESSION_RUNNING, session["state"])
        self.assertEqual(-1002, session["chat_id"])

    async def test_cancel_and_delete_session(self):
        first = await self.store.claim_next_file(self.session["_id"])
        await self.store.update_file(first["_id"], FILE_DOWNLOADED)
        await self.store.cancel_session(self.session["_id"])

        session = await self.store.get_session(self.session["_id"])
        self.assertEqual(SESSION_CANCELLED, session["state"])
        files = await self.store.list_files(self.session["_id"])
        self.assertEqual(FILE_DOWNLOADED, files[0]["status"])
        self.assertTrue(
            all(item["status"] == FILE_CANCELLED for item in files[1:])
        )

        self.assertTrue(await self.store.delete_session(self.session["_id"]))
        self.assertIsNone(await self.store.get_session(self.session["_id"]))
        self.assertEqual([], await self.store.list_files(self.session["_id"]))

    async def test_defer_active_file_moves_it_to_bottom(self):
        first = await self.store.claim_next_file(self.session["_id"])
        await self.store.update_file(
            first["_id"], FILE_DOWNLOADING, gid="123abc0000000000"
        )

        deferred = await self.store.defer_file_to_bottom(
            first["_id"], "Skipped by user"
        )

        self.assertIsNotNone(deferred)
        self.assertEqual(FILE_PENDING, deferred["status"])
        self.assertEqual(1, deferred["defer_count"])
        self.assertIsNone(deferred["gid"])
        files = await self.store.list_files(self.session["_id"])
        self.assertEqual(
            ["two.mp4", "three.mp4", "one.mp4"],
            [item["filename"] for item in files],
        )
        self.assertEqual([2, 3, 4], [item["position"] for item in files])
        next_file = await self.store.claim_next_file(self.session["_id"])
        self.assertEqual("two.mp4", next_file["filename"])

    async def test_defer_refuses_to_cycle_only_pending_file(self):
        one_file_session = await self.store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=56,
            source_url="https://bunkr.example/a/single",
            title="single",
            mode="normal",
            custom_filename=None,
            files=[("https://bunkr.example/f/only", "only.mp4")],
        )
        only_file = await self.store.claim_next_file(one_file_session["_id"])

        deferred = await self.store.defer_file_to_bottom(
            only_file["_id"], "Skipped by user"
        )

        self.assertIsNone(deferred)
        unchanged = await self.store.get_file(only_file["_id"])
        self.assertEqual(FILE_RESOLVING, unchanged["status"])

    async def test_automatic_defer_respects_per_file_limit(self):
        first = await self.store.claim_next_file(self.session["_id"])
        deferred = await self.store.defer_file_to_bottom(
            first["_id"],
            "slow",
            automatic=True,
            max_auto_defers=1,
        )
        self.assertEqual(1, deferred["auto_defer_count"])

        await self.store.update_file(first["_id"], FILE_RESOLVING)
        rejected = await self.store.defer_file_to_bottom(
            first["_id"],
            "still slow",
            automatic=True,
            max_auto_defers=1,
        )
        self.assertIsNone(rejected)

    async def test_cooling_host_is_deprioritized_but_can_fall_back(self):
        files = await self.store.list_files(self.session["_id"])
        await self.store.update_file(files[0]["_id"], cdn_host="slow.cdn.test")
        await self.store.update_file(files[1]["_id"], cdn_host="slow.cdn.test")
        await self.store.update_file(files[2]["_id"], cdn_host="fast.cdn.test")
        await self.store.set_host_cooldown(
            self.session["_id"], "slow.cdn.test", 300, now=100
        )

        cooled = await self.store.active_host_cooldowns(
            self.session["_id"], now=101
        )
        self.assertEqual({"slow.cdn.test"}, cooled)
        preferred = await self.store.claim_next_file(
            self.session["_id"], excluded_hosts=cooled
        )
        self.assertEqual("three.mp4", preferred["filename"])

        await self.store.update_file(preferred["_id"], FILE_DOWNLOADED)
        fallback = await self.store.claim_next_file(
            self.session["_id"], excluded_hosts=cooled
        )
        self.assertEqual("one.mp4", fallback["filename"])

    async def test_same_host_pending_files_are_grouped_at_bottom(self):
        files = await self.store.list_files(self.session["_id"])
        for item, host in zip(
            files,
            ("slow.cdn.test", "slow.cdn.test", "fast.cdn.test"),
        ):
            await self.store.update_file(item["_id"], cdn_host=host)
        first = await self.store.claim_next_file(self.session["_id"])
        deferred = await self.store.defer_file_to_bottom(
            first["_id"], "slow", automatic=True, max_auto_defers=2
        )
        self.assertIsNotNone(deferred)

        moved = await self.store.move_pending_host_to_bottom(
            self.session["_id"], "slow.cdn.test"
        )

        self.assertEqual(2, moved)
        queued = await self.store.list_files(self.session["_id"])
        self.assertEqual(
            ["three.mp4", "two.mp4", "one.mp4"],
            [item["filename"] for item in queued],
        )

    async def test_host_routing_does_not_consume_slow_file_retry(self):
        first = await self.store.claim_next_file(self.session["_id"])
        await self.store.update_file(first["_id"], cdn_host="slow.cdn.test")

        routed = await self.store.route_file_to_bottom(
            first["_id"], "CDN is cooling down"
        )

        self.assertEqual(FILE_PENDING, routed["status"])
        self.assertEqual(0, routed["defer_count"])
        self.assertEqual(1, routed["host_defer_count"])


class BunkrSessionCommandParsingTests(unittest.TestCase):
    def test_session_id_from_command_argument(self):
        message = SimpleNamespace(
            command=["continue", "ABCDEF123456"],
            reply_to_message=SimpleNamespace(empty=True),
        )
        self.assertEqual("abcdef123456", _bunkr_session_id_from_message(message))

    def test_slow_monitor_requires_continuous_slow_period(self):
        now = [0]
        monitor = leech._BunkrSlowDownloadMonitor(
            speed_limit_bps=100,
            grace_seconds=10,
            duration_seconds=5,
            clock=lambda: now[0],
        )

        now[0] = 9
        self.assertFalse(
            monitor.should_defer({"status": "active", "downloadSpeed": "0"})
        )
        now[0] = 10
        self.assertFalse(
            monitor.should_defer({"status": "active", "downloadSpeed": "50"})
        )
        now[0] = 14
        self.assertFalse(
            monitor.should_defer({"status": "active", "downloadSpeed": "150"})
        )
        now[0] = 20
        self.assertFalse(
            monitor.should_defer({"status": "active", "downloadSpeed": "50"})
        )
        now[0] = 25
        self.assertTrue(
            monitor.should_defer({"status": "active", "downloadSpeed": "50"})
        )

    def test_session_id_from_replied_summary(self):
        message = SimpleNamespace(
            command=["continue"],
            reply_to_message=SimpleNamespace(
                empty=False,
                text="Bunkr session: abcdef123456\nState: paused",
            ),
        )
        self.assertEqual("abcdef123456", _bunkr_session_id_from_message(message))

    def test_cdn_host_normalization(self):
        self.assertEqual(
            "cdn.example",
            leech._bunkr_cdn_host("https://CDN.Example:443/file?token=abc"),
        )


class BunkrSessionSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_automatic_defer_does_not_cycle_when_no_alternate_host_exists(self):
        store = BunkrSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://bunkr.example/a/album",
            title="album",
            mode="normal",
            custom_filename=None,
            files=[
                ("https://bunkr.example/f/one", "one.mp4"),
                ("https://bunkr.example/f/two", "two.mp4"),
            ],
        )
        files = await store.list_files(session["_id"])
        for item in files:
            await store.update_file(item["_id"], cdn_host="only.cdn.test")
        first = await store.claim_next_file(session["_id"])
        original_store = leech.bunkr_session_store
        leech.bunkr_session_store = store
        try:
            deferred, reason = await leech._defer_active_bunkr_download(
                session, "Automatically deferred: slow", automatic=True
            )
        finally:
            leech.bunkr_session_store = original_store

        self.assertIsNone(deferred)
        self.assertEqual("no_alternate_host", reason)
        unchanged = await store.get_file(first["_id"])
        self.assertEqual(FILE_RESOLVING, unchanged["status"])
        self.assertEqual(set(), await store.active_host_cooldowns(session["_id"]))

    async def test_automatic_slow_defer_cools_and_groups_host(self):
        store = BunkrSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://bunkr.example/a/album",
            title="album",
            mode="normal",
            custom_filename=None,
            files=[
                ("https://bunkr.example/f/one", "one.mp4"),
                ("https://bunkr.example/f/two", "two.mp4"),
                ("https://bunkr.example/f/three", "three.mp4"),
            ],
        )
        files = await store.list_files(session["_id"])
        for item, host in zip(
            files,
            ("slow.cdn.test", "slow.cdn.test", "fast.cdn.test"),
        ):
            await store.update_file(item["_id"], cdn_host=host)
        await store.claim_next_file(session["_id"])
        original_store = leech.bunkr_session_store
        leech.bunkr_session_store = store
        try:
            deferred, reason = await leech._defer_active_bunkr_download(
                session, "Automatically deferred: slow", automatic=True
            )
        finally:
            leech.bunkr_session_store = original_store

        self.assertIsNone(reason)
        self.assertEqual("slow.cdn.test", deferred["_cdn_host"])
        self.assertEqual(1, deferred["_same_host_moved"])
        self.assertEqual(
            {"slow.cdn.test"},
            await store.active_host_cooldowns(session["_id"]),
        )
        queued = await store.list_files(session["_id"])
        self.assertEqual(
            ["three.mp4", "two.mp4", "one.mp4"],
            [item["filename"] for item in queued],
        )

    async def test_resolved_file_on_cooling_host_is_routed_without_download(self):
        store = BunkrSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://bunkr.example/a/album",
            title="album",
            mode="normal",
            custom_filename=None,
            files=[
                ("https://bunkr.example/f/one", "one.mp4"),
                ("https://bunkr.example/f/two", "two.mp4"),
            ],
        )
        await store.set_host_cooldown(session["_id"], "slow.cdn.test", 300)
        first = await store.claim_next_file(session["_id"])
        original_store = leech.bunkr_session_store
        leech.bunkr_session_store = store
        initiate = AsyncMock()
        try:
            with (
                patch.object(
                    leech,
                    "resolve_bunkr_file",
                    AsyncMock(
                        return_value=(
                            "https://slow.cdn.test/one",
                            "one.mp4",
                            "https://bunkr.example/",
                        )
                    ),
                ),
                patch.object(leech, "initiate_directdl", initiate),
            ):
                result = await leech.process_bunkr_download(
                    None, SimpleNamespace(), session, first, ()
                )
        finally:
            leech.bunkr_session_store = original_store

        self.assertEqual("deferred", result)
        initiate.assert_not_awaited()
        routed = await store.get_file(first["_id"])
        self.assertEqual("slow.cdn.test", routed["cdn_host"])
        self.assertEqual(1, routed["host_defer_count"])

    async def test_skip_during_resolution_does_not_start_deferred_file(self):
        store = BunkrSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://bunkr.example/a/album",
            title="album",
            mode="normal",
            custom_filename=None,
            files=[
                ("https://bunkr.example/f/one", "one.mp4"),
                ("https://bunkr.example/f/two", "two.mp4"),
            ],
        )
        first = await store.claim_next_file(session["_id"])
        original_store = leech.bunkr_session_store
        leech.bunkr_session_store = store

        async def resolve_after_skip(*args):
            await store.defer_file_to_bottom(first["_id"], "Skipped by user")
            return "https://cdn.example/one", "one.mp4", "https://bunkr.example/"

        initiate = AsyncMock()
        try:
            with (
                patch.object(leech, "resolve_bunkr_file", resolve_after_skip),
                patch.object(leech, "initiate_directdl", initiate),
                patch.object(leech.asyncio, "sleep", AsyncMock()),
            ):
                result = await leech.process_bunkr_download(
                    None, SimpleNamespace(), session, first, ()
                )
        finally:
            leech.bunkr_session_store = original_store

        self.assertEqual("deferred", result)
        initiate.assert_not_awaited()
        next_file = await store.claim_next_file(session["_id"])
        self.assertEqual("two.mp4", next_file["filename"])

    async def test_manual_defer_removes_active_download_and_advances_queue(self):
        store = BunkrSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://bunkr.example/a/album",
            title="album",
            mode="normal",
            custom_filename=None,
            files=[
                ("https://bunkr.example/f/one", "one.mp4"),
                ("https://bunkr.example/f/two", "two.mp4"),
            ],
        )
        first = await store.claim_next_file(session["_id"])
        await store.update_file(
            first["_id"], FILE_DOWNLOADING, gid="123abc0000000000"
        )
        original_store = leech.bunkr_session_store
        original_tell_status = leech.aria2_tell_status
        original_remove = leech._remove_bunkr_download
        remove = AsyncMock()
        leech.bunkr_session_store = store
        leech.aria2_tell_status = AsyncMock(return_value={"status": "active"})
        leech._remove_bunkr_download = remove
        try:
            deferred, reason = await leech._defer_active_bunkr_download(
                session, "Skipped by user"
            )
        finally:
            leech.bunkr_session_store = original_store
            leech.aria2_tell_status = original_tell_status
            leech._remove_bunkr_download = original_remove

        self.assertIsNone(reason)
        self.assertEqual("one.mp4", deferred["filename"])
        remove.assert_awaited_once_with("123abc0000000000", cleanup=False)
        next_file = await store.claim_next_file(session["_id"])
        self.assertEqual("two.mp4", next_file["filename"])

    async def test_pause_stops_before_claiming_another_file(self):
        store = BunkrSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://bunkr.example/a/album",
            title="album",
            mode="normal",
            custom_filename=None,
            files=[
                ("https://bunkr.example/f/one", "one.mp4"),
                ("https://bunkr.example/f/two", "two.mp4"),
            ],
        )

        original_store = leech.bunkr_session_store
        original_process = leech.process_bunkr_download

        async def finish_one_then_pause(client, message, session_doc, file_doc, flags):
            await store.update_file(file_doc["_id"], FILE_DOWNLOADED)
            await store.set_state(session_doc["_id"], "paused")

        leech.bunkr_session_store = store
        leech.process_bunkr_download = finish_one_then_pause
        try:
            await leech._run_bunkr_session(None, SimpleNamespace(), session["_id"])
        finally:
            leech.bunkr_session_store = original_store
            leech.process_bunkr_download = original_process

        files = await store.list_files(session["_id"])
        self.assertEqual(FILE_DOWNLOADED, files[0]["status"])
        self.assertEqual(FILE_PENDING, files[1]["status"])

    async def test_scheduler_completes_and_reports_session(self):
        store = BunkrSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://bunkr.example/a/album",
            title="album",
            mode="normal",
            custom_filename=None,
            files=[("https://bunkr.example/f/one", "one.mp4")],
        )
        message = SimpleNamespace(reply_text=AsyncMock())
        original_store = leech.bunkr_session_store
        original_process = leech.process_bunkr_download

        async def finish_file(client, message, session_doc, file_doc, flags):
            await store.update_file(file_doc["_id"], FILE_DOWNLOADED)

        leech.bunkr_session_store = store
        leech.process_bunkr_download = finish_file
        try:
            await leech._run_bunkr_session(None, message, session["_id"])
        finally:
            leech.bunkr_session_store = original_store
            leech.process_bunkr_download = original_process

        completed = await store.get_session(session["_id"])
        self.assertEqual(SESSION_COMPLETED, completed["state"])
        message.reply_text.assert_awaited()

if __name__ == "__main__":
    unittest.main()
