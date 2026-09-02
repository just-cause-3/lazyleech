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
    SESSION_PAUSED,
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
        self.assertEqual(
            ["https://bunkr.example/a/album"], self.session["source_urls"]
        )
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

    async def test_cooling_host_can_disable_fallback_and_wait(self):
        files = await self.store.list_files(self.session["_id"])
        for item in files:
            await self.store.update_file(item["_id"], cdn_host="slow.cdn.test")

        claimed = await self.store.claim_next_file(
            self.session["_id"],
            excluded_hosts={"slow.cdn.test"},
            allow_excluded_fallback=False,
        )

        self.assertIsNone(claimed)
        counts = await self.store.counts(self.session["_id"])
        self.assertEqual(3, counts[FILE_PENDING])

    async def test_global_cdn_circuit_breaker_escalates_and_recovers(self):
        first = await self.store.record_cdn_slowdown(
            "SLOW.CDN.TEST", 300, 1200, now=100
        )
        self.assertEqual(1, first["slow_strikes"])
        self.assertEqual(300, first["cooldown_seconds"])
        self.assertEqual(
            {"slow.cdn.test"},
            await self.store.active_global_host_cooldowns(now=399),
        )

        second = await self.store.record_cdn_slowdown(
            "slow.cdn.test", 300, 1200, now=400
        )
        third = await self.store.record_cdn_slowdown(
            "slow.cdn.test", 300, 1200, now=1000
        )
        self.assertEqual(600, second["cooldown_seconds"])
        self.assertEqual(1200, third["cooldown_seconds"])

        recovered = await self.store.record_cdn_success("slow.cdn.test", now=2200)
        self.assertEqual(2, recovered["slow_strikes"])
        self.assertEqual(set(), await self.store.active_global_host_cooldowns(now=2200))

    async def test_single_active_file_can_be_parked_for_cooldown(self):
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

        parked = await self.store.park_file_for_cooldown(
            only_file["_id"], "CDN cooling", max_auto_defers=3
        )

        self.assertEqual(FILE_PENDING, parked["status"])
        self.assertEqual(1, parked["auto_defer_count"])
        self.assertEqual(1, parked["defer_count"])

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

    def test_slow_monitor_detects_peak_relative_collapse(self):
        now = [0]
        monitor = leech._BunkrSlowDownloadMonitor(
            speed_limit_bps=100,
            grace_seconds=5,
            duration_seconds=3,
            window_seconds=2,
            peak_ratio=0.20,
            clock=lambda: now[0],
        )

        self.assertFalse(
            monitor.should_defer({"status": "active", "downloadSpeed": "1000"})
        )
        now[0] = 5
        self.assertFalse(
            monitor.should_defer({"status": "active", "downloadSpeed": "150"})
        )
        self.assertEqual(200, monitor.effective_limit_bps)
        now[0] = 8
        self.assertTrue(
            monitor.should_defer({"status": "active", "downloadSpeed": "150"})
        )
        self.assertFalse(monitor.completed_healthy())

    def test_fast_download_with_slow_tail_counts_as_healthy(self):
        now = [0]
        monitor = leech._BunkrSlowDownloadMonitor(
            speed_limit_bps=100,
            grace_seconds=0,
            duration_seconds=30,
            window_seconds=5,
            clock=lambda: now[0],
        )
        self.assertFalse(
            monitor.should_defer({"status": "active", "downloadSpeed": "500"})
        )
        now[0] = 10
        self.assertFalse(
            monitor.should_defer({"status": "active", "downloadSpeed": "10"})
        )
        self.assertTrue(monitor.completed_healthy())

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

    def test_split_bunkr_accepts_url_then_size(self):
        message = SimpleNamespace(
            command=["splitbunkr", "https://bunkr.cr/a/album", "20"],
            reply_to_message=SimpleNamespace(empty=True),
        )
        self.assertEqual(
            ("https://bunkr.cr/a/album", 20),
            leech._split_bunkr_request_from_message(message),
        )

    def test_split_bunkr_accepts_size_with_replied_album(self):
        message = SimpleNamespace(
            command=["splitbunkr", "15"],
            reply_to_message=SimpleNamespace(
                empty=False,
                text="https://bunkr.cr/a/album",
                caption=None,
            ),
        )
        self.assertEqual(
            ("https://bunkr.cr/a/album", 15),
            leech._split_bunkr_request_from_message(message),
        )

    def test_split_bunkr_rejects_non_album_url(self):
        message = SimpleNamespace(
            command=["splitbunkr", "https://bunkr.cr/f/file", "10"],
            reply_to_message=SimpleNamespace(empty=True),
        )
        self.assertIsNone(leech._split_bunkr_request_from_message(message))


class BunkrQueueCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_list_paginates_and_remains_owner_scoped(self):
        store = BunkrSessionStore(db_url="")
        for index in range(12):
            await store.create_session(
                owner_id=123,
                chat_id=-1001,
                source_message_id=index + 1,
                source_url=f"https://bunkr.cr/f/{index}",
                title=f"owner-session-{index}",
                mode="normal",
                custom_filename=None,
                files=[
                    (f"https://bunkr.cr/f/{index}", f"video-{index}.mp4")
                ],
            )
        other = await store.create_session(
            owner_id=999,
            chat_id=-1001,
            source_message_id=99,
            source_url="https://bunkr.cr/f/other",
            title="other-user-session",
            mode="normal",
            custom_filename=None,
            files=[("https://bunkr.cr/f/other", "other.mp4")],
        )

        original_store = leech.bunkr_session_store
        leech.bunkr_session_store = store
        try:
            text, markup = await leech._bunkr_sessions_page(123, 2)
        finally:
            leech.bunkr_session_store = original_store

        expected = await store.list_sessions(owner_id=123, limit=10, skip=10)
        self.assertIn("Page 2/2", text)
        self.assertEqual(2, len(expected))
        for session_doc in expected:
            self.assertIn(session_doc["_id"], text)
        self.assertNotIn(other["_id"], text)
        self.assertIsNotNone(markup)
        button_labels = [
            button.text for row in markup.inline_keyboard for button in row
        ]
        self.assertIn("Previous", button_labels)
        self.assertNotIn("Next", button_labels)

    async def test_session_page_callback_rejects_another_user(self):
        callback = SimpleNamespace(
            data="bsessions_page:123:2",
            from_user=SimpleNamespace(id=999),
            answer=AsyncMock(),
            message=SimpleNamespace(edit_text=AsyncMock()),
        )

        await leech.bunkr_sessions_page_callback(None, callback)

        callback.answer.assert_awaited_once_with(
            "Only the user who opened this list can change its page.",
            show_alert=True,
        )
        callback.message.edit_text.assert_not_awaited()

    async def test_one_queue_creates_one_session_for_all_bunkr_sources(self):
        store = BunkrSessionStore(db_url="")
        original_store = leech.bunkr_session_store
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=77,
            reply_text=AsyncMock(),
        )
        reply = SimpleNamespace(edit_text=AsyncMock())
        links = [
            "https://bunkr.cr/a/first",
            "https://bunkr.cr/a/second",
        ]
        extracted = [
            [("https://bunkr.cr/f/one", "one.mp4")],
            [
                ("https://bunkr.cr/f/two", "two.mkv"),
                ("https://bunkr.cr/f/readme", "readme.txt"),
            ],
        ]
        leech.bunkr_session_store = store
        try:
            with (
                patch.object(
                    leech, "extract_album_urls", AsyncMock(side_effect=extracted)
                ),
                patch.object(leech, "_start_bunkr_session") as start_session,
                patch.object(leech.asyncio, "sleep", AsyncMock()),
            ):
                session_doc = await leech._process_queue_links(
                    None, message, links, (), reply
                )
        finally:
            leech.bunkr_session_store = original_store

        sessions = await store.list_sessions(owner_id=123, limit=0)
        self.assertEqual(1, len(sessions))
        self.assertEqual(session_doc["_id"], sessions[0]["_id"])
        self.assertEqual(2, sessions[0]["total_files"])
        self.assertEqual(links, sessions[0]["source_urls"])
        files = await store.list_files(session_doc["_id"])
        self.assertEqual(["one.mp4", "two.mkv"], [item["filename"] for item in files])
        start_session.assert_called_once_with(None, message, session_doc["_id"])
        message.reply_text.assert_awaited_once()
        reply.edit_text.assert_awaited_once()

    async def test_split_album_stores_later_sessions_paused(self):
        store = BunkrSessionStore(db_url="")
        original_store = leech.bunkr_session_store
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=78,
            reply_text=AsyncMock(),
        )
        reply = SimpleNamespace(edit_text=AsyncMock())
        album_url = "https://bunkr.cr/a/album"
        files = [
            (f"https://bunkr.cr/f/{index}", f"video-{index}.mp4")
            for index in range(1, 6)
        ]
        leech.bunkr_session_store = store
        try:
            with (
                patch.object(
                    leech,
                    "_extract_bunkr_video_files",
                    AsyncMock(return_value=files),
                ),
                patch.object(leech, "_start_bunkr_session") as start_session,
            ):
                sessions = await leech._create_split_bunkr_sessions(
                    None, message, album_url, 2, reply
                )
        finally:
            leech.bunkr_session_store = original_store

        self.assertEqual(3, len(sessions))
        self.assertEqual(
            [SESSION_RUNNING, SESSION_PAUSED, SESSION_PAUSED],
            [session["state"] for session in sessions],
        )
        self.assertEqual(
            [2, 2, 1],
            [len(await store.list_files(session["_id"])) for session in sessions],
        )
        self.assertEqual(
            [
                "album (part 1/3)",
                "album (part 2/3)",
                "album (part 3/3)",
            ],
            [session["title"] for session in sessions],
        )
        start_session.assert_called_once_with(None, message, sessions[0]["_id"])
        reply.edit_text.assert_awaited_once()

    async def test_continue_starts_a_stored_split_session(self):
        store = BunkrSessionStore(db_url="")
        session_doc = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=78,
            source_url="https://bunkr.cr/a/album",
            title="album (part 2/3)",
            mode="normal",
            custom_filename=None,
            files=[("https://bunkr.cr/f/three", "three.mp4")],
            initial_state=SESSION_PAUSED,
        )
        message = SimpleNamespace(
            command=["continue", session_doc["_id"]],
            reply_to_message=SimpleNamespace(empty=True),
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=79,
            reply_text=AsyncMock(),
        )
        original_store = leech.bunkr_session_store
        leech.bunkr_session_store = store
        try:
            with patch.object(leech, "_start_bunkr_session") as start_session:
                await leech.continue_bunkr_session_cmd(None, message)
        finally:
            leech.bunkr_session_store = original_store

        updated = await store.get_session(session_doc["_id"])
        self.assertEqual(SESSION_RUNNING, updated["state"])
        start_session.assert_called_once_with(None, message, session_doc["_id"])
        message.reply_text.assert_awaited_once()

    async def test_delete_session_command_removes_database_history_only(self):
        store = BunkrSessionStore(db_url="")
        session_doc = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=77,
            source_url="https://bunkr.cr/f/one",
            title="one",
            mode="normal",
            custom_filename=None,
            files=[("https://bunkr.cr/f/one", "one.mp4")],
        )
        message = SimpleNamespace(
            command=["deletesession", session_doc["_id"]],
            reply_to_message=SimpleNamespace(empty=True),
            from_user=SimpleNamespace(id=123),
            reply_text=AsyncMock(),
        )
        original_store = leech.bunkr_session_store
        leech.bunkr_session_store = store
        try:
            with patch.object(
                leech, "_cancel_bunkr_session_runtime", AsyncMock()
            ) as cancel_runtime:
                await leech.delete_bunkr_session_cmd(None, message)
        finally:
            leech.bunkr_session_store = original_store

        cancel_runtime.assert_awaited_once_with(session_doc["_id"])
        self.assertIsNone(await store.get_session(session_doc["_id"]))
        self.assertEqual([], await store.list_files(session_doc["_id"]))
        message.reply_text.assert_awaited_once()


class BunkrSessionSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_automatic_defer_cools_when_no_alternate_host_exists(self):
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

        self.assertIsNone(reason)
        self.assertIsNotNone(deferred)
        parked = await store.get_file(first["_id"])
        self.assertEqual(FILE_PENDING, parked["status"])
        self.assertEqual(
            {"only.cdn.test"}, await store.active_host_cooldowns(session["_id"])
        )
        self.assertEqual(
            {"only.cdn.test"}, await store.active_global_host_cooldowns()
        )

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
        self.assertEqual(
            {"slow.cdn.test"}, await store.active_global_host_cooldowns()
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

    async def test_scheduler_waits_when_only_pending_cdn_is_cooling(self):
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
        file_doc = (await store.list_files(session["_id"]))[0]
        await store.update_file(file_doc["_id"], cdn_host="slow.cdn.test")
        await store.record_cdn_slowdown("slow.cdn.test", 300, 1200)

        original_store = leech.bunkr_session_store
        original_process = leech.process_bunkr_download
        process = AsyncMock()

        async def pause_after_wait(_seconds):
            await store.set_state(session["_id"], "paused")

        leech.bunkr_session_store = store
        leech.process_bunkr_download = process
        try:
            with patch.object(leech.asyncio, "sleep", pause_after_wait):
                await leech._run_bunkr_session(
                    None, SimpleNamespace(), session["_id"]
                )
        finally:
            leech.bunkr_session_store = original_store
            leech.process_bunkr_download = original_process

        process.assert_not_awaited()
        waiting = await store.get_file(file_doc["_id"])
        self.assertEqual(FILE_PENDING, waiting["status"])

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
