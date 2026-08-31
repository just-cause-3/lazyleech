import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

import lazyleech.plugins.leech as leech
from lazyleech.plugins.leech import _bunkr_session_id_from_message
from lazyleech.utils.bunkr_sessions import (
    FILE_CANCELLED,
    FILE_DOWNLOADED,
    FILE_DOWNLOADING,
    FILE_FAILED,
    FILE_PENDING,
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


class BunkrSessionCommandParsingTests(unittest.TestCase):
    def test_session_id_from_command_argument(self):
        message = SimpleNamespace(
            command=["continue", "ABCDEF123456"],
            reply_to_message=SimpleNamespace(empty=True),
        )
        self.assertEqual("abcdef123456", _bunkr_session_id_from_message(message))

    def test_session_id_from_replied_summary(self):
        message = SimpleNamespace(
            command=["continue"],
            reply_to_message=SimpleNamespace(
                empty=False,
                text="Bunkr session: abcdef123456\nState: paused",
            ),
        )
        self.assertEqual("abcdef123456", _bunkr_session_id_from_message(message))


class BunkrSessionSchedulerTests(unittest.IsolatedAsyncioTestCase):
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
