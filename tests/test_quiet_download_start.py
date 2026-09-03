import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from lazyleech.plugins import leech
from lazyleech.utils import status


class QuietDownloadStartTests(unittest.IsolatedAsyncioTestCase):
    async def test_direct_download_uses_internal_reference_without_reply(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            reply_text=AsyncMock(),
        )

        with (
            patch.object(leech, "aria2_add_directdl", AsyncMock(return_value="gid1")),
            patch.object(leech, "handle_leech", AsyncMock(return_value="complete")) as handle,
        ):
            result = await leech.initiate_directdl(
                None, message, "https://example.test/file", "file.bin", ()
            )

        self.assertEqual("complete", result)
        message.reply_text.assert_not_awaited()
        reference = handle.await_args.args[3]
        self.assertEqual(message.chat, reference.chat)
        self.assertLess(reference.id, 0)
        self.assertFalse(hasattr(reference, "delete"))

    async def test_segmented_direct_download_uses_range_engine(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            reply_text=AsyncMock(),
        )

        with (
            patch.object(
                leech,
                "aria2_add_segmented_directdl",
                AsyncMock(return_value="gid3"),
            ) as add_segmented,
            patch.object(leech, "aria2_add_directdl", AsyncMock()) as add_aria,
            patch.object(leech, "handle_leech", AsyncMock(return_value="complete")),
        ):
            result = await leech.initiate_directdl(
                None,
                message,
                "https://example.test/file",
                "file.bin",
                (),
                segmented_total_length=123456,
                download_dir="stable-dir",
                max_connections=8,
            )

        self.assertEqual("complete", result)
        add_aria.assert_not_awaited()
        add_segmented.assert_awaited_once()
        self.assertEqual(123456, add_segmented.await_args.kwargs["total_length"])
        self.assertEqual(8, add_segmented.await_args.kwargs["max_connections"])

    async def test_torrent_uses_internal_reference_without_reply(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            reply_text=AsyncMock(),
        )

        with (
            patch.object(leech, "aria2_add_torrent", AsyncMock(return_value="gid2")),
            patch.object(leech, "handle_leech", AsyncMock()) as handle,
        ):
            await leech.initiate_torrent(
                None, message, "https://example.test/file.torrent", ()
            )

        message.reply_text.assert_not_awaited()
        self.assertLess(handle.await_args.args[3].id, 0)


class StatusMessageReuseTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        status.status_messages.clear()
        status.status_message_locks.clear()

    async def test_existing_status_message_is_edited_not_recreated(self):
        existing = SimpleNamespace(
            text="old", reply_markup=None, edit_text=AsyncMock(), delete=AsyncMock()
        )
        status.status_messages[-1001] = existing
        client = SimpleNamespace(send_message=AsyncMock())
        message = SimpleNamespace(chat=SimpleNamespace(id=-1001))

        with patch.object(
            status, "get_status_text", AsyncMock(return_value=("new", None))
        ):
            returned = await status.send_status_message(client, message)

        self.assertIs(existing, returned)
        existing.edit_text.assert_awaited_once_with("new", reply_markup=None)
        existing.delete.assert_not_awaited()
        client.send_message.assert_not_awaited()

    async def test_task_start_reuses_status_without_forcing_edit(self):
        existing = SimpleNamespace(
            text="old", reply_markup=None, edit_text=AsyncMock(), delete=AsyncMock()
        )
        status.status_messages[-1001] = existing
        client = SimpleNamespace(send_message=AsyncMock())
        message = SimpleNamespace(chat=SimpleNamespace(id=-1001))

        with patch.object(status, "update_status_message", AsyncMock()) as update:
            returned = await status.send_status_message(
                client, message, refresh_existing=False
            )

        self.assertIs(existing, returned)
        update.assert_not_awaited()
        existing.edit_text.assert_not_awaited()
        client.send_message.assert_not_awaited()

    async def test_manual_status_creates_a_fresh_tracked_message(self):
        existing = SimpleNamespace(
            text="old", reply_markup=None, edit_text=AsyncMock(), delete=AsyncMock()
        )
        fresh = SimpleNamespace(text=None, reply_markup=None)
        status.status_messages[-1001] = existing
        client_tool = SimpleNamespace(send_message=AsyncMock(return_value=fresh))
        message = SimpleNamespace(chat=SimpleNamespace(id=-1001))

        with patch.object(
            status, "get_status_text", AsyncMock(return_value=("current", None))
        ):
            returned = await status.send_status_message(
                client_tool, message, force_new=True
            )

        self.assertIs(fresh, returned)
        client_tool.send_message.assert_awaited_once_with(
            -1001, "current", reply_markup=None
        )
        existing.edit_text.assert_not_awaited()
        self.assertIs(fresh, status.status_messages[-1001])

    async def test_status_command_requests_a_fresh_message(self):
        client_tool = object()
        message = SimpleNamespace(chat=SimpleNamespace(id=-1001))

        with patch.object(leech, "send_status_message", AsyncMock()) as sender:
            await leech.list_leeches(client_tool, message)

        sender.assert_awaited_once_with(client_tool, message, force_new=True)


if __name__ == "__main__":
    unittest.main()
