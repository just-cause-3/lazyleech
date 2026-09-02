import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import lazyleech.plugins.terabox as terabox
from lazyleech.utils.terabox_sessions import (
    FILE_DOWNLOADED,
    FILE_PENDING,
    FILE_UPLOADED,
    SESSION_COMPLETED,
    SESSION_PAUSED,
    SESSION_RUNNING,
    TeraboxSessionStore,
    parse_size_limit,
    split_by_cumulative_size,
)


class TeraboxSizeSplitTests(unittest.TestCase):
    def test_parses_common_size_spellings_with_binary_units(self):
        self.assertEqual(40 * 1024**3, parse_size_limit("40GB"))
        self.assertEqual(40 * 1024**3, parse_size_limit("40 GiB"))
        self.assertEqual(int(1.5 * 1024**4), parse_size_limit("1.5TB"))

    def test_rejects_missing_or_tiny_size(self):
        with self.assertRaises(ValueError):
            parse_size_limit("40")
        with self.assertRaises(ValueError):
            parse_size_limit("500KB")

    def test_splits_greedily_without_reordering_files(self):
        gib = 1024**3
        files = [
            {"name": "one", "size_bytes": 25 * gib},
            {"name": "two", "size_bytes": 10 * gib},
            {"name": "three", "size_bytes": 20 * gib},
        ]

        groups = split_by_cumulative_size(files, 40 * gib)

        self.assertEqual([["one", "two"], ["three"]], [
            [item["name"] for item in group] for group in groups
        ])

    def test_oversized_file_gets_its_own_part(self):
        mib = 1024**2
        files = [
            {"name": "large", "size_bytes": 50 * mib},
            {"name": "small-1", "size_bytes": 10 * mib},
            {"name": "small-2", "size_bytes": 20 * mib},
        ]

        groups = split_by_cumulative_size(files, 40 * mib)

        self.assertEqual(
            [["large"], ["small-1", "small-2"]],
            [[item["name"] for item in group] for group in groups],
        )

    def test_command_accepts_separated_size_unit(self):
        message = SimpleNamespace(
            command=[
                "splittera",
                "https://1024terabox.com/s/1share_code",
                "40",
                "GB",
            ],
            reply_to_message=SimpleNamespace(empty=True),
        )

        link, max_bytes = terabox._split_terabox_request_from_message(message)

        self.assertEqual("https://1024terabox.com/s/1share_code", link)
        self.assertEqual(40 * 1024**3, max_bytes)

    def test_intelligent_cost_counts_source_and_split_copy(self):
        boundary = terabox.TELEGRAM_SPLIT_SIZE

        self.assertEqual(
            boundary, terabox._intelligent_workspace_bytes(
                {"size_bytes": boundary}
            )
        )
        self.assertEqual(
            (boundary + 1) * 2,
            terabox._intelligent_workspace_bytes(
                {"size_bytes": boundary + 1}
            ),
        )

    def test_intelligent_grouping_uses_peak_workspace_not_source_total(self):
        gib = 1024**3
        files = [
            {"name": "small", "size_bytes": 1 * gib},
            {"name": "split", "size_bytes": 3 * gib},
        ]

        groups = split_by_cumulative_size(
            files,
            6 * gib,
            size_getter=terabox._intelligent_workspace_bytes,
        )

        self.assertEqual(
            [["small"], ["split"]],
            [[item["name"] for item in group] for group in groups],
        )

    def test_final_index_is_hierarchical_and_numbers_split_parts(self):
        chain = [{"chain_id": "abc123", "title": "Share (part 1/1)", "_id": "s1"}]
        files = {
            "s1": [
                {
                    "filename": "large.zip",
                    "relative_path": "folder/large.zip",
                    "telegram_files": [
                        {"name": "large.zip.0001", "link": "https://t.me/c/1/1"},
                        {"name": "large.zip.0002", "link": "https://t.me/c/1/2"},
                    ],
                }
            ]
        }

        text = "\n".join(terabox._chain_index_lines(chain, files))

        self.assertIn("📁 <b>folder/</b>", text)
        self.assertIn("📦 1. <b>large.zip</b>", text)
        self.assertIn("1.1 large.zip.0001", text)
        self.assertIn('href="https://t.me/c/1/2"', text)


class TeraboxSessionChainTests(unittest.IsolatedAsyncioTestCase):
    async def test_next_part_activates_only_after_current_part_completes(self):
        store = TeraboxSessionStore(db_url="")
        common = {
            "owner_id": 123,
            "chat_id": -1001,
            "source_message_id": 55,
            "source_url": "https://terabox.com/s/1share",
            "mode": "normal",
            "custom_filename": None,
        }
        first = await store.create_session(
            **common,
            title="part 1",
            files=[{"page_url": common["source_url"], "filename": "one.bin"}],
            session_fields={"chain_id": "chain", "part_index": 1, "total_parts": 2},
        )
        second = await store.create_session(
            **common,
            title="part 2",
            files=[{"page_url": common["source_url"], "filename": "two.bin"}],
            initial_state=SESSION_PAUSED,
            session_fields={"chain_id": "chain", "part_index": 2, "total_parts": 2},
        )

        self.assertIsNone(await store.activate_next_chain_part(first["_id"]))
        await store.set_state(first["_id"], SESSION_COMPLETED)
        activated = await store.activate_next_chain_part(first["_id"])

        self.assertEqual(second["_id"], activated["_id"])
        self.assertEqual(SESSION_RUNNING, activated["state"])
        self.assertIsNone(await store.activate_next_chain_part(first["_id"]))

    async def test_runner_starts_next_part_only_after_all_current_uploads(self):
        store = TeraboxSessionStore(db_url="")
        common = {
            "owner_id": 123,
            "chat_id": -1001,
            "source_message_id": 55,
            "source_url": "https://terabox.com/s/1share",
            "mode": "normal",
            "custom_filename": None,
        }
        file_one = {
            "page_url": common["source_url"],
            "filename": "one.bin",
            "relative_path": "one.bin",
            "source_position": 1,
            "size_bytes": 10,
        }
        file_two = {
            "page_url": common["source_url"],
            "filename": "two.bin",
            "relative_path": "two.bin",
            "source_position": 2,
            "size_bytes": 10,
        }
        first = await store.create_session(
            **common,
            title="part 1",
            files=[file_one],
            session_fields={
                "chain_id": "chain",
                "part_index": 1,
                "total_parts": 2,
                "part_bytes": 10,
            },
        )
        second = await store.create_session(
            **common,
            title="part 2",
            files=[file_two],
            initial_state=SESSION_PAUSED,
            session_fields={
                "chain_id": "chain",
                "part_index": 2,
                "total_parts": 2,
                "part_bytes": 10,
            },
        )
        resolved_files = [
            {"name": "one.bin", "path": "one.bin", "size": 10,
             "normal_dlink": "https://cdn.test/one.bin"},
            {"name": "two.bin", "path": "two.bin", "size": 10,
             "normal_dlink": "https://cdn.test/two.bin"},
        ]
        message = SimpleNamespace(reply_text=AsyncMock())
        started = []
        upload_callbacks = []

        async def complete_download(*_args, **kwargs):
            self.assertTrue(await kwargs["on_gid"]("123abc"))
            await kwargs["on_downloaded"]()
            upload_callbacks.append(kwargs["on_uploaded"])
            self.assertTrue(kwargs["suppress_upload_summary"])
            return "complete"

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(terabox, "initiate_directdl", side_effect=complete_download),
            patch.object(
                terabox,
                "_start_terabox_session",
                side_effect=lambda _client, _message, session_id: started.append(session_id),
            ),
        ):
            await terabox._run_terabox_session(
                object(), message, first["_id"], resolved=(None, resolved_files)
            )
            waiting = await store.get_session(first["_id"])
            queued = await store.get_session(second["_id"])
            self.assertEqual(SESSION_RUNNING, waiting["state"])
            self.assertEqual(SESSION_PAUSED, queued["state"])
            self.assertEqual([], started)

            await upload_callbacks[0]([
                ("one.bin", "https://t.me/c/1/10")
            ], None)

            completed = await store.get_session(first["_id"])
            activated = await store.get_session(second["_id"])
            self.assertEqual(SESSION_COMPLETED, completed["state"])
            self.assertEqual(SESSION_RUNNING, activated["state"])
            self.assertEqual([second["_id"]], started)

    async def test_continue_redownloads_queued_but_not_uploaded_files(self):
        store = TeraboxSessionStore(db_url="")
        common = {
            "owner_id": 123,
            "chat_id": -1001,
            "source_message_id": 55,
            "source_url": "https://terabox.com/s/1share",
            "title": "part 1",
            "mode": "normal",
            "custom_filename": None,
        }
        session = await store.create_session(
            **common,
            files=[
                {"page_url": common["source_url"], "filename": "one.bin"},
                {"page_url": common["source_url"], "filename": "two.bin"},
            ],
            session_fields={"chain_id": "chain", "part_index": 1, "total_parts": 1},
        )
        files = await store.list_files(session["_id"])
        await store.update_file(files[0]["_id"], FILE_DOWNLOADED)
        await store.update_file(files[1]["_id"], FILE_UPLOADED)

        await store.prepare_continue(session["_id"], -1002, 99)
        resumed = await store.list_files(session["_id"])

        self.assertEqual(FILE_PENDING, resumed[0]["status"])
        self.assertEqual(FILE_UPLOADED, resumed[1]["status"])

    async def test_last_part_sends_one_final_chain_index(self):
        store = TeraboxSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://terabox.com/s/1share",
            title="TeraBox share (part 1/1)",
            mode="normal",
            custom_filename=None,
            files=[
                {
                    "page_url": "https://terabox.com/s/1share",
                    "filename": "large.zip",
                    "relative_path": "folder/large.zip",
                }
            ],
            session_fields={
                "chain_id": "chain",
                "part_index": 1,
                "total_parts": 1,
            },
        )
        file_doc = (await store.list_files(session["_id"]))[0]
        await store.update_file(
            file_doc["_id"],
            FILE_UPLOADED,
            telegram_files=[
                {"name": "large.zip.0001", "link": "https://t.me/c/1/1"},
                {"name": "large.zip.0002", "link": "https://t.me/c/1/2"},
            ],
        )
        message = SimpleNamespace(reply_text=AsyncMock())

        with patch.object(terabox, "terabox_session_store", store):
            completed = await terabox._maybe_complete_terabox_session(
                object(), message, session["_id"]
            )

        self.assertTrue(completed)
        message.reply_text.assert_awaited_once()
        final_text = message.reply_text.await_args.args[0]
        self.assertIn("TeraBox upload complete", final_text)
        self.assertIn("large.zip.0001", final_text)
        stored = await store.get_session(session["_id"])
        self.assertEqual(SESSION_COMPLETED, stored["state"])
        self.assertTrue(stored["index_sent"])


if __name__ == "__main__":
    unittest.main()
