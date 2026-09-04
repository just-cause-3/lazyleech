import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import lazyleech.plugins.terabox as terabox
from lazyleech.utils.terabox import TeraboxFile, TeraboxMetadataStaleError
from lazyleech.utils.terabox_sessions import (
    FILE_DOWNLOADED,
    FILE_FAILED,
    FILE_PENDING,
    FILE_UPLOADED,
    SESSION_COMPLETED,
    SESSION_FAILED,
    SESSION_PAUSED,
    SESSION_RUNNING,
    TeraboxSessionStore,
    infer_shared_folder_name,
    parse_size_limit,
    split_by_cumulative_size,
)


class TeraboxSizeSplitTests(unittest.TestCase):
    def test_infers_shared_top_level_folder_name(self):
        files = [
            {"relative_path": "Visual Novel/one.zip"},
            {"relative_path": "Visual Novel/sub/two.zip"},
        ]

        self.assertEqual(
            "Visual Novel", infer_shared_folder_name(files, fallback="share-code")
        )

    def test_folder_name_falls_back_for_root_level_files(self):
        files = [
            {"relative_path": "one.zip"},
            {"relative_path": "two.zip"},
        ]

        self.assertEqual(
            "TeraBox share-code",
            infer_shared_folder_name(files, fallback="TeraBox share-code"),
        )

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

    def test_intelligent_limit_detects_a_single_file_that_cannot_fit(self):
        gib = 1024**3
        files = [
            {"name": "fits.bin", "size_bytes": 2 * gib},
            {"name": "too-large.bin", "size_bytes": 4 * gib},
        ]

        violations = terabox._intelligent_limit_violations(files, 6 * gib)

        self.assertEqual(["too-large.bin"], [item["name"] for item in violations])

    def test_large_chain_plan_is_paginated_without_truncation(self):
        gib = 1024**3
        chain = {
            "_id": "chain123",
            "owner_id": 123,
            "name": "Large Folder",
            "total_files": 223,
            "max_bytes": 6 * gib,
            "planning_mode": "intelligent_workspace",
            "split_file_count": 50,
        }
        sessions = [
            {
                "_id": f"session{part:03d}",
                "part_index": part,
                "total_parts": 91,
                "total_files": 2,
                "part_bytes": gib,
                "workspace_bytes": 2 * gib,
                "state": SESSION_RUNNING if part == 1 else SESSION_PAUSED,
            }
            for part in range(1, 92)
        ]

        first_text, first_markup = terabox._terabox_plan_page(
            chain, sessions, requested_page=1
        )
        last_text, _ = terabox._terabox_plan_page(
            chain, sessions, requested_page=10
        )

        self.assertIn("Page:</b> 1/10", first_text)
        self.assertIn("Part 1/91", first_text)
        self.assertIn("Part 10/91", first_text)
        self.assertNotIn("Part 11/91", first_text)
        self.assertIn("Page:</b> 10/10", last_text)
        self.assertIn("Part 91/91", last_text)
        self.assertLess(len(first_text.encode("utf-8")), 4096)
        callback_values = [
            button.callback_data
            for row in first_markup.inline_keyboard
            for button in row
        ]
        self.assertIn("terachain_page:123:chain123:2", callback_values)

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

    def test_final_index_sorts_out_of_order_uploads_by_part_number(self):
        chain = [{"chain_id": "abc123", "title": "Share (part 1/1)", "_id": "s1"}]
        files = {
            "s1": [
                {
                    "filename": "large.zip",
                    "telegram_files": [
                        {"name": "large.zip.0010", "link": "https://t.me/c/1/10"},
                        {"name": "large.zip.0002", "link": "https://t.me/c/1/2"},
                        {"name": "large.zip.0001", "link": "https://t.me/c/1/1"},
                    ],
                }
            ]
        }

        text = "\n".join(terabox._chain_index_lines(chain, files))

        self.assertLess(text.index("large.zip.0001"), text.index("large.zip.0002"))
        self.assertLess(text.index("large.zip.0002"), text.index("large.zip.0010"))
        self.assertIn('href="https://t.me/c/1/10"', text)

    def test_upload_records_require_real_telegram_message_links(self):
        records = terabox._ordered_telegram_files(
            [
                ("archive.zip.0002", "https://t.me/c/1397057473/26112"),
                ("archive.zip.0001", "not-a-link"),
            ]
        )

        self.assertEqual(
            [
                {
                    "name": "archive.zip.0002",
                    "link": "https://t.me/c/1397057473/26112",
                }
            ],
            records,
        )

    def test_final_index_does_not_repeat_named_root_folder(self):
        chain = [
            {
                "chain_id": "abc123",
                "name": "Shared Folder",
                "title": "Shared Folder (part 1/1)",
                "_id": "s1",
            }
        ]
        files = {
            "s1": [
                {
                    "filename": "one.zip",
                    "relative_path": "Shared Folder/one.zip",
                    "telegram_files": [
                        {"name": "one.zip", "link": "https://t.me/c/1/1"}
                    ],
                }
            ]
        }

        text = "\n".join(terabox._chain_index_lines(chain, files))

        self.assertEqual(1, text.count("Shared Folder"))
        self.assertIn("one.zip", text)

    def test_final_index_reports_skipped_file_and_reason(self):
        chain = [
            {
                "chain_id": "abc123",
                "title": "Share (part 1/1)",
                "_id": "s1",
            }
        ]
        files = {
            "s1": [
                {
                    "filename": "tiny.zip",
                    "status": FILE_FAILED,
                    "error": "HTTP 200 did not match the expected body size",
                    "telegram_files": [],
                }
            ]
        }

        text = "\n".join(terabox._chain_index_lines(chain, files))

        self.assertIn("finished with 1 skipped file(s)", text)
        self.assertIn("tiny.zip", text)
        self.assertIn("skipped: HTTP 200", text)


class TeraboxSessionChainTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_scan_metadata_is_persisted_in_child_sessions(self):
        store = TeraboxSessionStore(db_url="")
        source_url = "https://terabox.com/s/1share"
        native_files = [
            {
                "name": "one.bin",
                "size_formatted": "10 B",
                "terabox_file": TeraboxFile(
                    name="one.bin",
                    relative_path="Folder/one.bin",
                    size=10,
                    download_url="https://dm-d.1024terabox.com/file/one",
                    fs_id="101",
                ),
            },
            {
                "name": "two.bin",
                "size_formatted": "10 B",
                "terabox_file": TeraboxFile(
                    name="two.bin",
                    relative_path="Folder/two.bin",
                    size=10,
                    download_url="https://dm-d.1024terabox.com/file/two",
                    fs_id="102",
                ),
            },
        ]
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=55,
        )
        reply = SimpleNamespace(edit_text=AsyncMock())

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(
                terabox,
                "_resolve_terabox_share",
                AsyncMock(return_value=(object(), native_files)),
            ) as scan,
            patch.object(terabox, "_start_terabox_session"),
        ):
            sessions = await terabox._create_split_terabox_sessions(
                object(), message, source_url, 10, reply
            )
            persisted = await store.list_chain_files(sessions[0]["chain_id"])

        scan.assert_awaited_once_with(source_url)
        self.assertEqual(["101", "102"], [doc["fs_id"] for doc in persisted])
        self.assertEqual(
            [
                "https://dm-d.1024terabox.com/file/one",
                "https://dm-d.1024terabox.com/file/two",
            ],
            [doc["source_dlink"] for doc in persisted],
        )
        self.assertEqual(
            ["Folder/one.bin", "Folder/two.bin"],
            [doc["relative_path"] for doc in persisted],
        )
        chain = await store.get_chain(sessions[0]["chain_id"])
        self.assertEqual(1, chain["metadata_revision"])

    async def test_intelligent_creation_rejects_an_unsafe_workspace_limit(self):
        gib = 1024**3
        store = TeraboxSessionStore(db_url="")
        files = [
            {
                "name": "large.bin",
                "path": "Folder/large.bin",
                "size": 4 * gib,
                "normal_dlink": "https://cdn.test/large.bin",
            }
        ]
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=55,
        )
        reply = SimpleNamespace(edit_text=AsyncMock())

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(
                terabox,
                "_resolve_terabox_share",
                AsyncMock(return_value=(None, files)),
            ),
            patch.object(terabox, "_start_terabox_session") as start,
        ):
            sessions = await terabox._create_split_terabox_sessions(
                object(),
                message,
                "https://terabox.com/s/1share",
                6 * gib,
                reply,
                intelligent=True,
            )

        self.assertEqual([], sessions)
        self.assertEqual([], await store.list_sessions(owner_id=123, limit=0))
        self.assertIn("Minimum for this share", reply.edit_text.await_args.args[0])
        start.assert_not_called()

    async def test_split_creation_persists_parent_and_folder_name(self):
        store = TeraboxSessionStore(db_url="")
        files = [
            {
                "name": "one.bin",
                "path": "Shared Folder/one.bin",
                "size": 10,
                "normal_dlink": "https://cdn.test/one.bin",
            },
            {
                "name": "two.bin",
                "path": "Shared Folder/two.bin",
                "size": 10,
                "normal_dlink": "https://cdn.test/two.bin",
            },
        ]
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=55,
        )
        reply = SimpleNamespace(edit_text=AsyncMock())
        client = object()

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(
                terabox,
                "_resolve_terabox_share",
                AsyncMock(return_value=(None, files)),
            ),
            patch.object(terabox, "_start_terabox_session") as start,
        ):
            sessions = await terabox._create_split_terabox_sessions(
                client,
                message,
                "https://terabox.com/s/1share",
                10,
                reply,
            )

        self.assertEqual(2, len(sessions))
        chains = await store.list_chains(owner_id=123, limit=0)
        self.assertEqual(1, len(chains))
        self.assertEqual("Shared Folder", chains[0]["name"])
        self.assertEqual(
            [session["_id"] for session in sessions], chains[0]["session_ids"]
        )
        self.assertTrue(
            all(session["name"] == "Shared Folder" for session in sessions)
        )
        start.assert_called_once_with(
            client, message, sessions[0]["_id"], resolved=(None, files)
        )

    async def test_parent_chain_and_child_names_are_persistent_records(self):
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
            title="Folder Name (part 1/2)",
            files=[
                {
                    "page_url": common["source_url"],
                    "filename": "one.bin",
                    "relative_path": "Folder Name/one.bin",
                }
            ],
            session_fields={
                "chain_id": "chain123",
                "name": "Folder Name",
                "part_index": 1,
                "total_parts": 2,
            },
        )
        second = await store.create_session(
            **common,
            title="Folder Name (part 2/2)",
            files=[
                {
                    "page_url": common["source_url"],
                    "filename": "two.bin",
                    "relative_path": "Folder Name/two.bin",
                }
            ],
            initial_state=SESSION_PAUSED,
            session_fields={
                "chain_id": "chain123",
                "name": "Folder Name",
                "part_index": 2,
                "total_parts": 2,
            },
        )
        await store.create_chain(
            chain_id="chain123",
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=common["source_url"],
            name="Folder Name",
            mode="normal",
            total_parts=2,
            total_files=2,
            session_ids=[first["_id"], second["_id"]],
        )

        chains = await store.list_chains(owner_id=123, limit=0)
        children = await store.list_chain("chain123")

        self.assertEqual(1, len(chains))
        self.assertEqual("Folder Name", chains[0]["name"])
        self.assertEqual([first["_id"], second["_id"]], chains[0]["session_ids"])
        self.assertEqual(["Folder Name", "Folder Name"], [
            child["name"] for child in children
        ])

    async def test_older_sessions_are_backfilled_into_named_parent_chain(self):
        store = TeraboxSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://terabox.com/s/1old",
            title="TeraBox old-code (part 1/1)",
            mode="normal",
            custom_filename=None,
            files=[
                {
                    "page_url": "https://terabox.com/s/1old",
                    "filename": "one.bin",
                    "relative_path": "Recovered Folder/one.bin",
                }
            ],
            session_fields={
                "chain_id": "oldchain",
                "part_index": 1,
                "total_parts": 1,
            },
        )

        chains = await store.list_chains(owner_id=123, limit=0)
        migrated = await store.get_session(session["_id"])

        self.assertEqual("Recovered Folder", chains[0]["name"])
        self.assertEqual([session["_id"]], chains[0]["session_ids"])
        self.assertEqual("Recovered Folder", migrated["name"])

    async def test_chain_list_visually_contains_every_child_session(self):
        store = TeraboxSessionStore(db_url="")
        common = {
            "owner_id": 123,
            "chat_id": -1001,
            "source_message_id": 55,
            "source_url": "https://terabox.com/s/1share",
            "mode": "normal",
            "custom_filename": None,
        }
        children = []
        for part in (1, 2):
            children.append(
                await store.create_session(
                    **common,
                    title=f"Parent Folder (part {part}/2)",
                    files=[
                        {
                            "page_url": common["source_url"],
                            "filename": f"{part}.bin",
                            "relative_path": f"Parent Folder/{part}.bin",
                        }
                    ],
                    initial_state=(
                        SESSION_RUNNING if part == 1 else SESSION_PAUSED
                    ),
                    session_fields={
                        "chain_id": "visualchain",
                        "name": "Parent Folder",
                        "part_index": part,
                        "total_parts": 2,
                        "part_bytes": part * 1024,
                    },
                )
            )
        await store.create_chain(
            chain_id="visualchain",
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=common["source_url"],
            name="Parent Folder",
            mode="normal",
            total_parts=2,
            total_files=2,
            session_ids=[child["_id"] for child in children],
        )

        with patch.object(terabox, "terabox_session_store", store):
            pages = await terabox._terabox_sessions_pages(123)

        text = "\n".join(pages)
        self.assertIn("📁 <b>Parent Folder</b>", text)
        self.assertIn("Chain: <code>visualchain</code>", text)
        self.assertIn(children[0]["_id"], text)
        self.assertIn(children[1]["_id"], text)
        self.assertIn("Part 1/2", text)
        self.assertIn("Part 2/2", text)

    async def test_no_argument_selects_the_only_running_session(self):
        store = TeraboxSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://terabox.com/s/1share",
            title="part 1",
            mode="normal",
            custom_filename=None,
            files=[{"page_url": "https://terabox.com/s/1share", "filename": "one"}],
            session_fields={"chain_id": "chain", "part_index": 1, "total_parts": 1},
        )
        message = SimpleNamespace(
            command=["terasession"],
            reply_to_message=SimpleNamespace(empty=True),
            from_user=SimpleNamespace(id=123),
        )

        with patch.object(terabox, "terabox_session_store", store):
            selected = await terabox._owned_terabox_session(
                message, default_states=(SESSION_RUNNING,)
            )

        self.assertEqual(session["_id"], selected["_id"])

    async def test_no_argument_selects_latest_when_multiple_sessions_run(self):
        store = TeraboxSessionStore(db_url="")
        common = {
            "owner_id": 123,
            "chat_id": -1001,
            "source_message_id": 55,
            "source_url": "https://terabox.com/s/1share",
            "mode": "normal",
            "custom_filename": None,
        }
        await store.create_session(
            **common,
            title="older",
            files=[{"page_url": common["source_url"], "filename": "one"}],
            session_fields={"chain_id": "old", "part_index": 1, "total_parts": 1},
        )
        # MongoDB stores datetimes at millisecond precision, so keep the
        # intended newest-first ordering unambiguous in both store backends.
        await asyncio.sleep(0.002)
        latest = await store.create_session(
            **common,
            title="latest",
            files=[{"page_url": common["source_url"], "filename": "two"}],
            session_fields={"chain_id": "new", "part_index": 1, "total_parts": 1},
        )
        message = SimpleNamespace(
            command=["terasession"],
            reply_to_message=SimpleNamespace(empty=True),
            from_user=SimpleNamespace(id=123),
        )

        with patch.object(terabox, "terabox_session_store", store):
            selected = await terabox._owned_terabox_session(
                message, default_states=(SESSION_RUNNING,)
            )

        self.assertEqual(latest["_id"], selected["_id"])

    async def test_no_argument_falls_back_to_latest_non_running_session(self):
        store = TeraboxSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="https://terabox.com/s/1share",
            title="failed",
            mode="normal",
            custom_filename=None,
            files=[{"page_url": "https://terabox.com/s/1share", "filename": "one"}],
            session_fields={"chain_id": "chain", "part_index": 1, "total_parts": 1},
        )
        await store.set_state(session["_id"], SESSION_FAILED)
        message = SimpleNamespace(
            command=["terasession"],
            reply_to_message=SimpleNamespace(empty=True),
            from_user=SimpleNamespace(id=123),
        )

        with patch.object(terabox, "terabox_session_store", store):
            selected = await terabox._owned_terabox_session(
                message, default_states=(SESSION_RUNNING,)
            )

        self.assertEqual(session["_id"], selected["_id"])

    async def test_deleting_child_reindexes_remaining_chain(self):
        store = TeraboxSessionStore(db_url="")
        common = {
            "owner_id": 123,
            "chat_id": -1001,
            "source_message_id": 55,
            "source_url": "https://terabox.com/s/1share",
            "mode": "normal",
            "custom_filename": None,
        }
        children = []
        for part in (1, 2, 3):
            children.append(
                await store.create_session(
                    **common,
                    title=f"Folder (part {part}/3)",
                    files=[
                        {
                            "page_url": common["source_url"],
                            "filename": f"{part}.bin",
                        }
                    ],
                    initial_state=(
                        SESSION_RUNNING if part == 1 else SESSION_PAUSED
                    ),
                    session_fields={
                        "chain_id": "deletechain",
                        "name": "Folder",
                        "part_index": part,
                        "total_parts": 3,
                    },
                )
            )
        await store.create_chain(
            chain_id="deletechain",
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=common["source_url"],
            name="Folder",
            mode="normal",
            total_parts=3,
            total_files=3,
            session_ids=[doc["_id"] for doc in children],
        )

        result = await store.delete_session_from_chain(children[1]["_id"], 123)
        remaining = await store.list_chain("deletechain")
        chain = await store.get_chain("deletechain", owner_id=123)

        self.assertEqual(2, result["remaining_sessions"])
        self.assertEqual([1, 2], [doc["part_index"] for doc in remaining])
        self.assertEqual([2, 2], [doc["total_parts"] for doc in remaining])
        self.assertEqual("Folder (part 2/2)", remaining[1]["title"])
        self.assertEqual([children[0]["_id"], children[2]["_id"]], chain["session_ids"])
        self.assertEqual(2, chain["total_files"])
        self.assertIsNone(await store.get_session(children[1]["_id"]))

    async def test_delete_chain_tree_is_owner_scoped(self):
        store = TeraboxSessionStore(db_url="")

        async def create_owned_chain(owner_id, chain_id):
            source_url = f"https://terabox.com/s/{chain_id}"
            child = await store.create_session(
                owner_id=owner_id,
                chat_id=-1001,
                source_message_id=55,
                source_url=source_url,
                title=f"{chain_id} (part 1/1)",
                mode="normal",
                custom_filename=None,
                files=[{"page_url": source_url, "filename": "one.bin"}],
                session_fields={
                    "chain_id": chain_id,
                    "name": chain_id,
                    "part_index": 1,
                    "total_parts": 1,
                },
            )
            await store.create_chain(
                chain_id=chain_id,
                owner_id=owner_id,
                chat_id=-1001,
                source_message_id=55,
                source_url=source_url,
                name=chain_id,
                mode="normal",
                total_parts=1,
                total_files=1,
                session_ids=[child["_id"]],
            )
            return child

        owned = await create_owned_chain(123, "ownedchain")
        foreign = await create_owned_chain(456, "foreignchain")

        self.assertIsNone(await store.delete_chain_tree("foreignchain", 123))
        result = await store.delete_chain_tree("ownedchain", 123)

        self.assertEqual(1, result["deleted_sessions"])
        self.assertIsNone(await store.get_chain("ownedchain", owner_id=123))
        self.assertIsNone(await store.get_session(owned["_id"]))
        self.assertIsNotNone(await store.get_chain("foreignchain", owner_id=456))
        self.assertIsNotNone(await store.get_session(foreign["_id"]))

    async def test_continue_accepts_parent_chain_id(self):
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
            title="Folder (part 1/2)",
            files=[{"page_url": common["source_url"], "filename": "one"}],
            session_fields={
                "chain_id": "resumechain",
                "name": "Folder",
                "part_index": 1,
                "total_parts": 2,
            },
        )
        await store.set_state(first["_id"], SESSION_COMPLETED)
        second = await store.create_session(
            **common,
            title="Folder (part 2/2)",
            files=[{"page_url": common["source_url"], "filename": "two"}],
            initial_state=SESSION_PAUSED,
            session_fields={
                "chain_id": "resumechain",
                "name": "Folder",
                "part_index": 2,
                "total_parts": 2,
            },
        )
        await store.create_chain(
            chain_id="resumechain",
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=common["source_url"],
            name="Folder",
            mode="normal",
            total_parts=2,
            total_files=2,
            session_ids=[first["_id"], second["_id"]],
        )
        message = SimpleNamespace(
            command=["continuetera", "resumechain"],
            reply_to_message=SimpleNamespace(empty=True),
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=99,
            reply_text=AsyncMock(),
        )
        client = object()

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(terabox, "_start_terabox_session") as start,
        ):
            await terabox.continue_terabox_session_cmd(client, message)

        resumed = await store.get_session(second["_id"])
        self.assertEqual(SESSION_RUNNING, resumed["state"])
        start.assert_called_once_with(client, message, second["_id"])
        self.assertIn(second["_id"], message.reply_text.await_args.args[0])

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

    async def test_restarted_runner_uses_mongodb_metadata_without_share_scan(self):
        store = TeraboxSessionStore(db_url="")
        source_url = "https://terabox.com/s/1share"
        child = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=source_url,
            title="Folder (part 1/1)",
            mode="normal",
            custom_filename=None,
            files=[
                {
                    "page_url": source_url,
                    "filename": "one.bin",
                    "relative_path": "Folder/one.bin",
                    "source_position": 1,
                    "size_bytes": 10,
                    "fs_id": "101",
                    "source_dlink": "https://dm-d.1024terabox.com/file/old",
                    "metadata_revision": 1,
                }
            ],
            session_fields={
                "chain_id": "durablechain",
                "part_index": 1,
                "total_parts": 1,
                "part_bytes": 10,
            },
        )
        await store.create_chain(
            chain_id="durablechain",
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=source_url,
            name="Folder",
            mode="normal",
            total_parts=1,
            total_files=1,
            session_ids=[child["_id"]],
            chain_fields={"metadata_revision": 1},
        )
        message = SimpleNamespace(reply_text=AsyncMock())

        async def complete_download(*_args, **kwargs):
            self.assertTrue(await kwargs["on_gid"]("gid"))
            await kwargs["on_downloaded"]()
            return "complete"

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(
                terabox,
                "_resolve_terabox_share",
                AsyncMock(side_effect=AssertionError("unexpected complete scan")),
            ) as scan,
            patch.object(
                terabox,
                "_authorize_stored_terabox_file",
                AsyncMock(return_value=("https://storage.test/file", [])),
            ) as authorize,
            patch.object(
                terabox, "initiate_directdl", side_effect=complete_download
            ),
        ):
            await terabox._run_terabox_session(
                object(), message, child["_id"]
            )

        scan.assert_not_awaited()
        authorize.assert_awaited_once()

    async def test_stale_metadata_refreshes_whole_chain_once_by_fs_id(self):
        store = TeraboxSessionStore(db_url="")
        source_url = "https://terabox.com/s/1share"
        children = []
        for part, fs_id in ((1, "101"), (2, "102")):
            children.append(
                await store.create_session(
                    owner_id=123,
                    chat_id=-1001,
                    source_message_id=55,
                    source_url=source_url,
                    title=f"Folder (part {part}/2)",
                    mode="normal",
                    custom_filename=None,
                    files=[
                        {
                            "page_url": source_url,
                            "filename": f"old-{part}.bin",
                            "relative_path": f"Folder/old-{part}.bin",
                            "source_position": part,
                            "size_bytes": part * 10,
                            "fs_id": fs_id,
                            "source_dlink": (
                                f"https://dm-d.1024terabox.com/file/old-{part}"
                            ),
                            "metadata_revision": 1,
                        }
                    ],
                    initial_state=(
                        SESSION_RUNNING if part == 1 else SESSION_PAUSED
                    ),
                    session_fields={
                        "chain_id": "refreshchain",
                        "part_index": part,
                        "total_parts": 2,
                    },
                )
            )
        await store.create_chain(
            chain_id="refreshchain",
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=source_url,
            name="Folder",
            mode="normal",
            total_parts=2,
            total_files=2,
            session_ids=[doc["_id"] for doc in children],
            chain_fields={"metadata_revision": 1},
        )
        stale = (await store.list_files(children[0]["_id"]))[0]
        refreshed_files = [
            {
                "name": "renamed-one.bin",
                "terabox_file": TeraboxFile(
                    name="renamed-one.bin",
                    relative_path="Folder/renamed-one.bin",
                    size=11,
                    download_url="https://dm-d.1024terabox.com/file/new-1",
                    fs_id="101",
                ),
            },
            {
                "name": "renamed-two.bin",
                "terabox_file": TeraboxFile(
                    name="renamed-two.bin",
                    relative_path="Folder/renamed-two.bin",
                    size=22,
                    download_url="https://dm-d.1024terabox.com/file/new-2",
                    fs_id="102",
                ),
            },
        ]

        async def scan_once(_source_url, _cookie):
            await asyncio.sleep(0.01)
            return object(), refreshed_files

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(
                terabox.TERABOX_CONFIG,
                "get_cookie",
                AsyncMock(return_value="current-cookie"),
            ),
            patch.object(
                terabox, "_resolve_with_cookie", AsyncMock(side_effect=scan_once)
            ) as scan,
        ):
            first, second = await asyncio.gather(
                terabox._refresh_terabox_chain_metadata(children[0], stale),
                terabox._refresh_terabox_chain_metadata(children[0], stale),
            )
            persisted = await store.list_chain_files("refreshchain")

        scan.assert_awaited_once_with(source_url, "current-cookie")
        self.assertEqual("https://dm-d.1024terabox.com/file/new-1", first["source_dlink"])
        self.assertEqual("https://dm-d.1024terabox.com/file/new-1", second["source_dlink"])
        self.assertEqual(["101", "102"], [doc["fs_id"] for doc in persisted])
        self.assertEqual(
            ["renamed-one.bin", "renamed-two.bin"],
            [doc["filename"] for doc in persisted],
        )
        self.assertEqual([2, 2], [doc["metadata_revision"] for doc in persisted])

    async def test_authorization_uses_cookie_current_at_download_time(self):
        store = TeraboxSessionStore(db_url="")
        source_url = "https://terabox.com/s/1share"
        child = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=source_url,
            title="Folder (part 1/1)",
            mode="normal",
            custom_filename=None,
            files=[
                {
                    "page_url": source_url,
                    "filename": "one.bin",
                    "relative_path": "Folder/one.bin",
                    "source_position": 1,
                    "size_bytes": 10,
                    "fs_id": "101",
                    "source_dlink": "https://dm-d.1024terabox.com/file/source",
                    "metadata_revision": 1,
                }
            ],
            session_fields={
                "chain_id": "cookiechain",
                "part_index": 1,
                "total_parts": 1,
            },
        )
        await store.create_chain(
            chain_id="cookiechain",
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=source_url,
            name="Folder",
            mode="normal",
            total_parts=1,
            total_files=1,
            session_ids=[child["_id"]],
            chain_fields={"metadata_revision": 1},
        )
        file_doc = (await store.list_files(child["_id"]))[0]
        fake_resolver = SimpleNamespace(
            authorize_download_url=AsyncMock(
                return_value="https://storage.test/authorized"
            )
        )

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(
                terabox.TERABOX_CONFIG,
                "get_cookie",
                AsyncMock(return_value="newest-cookie"),
            ),
            patch.object(
                terabox, "TeraboxResolver", return_value=fake_resolver
            ) as resolver_type,
        ):
            url, headers = await terabox._authorize_stored_terabox_file(
                child, file_doc
            )

        resolver_type.assert_called_once_with(
            terabox.session, "newest-cookie", terabox.TERABOX_BASE_URL
        )
        fake_resolver.authorize_download_url.assert_awaited_once_with(
            "https://dm-d.1024terabox.com/file/source"
        )
        self.assertEqual("https://storage.test/authorized", url)
        self.assertFalse(any(header.startswith("Cookie:") for header in headers))

    async def test_stale_authorization_refreshes_and_retries_once(self):
        session_doc = {
            "_id": "session",
            "chain_id": "chain",
            "source_url": "https://terabox.com/s/1share",
        }
        stale = {
            "_id": "session:1",
            "filename": "one.bin",
            "fs_id": "101",
            "source_dlink": "https://dm-d.1024terabox.com/file/old",
            "metadata_revision": 1,
        }
        refreshed = {
            **stale,
            "source_dlink": "https://dm-d.1024terabox.com/file/new",
            "metadata_revision": 2,
        }
        old_resolver = SimpleNamespace(
            authorize_download_url=AsyncMock(
                side_effect=TeraboxMetadataStaleError("expired")
            )
        )
        new_resolver = SimpleNamespace(
            authorize_download_url=AsyncMock(
                return_value="https://storage.test/authorized"
            )
        )

        with (
            patch.object(
                terabox,
                "_cached_terabox_file",
                AsyncMock(return_value=stale),
            ),
            patch.object(
                terabox,
                "_refresh_terabox_chain_metadata",
                AsyncMock(return_value=refreshed),
            ) as refresh,
            patch.object(
                terabox.TERABOX_CONFIG,
                "get_cookie",
                AsyncMock(side_effect=["cookie-one", "cookie-two"]),
            ),
            patch.object(
                terabox,
                "TeraboxResolver",
                side_effect=[old_resolver, new_resolver],
            ),
        ):
            url, _headers = await terabox._authorize_stored_terabox_file(
                session_doc, stale
            )

        refresh.assert_awaited_once_with(session_doc, stale)
        new_resolver.authorize_download_url.assert_awaited_once_with(
            refreshed["source_dlink"]
        )
        self.assertEqual("https://storage.test/authorized", url)

    async def test_runner_skips_failed_download_and_finishes_after_other_uploads(self):
        store = TeraboxSessionStore(db_url="")
        source_url = "https://terabox.com/s/1share"
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url=source_url,
            title="Folder (part 1/1)",
            mode="normal",
            custom_filename=None,
            files=[
                {
                    "page_url": source_url,
                    "filename": "tiny.zip",
                    "relative_path": "tiny.zip",
                    "source_position": 1,
                    "size_bytes": 59532,
                },
                {
                    "page_url": source_url,
                    "filename": "good.zip",
                    "relative_path": "good.zip",
                    "source_position": 2,
                    "size_bytes": 100,
                },
            ],
            session_fields={
                "chain_id": "skipchain",
                "part_index": 1,
                "total_parts": 1,
                "part_bytes": 59632,
            },
        )
        resolved_files = [
            {
                "name": "tiny.zip",
                "path": "tiny.zip",
                "size": 59532,
                "normal_dlink": "https://cdn.test/tiny.zip",
            },
            {
                "name": "good.zip",
                "path": "good.zip",
                "size": 100,
                "normal_dlink": "https://cdn.test/good.zip",
            },
        ]
        message = SimpleNamespace(reply_text=AsyncMock())
        upload_callbacks = []
        calls = 0

        async def download(*_args, **kwargs):
            nonlocal calls
            calls += 1
            self.assertTrue(kwargs["suppress_download_errors"])
            if calls == 1:
                self.assertTrue(await kwargs["on_gid"]("failedgid"))
                return "Aria2 24: range server ignored request"
            self.assertTrue(await kwargs["on_gid"]("goodgid"))
            await kwargs["on_downloaded"]()
            upload_callbacks.append(kwargs["on_uploaded"])
            return "complete"

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(terabox, "initiate_directdl", side_effect=download),
        ):
            await terabox._run_terabox_session(
                object(), message, session["_id"], resolved=(None, resolved_files)
            )
            files = await store.list_files(session["_id"])
            self.assertEqual(FILE_FAILED, files[0]["status"])
            self.assertIn("ignored request", files[0]["error"])
            self.assertEqual(FILE_DOWNLOADED, files[1]["status"])

            await upload_callbacks[0]([
                ("good.zip", "https://t.me/c/1/10")
            ], None)

        completed = await store.get_session(session["_id"])
        self.assertEqual(SESSION_COMPLETED, completed["state"])
        self.assertEqual(1, completed["skipped_files"])
        final_text = "\n".join(
            call.args[0] for call in message.reply_text.await_args_list
        )
        self.assertIn("finished with 1 skipped file(s)", final_text)
        self.assertIn("tiny.zip", final_text)

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

    async def test_continue_preserves_terminally_skipped_files(self):
        store = TeraboxSessionStore(db_url="")
        session = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="terabox-account:/Library",
            title="Library",
            mode="normal",
            custom_filename=None,
            files=[
                {"page_url": "terabox-account:/Library", "filename": "large.zip"},
                {"page_url": "terabox-account:/Library", "filename": "retry.zip"},
            ],
            session_fields={"chain_id": "chain", "part_index": 1, "total_parts": 1},
        )
        files = await store.list_files(session["_id"])
        await store.update_file(
            files[0]["_id"],
            FILE_FAILED,
            error="TeraBox batch package is too large (error 31090)",
            terminal_skip=True,
        )
        await store.update_file(
            files[1]["_id"], FILE_FAILED, error="temporary failure"
        )

        await store.prepare_continue(session["_id"], -1002, 99)
        resumed = await store.list_files(session["_id"])

        self.assertEqual(FILE_FAILED, resumed[0]["status"])
        self.assertTrue(resumed[0]["terminal_skip"])
        self.assertEqual(FILE_PENDING, resumed[1]["status"])

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
