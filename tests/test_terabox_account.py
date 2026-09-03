import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import lazyleech.plugins.terabox as terabox
from lazyleech.utils.terabox import TeraboxError
from lazyleech.utils.terabox_account import (
    TeraboxAccountClient,
    TeraboxBatchDownload,
    _rc4_signature,
    account_source_url,
    is_account_directory,
    normalize_account_path,
    safe_archive_name,
)
from lazyleech.utils.terabox_sessions import (
    FILE_DOWNLOADED,
    SESSION_PAUSED,
    SESSION_RUNNING,
    TeraboxSessionStore,
)


class TeraboxAccountPathTests(unittest.TestCase):
    def test_normalizes_relative_quoted_and_windows_paths(self):
        self.assertEqual("/Anime/Done", normalize_account_path("Anime/Done"))
        self.assertEqual(
            "/My Folder/Done",
            normalize_account_path('"/My Folder\\Done"'),
        )
        self.assertEqual(
            "terabox-account:/Anime/Done",
            account_source_url("Anime/Done"),
        )

    def test_rejects_traversal_and_control_characters(self):
        with self.assertRaises(TeraboxError):
            normalize_account_path("/Anime/../Private")
        with self.assertRaises(TeraboxError):
            normalize_account_path("/Anime\nPrivate")

    def test_archive_names_preserve_unicode_and_end_in_zip(self):
        self.assertEqual("神聖昂燐.zip", safe_archive_name("神聖昂燐"))
        self.assertEqual("archive.zip", safe_archive_name("archive.zip"))

    def test_batch_command_parses_spaces_and_separate_unit(self):
        message = SimpleNamespace(
            command=["batchdltera", '"/My', 'Folder/Done"', "15", "GB"]
        )
        path, size = terabox._batch_terabox_request_from_message(message)
        self.assertEqual("/My Folder/Done", path)
        self.assertEqual(15 * 1024**3, size)

    def test_rc4_signature_is_stable(self):
        self.assertEqual("fQ1YmEE=", _rc4_signature("key", "value"))

    def test_directory_flag_accepts_api_boolean_and_integer_shapes(self):
        self.assertTrue(is_account_directory({"isdir": 1}))
        self.assertTrue(is_account_directory({"isdir": True}))
        self.assertFalse(is_account_directory({"isdir": 0}))


class TeraboxAccountClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_client_uses_my_cloud_bootstrap(self):
        resolver = SimpleNamespace(
            ndus="disposable",
            origin="https://dm.1024terabox.com",
            bootstrap_path="/ai/index",
            timeout=object(),
        )
        with patch(
            "lazyleech.utils.terabox_account.TeraboxResolver",
            return_value=resolver,
        ):
            account = TeraboxAccountClient(Mock(), "disposable")

        self.assertEqual("/main", account.resolver.bootstrap_path)

    async def test_authorizes_batch_with_fresh_home_metadata(self):
        class StreamResponse:
            status = 206
            headers = {"Content-Type": "application/octet-stream"}
            url = "https://dm-data.1024terabox.com/rest/2.0/pcs/file"
            content = SimpleNamespace(read=AsyncMock(return_value=b"P"))

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        http_session = Mock()
        http_session.get.return_value = StreamResponse()
        resolver = SimpleNamespace(
            ndus="disposable",
            origin="https://dm.1024terabox.com",
            js_token="token",
            _share_authenticated=True,
            timeout=object(),
            session=http_session,
            _request_headers=Mock(
                return_value={
                    "User-Agent": "test",
                    "Cookie": "lang=en; ndus=disposable",
                }
            ),
            _bootstrap=AsyncMock(),
            _json_get=AsyncMock(
                side_effect=[
                    {
                        "errno": 0,
                        "data": {
                            "uk": 123,
                            "sign1": "value",
                            "sign3": "key",
                            "timestamp": 456,
                        },
                    },
                    {
                        "errno": 0,
                        "dlink": (
                            "https://dm-data.1024terabox.com/rest/2.0/pcs/file"
                            "?method=batchdownload"
                        ),
                    },
                ]
            ),
        )
        with patch(
            "lazyleech.utils.terabox_account.TeraboxResolver",
            return_value=resolver,
        ):
            account = TeraboxAccountClient(Mock(), "disposable")

        download = await account.authorize_batch_download(
            [11, "12"], "Folder.zip"
        )

        self.assertIn("method=batchdownload", download.url)
        self.assertEqual(8, download.max_connections)
        self.assertTrue(any(header.startswith("Cookie:") for header in download.headers))
        download_call = resolver._json_get.await_args_list[1]
        params = download_call.args[1]
        self.assertEqual("batch", params["type"])
        self.assertEqual("[11,12]", params["fidlist"])
        self.assertEqual("fQ1YmEE=", params["sign"])
        self.assertEqual("456", params["timestamp"])
        self.assertIn("ndus=disposable", account.download_headers[-1])

    async def test_rejects_cross_site_batch_url(self):
        resolver = SimpleNamespace(
            ndus="disposable",
            origin="https://dm.1024terabox.com",
            js_token="token",
            _share_authenticated=True,
            timeout=object(),
            session=Mock(),
            _request_headers=Mock(return_value={}),
            _bootstrap=AsyncMock(),
            _json_get=AsyncMock(
                side_effect=[
                    {
                        "errno": 0,
                        "data": {
                            "uk": 123,
                            "sign1": "value",
                            "sign3": "key",
                            "timestamp": 456,
                        },
                    },
                    {"errno": 0, "dlink": "https://attacker.example/file"},
                ]
            ),
        )
        with patch(
            "lazyleech.utils.terabox_account.TeraboxResolver",
            return_value=resolver,
        ):
            account = TeraboxAccountClient(Mock(), "disposable")

        with self.assertRaisesRegex(TeraboxError, "unsafe"):
            await account.authorize_batch_download([11], "Folder.zip")

    async def test_strips_cookie_after_cross_site_redirect(self):
        class RedirectResponse:
            status = 302
            headers = {"Location": "https://storage.example/signed.zip"}
            url = "https://dm-data.1024terabox.com/batch"
            content = SimpleNamespace(read=AsyncMock(return_value=b""))

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        class StorageResponse:
            status = 206
            headers = {"Content-Type": "application/zip"}
            url = "https://storage.example/signed.zip"
            content = SimpleNamespace(read=AsyncMock(return_value=b"P"))

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        http_session = Mock()
        http_session.get.side_effect = [RedirectResponse(), StorageResponse()]
        resolver = SimpleNamespace(
            ndus="disposable",
            origin="https://dm.1024terabox.com",
            js_token="token",
            _share_authenticated=True,
            timeout=object(),
            session=http_session,
            _request_headers=Mock(
                side_effect=lambda authenticated: {
                    "User-Agent": "test",
                    **(
                        {"Cookie": "lang=en; ndus=disposable"}
                        if authenticated
                        else {}
                    ),
                }
            ),
            _bootstrap=AsyncMock(),
            _json_get=AsyncMock(
                side_effect=[
                    {
                        "errno": 0,
                        "data": {
                            "uk": 123,
                            "sign1": "value",
                            "sign3": "key",
                            "timestamp": 456,
                        },
                    },
                    {
                        "errno": 0,
                        "dlink": "https://dm-data.1024terabox.com/batch",
                    },
                ]
            ),
        )
        with patch(
            "lazyleech.utils.terabox_account.TeraboxResolver",
            return_value=resolver,
        ):
            account = TeraboxAccountClient(Mock(), "disposable")

        download = await account.authorize_batch_download([11], "Folder.zip")

        self.assertEqual("https://storage.example/signed.zip", download.url)
        self.assertEqual(8, download.max_connections)
        self.assertFalse(
            any(header.lower().startswith("cookie:") for header in download.headers)
        )
        second_headers = http_session.get.call_args_list[1].kwargs["headers"]
        self.assertNotIn("Cookie", second_headers)

    async def test_falls_back_to_one_connection_when_ranges_are_ignored(self):
        class StreamResponse:
            status = 200
            headers = {"Content-Type": "application/zip"}
            url = "https://dm-data.1024terabox.com/batch"
            content = SimpleNamespace(read=AsyncMock(return_value=b"PK"))

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        resolver = SimpleNamespace(
            ndus="disposable",
            origin="https://dm.1024terabox.com",
            js_token="token",
            _share_authenticated=True,
            timeout=object(),
            session=SimpleNamespace(get=Mock(return_value=StreamResponse())),
            _request_headers=Mock(return_value={}),
            _bootstrap=AsyncMock(),
            _json_get=AsyncMock(
                side_effect=[
                    {
                        "errno": 0,
                        "data": {
                            "uk": 123,
                            "sign1": "value",
                            "sign3": "key",
                            "timestamp": 456,
                        },
                    },
                    {
                        "errno": 0,
                        "dlink": "https://dm-data.1024terabox.com/batch",
                    },
                ]
            ),
        )
        with patch(
            "lazyleech.utils.terabox_account.TeraboxResolver",
            return_value=resolver,
        ):
            account = TeraboxAccountClient(Mock(), "disposable")

        download = await account.authorize_batch_download(
            [11], "Folder.zip", preferred_connections=8
        )

        self.assertEqual(1, download.max_connections)


class FakeAccountTree:
    def __init__(self, tree, root):
        self.tree = tree
        self.root = root

    async def get_directory(self, path):
        return self.root

    async def list_directory(self, path):
        return self.tree[path]


def folder(name, path, fs_id):
    return {"server_filename": name, "path": path, "fs_id": fs_id, "isdir": 1}


def file(name, path, fs_id, size):
    return {
        "server_filename": name,
        "path": path,
        "fs_id": fs_id,
        "isdir": 0,
        "size": size,
    }


class TeraboxAccountPlannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_recursive_scan_batches_leaf_folders_without_duplicates(self):
        root = folder("Library", "/Library", 10)
        tree = {
            "/Library": [
                file("root.txt", "/Library/root.txt", 1, 10),
                folder("A", "/Library/A", 20),
                folder("B", "/Library/B", 30),
            ],
            "/Library/A": [
                file("a1.bin", "/Library/A/a1.bin", 2, 100),
                file("a2.bin", "/Library/A/a2.bin", 3, 100),
            ],
            "/Library/B": [folder("C", "/Library/B/C", 40)],
            "/Library/B/C": [
                file("c.bin", "/Library/B/C/c.bin", 4, 200)
            ],
        }

        scan = await terabox._scan_account_batch_archives(
            FakeAccountTree(tree, root), "/Library", 1024**3
        )

        self.assertEqual(4, scan["source_file_count"])
        self.assertEqual(410, scan["source_bytes"])
        self.assertEqual(
            ["Library.files.zip", "A.zip", "C.zip"],
            [item["filename"] for item in scan["archives"]],
        )
        self.assertEqual(
            [[1], [20], [40]],
            [item["batch_fs_ids"] for item in scan["archives"]],
        )
        self.assertEqual(
            ["Library.files.zip", "Library/A.zip", "Library/B/C.zip"],
            [item["relative_path"] for item in scan["archives"]],
        )

    async def test_oversized_leaf_is_partitioned_into_bounded_batches(self):
        mib = 1024**2
        root = folder("Leaf", "/Leaf", 10)
        tree = {
            "/Leaf": [
                file("one.bin", "/Leaf/one.bin", 1, 2 * mib),
                file("two.bin", "/Leaf/two.bin", 2, 2 * mib),
            ]
        }

        with patch.object(terabox, "TERABOX_BATCH_ARCHIVE_OVERHEAD", 8 * mib):
            scan = await terabox._scan_account_batch_archives(
                FakeAccountTree(tree, root), "/Leaf", 10 * mib
            )

        self.assertEqual(
            ["Leaf.part001.zip", "Leaf.part002.zip"],
            [item["filename"] for item in scan["archives"]],
        )
        self.assertEqual(
            [[1], [2]], [item["batch_fs_ids"] for item in scan["archives"]]
        )

    async def test_creation_persists_batch_provider_and_starts_first_part(self):
        store = TeraboxSessionStore(db_url="")
        mib = 1024**2
        archives = [
            {
                "page_url": "terabox-account:/Library",
                "filename": f"archive{index}.zip",
                "relative_path": f"Library/archive{index}.zip",
                "size_bytes": 9 * mib,
                "batch_source_bytes": 1 * mib,
                "batch_source_count": 2,
                "batch_fs_ids": [index],
                "source_position": index,
            }
            for index in (1, 2)
        ]
        scan = {
            "root_path": "/Library",
            "name": "Library",
            "archives": archives,
            "source_file_count": 4,
            "source_bytes": 2 * mib,
        }
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=-1001),
            id=55,
        )
        reply = SimpleNamespace(edit_text=AsyncMock())

        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(terabox.TERABOX_CONFIG, "get_cookie", AsyncMock(return_value="cookie")),
            patch.object(
                terabox,
                "_scan_account_batch_archives",
                AsyncMock(return_value=scan),
            ),
            patch.object(terabox, "_start_terabox_session") as start,
        ):
            sessions = await terabox._create_account_batch_sessions(
                object(), message, "/Library", 10 * mib, reply
            )

        self.assertEqual(2, len(sessions))
        self.assertEqual(SESSION_RUNNING, sessions[0]["state"])
        self.assertEqual(SESSION_PAUSED, sessions[1]["state"])
        stored_file = (await store.list_files(sessions[0]["_id"]))[0]
        self.assertEqual([1], stored_file["batch_fs_ids"])
        self.assertNotIn("cookie", str(stored_file))
        chain = (await store.list_chains(owner_id=123, limit=0))[0]
        self.assertEqual("terabox_account_batch", chain["provider"])
        self.assertEqual(4, chain["total_source_files"])
        start.assert_called_once()

    async def test_runner_uses_fresh_batch_url_and_supported_connections(self):
        store = TeraboxSessionStore(db_url="")
        session_doc = await store.create_session(
            owner_id=123,
            chat_id=-1001,
            source_message_id=55,
            source_url="terabox-account:/Library",
            title="Library (part 1/1)",
            mode="normal",
            custom_filename=None,
            files=[
                {
                    "page_url": "terabox-account:/Library",
                    "filename": "Leaf.zip",
                    "relative_path": "Library/Leaf.zip",
                    "size_bytes": 10,
                    "batch_fs_ids": [42],
                }
            ],
            session_fields={
                "provider": "terabox_account_batch",
                "chain_id": "chain",
                "part_index": 1,
                "total_parts": 1,
                "part_bytes": 10,
            },
        )
        fake_account = Mock(spec=TeraboxAccountClient)
        fake_account.authorize_batch_download = AsyncMock(
            return_value=TeraboxBatchDownload(
                "https://dm-data.1024terabox.com/batch",
                ["User-Agent: test", "Cookie: lang=en; ndus=disposable"],
                8,
            )
        )

        async def complete_download(*_args, **kwargs):
            self.assertEqual(8, kwargs["max_connections"])
            self.assertEqual(
                ["User-Agent: test", "Cookie: lang=en; ndus=disposable"],
                kwargs["headers"],
            )
            self.assertTrue(await kwargs["on_gid"]("batchgid"))
            await kwargs["on_downloaded"]()
            return "complete"

        message = SimpleNamespace(reply_text=AsyncMock())
        with (
            patch.object(terabox, "terabox_session_store", store),
            patch.object(
                terabox.TERABOX_CONFIG,
                "get_cookie",
                AsyncMock(return_value="disposable"),
            ),
            patch.object(
                terabox,
                "TeraboxAccountClient",
                return_value=fake_account,
            ),
            patch.object(
                terabox,
                "initiate_directdl",
                side_effect=complete_download,
            ),
        ):
            await terabox._run_terabox_session(
                object(), message, session_doc["_id"]
            )

        fake_account.authorize_batch_download.assert_awaited_once_with(
            [42],
            "Leaf.zip",
            preferred_connections=terabox.TERABOX_BATCH_CONNECTIONS,
        )
        stored_file = (await store.list_files(session_doc["_id"]))[0]
        self.assertEqual(FILE_DOWNLOADED, stored_file["status"])


if __name__ == "__main__":
    unittest.main()
