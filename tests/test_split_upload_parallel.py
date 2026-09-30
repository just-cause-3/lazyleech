import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from lazyleech.utils import upload_worker


class ThumbnailValidationTests(unittest.TestCase):
    def test_zero_byte_thumbnail_is_not_sent_to_telegram(self):
        with tempfile.TemporaryDirectory() as tempdir:
            thumbnail = Path(tempdir) / "thumbnail.jpg"
            thumbnail.touch()

            self.assertIsNone(upload_worker._usable_thumbnail(str(thumbnail)))

            thumbnail.write_bytes(b"valid-image-placeholder")
            self.assertEqual(
                str(thumbnail), upload_worker._usable_thumbnail(str(thumbnail))
            )


class ParallelSplitUploadTests(unittest.IsolatedAsyncioTestCase):
    async def test_per_source_callback_precedes_sibling_completion_and_receives_cleanup(
        self,
    ):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            first = Path(workdir) / "a.bin"
            second = Path(workdir) / "b.bin"
            first.write_bytes(b"a")
            second.write_bytes(b"b")
            first_reported = asyncio.Event()
            reports = []

            async def transfer(*args, **kwargs):
                path = Path(args[4])
                self.assertEqual(workdir, kwargs["workspace_temp_root"])
                if path == second:
                    await asyncio.wait_for(first_reported.wait(), timeout=2)
                path.unlink()
                return upload_worker.UploadResult(
                    [(path.name, "https://t.me/c/1/1")],
                    complete=True,
                    source_removed=True,
                )

            async def report(result):
                reports.append(result)
                if result["relative_name"] == "a.bin":
                    self.assertFalse(first.exists())
                    self.assertTrue(second.exists())
                    first_reported.set()

            with patch.object(upload_worker, "_upload_file", side_effect=transfer):
                result = await upload_worker._upload_worker(
                    object(),
                    SimpleNamespace(from_user=SimpleNamespace(id=123)),
                    SimpleNamespace(chat=SimpleNamespace(id=-1001), id=55),
                    {
                        "dir": workdir,
                        "files": [{"path": str(first)}, {"path": str(second)}],
                    },
                    workdir,
                    (),
                    None,
                    {
                        "parallel_files": 3,
                        "suppress_summary": True,
                        "workspace_temp_root": workdir,
                        "on_source_uploaded": report,
                    },
                )
            self.assertTrue(result.complete)
            self.assertEqual(2, len(reports))
            self.assertTrue(all(item["source_removed"] for item in reports))

    async def test_torrent_job_runs_three_source_uploads_concurrently(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            source_paths = [
                str(Path(workdir) / f"file-{index}.bin") for index in range(4)
            ]
            torrent_info = {
                "dir": workdir,
                "files": [{"path": path, "selected": "true"} for path in source_paths],
            }
            message = SimpleNamespace(from_user=SimpleNamespace(id=123))
            reply = SimpleNamespace(chat=SimpleNamespace(id=-1001), id=55)
            three_started = asyncio.Event()
            active = 0
            maximum = 0
            transfer_slots = set()
            split_slots = set()

            async def fake_upload_file(*args, **kwargs):
                nonlocal active, maximum
                active += 1
                maximum = max(maximum, active)
                transfer_slots.add(id(kwargs["transfer_semaphore"]))
                split_slots.add(id(kwargs["split_semaphore"]))
                if active == 3:
                    three_started.set()
                await three_started.wait()
                await asyncio.sleep(0)
                active -= 1
                filename = args[3]
                return upload_worker.UploadResult(
                    [(filename, f"https://t.me/c/1/{args[7]}")], complete=True
                )

            with patch.object(
                upload_worker, "_upload_file", side_effect=fake_upload_file
            ):
                result = await upload_worker._upload_worker(
                    object(),
                    message,
                    reply,
                    torrent_info,
                    workdir,
                    (),
                    None,
                    {"parallel_files": 3, "suppress_summary": True},
                )

        self.assertTrue(result.complete)
        self.assertEqual(3, maximum)
        self.assertEqual(1, len(transfer_slots))
        self.assertEqual(1, len(split_slots))
        self.assertEqual(4, len(result.source_results))

    async def test_shared_transfer_limit_caps_split_parts_at_three(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as tempdir:
            parts = []
            for index in range(4):
                part = Path(tempdir) / f"archive.zip.{index + 1:04d}"
                part.write_bytes(b"part")
                parts.append((str(part), part.name))
            active = 0
            maximum = 0
            three_started = asyncio.Event()

            async def reply_document(_path, **_kwargs):
                nonlocal active, maximum
                active += 1
                maximum = max(maximum, active)
                if active == 3:
                    three_started.set()
                await three_started.wait()
                await asyncio.sleep(0)
                active -= 1
                return SimpleNamespace(link=f"https://t.me/c/1/{maximum}")

            message = SimpleNamespace(
                chat=SimpleNamespace(id=-1001),
                reply_document=reply_document,
                reply_text=AsyncMock(),
            )
            with patch.object(upload_worker, "update_upload_status_state", AsyncMock()):
                result = await upload_worker._upload_split_parts(
                    object(),
                    message,
                    (-1001, 55),
                    123,
                    parts,
                    None,
                    tempdir,
                    transfer_semaphore=asyncio.Semaphore(3),
                )

        self.assertEqual(3, maximum)
        self.assertEqual(4, len(result))

    async def test_parts_are_started_together_but_results_are_numerically_ordered(self):
        both_started = asyncio.Event()
        release_first = asyncio.Event()
        started = []
        finished = []

        async def fake_upload_part(
            _client,
            _message,
            _worker_identifier,
            _user_id,
            _path,
            filename,
            _thumbnail,
            _tempdir,
        ):
            started.append(filename)
            if len(started) == 2:
                both_started.set()
            await both_started.wait()
            if filename.endswith(".0001"):
                await release_first.wait()
            else:
                finished.append(filename)
                release_first.set()
            finished.append(filename)
            return filename, f"https://t.me/c/1/{filename[-1]}"

        parts = [
            ("part-one", "archive.zip.0001"),
            ("part-two", "archive.zip.0002"),
        ]
        with patch.object(
            upload_worker, "_upload_split_part", side_effect=fake_upload_part
        ):
            result = await upload_worker._upload_split_parts(
                object(),
                SimpleNamespace(),
                (1, 2),
                3,
                parts,
                None,
                "temp",
            )

        self.assertCountEqual(["archive.zip.0001", "archive.zip.0002"], started)
        self.assertEqual("archive.zip.0002", finished[0])
        self.assertEqual(
            ["archive.zip.0001", "archive.zip.0002"],
            [name for name, _ in result],
        )

    async def test_successful_part_is_deleted_immediately(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as tempdir:
            part = Path(tempdir) / "archive.zip.0002"
            part.write_bytes(b"part")
            message = SimpleNamespace(
                chat=SimpleNamespace(id=-1001),
                reply_document=AsyncMock(
                    return_value=SimpleNamespace(link="https://t.me/c/1/26112")
                ),
                reply_text=AsyncMock(),
            )

            with patch.object(upload_worker, "update_upload_status_state", AsyncMock()):
                result = await upload_worker._upload_split_part(
                    object(),
                    message,
                    (-1001, 55),
                    123,
                    str(part),
                    part.name,
                    None,
                    tempdir,
                )

            self.assertEqual(("archive.zip.0002", "https://t.me/c/1/26112"), result)
            self.assertFalse(part.exists())

    async def test_incomplete_upload_does_not_delete_download_directory(self):
        reply = SimpleNamespace(chat=SimpleNamespace(id=-1001), id=55)
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            download_root = Path(workdir) / "download-root"
            download_root.mkdir()
            task = asyncio.create_task(
                self._return_result(upload_worker.UploadResult([], complete=False))
            )

            with patch.object(upload_worker.shutil, "rmtree") as remove_tree:
                await upload_worker.cleanup_upload(
                    task,
                    (-1001, 55),
                    {"dir": str(download_root)},
                    reply,
                    123,
                    {},
                )

            remove_tree.assert_not_called()
            self.assertTrue(download_root.exists())

    async def test_success_callback_runs_only_after_download_directory_is_deleted(self):
        reply = SimpleNamespace(chat=SimpleNamespace(id=-1001), id=55)
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            download_root = Path(workdir) / "download-root"
            download_root.mkdir()
            (download_root / "uploaded.bin").write_bytes(b"uploaded")
            task = asyncio.create_task(
                self._return_result(
                    upload_worker.UploadResult(
                        [("uploaded.bin", "https://t.me/c/1/2")], complete=True
                    )
                )
            )
            callback = AsyncMock()

            async def verify_cleanup(_sent_files, error):
                self.assertIsNone(error)
                self.assertFalse(download_root.exists())

            callback.side_effect = verify_cleanup
            with patch.object(upload_worker, "TESTMODE", False):
                await upload_worker.cleanup_upload(
                    task,
                    (-1001, 55),
                    {"dir": str(download_root)},
                    reply,
                    123,
                    {"on_uploaded": callback},
                )

            callback.assert_awaited_once()
            self.assertFalse(download_root.exists())

    async def test_cleanup_failure_is_reported_to_completion_callback(self):
        reply = SimpleNamespace(chat=SimpleNamespace(id=-1001), id=55)
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            download_root = Path(workdir) / "download-root"
            download_root.mkdir()
            task = asyncio.create_task(
                self._return_result(
                    upload_worker.UploadResult(
                        [("uploaded.bin", "https://t.me/c/1/2")], complete=True
                    )
                )
            )
            callback = AsyncMock()

            with (
                patch.object(upload_worker, "TESTMODE", False),
                patch.object(
                    upload_worker.shutil,
                    "rmtree",
                    side_effect=OSError("busy"),
                ),
            ):
                await upload_worker.cleanup_upload(
                    task,
                    (-1001, 55),
                    {"dir": str(download_root)},
                    reply,
                    123,
                    {"on_uploaded": callback},
                )

            callback.assert_awaited_once()
            self.assertIn("download cleanup failed", callback.await_args.args[1])
            self.assertTrue(download_root.exists())

    async def test_split_source_is_removed_before_parts_start_uploading(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            root = Path(workdir) / "download-root"
            root.mkdir()
            source = root / "archive.zip"
            source.write_bytes(b"source")
            user_dir = Path(workdir) / "user"
            user_dir.mkdir()
            message = SimpleNamespace(
                from_user=SimpleNamespace(id=user_dir),
                chat=SimpleNamespace(id=-1001),
                reply_text=AsyncMock(),
            )
            reply = SimpleNamespace(chat=SimpleNamespace(id=-1001), id=55)

            async def fake_split(_source, destination, _force_document):
                parts = []
                for number in (1, 2):
                    part = Path(destination) / f"archive.zip.{number:04d}"
                    part.write_bytes(b"part")
                    parts.append(str(part))
                return parts

            async def assert_source_released(
                _client,
                _message,
                _worker_identifier,
                _user_id,
                parts,
                _thumbnail,
                _tempdir,
            ):
                self.assertFalse(source.exists())
                self.assertEqual(2, len(parts))
                return [
                    (name, f"https://t.me/c/1/{index}")
                    for index, (_path, name) in enumerate(parts, 1)
                ]

            with (
                patch.object(upload_worker, "TELEGRAM_SPLIT_SIZE", 1),
                patch.object(upload_worker, "PROGRESS_UPDATE_DELAY", 0),
                patch.object(upload_worker, "split_files", side_effect=fake_split),
                patch.object(
                    upload_worker,
                    "_upload_split_parts",
                    side_effect=assert_source_released,
                ),
            ):
                result = await upload_worker._upload_file(
                    object(),
                    message,
                    reply,
                    source.name,
                    str(source),
                    True,
                    None,
                    1,
                    cleanup_source=True,
                    download_root=str(root),
                )

            self.assertTrue(result.complete)
            self.assertFalse(source.exists())

    async def test_failed_split_retains_its_source(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            root = Path(workdir) / "download-root"
            root.mkdir()
            source = root / "archive.zip"
            source.write_bytes(b"source")
            user_dir = Path(workdir) / "user"
            user_dir.mkdir()
            message = SimpleNamespace(
                from_user=SimpleNamespace(id=user_dir),
                chat=SimpleNamespace(id=-1001),
                reply_text=AsyncMock(),
            )
            reply = SimpleNamespace(chat=SimpleNamespace(id=-1001), id=55)

            with (
                patch.object(upload_worker, "TELEGRAM_SPLIT_SIZE", 1),
                patch.object(upload_worker, "PROGRESS_UPDATE_DELAY", 0),
                patch.object(
                    upload_worker,
                    "split_files",
                    AsyncMock(side_effect=OSError("No space left on device")),
                ),
            ):
                result = await upload_worker._upload_file(
                    object(),
                    message,
                    reply,
                    source.name,
                    str(source),
                    True,
                    None,
                    1,
                    cleanup_source=True,
                    download_root=str(root),
                )

            self.assertFalse(result.complete)
            self.assertTrue(source.exists())

    @staticmethod
    async def _return_result(result):
        return result


if __name__ == "__main__":
    unittest.main()
