import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from lazyleech.utils import upload_worker


class ParallelSplitUploadTests(unittest.IsolatedAsyncioTestCase):
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

        self.assertCountEqual(
            ["archive.zip.0001", "archive.zip.0002"], started
        )
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

            with patch.object(
                upload_worker, "update_upload_status_state", AsyncMock()
            ):
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

            self.assertEqual(
                ("archive.zip.0002", "https://t.me/c/1/26112"), result
            )
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

    @staticmethod
    async def _return_result(result):
        return result


if __name__ == "__main__":
    unittest.main()
