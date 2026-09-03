import importlib.util
import os
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "lazyleech" / "utils" / "file_cleanup.py"
SPEC = importlib.util.spec_from_file_location("_test_file_cleanup", MODULE_PATH)
FILE_CLEANUP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FILE_CLEANUP)
remove_uploaded_source = FILE_CLEANUP.remove_uploaded_source
clear_abandoned_download_directories = (
    FILE_CLEANUP.clear_abandoned_download_directories
)


class UploadedSourceCleanupTests(unittest.TestCase):
    def test_removes_uploaded_file_and_only_empty_subdirectories(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            root = Path(workdir) / "download"
            uploaded = root / "season" / "episode" / "video.mkv"
            sibling = root / "keep.mkv"
            uploaded.parent.mkdir(parents=True)
            uploaded.write_bytes(b"uploaded")
            sibling.write_bytes(b"waiting")

            self.assertTrue(remove_uploaded_source(uploaded, root))

            self.assertFalse(uploaded.exists())
            self.assertFalse(uploaded.parent.exists())
            self.assertTrue(root.exists())
            self.assertTrue(sibling.exists())

    def test_refuses_to_remove_a_file_outside_download_root(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            workdir = Path(workdir)
            root = workdir / "download"
            outside = workdir / "outside.bin"
            root.mkdir()
            outside.write_bytes(b"keep")

            self.assertFalse(remove_uploaded_source(outside, root))
            self.assertTrue(outside.exists())


class StartupDownloadCleanupTests(unittest.TestCase):
    def test_removes_only_recognized_job_directories(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            root = Path(workdir)
            user_root = root / "804248372"
            user_root.mkdir()
            removable = [
                user_root / "1788426579.8072455",
                user_root / "bunkr_sessions",
                user_root / "terabox_sessions",
                user_root / "tmpabcd1234",
            ]
            for directory in removable:
                directory.mkdir()
                (directory / "payload.bin").write_bytes(b"payload")
            thumbnail = user_root / "thumbnail.jpg"
            thumbnail.write_bytes(b"thumbnail")
            unrelated = user_root / "keep-me"
            unrelated.mkdir()
            (unrelated / "data.bin").write_bytes(b"data")
            non_user = root / "lazyleech"
            non_user.mkdir()

            removed = clear_abandoned_download_directories(str(root))

            self.assertEqual(
                {str(path.resolve()) for path in removable}, set(removed)
            )
            for directory in removable:
                self.assertFalse(directory.exists())
            self.assertTrue(thumbnail.exists())
            self.assertTrue(unrelated.exists())
            self.assertTrue(non_user.exists())

    def test_removes_empty_user_directory_but_preserves_user_assets(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            root = Path(workdir)
            empty_after_cleanup = root / "123"
            (empty_after_cleanup / "1234.5").mkdir(parents=True)
            preserved = root / "456"
            preserved.mkdir()
            (preserved / "watermark.jpg").write_bytes(b"watermark")

            clear_abandoned_download_directories(str(root))

            self.assertFalse(empty_after_cleanup.exists())
            self.assertTrue((preserved / "watermark.jpg").exists())


if __name__ == "__main__":
    unittest.main()
