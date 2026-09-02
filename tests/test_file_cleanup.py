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


if __name__ == "__main__":
    unittest.main()
