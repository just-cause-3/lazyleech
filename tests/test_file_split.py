import os
import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "lazyleech" / "utils" / "file_split.py"
SPEC = importlib.util.spec_from_file_location("_test_file_split", MODULE_PATH)
FILE_SPLIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FILE_SPLIT)
split_binary_file = FILE_SPLIT.split_binary_file


class BinaryFileSplitTests(unittest.TestCase):
    def test_creates_all_numbered_parts_and_preserves_bytes(self):
        payload = bytes(range(32))
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            source = os.path.join(workdir, "sample.rar")
            destination = os.path.join(workdir, "parts")
            with open(source, "wb") as output:
                output.write(payload)

            parts = split_binary_file(source, destination, part_size=10)

            self.assertEqual(
                [
                    "sample.rar.0001",
                    "sample.rar.0002",
                    "sample.rar.0003",
                    "sample.rar.0004",
                ],
                [os.path.basename(part) for part in parts],
            )
            self.assertEqual(
                [10, 10, 10, 2], [os.path.getsize(part) for part in parts]
            )
            rebuilt = b""
            for part in parts:
                with open(part, "rb") as split_part:
                    rebuilt += split_part.read()
            self.assertEqual(payload, rebuilt)

    def test_rejects_invalid_part_size(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as workdir:
            source = os.path.join(workdir, "sample.bin")
            with open(source, "wb") as output:
                output.write(b"data")

            with self.assertRaises(ValueError):
                split_binary_file(source, workdir, part_size=0)


if __name__ == "__main__":
    unittest.main()
