import unittest

from lazyleech.utils.status import active_uploads, update_upload_status
from lazyleech.utils.upload_worker import sanitize_upload_filename


class UploadStatusTests(unittest.TestCase):
    def tearDown(self):
        active_uploads.clear()

    def test_upload_replaces_splitting_source_name_with_part_name(self):
        identifier = (-100123, 456)
        active_uploads[identifier] = {
            "start_time": 1,
            "filename": "p1.rar",
            "chat_id": identifier[0],
            "state": "Splitting",
            "current": 0,
            "total": 1,
        }

        update_upload_status(
            identifier,
            current=1024,
            total=2048,
            filename="p1.rar.0001",
            chat_id=identifier[0],
        )

        self.assertEqual("p1.rar.0001", active_uploads[identifier]["filename"])
        self.assertEqual("Uploading", active_uploads[identifier]["state"])

    def test_upload_filename_preserves_japanese_text(self):
        filenames = (
            "暴淫荒野 白濁のビッチ姫.zip",
            "7th Divine ～白銀の聖女と漆黒の魔王～.zip",
        )

        for filename in filenames:
            with self.subTest(filename=filename):
                self.assertEqual(filename, sanitize_upload_filename(filename))

    def test_upload_filename_removes_only_unsafe_path_and_control_text(self):
        self.assertEqual(
            "悪名.zip", sanitize_upload_filename("folder\\悪\n名.zip")
        )

    def test_upload_filename_truncates_by_utf8_bytes_and_keeps_extension(self):
        filename = sanitize_upload_filename("姫" * 200 + ".zip")

        self.assertLessEqual(len(filename.encode("utf-8")), 250)
        self.assertTrue(filename.endswith(".zip"))


if __name__ == "__main__":
    unittest.main()
