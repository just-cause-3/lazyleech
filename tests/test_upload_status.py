import unittest

from lazyleech.utils.status import active_uploads, update_upload_status


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


if __name__ == "__main__":
    unittest.main()
