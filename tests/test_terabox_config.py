import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_NAME = "_test_terabox_config"
MODULE_PATH = Path(__file__).parents[1] / "lazyleech" / "utils" / "terabox_config.py"
SPEC = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[MODULE_NAME] = MODULE
SPEC.loader.exec_module(MODULE)
TeraboxConfigStore = MODULE.TeraboxConfigStore


class FakeCollection:
    def __init__(self):
        self.document = None

    async def find_one(self, _query):
        return dict(self.document) if self.document else None

    async def update_one(self, _query, update, upsert=False):
        assert upsert
        self.document = {"_id": "global", **update["$set"]}

    async def delete_one(self, _query):
        self.document = None


class TeraboxConfigStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_database_override_survives_new_store_instance(self):
        collection = FakeCollection()
        first = TeraboxConfigStore(collection=collection, env_cookie="from-env")
        await first.set_cookie("ndus=runtime-cookie", updated_by=123)

        restarted = TeraboxConfigStore(collection=collection, env_cookie="from-env")
        self.assertEqual("runtime-cookie", await restarted.get_cookie())
        self.assertEqual(123, collection.document["updated_by"])

    async def test_clear_restores_environment_fallback(self):
        collection = FakeCollection()
        store = TeraboxConfigStore(collection=collection, env_cookie="from-env")
        await store.set_cookie("runtime-cookie")
        await store.clear_cookie()
        self.assertEqual("from-env", await store.get_cookie())

    async def test_memory_fallback_updates_current_process(self):
        store = TeraboxConfigStore(db_url="", env_cookie="from-env")
        await store.set_cookie("runtime-cookie")
        self.assertFalse(store.persistent)
        self.assertEqual("runtime-cookie", await store.get_cookie())


if __name__ == "__main__":
    unittest.main()
