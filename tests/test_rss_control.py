import copy
import unittest

from lazyleech.utils.rss_control import RSSControlStore


class FakeCollection:
    def __init__(self):
        self.docs = {}

    async def find_one(self, query):
        doc = self.docs.get(query["_id"])
        return copy.deepcopy(doc) if doc is not None else None

    async def update_one(self, query, update, upsert=False):
        doc = self.docs.setdefault(query["_id"], {"_id": query["_id"]})
        doc.update(copy.deepcopy(update["$set"]))


class RSSControlStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_pause_and_resume_in_memory(self):
        store = RSSControlStore()
        self.assertFalse(await store.is_paused())

        paused = await store.set_paused(True, updated_by=123)
        self.assertTrue(paused["paused"])
        self.assertEqual(123, paused["updated_by"])
        self.assertTrue(await store.is_paused())

        await store.set_paused(False, updated_by=456)
        self.assertFalse(await store.is_paused())

    async def test_mongo_state_is_visible_to_a_new_store_instance(self):
        collection = FakeCollection()
        first_process = RSSControlStore(collection)
        await first_process.set_paused(True, updated_by=123)

        restarted_process = RSSControlStore(collection)
        state = await restarted_process.get_state()
        self.assertTrue(state["paused"])
        self.assertEqual(123, state["updated_by"])


if __name__ == "__main__":
    unittest.main()
