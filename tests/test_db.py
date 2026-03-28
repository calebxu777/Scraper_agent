import os
import asyncio
import unittest
from pathlib import Path

import db


class DatabaseQueueTests(unittest.TestCase):
    def setUp(self):
        root_dir = Path(__file__).resolve().parents[1]
        artifacts_dir = root_dir / "artifacts_api" / "test_runs"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = artifacts_dir / "test_crawl.db"
        if self.db_path.exists():
            self.db_path.unlink()

        self.original_path = os.environ.get("CRAWL_DB_PATH")
        os.environ["CRAWL_DB_PATH"] = str(self.db_path)

    def tearDown(self):
        if self.original_path is None:
            os.environ.pop("CRAWL_DB_PATH", None)
        else:
            os.environ["CRAWL_DB_PATH"] = self.original_path

        if self.db_path.exists():
            self.db_path.unlink()

    def test_processing_rows_are_requeued_after_restart(self):
        async def run():
            await db.init_db()
            await db.add_to_queue("https://example.com/a", "CATEGORY")
            claimed = await db.get_next_pending(limit=1)
            self.assertEqual(len(claimed), 1)

            recovered = await db.requeue_processing_tasks()
            self.assertEqual(recovered, 1)

            claimed_again = await db.get_next_pending(limit=1)
            self.assertEqual(claimed_again[0]["url"], "https://example.com/a")
        asyncio.run(run())

    def test_incomplete_products_are_saved_but_not_counted_as_complete(self):
        async def run():
            await db.init_db()
            await db.add_to_queue("https://example.com/p1", "PRODUCT")
            await db.save_product(
                "https://example.com/p1",
                '{"product_name":"Example","source_url":"https://example.com/p1","variations":[]}',
                record_status="incomplete",
                queue_status="FAILED",
                detail="saved incomplete product record",
                error="missing price",
            )

            complete_count = await db.count_products()
            all_products = await db.get_products(include_incomplete=True)

            self.assertEqual(complete_count, 0)
            self.assertEqual(len(all_products), 1)
            self.assertEqual(all_products[0]["record_status"], "incomplete")
            self.assertEqual(all_products[0]["queue_status"], "FAILED")

        asyncio.run(run())

    def test_get_queue_rows_can_return_skipped_urls(self):
        async def run():
            await db.init_db()
            await db.add_to_queue("https://example.com/skipped", "CATEGORY")
            await db.update_status("https://example.com/skipped", "SKIPPED", detail="out of scope")

            skipped_rows = await db.get_queue_rows(status="SKIPPED")

            self.assertEqual(len(skipped_rows), 1)
            self.assertEqual(skipped_rows[0]["url"], "https://example.com/skipped")
            self.assertEqual(skipped_rows[0]["detail"], "out of scope")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
