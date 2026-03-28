import os
import unittest
from pathlib import Path

import db


class DatabaseQueueTests(unittest.TestCase):
    def test_processing_rows_are_requeued_after_restart(self):
        root_dir = Path(__file__).resolve().parents[1]
        artifacts_dir = root_dir / "artifacts" / "test_runs"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        db_path = artifacts_dir / "test_crawl.db"

        if db_path.exists():
            db_path.unlink()

        original_path = os.environ.get("CRAWL_DB_PATH")
        os.environ["CRAWL_DB_PATH"] = str(db_path)
        try:
            db.init_db()
            db.add_to_queue("https://example.com/a", "CATEGORY")
            claimed = db.get_next_pending(limit=1)
            self.assertEqual(len(claimed), 1)

            recovered = db.requeue_processing_tasks()
            self.assertEqual(recovered, 1)

            claimed_again = db.get_next_pending(limit=1)
            self.assertEqual(claimed_again[0]["url"], "https://example.com/a")
        finally:
            if original_path is None:
                os.environ.pop("CRAWL_DB_PATH", None)
            else:
                os.environ["CRAWL_DB_PATH"] = original_path

            if db_path.exists():
                db_path.unlink()


if __name__ == "__main__":
    unittest.main()
