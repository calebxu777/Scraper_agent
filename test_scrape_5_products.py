import json
import os
from pathlib import Path

from db import get_products
from main import worker_loop


def main():
    artifacts_dir = Path(__file__).resolve().parent / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)

    os.environ["CRAWL_DB_PATH"] = str(artifacts_dir / "scrape_5.db")
    worker_loop(max_products=5, sleep_seconds=1.0)

    products = get_products(limit=5)
    export_path = artifacts_dir / "scrape_5_products.json"
    parsed_products = [json.loads(row["product_data"]) for row in products]
    export_path.write_text(json.dumps(parsed_products, indent=2), encoding="utf-8")

    print(f"Saved {len(parsed_products)} products to {export_path}")


if __name__ == "__main__":
    main()
