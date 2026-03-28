"""
run_full_crawl.py - Production Crawl Script for Safco Dental Supply
====================================================================

Usage:
    # Gold-standard API-only crawl (USE_HANDYMAN=false):
    python run_full_crawl.py --mode api

    # Tiered local inference + escalation crawl (USE_HANDYMAN=true):
    python run_full_crawl.py --mode local

    # Control max products (default: unlimited within the two categories):
    python run_full_crawl.py --mode api --max-products 50

Outputs:
    artifacts_api/       (api mode)
    artifacts_handyman/  (local mode)
        - crawl.db                  SQLite crawl state database
        - products.json             Structured JSON (full schema + metadata)
        - products.csv              Flattened CSV for spreadsheet analysis
"""

import argparse
import asyncio
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from db import get_products, get_queue_counts, get_queue_rows
from main import worker_loop


# Assignment seed URLs
SEED_URLS = [
    "https://www.safcodental.com/catalog/sutures-surgical-products",
    "https://www.safcodental.com/catalog/gloves",
]

# Schema documentation (embedded in JSON output)
SCHEMA_DOC = {
    "product_name": "Name of the product as listed on the page",
    "brand": "Brand or manufacturer name (null if not listed)",
    "category_hierarchy": "List of breadcrumb categories, e.g. ['Gloves', 'Nitrile Gloves']",
    "description": "Product description or summary text from the page",
    "variations": [
        {
            "sku": "SKU / item number / product code",
            "size": "Size, gauge, or dimension (null if N/A)",
            "package_count": "Quantity per box/pack, e.g. 'Box of 100'",
            "price": "Price as float (null if not publicly visible)",
            "availability": "true if in stock, false if out of stock, null if unknown",
        }
    ],
    "image_urls": "List of product image URLs",
    "alternative_products": "List of related/alternative products mentioned on the page",
    "source_url": "The URL where this product was scraped from",
    "extraction_method": "One of: local_qwen | local_qwen_fixed | api_gpt4o_mini | api_gpt4o_mini_fixed",
    "extraction_latency": "Seconds taken to extract this product's structured data",
    "quality_status": "Extraction quality classification: complete or incomplete",
    "quality_notes": "Known extraction-quality issues for this record",
    "record_status": "Stored record state in SQLite: complete or incomplete",
    "crawl_status": "Queue outcome for the source URL: COMPLETED | FAILED | SKIPPED",
    "crawl_detail": "Short crawl/extraction detail message",
    "crawl_error": "Last validation or extraction error string, if any",
    "skipped_urls": [
        {
            "url": "Queue URL that was skipped",
            "page_type": "CATEGORY | PRODUCT | UNKNOWN",
            "status": "SKIPPED",
            "retry_count": "Retry count recorded for the queue row",
            "last_updated": "UTC timestamp of the last queue update",
            "detail": "Skip reason or routing note",
            "last_error": "Last error string if one was recorded",
        }
    ],
}


def export_json(products: list[dict], skipped_urls: list[dict], output_path: Path, mode: str, queue_counts: dict):
    """Export products to JSON with metadata and schema documentation."""
    local_methods = {"local_qwen", "local_qwen_fixed"}
    api_methods = {"api_gpt4o_mini", "api_gpt4o_mini_fixed"}

    local_lats = [p.get("extraction_latency", 0.0) for p in products if p.get("extraction_method", "") in local_methods]
    api_lats = [p.get("extraction_latency", 0.0) for p in products if p.get("extraction_method", "") in api_methods]
    escalated = sum(1 for p in products if p.get("extraction_method", "") in api_methods)
    total = len(products)
    complete_records = sum(1 for p in products if p.get("record_status", "complete") == "complete")
    incomplete_records = total - complete_records

    payload = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "mode": mode,
            "seed_urls": SEED_URLS,
            "total_products": total,
            "complete_products": complete_records,
            "incomplete_products": incomplete_records,
            "skipped_urls_count": len(skipped_urls),
            "escalated_to_api": escalated,
            "handled_locally": total - escalated,
            "escalation_percentage": f"{(escalated / total * 100):.1f}%" if total else "N/A",
            "average_latency_local_sec": round(sum(local_lats) / len(local_lats), 2) if local_lats else None,
            "average_latency_api_sec": round(sum(api_lats) / len(api_lats), 2) if api_lats else None,
            "crawl_stats": {
                "total_urls_discovered": sum(queue_counts.values()),
                "completed": queue_counts.get("COMPLETED", 0),
                "skipped": queue_counts.get("SKIPPED", 0),
                "failed": queue_counts.get("FAILED", 0),
            },
        },
        "schema": SCHEMA_DOC,
        "data": products,
        "skipped_urls": skipped_urls,
    }

    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  JSON -> {output_path} ({total} products, {len(skipped_urls)} skipped URLs)")


def export_csv(products: list[dict], output_path: Path):
    """Export products to a flattened CSV for spreadsheet analysis."""
    rows = []
    for p in products:
        base = {
            "product_name": p.get("product_name", ""),
            "brand": p.get("brand", ""),
            "category_hierarchy": " > ".join(p.get("category_hierarchy", [])),
            "description": p.get("description", ""),
            "image_urls": " | ".join(p.get("image_urls", [])),
            "alternative_products": " | ".join(p.get("alternative_products", [])),
            "source_url": p.get("source_url", ""),
            "extraction_method": p.get("extraction_method", ""),
            "extraction_latency": p.get("extraction_latency", 0.0),
            "quality_status": p.get("quality_status", ""),
            "record_status": p.get("record_status", "complete"),
            "crawl_status": p.get("crawl_status", "COMPLETED"),
            "crawl_detail": p.get("crawl_detail", ""),
        }
        variations = p.get("variations", [])
        if variations:
            for v in variations:
                rows.append({
                    **base,
                    "sku": v.get("sku", ""),
                    "size": v.get("size", ""),
                    "package_count": v.get("package_count", ""),
                    "price": v.get("price"),
                    "availability": v.get("availability"),
                })
        else:
            rows.append({**base, "sku": "", "size": "", "package_count": "", "price": None, "availability": None})

    fieldnames = [
        "product_name", "brand", "sku", "category_hierarchy", "source_url",
        "price", "size", "package_count", "availability",
        "description", "image_urls", "alternative_products",
        "extraction_method", "extraction_latency",
        "quality_status", "record_status", "crawl_status", "crawl_detail",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  CSV  -> {output_path} ({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Safco Dental Full Crawl")
    parser.add_argument("--mode", choices=["api", "local"], default="api",
                        help="'api' = GPT-4o-mini only (gold standard), 'local' = tiered local + escalation")
    parser.add_argument("--max-products", type=int, default=None,
                        help="Max products to extract (default: unlimited)")
    args = parser.parse_args()

    load_dotenv()

    # Configure mode
    if args.mode == "api":
        os.environ["USE_HANDYMAN"] = "false"
        folder = "artifacts_api"
    else:
        os.environ["USE_HANDYMAN"] = "true"
        folder = "artifacts_handyman"

    artifacts_dir = Path(__file__).resolve().parent / folder
    artifacts_dir.mkdir(exist_ok=True)

    os.environ["CRAWL_DB_PATH"] = str(artifacts_dir / "crawl.db")

    print("=" * 60)
    print("  Safco Dental Scraper - Full Crawl")
    print(f"  Mode: {'API-only (gold standard)' if args.mode == 'api' else 'Tiered Local + Escalation'}")
    print(f"  Seed URLs: {len(SEED_URLS)}")
    print(f"  Max Products: {args.max_products or 'unlimited'}")
    print(f"  Output: {artifacts_dir}/")
    print("=" * 60)

    # Run crawl
    asyncio.run(worker_loop(
        max_products=args.max_products,
        sleep_seconds=1.0,
        seed_urls=SEED_URLS,
        concurrency_limit=5,
    ))

    # Collect results
    products_raw = asyncio.run(get_products(limit=9999, include_incomplete=True))
    skipped_rows = asyncio.run(get_queue_rows(status="SKIPPED"))
    queue_counts = asyncio.run(get_queue_counts())
    parsed = []
    for row in products_raw:
        payload = json.loads(row["product_data"])
        payload["record_status"] = row.get("record_status", "complete")
        payload["crawl_status"] = row.get("queue_status", "COMPLETED")
        payload["crawl_detail"] = row.get("detail")
        payload["crawl_error"] = row.get("last_error")
        parsed.append(payload)

    skipped_urls = [
        {
            "url": row.get("url"),
            "page_type": row.get("page_type"),
            "status": row.get("status"),
            "retry_count": row.get("retry_count"),
            "last_updated": row.get("last_updated"),
            "detail": row.get("detail"),
            "last_error": row.get("last_error"),
        }
        for row in skipped_rows
    ]

    if not parsed:
        print("\nNo products extracted. Check scraper.log for details.")
        return

    # Deduplication pass
    seen = set()
    deduped = []
    for p in parsed:
        key = (p.get("product_name", "").strip().lower(), p.get("source_url", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(p)
    dupes_removed = len(parsed) - len(deduped)
    if dupes_removed:
        print(f"\n  Deduplicator: removed {dupes_removed} duplicate(s)")
    parsed = deduped

    print("\n=== Export ===")
    export_json(parsed, skipped_urls, artifacts_dir / "products.json", args.mode, queue_counts)
    export_csv(parsed, artifacts_dir / "products.csv")

    # Print summary
    escalated = sum(1 for p in parsed if p.get("extraction_method") in {"api_gpt4o_mini", "api_gpt4o_mini_fixed"})
    print("\n=== Summary ===")
    print(f"  Total Products: {len(parsed)}")
    print(f"  Escalation Rate: {(escalated / len(parsed) * 100):.1f}%")
    print(f"  Crawl Stats: {dict(queue_counts)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
