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
        - products.json             Business-facing JSON output
        - products.csv              Business-facing CSV output
        - products_detailed.json    Debug/status-rich JSON output
        - products_detailed.csv     Debug/status-rich CSV output
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
    "specifications": "Key-value specifications or attributes explicitly shown on the page",
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
    "product_url": "The product detail URL where this product was scraped from",
    "record_status": "Simple export status for the record: complete or incomplete",
}

DETAILED_SCHEMA_DOC = {
    **SCHEMA_DOC,
    "source_url": "Raw crawler source URL stored alongside the business-facing product_url",
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


def _load_json_list(value, default: list[str] | None = None) -> list:
    if not value:
        return list(default or [])
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else list(default or [])
    except Exception:
        return [str(value)]


def build_business_product(payload: dict) -> dict:
    business = dict(payload)
    business["product_url"] = business.pop("source_url", "")
    business["record_status"] = payload.get("record_status", "complete")
    return business


def build_detailed_product(row: dict) -> dict:
    payload = json.loads(row["product_data"])
    payload["extraction_method"] = row.get("extraction_method")
    payload["extraction_latency"] = row.get("extraction_latency")
    payload["quality_status"] = row.get("quality_status")
    payload["quality_notes"] = _load_json_list(row.get("quality_notes"))
    payload["record_status"] = row.get("record_status", "complete")
    payload["crawl_status"] = row.get("queue_status", "COMPLETED")
    payload["crawl_detail"] = row.get("detail")
    payload["crawl_error"] = row.get("last_error")
    payload["product_url"] = payload.get("source_url", "")
    return payload


def export_clean_json(products: list[dict], output_path: Path):
    payload = {
        "schema": SCHEMA_DOC,
        "data": products,
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  JSON -> {output_path} ({len(products)} products)")


def export_detailed_json(products: list[dict], skipped_urls: list[dict], output_path: Path, mode: str, queue_counts: dict):
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
        "schema": DETAILED_SCHEMA_DOC,
        "data": products,
        "skipped_urls": skipped_urls,
    }

    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  JSON -> {output_path} ({total} products, {len(skipped_urls)} skipped URLs)")


def export_clean_csv(products: list[dict], output_path: Path):
    """Export products to a flattened CSV for spreadsheet analysis."""
    rows = []
    for p in products:
        base = {
            "product_name": p.get("product_name", ""),
            "brand": p.get("brand", ""),
            "category_hierarchy": " > ".join(p.get("category_hierarchy", [])),
            "description": p.get("description", ""),
            "specifications": json.dumps(p.get("specifications", {}), ensure_ascii=False),
            "image_urls": " | ".join(p.get("image_urls", [])),
            "alternative_products": " | ".join(p.get("alternative_products", [])),
            "product_url": p.get("product_url", ""),
            "record_status": p.get("record_status", "complete"),
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
        "product_name", "brand", "sku", "category_hierarchy", "product_url",
        "price", "size", "package_count", "availability",
        "description", "specifications", "image_urls", "alternative_products", "record_status",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  CSV  -> {output_path} ({len(rows)} rows)")


def export_detailed_csv(products: list[dict], output_path: Path):
    """Export products to a flattened CSV for spreadsheet analysis."""
    rows = []
    for p in products:
        base = {
            "product_name": p.get("product_name", ""),
            "brand": p.get("brand", ""),
            "category_hierarchy": " > ".join(p.get("category_hierarchy", [])),
            "description": p.get("description", ""),
            "specifications": json.dumps(p.get("specifications", {}), ensure_ascii=False),
            "image_urls": " | ".join(p.get("image_urls", [])),
            "alternative_products": " | ".join(p.get("alternative_products", [])),
            "product_url": p.get("product_url", ""),
            "extraction_method": p.get("extraction_method", ""),
            "extraction_latency": p.get("extraction_latency", 0.0),
            "quality_status": p.get("quality_status", ""),
            "quality_notes": " | ".join(p.get("quality_notes", [])),
            "record_status": p.get("record_status", "complete"),
            "crawl_status": p.get("crawl_status", "COMPLETED"),
            "crawl_detail": p.get("crawl_detail", ""),
            "crawl_error": p.get("crawl_error", ""),
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
        "product_name", "brand", "sku", "category_hierarchy", "product_url",
        "price", "size", "package_count", "availability",
        "description", "specifications", "image_urls", "alternative_products",
        "extraction_method", "extraction_latency",
        "quality_status", "quality_notes", "record_status", "crawl_status", "crawl_detail", "crawl_error",
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
    detailed = [build_detailed_product(row) for row in products_raw]

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

    if not detailed:
        print("\nNo products extracted. Check scraper.log for details.")
        return

    # Deduplication pass
    seen = set()
    deduped = []
    for p in detailed:
        key = (p.get("product_name", "").strip().lower(), p.get("source_url", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(p)
    dupes_removed = len(detailed) - len(deduped)
    if dupes_removed:
        print(f"\n  Deduplicator: removed {dupes_removed} duplicate(s)")
    detailed = deduped
    clean = [build_business_product(p) for p in detailed]

    print("\n=== Export ===")
    export_clean_json(clean, artifacts_dir / "products.json")
    export_clean_csv(clean, artifacts_dir / "products.csv")
    export_detailed_json(detailed, skipped_urls, artifacts_dir / "products_detailed.json", args.mode, queue_counts)
    export_detailed_csv(detailed, artifacts_dir / "products_detailed.csv")

    # Print summary
    escalated = sum(1 for p in detailed if p.get("extraction_method") in {"api_gpt4o_mini", "api_gpt4o_mini_fixed"})
    print("\n=== Summary ===")
    print(f"  Total Products: {len(detailed)}")
    print(f"  Escalation Rate: {(escalated / len(detailed) * 100):.1f}%")
    print(f"  Crawl Stats: {dict(queue_counts)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
