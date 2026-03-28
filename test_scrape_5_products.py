import asyncio
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from db import get_products
from main import worker_loop


def main():
    load_dotenv()
    use_handyman = os.getenv("USE_HANDYMAN", "true").lower() == "true"
    folder_name = "artifacts_handyman" if use_handyman else "artifacts_api"
    max_products = 5
    
    artifacts_dir = Path(__file__).resolve().parent / folder_name
    artifacts_dir.mkdir(exist_ok=True)

    os.environ["CRAWL_DB_PATH"] = str(artifacts_dir / f"scrape_{max_products}.db")
    asyncio.run(worker_loop(max_products=max_products, sleep_seconds=1.0))

    products = asyncio.run(get_products(limit=max_products, include_incomplete=True))
    export_path = artifacts_dir / f"scrape_{max_products}_products.json"
    parsed_products = [json.loads(row["product_data"]) for row in products]
    
    if not parsed_products:
        print("No products extracted.")
        return
        
    api_methods = {"api_gpt4o_mini", "api_gpt4o_mini_fixed"}
    escalated_count = sum(1 for p in parsed_products if p.get("extraction_method") in api_methods)
    total_count = len(parsed_products)
    escalation_pct = (escalated_count / total_count) * 100
    
    local_lats = [p.get("extraction_latency", 0.0) for p in parsed_products if p.get("extraction_method", "").startswith("local_qwen")]
    api_lats = [p.get("extraction_latency", 0.0) for p in parsed_products if p.get("extraction_method") in api_methods]
    avg_local = sum(local_lats) / len(local_lats) if local_lats else 0.0
    avg_api = sum(api_lats) / len(api_lats) if api_lats else 0.0

    output_payload = {
        "metadata": {
            "total_products": total_count,
            "escalated_to_api": escalated_count,
            "handled_locally": total_count - escalated_count,
            "escalation_percentage": f"{escalation_pct:.1f}%",
            "average_latency_local_sec": round(avg_local, 2),
            "average_latency_api_sec": round(avg_api, 2)
        },
        "data": parsed_products
    }

    export_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print(f"Saved {total_count} products to {export_path}")
    print(f"API Escalation Rate: {escalation_pct:.1f}%")


if __name__ == "__main__":
    main()
