import argparse
import asyncio
import json
from collections import Counter
from pathlib import Path
from statistics import mean

from evaluation.llm_eval import evaluate_product_llm
from evaluation.rule_eval import evaluate_product_rules, summarize_rule_results


def _load_products(input_path: Path) -> list[dict]:
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input export not found: {input_path}. "
            "Point --input at an existing scrape_10_products.json or products.json file."
        )
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    if isinstance(payload, list):
        return payload
    raise ValueError(f"Unsupported product export format: {input_path}")


def _merge_quality(rule_result: dict, llm_result: dict | None) -> dict:
    if rule_result["decision"] == "fail":
        return {
            "decision": "invalid",
            "reason": "failed deterministic quality checks",
        }

    if llm_result is None:
        return {
            "decision": "complete" if rule_result["decision"] == "pass" else "incomplete",
            "reason": "rule-based evaluation only",
        }

    if llm_result["decision"] == "fail":
        return {
            "decision": "invalid",
            "reason": "failed llm semantic quality review",
        }

    if rule_result["decision"] == "pass" and llm_result["decision"] == "pass":
        return {
            "decision": "complete",
            "reason": "passed deterministic and llm quality checks",
        }

    return {
        "decision": "incomplete",
        "reason": "product looks valid but some fields remain incomplete or uncertain",
    }


async def _evaluate_products(products: list[dict], use_llm: bool) -> list[dict]:
    records = []
    for product in products:
        rule_result = evaluate_product_rules(product)
        llm_result = await evaluate_product_llm(product) if use_llm else None
        records.append(
            {
                "source_url": product.get("source_url"),
                "product_name": product.get("product_name"),
                "python_quality": rule_result,
                "llm_quality": llm_result,
                "final_quality": _merge_quality(rule_result, llm_result),
            }
        )
    return records


def _summarize(records: list[dict]) -> dict:
    final_counts = Counter(record["final_quality"]["decision"] for record in records)
    llm_fidelity = [
        record["llm_quality"]["fidelity_score"]
        for record in records
        if record["llm_quality"] is not None
    ]
    llm_completeness = [
        record["llm_quality"]["completeness_score"]
        for record in records
        if record["llm_quality"] is not None
    ]

    summary = summarize_rule_results(records)
    summary.update(
        {
            "complete_count": final_counts.get("complete", 0),
            "incomplete_count": final_counts.get("incomplete", 0),
            "invalid_count": final_counts.get("invalid", 0),
            "avg_llm_fidelity": round(mean(llm_fidelity), 3) if llm_fidelity else None,
            "avg_llm_completeness": round(mean(llm_completeness), 3) if llm_completeness else None,
        }
    )
    return summary


async def main():
    parser = argparse.ArgumentParser(description="Evaluate extraction quality for exported products")
    parser.add_argument(
        "--input",
        default="artifacts_api/scrape_10_products.json",
        help="Path to products export JSON",
    )
    parser.add_argument(
        "--output",
        default="artifacts_api/quality_report.json",
        help="Path to write the merged quality report",
    )
    parser.add_argument(
        "--with-llm",
        action="store_true",
        help="Run the LLM evaluator in addition to the deterministic rule evaluator",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    products = _load_products(input_path)
    records = await _evaluate_products(products, use_llm=args.with_llm)

    payload = {
        "input_file": str(input_path),
        "record_count": len(records),
        "summary": _summarize(records),
        "records": records,
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote quality report to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
