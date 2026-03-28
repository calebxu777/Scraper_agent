import re
from collections import Counter
from statistics import mean
from urllib.parse import urlparse


PLACEHOLDER_SKUS = {"N/A", "NA", "NONE", "NULL", "UNKNOWN"}


def _clean_text(value) -> str:
    return str(value or "").strip()


def _normalize_sku(value) -> str:
    return _clean_text(value).upper()


def _normalized_title_tokens(product_name: str) -> list[str]:
    normalized = (
        _clean_text(product_name)
        .upper()
        .replace("®", "")
        .replace("™", "")
        .replace("+", " ")
    )
    return [token for token in normalized.split() if len(token) >= 4]


def _is_synthetic_sku(sku: str, product_name: str) -> bool:
    normalized_sku = _normalize_sku(sku)
    if not normalized_sku:
        return False
    if normalized_sku.startswith("Q1-") or normalized_sku.endswith("-Q1"):
        return True

    title_tokens = _normalized_title_tokens(product_name)
    matching_title_tokens = [token for token in title_tokens if token in normalized_sku]
    return bool(matching_title_tokens and ("Q1" in normalized_sku or len(matching_title_tokens) >= 2))


def _has_valid_source_url(source_url: str) -> bool:
    parsed = urlparse(_clean_text(source_url))
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _infer_expected_variants(product: dict) -> bool:
    description = _clean_text(product.get("description")).lower()
    if not description:
        return False

    glove_size_terms = ("xs", "small", "medium", "large", "x-large", "xl")
    return any(term in description for term in glove_size_terms) or "sizes available" in description


def evaluate_product_rules(product: dict) -> dict:
    checks: dict[str, bool] = {}
    issues: list[str] = []
    score = 100

    product_name = _clean_text(product.get("product_name"))
    brand = _clean_text(product.get("brand"))
    categories = [item for item in product.get("category_hierarchy", []) if _clean_text(item)]
    variations = product.get("variations", []) or []
    image_urls = product.get("image_urls", []) or []
    source_url = _clean_text(product.get("source_url"))

    checks["has_product_name"] = bool(product_name)
    if not checks["has_product_name"]:
        issues.append("missing product name")
        score -= 25

    checks["has_category_hierarchy"] = bool(categories)
    if not checks["has_category_hierarchy"]:
        issues.append("missing category hierarchy")
        score -= 15

    checks["has_brand_or_null"] = brand != ""
    if not checks["has_brand_or_null"]:
        issues.append("missing brand")
        score -= 8

    checks["has_image_urls"] = bool(image_urls)
    if not checks["has_image_urls"]:
        issues.append("missing image urls")
        score -= 5

    checks["has_source_url"] = _has_valid_source_url(source_url)
    if not checks["has_source_url"]:
        issues.append("invalid source url")
        score -= 25

    checks["has_variations"] = bool(variations)
    if not checks["has_variations"]:
        issues.append("no variations extracted")
        score -= 15

    sku_values: list[str] = []
    conflicting_duplicates = False
    missing_size_rows = 0
    variation_signatures: dict[str, tuple[str | None, str | None]] = {}

    for variation in variations:
        sku = _clean_text(variation.get("sku"))
        normalized_sku = _normalize_sku(sku)
        size = variation.get("size")
        package_count = variation.get("package_count")

        if not sku:
            issues.append("variation has empty sku")
            score -= 20
            continue

        sku_values.append(normalized_sku)

        if normalized_sku in PLACEHOLDER_SKUS:
            issues.append(f"variation has placeholder sku: {sku}")
            score -= 25

        if len(normalized_sku) <= 2:
            issues.append(f"variation has suspiciously short sku: {sku}")
            score -= 20

        if normalized_sku.isalpha() and len(normalized_sku) <= 4:
            issues.append(f"variation sku looks like a non-catalog short code: {sku}")
            score -= 15

        if _is_synthetic_sku(sku, product_name):
            issues.append(f"variation sku appears synthetic or title-derived: {sku}")
            score -= 25

        if not size:
            missing_size_rows += 1

        prior_signature = variation_signatures.get(normalized_sku)
        signature = (size, package_count)
        if prior_signature is not None and prior_signature != signature:
            conflicting_duplicates = True
            issues.append(f"duplicate sku with conflicting variation details: {sku}")
            score -= 20
        else:
            variation_signatures[normalized_sku] = signature

    checks["has_valid_skus"] = bool(variations) and all(
        sku and sku not in PLACEHOLDER_SKUS and len(sku) > 2 for sku in sku_values
    )
    checks["has_duplicate_conflicts"] = conflicting_duplicates
    checks["has_synthetic_sku"] = any(_is_synthetic_sku(v.get("sku"), product_name) for v in variations)

    if _infer_expected_variants(product) and variations and missing_size_rows == len(variations):
        issues.append("all variation sizes are missing despite likely size run")
        score -= 10

    decision = "pass"
    if score < 60 or any("synthetic" in issue or "placeholder" in issue for issue in issues):
        decision = "fail"
    elif score < 85 or issues:
        decision = "warn"

    return {
        "score": max(score, 0),
        "decision": decision,
        "issues": list(dict.fromkeys(issues)),
        "checks": checks,
    }


def summarize_rule_results(records: list[dict]) -> dict:
    scores = [record["python_quality"]["score"] for record in records]
    decisions = Counter(record["python_quality"]["decision"] for record in records)
    issue_counter = Counter(
        issue
        for record in records
        for issue in record["python_quality"]["issues"]
    )

    return {
        "avg_python_score": round(mean(scores), 2) if scores else None,
        "decision_counts": dict(decisions),
        "top_python_issues": [issue for issue, _ in issue_counter.most_common(10)],
    }

