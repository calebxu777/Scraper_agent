import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from urllib.parse import urlparse

from classifier import looks_like_product_page
from db import (
    add_to_queue,
    count_products,
    get_next_pending,
    get_queue_counts,
    init_db,
    requeue_processing_tasks,
    save_product,
    update_status,
)
from handyman import (
    handyman_backend_status,
    handyman_extract,
    handyman_prune,
    handyman_route,
    handyman_verify_extraction,
    handyman_fix,
    is_handyman_enabled,
)
from llm_workers import api_extract_product, api_fix_product, api_recover_variations, api_verify_product
from navigator import fetch_page
from pydantic import ValidationError

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="scraper.log",
    filemode="a",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)


@dataclass(frozen=True)
class PageDecision:
    label: str
    reason: str
    used_handyman_router: bool = False


def normalize_path(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    return path or "/"


def build_seed_category_prefixes(seed_urls: list[str]) -> tuple[str, ...]:
    return tuple(normalize_path(url) for url in seed_urls)


def build_seed_scope_terms(seed_urls: list[str]) -> tuple[str, ...]:
    generic_tokens = {"catalog", "product", "products", "surgical"}
    terms: list[str] = []
    for url in seed_urls:
        slug = normalize_path(url).split("/")[-1]
        normalized_parts = [part.strip().lower() for part in slug.split("-") if part.strip()]
        for part in normalized_parts:
            if part not in generic_tokens:
                terms.append(part)
    return tuple(dict.fromkeys(terms))


def is_seed_scoped_category_path(path: str, allowed_category_prefixes: tuple[str, ...]) -> bool:
    normalized_path = path.rstrip("/") or "/"
    return any(
        normalized_path == prefix or normalized_path.startswith(f"{prefix}/")
        for prefix in allowed_category_prefixes
    )


def should_enqueue_link(discovered_from_url: str, link: str, allowed_category_prefixes: tuple[str, ...]) -> bool:
    source_path = normalize_path(discovered_from_url).lower()
    target_path = normalize_path(link).lower()

    if target_path.startswith("/catalog/"):
        return is_seed_scoped_category_path(target_path, allowed_category_prefixes)

    if target_path.startswith("/product/"):
        return is_seed_scoped_category_path(source_path, allowed_category_prefixes)

    return False


async def log_queue_health():
    counts = await get_queue_counts()
    if not counts:
        logging.info("Queue summary: empty")
        return
    summary = ", ".join(f"{status}={count}" for status, count in sorted(counts.items()))
    logging.info(f"Queue summary: {summary}")


async def upsert_progress_bar(progress_bar=None):
    counts = await get_queue_counts()
    total = sum(counts.values())
    done = counts.get("COMPLETED", 0) + counts.get("FAILED", 0) + counts.get("SKIPPED", 0)
    pending = counts.get("PENDING", 0)
    processing = counts.get("PROCESSING", 0)
    products = await count_products()
    
    if progress_bar is None or done % 5 == 0:
        logging.info(f"Crawl Progress: {done}/{total} | Pending: {pending} | Processing: {processing} | Products Found: {products}")
        
    return progress_bar


def is_product_within_seed_scope(product_data, seed_scope_terms: tuple[str, ...]) -> bool:
    normalized_categories = [item.lower().strip() for item in product_data.category_hierarchy if item and item.strip()]
    if not normalized_categories:
        return False

    for category in normalized_categories:
        if "gloves" in seed_scope_terms and ("glove" in category or "gloves" in category):
            return True
        if "sutures" in seed_scope_terms and "suture" in category and "surgical products" not in category:
            return True

    for term in seed_scope_terms:
        if term in {"gloves", "sutures"}:
            continue
        for category in normalized_categories:
            if category == term or category.startswith(f"{term} ") or category.endswith(f" {term}") or f" {term} " in category:
                return True

    return False


def find_suspicious_variation_issues(product_data) -> list[str]:
    issues: list[str] = []
    seen_by_sku: dict[str, tuple[str | None, str | None]] = {}
    normalized_title = (product_data.product_name or "").strip().upper()
    title_tokens = [
        token for token in normalized_title.replace("®", "").replace("™", "").replace("+", " ").split()
        if len(token) >= 4
    ]

    for variation in product_data.variations:
        sku = (variation.sku or "").strip()
        normalized_sku = sku.upper()

        if not sku:
            issues.append("variation has empty sku")
            continue

        if normalized_sku in {"N/A", "NA", "NONE", "NULL", "UNKNOWN"}:
            issues.append(f"variation has placeholder sku: {sku}")
            continue

        if len(normalized_sku) <= 2:
            issues.append(f"variation has suspiciously short sku: {sku}")

        if normalized_sku.isalpha() and len(normalized_sku) <= 4:
            issues.append(f"variation sku looks like a non-catalog short code: {sku}")

        if normalized_sku.startswith("Q1-") or normalized_sku.endswith("-Q1"):
            issues.append(f"variation sku looks like a synthetic Q1 placeholder: {sku}")

        if title_tokens:
            matching_title_tokens = [token for token in title_tokens if token in normalized_sku]
            if matching_title_tokens and ("Q1" in normalized_sku or len(matching_title_tokens) >= 2):
                issues.append(f"variation sku appears derived from product title instead of page sku: {sku}")

        prior = seen_by_sku.get(normalized_sku)
        current_signature = (variation.size, variation.package_count)
        if prior is not None and prior != current_signature:
            issues.append(f"duplicate sku with conflicting variation details: {sku}")
        else:
            seen_by_sku[normalized_sku] = current_signature

    return list(dict.fromkeys(issues))


def collect_rejection_issues(url: str, cleaned_md: str, product_data, seed_scope_terms: tuple[str, ...] | None = None) -> list[str]:
    issues: list[str] = []
    path = urlparse(url).path.lower()
    text = (cleaned_md or "").lower()
    title = (product_data.product_name or "").strip().lower()

    if any(token in path for token in ("/catalog/product_compare", "/catalog/category/view/")):
        issues.append("utility/category url should not be saved as product")

    if any(token in path for token in ("/privacy-policy", "/sitemap", "/about-us", "/blog", "/shop-by-manufacturer")):
        issues.append("non-product site page should not be saved as product")

    if "/catalog/" in path and "/product/" not in path and "/item/" not in path:
        if len(product_data.variations) <= 1 and ("filter by" in text or "sort by" in text or "results" in text):
            issues.append("catalog listing page appears to have been extracted as a single product")

    if not title:
        issues.append("empty product name")

    if title in {"products comparison list - dental products & equipment", "dental supplies", "safco sitemap"}:
        issues.append("generic page title is not a product name")

    if seed_scope_terms is not None and not is_product_within_seed_scope(product_data, seed_scope_terms):
        issues.append("product category hierarchy falls outside seed scope")

    issues.extend(find_suspicious_variation_issues(product_data))
    return list(dict.fromkeys(issues))


def build_fix_issue_list(verification_issues: list[str], rejection_issues: list[str]) -> list[str]:
    return list(dict.fromkeys([*verification_issues, *rejection_issues]))


HARD_VERIFICATION_MARKERS = (
    "duplicate sku",
    "duplicate skus",
    "conflicting",
    "placeholder sku",
    "suspiciously short sku",
    "synthetic q1",
    "synthesized",
    "derived from product title",
    "repeated duplicate values",
    "listing page",
    "not a product",
)


def has_hard_verification_issues(verification) -> bool:
    texts = [verification.notes, *(verification.issues or [])]
    lowered = " ".join(text.lower() for text in texts if text)
    return any(marker in lowered for marker in HARD_VERIFICATION_MARKERS)


HARD_REJECTION_ISSUES = {
    "utility/category url should not be saved as product",
    "non-product site page should not be saved as product",
    "catalog listing page appears to have been extracted as a single product",
    "empty product name",
    "generic page title is not a product name",
    "product category hierarchy falls outside seed scope",
}


def split_record_issues(url: str, cleaned_md: str, product_data, seed_scope_terms: tuple[str, ...] | None = None) -> tuple[list[str], list[str]]:
    issues = collect_rejection_issues(url, cleaned_md, product_data, seed_scope_terms=seed_scope_terms)
    hard_issues = [issue for issue in issues if issue in HARD_REJECTION_ISSUES]
    soft_issues = [issue for issue in issues if issue not in HARD_REJECTION_ISSUES]
    return hard_issues, soft_issues


def should_reject_product_record(url: str, cleaned_md: str, product_data, seed_scope_terms: tuple[str, ...] | None = None) -> str | None:
    hard_issues, _ = split_record_issues(url, cleaned_md, product_data, seed_scope_terms=seed_scope_terms)
    return hard_issues[0] if hard_issues else None


def _has_glove_size_run(text: str) -> bool:
    lowered = (text or "").lower()
    matches = set(re.findall(r"\b(?:xs|s|m|l|xl|small|medium|large|x-large)\b", lowered))
    expanded = {
        "xs" if token == "extra small" else token
        for token in matches
    }
    return len(expanded) >= 3


def looks_like_glove_product(product_data, cleaned_md: str) -> bool:
    categories = " ".join(item.lower() for item in product_data.category_hierarchy if item)
    return "glove" in categories or "glove" in (cleaned_md or "").lower()


def collect_incomplete_issues(cleaned_md: str, product_data) -> list[str]:
    issues: list[str] = []
    text = (cleaned_md or "").lower()

    if not product_data.brand and any(marker in text for marker in ("brand:", "manufacturer:", "manufactured by:")):
        issues.append("brand appears present on page but missing from extraction")

    if looks_like_glove_product(product_data, cleaned_md) and _has_glove_size_run(cleaned_md):
        if not product_data.variations:
            issues.append("glove size run visible but no variations were extracted")
        elif len(product_data.variations) == 1 and not product_data.variations[0].size:
            issues.append("glove size run visible but only one incomplete variation was extracted")

    return list(dict.fromkeys(issues))


def should_attempt_variant_recovery(cleaned_md: str, product_data) -> bool:
    return any(
        issue.startswith("glove size run visible")
        for issue in collect_incomplete_issues(cleaned_md, product_data)
    )


def mark_product_quality(product_data, incomplete_issues: list[str]) -> None:
    if incomplete_issues:
        product_data.quality_status = "incomplete"
        product_data.quality_notes = incomplete_issues
    else:
        product_data.quality_status = "complete"
        product_data.quality_notes = []


def build_business_product_payload(product_data) -> dict:
    specifications = {
        spec.name: spec.value
        for spec in (product_data.specifications or [])
        if getattr(spec, "name", None) and getattr(spec, "value", None)
    }
    return {
        "product_name": product_data.product_name,
        "brand": product_data.brand,
        "category_hierarchy": list(product_data.category_hierarchy or []),
        "description": product_data.description,
        "specifications": specifications,
        "variations": [variation.model_dump() for variation in product_data.variations],
        "image_urls": list(product_data.image_urls or []),
        "alternative_products": list(product_data.alternative_products or []),
        "source_url": product_data.source_url,
    }


async def persist_product_record(
    url: str,
    product_data,
    record_status: str,
    queue_status: str,
    detail: str,
    issues: list[str] | None = None,
):
    issues = list(dict.fromkeys(issues or []))
    mark_product_quality(product_data, issues)
    business_payload = build_business_product_payload(product_data)
    await save_product(
        url,
        json.dumps(business_payload, ensure_ascii=False),
        extraction_method=product_data.extraction_method,
        extraction_latency=product_data.extraction_latency,
        quality_status=product_data.quality_status,
        quality_notes_json=json.dumps(product_data.quality_notes, ensure_ascii=False),
        record_status=record_status,
        queue_status=queue_status,
        detail=detail,
        error="; ".join(issues) if issues else None,
    )


def decide_page_with_rules(url: str, markdown_text: str) -> PageDecision:
    heuristic = looks_like_product_page(url, markdown_text)
    path = urlparse(url).path.lower()

    if any(token in path for token in ("/privacy-policy", "/sitemap", "/about-us", "/blog", "/shop-by-manufacturer", "/catalog/product_compare")):
        return PageDecision(label="other", reason=heuristic.reason)

    if any(token in path for token in ("/catalog/category/view/",)):
        return PageDecision(label="category", reason=heuristic.reason)

    if heuristic.is_product and heuristic.product_score >= max(2, heuristic.category_score + 1):
        return PageDecision(label="product", reason=heuristic.reason)

    if heuristic.category_score >= max(2, heuristic.product_score + 1):
        return PageDecision(label="category", reason=heuristic.reason)

    return PageDecision(label="uncertain", reason=heuristic.reason)


async def route_page(url: str, markdown_text: str) -> PageDecision:
    baseline = decide_page_with_rules(url, markdown_text)

    if not is_handyman_enabled():
        return baseline

    if baseline.label != "uncertain":
        return baseline

    route = await handyman_route(url, markdown_text)
    return PageDecision(
        label=route.label,
        reason=route.reason,
        used_handyman_router=True,
    )


async def process_url(
    url: str,
    semaphore: asyncio.Semaphore,
    allowed_category_prefixes: tuple[str, ...],
    seed_scope_terms: tuple[str, ...],
):
    async with semaphore:
        logging.info(f"Processing URL: {url}")

        fetch_result = await fetch_page(url)
        if not fetch_result.markdown:
            if fetch_result.http_status:
                detail = f"fetch failed with status={fetch_result.http_status}"
            elif fetch_result.error_type:
                detail = f"fetch failed with {fetch_result.error_type}"
            else:
                detail = "fetch failed before response"
            logging.warning(f"Failed to fetch markdown. {detail}")
            await update_status(url, "FAILED", increment_retry=True, detail=detail, error=fetch_result.error)
            return

        for link in set(fetch_result.links):
            if should_enqueue_link(url, link, allowed_category_prefixes):
                await add_to_queue(link, "UNKNOWN")

        page_decision = await route_page(url, fetch_result.markdown)
        decision_prefix = "handyman_route" if page_decision.used_handyman_router else "rules_route"

        if page_decision.label in {"category", "other"}:
            logging.info(f"Skipping non-product page: {page_decision.reason}. URL: {url}")
            await update_status(url, "SKIPPED", detail=f"{decision_prefix}={page_decision.label}; {page_decision.reason}")
            return

        if page_decision.label == "uncertain":
            logging.info(f"Proceeding despite uncertain route decision for {url}: {page_decision.reason}")

        if is_handyman_enabled():
            logging.info(f"Pruning markdown via Handyman ({await handyman_backend_status()})...")
            cleaned_md = await handyman_prune(url, fetch_result.markdown)
        else:
            cleaned_md = fetch_result.markdown

        if len(cleaned_md) < 200:
            logging.info(f"Page skipped: likely not a product (too short). URL: {url}")
            await update_status(url, "SKIPPED", detail="cleaned markdown shorter than 200 characters")
            return

        logging.info("Running Extraction Phase...")
        try:
            product_data = None

            if is_handyman_enabled():
                # === TIERED LOCAL INFERENCE PATH ===
                logging.info(f"Attempting local Handyman extraction for {url}")
                local_start = time.time()
                product_data = await handyman_extract(url, cleaned_md)
                
                if product_data:
                    product_data.extraction_latency = round(time.time() - local_start, 2)
                    verification = await handyman_verify_extraction(url, cleaned_md, product_data)
                    rejection_issues = collect_rejection_issues(url, cleaned_md, product_data, seed_scope_terms=seed_scope_terms)
                    
                    fix_attempts = 0
                    while (verification.decision == "fail" or rejection_issues) and fix_attempts < 3:
                        fix_issues = build_fix_issue_list(verification.issues, rejection_issues)
                        logging.warning(
                            f"Handyman Extraction needs repair {fix_attempts+1}/3. "
                            f"Validation={verification.decision}. Issues: {fix_issues or [verification.notes]}"
                        )
                        fix_start = time.time()
                        fixed_data = await handyman_fix(url, cleaned_md, product_data, fix_issues or [verification.notes])
                        if not fixed_data:
                            logging.warning("Handyman fix hallucinated, aborting recursion.")
                            break
                        
                        product_data = fixed_data
                        product_data.extraction_latency += round(time.time() - fix_start, 2)
                        
                        verification = await handyman_verify_extraction(url, cleaned_md, product_data)
                        rejection_issues = collect_rejection_issues(url, cleaned_md, product_data, seed_scope_terms=seed_scope_terms)
                        fix_attempts += 1

                    if verification.decision == "fail" or rejection_issues:
                        combined_issues = build_fix_issue_list(verification.issues, rejection_issues)
                        logging.warning(
                            f"Handyman Local Extraction irreparably failed quality checks. "
                            f"Escalating to API. Issues: {combined_issues or [verification.notes]}"
                        )
                        product_data = None
                    elif verification.decision == "warn":
                        logging.warning(f"Handyman Local Extraction warning after {fix_attempts} fixes: {verification.notes}")
                
                # Escalation fallback
                if not product_data:
                    logging.info(f"Escalating {url} to API Extractor...")
                    api_start = time.time()
                    product_data = await api_extract_product(cleaned_md, source_url=url)
                    product_data.extraction_method = "api_gpt4o_mini"
                    product_data.extraction_latency = round(time.time() - api_start, 2)

            else:
                # === DIRECT API PATH (gold-standard baseline) ===
                logging.info(f"API-only mode: extracting {url} via {os.getenv('API_MODEL', 'gpt-4o-mini')}")
                api_start = time.time()
                product_data = await api_extract_product(cleaned_md, source_url=url)
                product_data.extraction_method = "api_gpt4o_mini"
                product_data.extraction_latency = round(time.time() - api_start, 2)

                # Keep the older, more permissive acceptance behavior:
                # rule verification remains the gate, while API verification is advisory unless it finds hard failures.
                verification = await handyman_verify_extraction(url, cleaned_md, product_data)
                logging.info(f"Running API-based validation for {url}")
                api_verification = await api_verify_product(cleaned_md, product_data)
                if api_verification.decision == "warn":
                    verification = api_verification
                elif api_verification.decision == "fail" and has_hard_verification_issues(api_verification):
                    verification = api_verification

                rejection_issues = collect_rejection_issues(url, cleaned_md, product_data, seed_scope_terms=seed_scope_terms)
                should_try_api_fix = verification.decision == "fail" or bool(rejection_issues)

                if should_try_api_fix:
                    fix_issues = list(dict.fromkeys([*verification.issues, *rejection_issues]))
                    logging.info(f"Attempting one-shot API fix for {url}. Issues: {fix_issues}")
                    fix_start = time.time()
                    fixed_product = await api_fix_product(cleaned_md, product_data, fix_issues, source_url=url)
                    fixed_product.extraction_method = "api_gpt4o_mini_fixed"
                    fixed_product.extraction_latency = round(product_data.extraction_latency + (time.time() - fix_start), 2)
                    product_data = fixed_product

                    verification = await handyman_verify_extraction(url, cleaned_md, product_data)
                    logging.info(f"Re-running API-based validation after fix for {url}")
                    api_verification = await api_verify_product(cleaned_md, product_data)
                    if api_verification.decision == "warn":
                        verification = api_verification
                    elif api_verification.decision == "fail" and has_hard_verification_issues(api_verification):
                        verification = api_verification

                if verification.decision == "fail":
                    failure_issues = verification.issues or [verification.notes]
                    await persist_product_record(
                        url,
                        product_data,
                        record_status="incomplete",
                        queue_status="FAILED",
                        detail=f"API verification failed: {verification.notes}",
                        issues=failure_issues,
                    )
                    logging.warning(f"API extraction verification failed for {url}: {verification.notes}")
                    return
                if verification.decision == "warn":
                    logging.warning(f"API extraction verification warning for {url}: {verification.notes}")

            if should_attempt_variant_recovery(cleaned_md, product_data):
                logging.info(f"Attempting focused variant recovery for {url}")
                recover_start = time.time()
                recovered_product = await api_recover_variations(cleaned_md, product_data, source_url=url)
                recovered_product.extraction_method = product_data.extraction_method
                recovered_product.extraction_latency = round(product_data.extraction_latency + (time.time() - recover_start), 2)
                recovered_product.quality_status = product_data.quality_status
                recovered_product.quality_notes = list(product_data.quality_notes)
                product_data = recovered_product

            hard_issues, soft_issues = split_record_issues(url, cleaned_md, product_data, seed_scope_terms=seed_scope_terms)
            if hard_issues:
                rejection_reason = hard_issues[0]
                await update_status(
                    url,
                    "SKIPPED",
                    detail=f"rejected extracted record: {rejection_reason}",
                    error=None,
                )
                logging.info(f"Rejected non-product extraction for {url}: {rejection_reason}")
                return

            incomplete_issues = list(dict.fromkeys([
                *collect_incomplete_issues(cleaned_md, product_data),
                *soft_issues,
            ]))
            record_status = "incomplete" if incomplete_issues else "complete"
            queue_status = "FAILED" if incomplete_issues else "COMPLETED"
            detail = "saved incomplete product record" if incomplete_issues else "saved product record"

            await persist_product_record(
                url,
                product_data,
                record_status=record_status,
                queue_status=queue_status,
                detail=detail,
                issues=incomplete_issues,
            )
            logging.info(
                f"SUCCESS: Saved data for {product_data.product_name} via {product_data.extraction_method} "
                f"(quality={product_data.quality_status})"
            )
        except ValidationError as exc:
            logging.error(f"Validation Error on {url}: {exc}")
            await update_status(url, "FAILED", increment_retry=True, detail="schema validation failed", error=str(exc))
        except Exception as exc:
            logging.warning(f"Failed extraction for {url}. Error: {exc}")
            if "rate limit" in str(exc).lower() or "503" in str(exc).lower() or "timeout" in str(exc).lower():
                await update_status(url, "FAILED", increment_retry=True, detail="transient extractor failure", error=str(exc))
            else:
                await update_status(url, "FAILED", increment_retry=True, detail="extractor failed after product classification", error=str(exc))


async def worker_loop(max_products: int | None = None, sleep_seconds: float = 2.0, seed_urls: list[str] | None = None, concurrency_limit: int = 5):
    await init_db()
    recovered = await requeue_processing_tasks()
    logging.info("Starting Worker Loop...")
    logging.info(f"Handyman status: enabled={is_handyman_enabled()} backend={await handyman_backend_status()}")
    if recovered:
        logging.info(f"Recovered {recovered} stuck PROCESSING tasks back to PENDING.")

    effective_seed_urls = seed_urls or [
        "https://www.safcodental.com/catalog/sutures-surgical-products",
        "https://www.safcodental.com/catalog/gloves",
    ]
    allowed_category_prefixes = build_seed_category_prefixes(effective_seed_urls)
    seed_scope_terms = build_seed_scope_terms(effective_seed_urls)

    for seed_url in effective_seed_urls:
        await add_to_queue(seed_url, "CATEGORY")

    starting_product_count = await count_products()
    progress_bar = await upsert_progress_bar(None)
    await log_queue_health()

    semaphore = asyncio.Semaphore(concurrency_limit)
    tasks = []

    while True:
        if max_products is not None and (await count_products()) - starting_product_count >= max_products:
            logging.info(f"Reached max_products={max_products}. Exiting worker loop.")
            break

        # If we have reached concurrency limit, just wait until one finishes
        if len(tasks) >= concurrency_limit:
            done, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            tasks = list(pending_tasks)
            progress_bar = await upsert_progress_bar(progress_bar)
            await log_queue_health()
            continue

        pending = await get_next_pending(limit=1)
        progress_bar = await upsert_progress_bar(progress_bar)
        
        if not pending:
            if tasks:
                done, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = list(pending_tasks)
                await log_queue_health()
            else:
                logging.info("Queue is empty or all pending tasks are exhausted.")
                await log_queue_health()
                break
            continue

        task = pending[0]
        url = task["url"]
        retry_count = task["retry_count"]

        if retry_count > 3:
            logging.error(f"Giving up on {url} after 3 retries.")
            await update_status(url, "FAILED", detail="exceeded retry limit")
            continue

        # Start the background task
        t = asyncio.create_task(process_url(url, semaphore, allowed_category_prefixes, seed_scope_terms))
        tasks.append(t)
        
        # very tiny sleep to yield control to the loop so tasks can begin scheduling
        await asyncio.sleep(0.05)

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    
    await upsert_progress_bar(progress_bar)


if __name__ == "__main__":
    asyncio.run(worker_loop())
