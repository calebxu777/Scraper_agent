import logging
import time

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
    handyman_prune,
    handyman_route,
    handyman_verify_extraction,
    is_handyman_enabled,
    is_verify_enabled,
)
from llm_workers import api_extract_product
from navigator import fetch_page
from pydantic import ValidationError
from tqdm import tqdm

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


def log_queue_health():
    counts = get_queue_counts()
    if not counts:
        logging.info("Queue summary: empty")
        return
    summary = ", ".join(f"{status}={count}" for status, count in sorted(counts.items()))
    logging.info(f"Queue summary: {summary}")


def upsert_progress_bar(progress_bar: tqdm | None) -> tqdm:
    counts = get_queue_counts()
    total = sum(counts.values())
    done = counts.get("COMPLETED", 0) + counts.get("FAILED", 0) + counts.get("SKIPPED", 0)

    if progress_bar is None:
        progress_bar = tqdm(
            total=max(total, 1),
            desc="Crawl Progress",
            unit="url",
            dynamic_ncols=True,
            leave=True,
        )

    progress_bar.total = max(total, 1)
    progress_bar.n = done
    progress_bar.set_postfix({
        "pending": counts.get("PENDING", 0),
        "processing": counts.get("PROCESSING", 0),
        "products": count_products(),
    })
    progress_bar.refresh()
    return progress_bar


def process_url(url: str):
    logging.info(f"Processing URL: {url}")

    fetch_result = fetch_page(url)
    if not fetch_result.markdown:
        if fetch_result.http_status:
            detail = f"fetch failed with status={fetch_result.http_status}"
        elif fetch_result.error_type:
            detail = f"fetch failed with {fetch_result.error_type}"
        else:
            detail = "fetch failed before response"
        logging.warning(f"Failed to fetch markdown. {detail}")
        update_status(url, "FAILED", increment_retry=True, detail=detail, error=fetch_result.error)
        return

    for link in set(fetch_result.links):
        add_to_queue(link, "UNKNOWN")

    route = handyman_route(url, fetch_result.markdown)
    if route.label in {"category", "other"}:
        logging.info(f"Skipping non-product page: {route.reason}. URL: {url}")
        update_status(url, "SKIPPED", detail=f"route={route.label}; {route.reason}")
        return

    if route.label == "uncertain":
        logging.info(f"Proceeding despite uncertain route decision for {url}: {route.reason}")

    if is_handyman_enabled():
        logging.info(f"Pruning markdown via Handyman ({handyman_backend_status()})...")
        cleaned_md = handyman_prune(url, fetch_result.markdown)
    else:
        cleaned_md = fetch_result.markdown

    if len(cleaned_md) < 200:
        logging.info(f"Page skipped: likely not a product (too short). URL: {url}")
        update_status(url, "SKIPPED", detail="cleaned markdown shorter than 200 characters")
        return

    logging.info("Running API Extractor...")
    try:
        product_data = api_extract_product(cleaned_md, source_url=url)

        if is_verify_enabled():
            verification = handyman_verify_extraction(url, cleaned_md, product_data)
            if verification.decision == "fail":
                update_status(
                    url,
                    "FAILED",
                    increment_retry=True,
                    detail=f"verification failed: {verification.notes}",
                    error="; ".join(verification.issues),
                )
                logging.warning(f"Verification failed for {url}: {verification.notes}")
                return
            if verification.decision == "warn":
                logging.warning(f"Verification warning for {url}: {verification.notes}")

        save_product(url, product_data.model_dump_json())
        logging.info(f"SUCCESS: Saved data for {product_data.product_name}")
    except ValidationError as exc:
        logging.error(f"Validation Error on {url}: {exc}")
        update_status(url, "FAILED", increment_retry=True, detail="schema validation failed", error=str(exc))
    except Exception as exc:
        logging.warning(f"Failed extraction for {url}. Error: {exc}")
        if "rate limit" in str(exc).lower() or "503" in str(exc).lower() or "timeout" in str(exc).lower():
            update_status(url, "FAILED", increment_retry=True, detail="transient extractor failure", error=str(exc))
        else:
            update_status(url, "FAILED", increment_retry=True, detail="extractor failed after product classification", error=str(exc))


def worker_loop(max_products: int | None = None, sleep_seconds: float = 2.0, seed_urls: list[str] | None = None):
    init_db()
    recovered = requeue_processing_tasks()
    logging.info("Starting Worker Loop...")
    logging.info(f"Handyman status: enabled={is_handyman_enabled()} backend={handyman_backend_status()}")
    if recovered:
        logging.info(f"Recovered {recovered} stuck PROCESSING tasks back to PENDING.")

    for seed_url in seed_urls or [
        "https://www.safcodental.com/catalog/sutures-surgicalproducts",
        "https://www.safcodental.com/catalog/gloves",
    ]:
        add_to_queue(seed_url, "CATEGORY")

    starting_product_count = count_products()
    progress_bar = upsert_progress_bar(None)
    log_queue_health()

    while True:
        if max_products is not None and count_products() - starting_product_count >= max_products:
            logging.info(f"Reached max_products={max_products}. Exiting worker loop.")
            break

        pending = get_next_pending(limit=1)
        progress_bar = upsert_progress_bar(progress_bar)
        if not pending:
            logging.info("Queue is empty or all pending tasks are exhausted.")
            log_queue_health()
            break

        task = pending[0]
        url = task["url"]
        retry_count = task["retry_count"]

        if retry_count > 3:
            logging.error(f"Giving up on {url} after 3 retries.")
            update_status(url, "FAILED", detail="exceeded retry limit")
            continue

        process_url(url)
        progress_bar = upsert_progress_bar(progress_bar)
        log_queue_health()
        time.sleep(sleep_seconds)

    progress_bar = upsert_progress_bar(progress_bar)
    progress_bar.close()


if __name__ == "__main__":
    worker_loop()
