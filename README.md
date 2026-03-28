# Safco Dental Scraper Agent

This repository is my take-home implementation of an agentic product scraper for Safco Dental. The system starts from a small set of seed category pages, discovers in-scope product pages, extracts structured product data, and stores results in a resumable local pipeline.

The current implementation is API-first for the final extraction path, and I am also testing a local mode using `sglang` to host open-source models for lower token usage and faster inference.

## Architecture Overview

The scraper runs as a staged pipeline:

1. Seed the crawl from assigned Safco category pages
2. Fetch and render pages with `Playwright` or `httpx`
3. Clean the DOM into markdown for cheaper downstream processing
4. Classify pages as product, category, or other
5. Discover and enqueue new in-scope links
6. Extract a structured `DentalProduct` record
7. Verify, repair, or reject low-quality outputs
8. Persist queue state and product records in SQLite

Main components:

- `main.py`: orchestration loop and crawl policy
- `navigator.py`: fetching, rendering, HTML cleanup, link discovery
- `db.py`: SQLite-backed crawl queue and product storage
- `classifier.py`: cheap rule-based page classification
- `handyman.py`: optional low-cost worker for route/prune/verify/fix
- `llm_workers.py`: API extraction, repair, verification, and variation recovery
- `models.py`: Pydantic schema for structured output

The full crawl currently starts from:

- `https://www.safcodental.com/catalog/sutures-surgical-products`
- `https://www.safcodental.com/catalog/gloves`

## Why I Chose My Approach

I optimized for a practical take-home design rather than a purely academic crawler.

- I used SQLite because it gives resumability, idempotent queueing, and easy local inspection without external infrastructure.
- I used `Playwright` by default because Safco category discovery depends on rendered content, so raw HTTP is not always reliable.
- I used rules before LLM extraction so obviously non-product pages do not consume model calls.
- I used OpenAI structured outputs for the primary extraction path because it is the most reliable way in this repo to enforce a consistent schema.
- I am also testing a local serving path with `sglang` and open-source models to reduce paid token usage and improve inference speed where possible.

This means the current repo treats the API-based approach as the stable baseline, while local inference is an optimization path that can take on more work over time.

## Agent Responsibilities

- `navigator.py`
  Fetches pages, renders dynamic content when needed, removes noisy HTML, converts the useful page content into markdown, and extracts discoverable in-domain links.

- `classifier.py`
  Performs cheap heuristic product-page detection so the pipeline avoids sending every crawled page to an LLM.

- `handyman.py`
  Acts as a lower-cost control-plane worker. Depending on configuration, it can route pages, prune noisy content, verify extracted fields, and attempt repairs. This is the main place where I am experimenting with local open-source models served through `sglang`.

- `llm_workers.py`
  Runs the final schema-producing extraction path. It also handles repair passes, verification, and focused variation recovery when a page appears valid but the extracted SKU/size rows look incomplete.

- `db.py`
  Owns persistence for crawl progress and extracted records. It makes the crawl resumable, preserves failure context, and now keeps incomplete structured records alongside completed ones for later review.

- `main.py`
  Coordinates all of the above: it decides what to fetch, what to queue, what to reject, when to retry, and when a product is good enough to save.

## Setup & Execution Instructions

### 1. Create and activate a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
playwright install chromium
```

### 3. Configure environment variables

```powershell
Copy-Item .env.example .env
```

Minimum useful settings:

```env
OPENAI_API_KEY=your_openai_key_here
USE_HANDYMAN=true
HANDYMAN_BACKEND=rules
FETCH_BROWSER=playwright
API_MODEL=gpt-4o-mini
MAX_CONCURRENCY=5
```

Optional local-mode direction:

- keep the API path as the reliable baseline
- use `USE_HANDYMAN=true` to enable the low-cost helper layer
- test local inference through the `handyman` path while serving an open-source model via `sglang`

### 4. Run a bounded demo crawl

```powershell
python test_scrape_5_products.py
```

This produces a small reproducible output in `artifacts_api/` or `artifacts_handyman/` depending on configuration.

### 5. Run the full crawl

API-first mode:

```powershell
python run_full_crawl.py --mode api
```

Tiered local/rules + escalation mode:

```powershell
python run_full_crawl.py --mode local
```

Optional cap:

```powershell
python run_full_crawl.py --mode local --max-products 50
```

### 6. Review results in the dashboard

```powershell
streamlit run dashboard.py
```

## Sample Output Schema

The output schema is defined in `models.py` and exported as JSON in the crawl artifacts.

```json
{
  "product_name": "Example Product",
  "brand": "Example Brand",
  "category_hierarchy": ["Gloves", "Nitrile Gloves"],
  "description": "Short product description",
  "variations": [
    {
      "sku": "ABC123",
      "size": "M",
      "package_count": "Box of 100",
      "price": 19.99,
      "availability": true
    }
  ],
  "image_urls": ["https://example.com/image.jpg"],
  "alternative_products": [],
  "source_url": "https://www.safcodental.com/product/example",
  "extraction_method": "api_gpt4o_mini",
  "extraction_latency": 4.21,
  "quality_status": "complete",
  "quality_notes": []
}
```

Typical artifact outputs include:

- crawl database
- `products.json`
- `products.csv`
- bounded-run exports such as `scrape_5_products.json`
- `scraper.log`

The full exports now include both completed and incomplete structured records, with status fields such as `record_status`, `crawl_status`, `crawl_detail`, and `crawl_error` so partial extractions are visible instead of being silently dropped. `products.json` also includes a top-level `skipped_urls` list with each skipped queue URL and its skip reason.

## Limitations

- The most reliable final schema-producing path in the current repo is still API-based.
- The local mode is still being tested with a relatively small open-source model, so it is not yet the target production-quality ceiling.
- In production, the same local-serving approach can scale to larger open-source models and potentially to post-trained models specialized for this task.
- Even if local inference reduces token spend, I would still weigh that against the infrastructure cost of hosting larger models, including GPU cost, latency, memory footprint, and operational overhead.
- `Playwright` is currently important for reliable category-page discovery on Safco.
- The crawler is single-process today and not yet distributed across workers or machines.
- Some artifact folders may contain older runs from earlier iterations, while the current code includes stricter rejection logic.

## Failure Handling

The crawler is designed to fail in a controlled way rather than silently corrupt output.

- The queue uses explicit statuses such as `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, and `SKIPPED`.
- Any rows left in `PROCESSING` are re-queued on restart.
- Failed fetches and extraction errors are saved with retry counts and error details.
- Product-like outputs can still be rejected if downstream checks detect catalog pages, sitemap pages, synthetic SKUs, or out-of-scope products.
- The pipeline includes verification and repair passes before saving a product.
- Products can be marked `complete` or `incomplete` so partial success is visible instead of hidden.
- Incomplete structured records are exported for inspection, while obvious non-product pages are still excluded from product exports.

This makes it easier to inspect what went wrong and restart without losing crawl state.

## How I Would Scale To Full-Site Crawling In Production

To scale this beyond a take-home implementation, I would keep the same pipeline shape but replace single-node bottlenecks.

- Replace the SQLite queue with Redis, SQS, or Kafka so multiple workers can crawl in parallel.
- Split discovery, extraction, and evaluation into separate worker pools.
- Serve browser rendering as a dedicated pool for dynamic pages instead of mixing it directly into the main worker loop.
- Expand crawl policy from seed-limited categories to broader site coverage with stronger URL prioritization and deduplication.
- Use tiered extraction: rules first, local model second, API escalation only when confidence is low or the page is high value.
- If local inference proves cost-effective, host larger open-source models through `sglang` for more of the extraction and verification workload.
- If the workload is stable enough, post-train or adapt an open-source model specifically for this schema and domain.

The core tradeoff I would continue to monitor is token cost versus model-hosting infrastructure cost. API usage is simpler operationally; larger self-hosted models can reduce token spend, but only if GPU utilization and latency justify the additional infrastructure.

## How I Would Monitor Data Quality

I would monitor quality at both extraction time and after the crawl.

- During extraction, I already check for invalid page types, missing support in page text, suspicious SKUs, duplicate conflicts, and missing expected variations.
- After the crawl, I use the `evaluation/` utilities to score records as `complete`, `incomplete`, or `invalid`.
- I would track quality metrics over time such as rejection rate, repair rate, average variation count, missing brand rate, invalid SKU rate, and percentage of incomplete products.
- I would sample records by category and model backend to compare API outputs versus local-model outputs.
- I would log queue outcomes and extraction metadata so regressions are easy to spot after prompt, model, or crawl-policy changes.
- I would keep a small benchmark set of manually reviewed product pages and run it on every major pipeline change.

Example evaluation commands:

```powershell
python -m evaluation.run_eval --input artifacts_api\products.json --output artifacts_api\quality_report.json
python -m evaluation.run_eval --input artifacts_api\products.json --output artifacts_api\quality_report.json --with-llm
```

These reports make it easier to compare extraction quality across runs and decide whether more work should move from the API path to locally hosted models.
