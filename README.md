# Safco Dental Data Extraction Agent

A production-minded scraping agent built to discover, crawl, and extract structured product data from Safco Dental Supply.

## Architecture Overview
This proof-of-concept emphasizes practical engineering, observability, and cost-efficiency.

1. **State-Driven SQLite Queue (`db.py`)**
   - Stores crawl state, retries, and extracted products.
   - Recovers interrupted `PROCESSING` rows on restart.
2. **Pure Python Worker Loop (`main.py`)**
   - Orchestrates fetching, routing, pruning, extraction, verification, and persistence.
3. **Multi-Tiered Agent Pipeline**
   - **Navigator (`navigator.py`)**: Fetches HTML and removes obvious structural noise with `BeautifulSoup`.
   - **Classifier (`classifier.py`)**: Cheap rule-based product-page detection for obvious cases.
   - **Handyman (`handyman.py`)**: Optional low-cost control-plane worker that can route, prune, and verify using either rules or a tiny local model.
   - **Extractor (`llm_workers.py`)**: `gpt-4o-mini` with Pydantic Structured Outputs for final schema extraction.

## Handyman Design
The Handyman worker is intentionally configurable.

- `HANDYMAN_BACKEND=rules`
  - Uses deterministic cleanup and verification.
  - Best default for Windows setup and interview reproducibility.
- `HANDYMAN_BACKEND=local_llm`
  - Attempts to load a tiny local model such as `Qwen/Qwen2.5-0.5B-Instruct`.
  - Intended for local GPU experimentation.
  - Falls back to rules if model loading fails.

This lets you compare outcomes:
- with pruning vs without pruning
- rules-only vs local-LLM routing
- with verification vs without verification

## Setup & Execution

### Prerequisites
- Python 3.10+
- OpenAI API key

### Environment
Repo-local Conda environment:

```bash
conda activate C:\Users\Caleb\Desktop\frontier_dental\.conda-env
```

If you are creating it from scratch:

```bash
conda create -y -p .conda-env python=3.11 pip
```

### Install
```bash
pip install -r requirements.txt
```

### Configure
```bash
cp .env.example .env
```

Set at least:

```env
OPENAI_API_KEY=your_openai_key_here
USE_HANDYMAN=true
HANDYMAN_BACKEND=rules
```

### Run
```bash
python main.py
```

### Tests
```bash
python -m unittest discover -s tests
```

### Bounded Demo Run
```bash
python test_scrape_5_products.py
```

### Dashboard
```bash
streamlit run dashboard.py
```

The dashboard is intentionally lightweight and read-only. It shows queue counts, crawl completion, recent failures/skips, extracted products, and a JSON detail view for saved product records.

## Operational Notes
- Any rows left in `PROCESSING` are automatically moved back to `PENDING` on startup.
- Queue rows store `detail` and `last_error` for easier inspection.
- `source_url` is injected deterministically outside the extractor model.
- Verification can reject suspicious extractions before they are persisted.

## Production Hardening Considerations
- **Resumability:** Restarting the worker resumes from SQLite and re-queues stuck work.
- **Cost Awareness:** Deterministic rules and the optional handyman layer reduce paid API usage.
- **Quality Control:** The extractor is the schema-producing authority; handyman acts as a low-cost router/pruner/verifier.
- **Scale-Up Path:** A future version could replace the local handyman backend with a faster served local model such as `vLLM` on Linux/CUDA infrastructure.

## Limitations
- The local-LLM handyman path depends on the machine's PyTorch/CUDA setup and may be less portable than the rules backend.
- The extractor still depends on OpenAI for final structured output.
- If Safco becomes JavaScript-heavy, the fetch layer may need Playwright or another browser-based crawler.
