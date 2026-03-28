import json
import logging
import os
import re
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel

from classifier import looks_like_product_page
from models import DentalProduct, DentalProductExtraction, HandymanRouteDecision, HandymanVerifyResult
from prompts import EXTRACTOR_PROMPT, PRUNER_PROMPT, ROUTER_PROMPT, VALIDATOR_PROMPT, FIXER_PROMPT

load_dotenv()

HANDYMAN_BACKEND = os.getenv("HANDYMAN_BACKEND", "sglang")
HANDYMAN_MODEL = os.getenv("HANDYMAN_MODEL", "qwen3.5:0.8b")
USE_RULE_ROUTER = os.getenv("USE_RULE_ROUTER", "true").lower() == "true"


def _env_flag(name: str, default: str) -> bool:
    return os.getenv(name, default).lower() == "true"


NOISE_PATTERNS = [
    r"(?im)^.*(privacy policy|terms of use|terms and conditions).*$",
    r"(?im)^.*(sign in|log in|create account|my account).*$",
    r"(?im)^.*(shopping cart|view cart|checkout).*$",
    r"(?im)^.*(customer service|contact us|call us).*$",
    r"(?im)^.*(returns|shipping information|free shipping).*$",
    r"(?im)^.*(newsletter|subscribe).*$",
]


def is_handyman_enabled() -> bool:
    return _env_flag("USE_HANDYMAN", "true")


def _collapse_blank_lines(text: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _normalize_for_match(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _has_normalized_match(needle: str, haystack: str) -> bool:
    normalized_needle = _normalize_for_match(needle)
    normalized_haystack = _normalize_for_match(haystack)
    return bool(normalized_needle) and normalized_needle in normalized_haystack


def _has_duplicate_values(values: list[str]) -> bool:
    normalized = [_normalize_for_match(value) for value in values if _normalize_for_match(value)]
    return len(normalized) != len(set(normalized))


def rules_prune(markdown_text: str) -> str:
    cleaned = markdown_text or ""

    for pattern in NOISE_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned)

    cleaned = re.sub(r"(?im)^\s*[\*\-]\s*$", "", cleaned)
    cleaned = re.sub(r"(?im)^\s*#+\s*(menu|navigation|footer)\s*$", "", cleaned)

    lines = []
    for line in cleaned.splitlines():
        stripped = line.strip()
        if not stripped:
            lines.append("")
            continue

        link_like_density = stripped.count("[") + stripped.count("]") + stripped.count("(") + stripped.count(")")
        if len(stripped) < 160 and link_like_density >= 6:
            continue

        lines.append(line)

    return _collapse_blank_lines("\n".join(lines))


# Schema for pruner (no Pydantic model, just a simple one-key object)
PRUNER_SCHEMA = {
    "type": "object",
    "properties": {
        "cleaned_markdown": {"type": "string"}
    },
    "required": ["cleaned_markdown"]
}


async def _generate_json(prompt: str, schema: dict | type[BaseModel] | None = None) -> dict[str, Any] | None:
    """Generate JSON from SGLang with optional Compressed FSM constrained decoding.
    
    If a Pydantic model or raw JSON schema dict is provided, SGLang's XGrammar
    backend compiles it into a Compressed Finite State Machine. This guarantees
    structurally valid output and enables jump-forward decoding over boilerplate
    tokens for significant speed gains.
    """
    if HANDYMAN_BACKEND != "sglang":
        return None

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(base_url="http://localhost:30000/v1", api_key="EMPTY")

        # Build the JSON schema for constrained decoding
        if schema is not None:
            if isinstance(schema, type) and issubclass(schema, BaseModel):
                json_schema = schema.model_json_schema()
            else:
                json_schema = schema
            extra_body = {"json_schema": json.dumps(json_schema)}
        else:
            extra_body = None

        response = await client.chat.completions.create(
            model=HANDYMAN_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=1500,
            response_format={"type": "json_object"},
            extra_body=extra_body,
        )
        text = response.choices[0].message.content.strip()
        logging.info(f"🤖 [SGLang Handyman Thought]:\n{text}")
        
        # With FSM the output is guaranteed valid, but keep fallback for safety
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        return json.loads(text)
    except Exception as exc:
        print(f"Handyman generation failed: {exc}")
        return None


async def handyman_route(url: str, markdown_text: str) -> HandymanRouteDecision:
    if USE_RULE_ROUTER:
        heuristic = looks_like_product_page(url, markdown_text)
        if heuristic.is_product:
            baseline = HandymanRouteDecision(
                label="product",
                confidence=min(0.95, 0.55 + (0.08 * heuristic.product_score)),
                reason=heuristic.reason,
            )
        else:
            baseline = HandymanRouteDecision(
                label="category" if heuristic.category_score >= heuristic.product_score else "other",
                confidence=min(0.95, 0.55 + (0.08 * max(heuristic.category_score, 1))),
                reason=heuristic.reason,
            )
        if HANDYMAN_BACKEND != "sglang":
            return baseline
    else:
        baseline = HandymanRouteDecision(label="uncertain", confidence=0.5, reason="rules router disabled")

    payload = await _generate_json(
        f"{ROUTER_PROMPT}\n"
        f"URL: {url}\n"
        f"PAGE:\n{markdown_text[:6000]}",
        schema=HandymanRouteDecision,
    )
    if payload is None:
        return baseline

    try:
        decision = HandymanRouteDecision.model_validate(payload)
        if decision.label not in {"product", "category", "other", "uncertain"}:
            return baseline
        return decision
    except Exception:
        return baseline


async def handyman_prune(url: str, markdown_text: str) -> str:
    cleaned = rules_prune(markdown_text)
    if HANDYMAN_BACKEND != "sglang":
        return cleaned

    payload = await _generate_json(
        f"{PRUNER_PROMPT}\n"
        f"URL: {url}\n"
        f"PAGE:\n{cleaned[:6000]}",
        schema=PRUNER_SCHEMA,
    )
    if payload is None:
        return cleaned

    maybe_cleaned = payload.get("cleaned_markdown")
    if isinstance(maybe_cleaned, str) and maybe_cleaned.strip():
        return maybe_cleaned.strip()
    return cleaned


async def handyman_extract(url: str, markdown_text: str) -> DentalProduct | None:
    if HANDYMAN_BACKEND != "sglang":
        return None

    payload = await _generate_json(
        f"{EXTRACTOR_PROMPT}\n"
        f"URL: {url}\n"
        f"PAGE:\n{markdown_text[:6000]}",
        schema=DentalProductExtraction,
    )
    if payload is None:
        return None

    try:
        product = DentalProduct.model_validate({
            **payload,
            "source_url": url,
            "extraction_method": "local_qwen"
        })
        return product
    except Exception as exc:
        logging.warning(f"Handyman extraction failed validation for {url}: {exc}")
        return None


async def handyman_verify_extraction(url: str, markdown_text: str, product: DentalProduct) -> HandymanVerifyResult:
    page = markdown_text or ""
    hard_issues: list[str] = []
    soft_issues: list[str] = []

    if product.product_name and len(_normalize_for_match(product.product_name)) >= 6 and not _has_normalized_match(product.product_name, page):
        soft_issues.append("product_name not directly supported by normalized page text")
    if not product.category_hierarchy:
        soft_issues.append("missing category hierarchy")

    # Check for hallucinated SKUs (hard signal — SKU should appear on page)
    for variation in product.variations:
        if variation.sku and len(_normalize_for_match(variation.sku)) >= 4 and not _has_normalized_match(variation.sku, page):
            hard_issues.append(f"sku {variation.sku} not directly supported by normalized page text")

    # Check for duplicate/repeated values (structural error)
    for field_name, field_val in [("category_hierarchy", product.category_hierarchy), ("alternative_products", product.alternative_products)]:
        if _has_duplicate_values(field_val):
            hard_issues.append(f"repeated duplicate values in {field_name}")

    issues = list(dict.fromkeys([*hard_issues, *soft_issues]))

    if issues:
        baseline = HandymanVerifyResult(
            decision="fail" if hard_issues or len(soft_issues) >= 3 else "warn",
            confidence=0.82 if hard_issues or len(soft_issues) >= 3 else 0.7,
            issues=issues,
            notes="rule-based verification found unsupported or missing fields",
        )
    else:
        baseline = HandymanVerifyResult(
            decision="pass",
            confidence=0.88,
            issues=[],
            notes="rule-based verification found support for key product fields",
        )

    if HANDYMAN_BACKEND != "sglang":
        return baseline

    payload = await _generate_json(
        f"{VALIDATOR_PROMPT}\n"
        f"URL: {url}\n"
        f"PAGE:\n{markdown_text[:5000]}\n"
        f"EXTRACTED_JSON:\n{product.model_dump_json(indent=2)}",
        schema=HandymanVerifyResult,
    )
    if payload is None:
        return baseline

    try:
        verification = HandymanVerifyResult.model_validate(payload)
        if verification.decision not in {"pass", "warn", "fail"}:
            return baseline
        return verification
    except Exception:
        return baseline


async def handyman_fix(url: str, markdown_text: str, current_data: DentalProduct, issues: list[str]) -> DentalProduct | None:
    if HANDYMAN_BACKEND != "sglang":
        return None
        
    prompt = (
        f"{FIXER_PROMPT}\n"
        f"URL: {url}\n"
        f"ISSUES IDENTIFIED BY VALIDATOR:\n"
        f"{' | '.join(issues)}\n\n"
        f"PREVIOUS BROKEN EXTRACTION:\n"
        f"{current_data.model_dump_json(indent=2)}\n\n"
        f"ORIGINAL MARKDOWN:\n"
        f"{markdown_text[:6000]}"
    )
    
    payload = await _generate_json(prompt, schema=DentalProductExtraction)
    if payload is None:
        return None
        
    try:
        from pydantic import ValidationError
        product = DentalProduct.model_validate({
            **payload,
            "source_url": url,
            "extraction_method": "local_qwen_fixed"
        })
        return product
    except ValidationError as exc:
        logging.warning(f"Handyman fix hallucinated invalid schema for {url}: {exc}")
        return None


async def handyman_backend_status() -> str:
    if HANDYMAN_BACKEND == "sglang":
        return f"sglang ({HANDYMAN_MODEL})"
    return "rules"
