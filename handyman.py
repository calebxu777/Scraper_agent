import json
import os
import re
from typing import Any

from dotenv import load_dotenv

from classifier import looks_like_product_page
from models import DentalProduct, HandymanRouteDecision, HandymanVerifyResult

load_dotenv()

USE_HANDYMAN = os.getenv("USE_HANDYMAN", "true").lower() == "true"
HANDYMAN_BACKEND = os.getenv("HANDYMAN_BACKEND", "rules")
HANDYMAN_MODEL = os.getenv("HANDYMAN_MODEL", "Qwen/Qwen2.5-0.5B-Instruct")
HANDYMAN_DEVICE = os.getenv("HANDYMAN_DEVICE", "cuda")
HANDYMAN_DTYPE = os.getenv("HANDYMAN_DTYPE", "auto")
HANDYMAN_MAX_NEW_TOKENS = int(os.getenv("HANDYMAN_MAX_NEW_TOKENS", "384"))
HANDYMAN_TEMPERATURE = float(os.getenv("HANDYMAN_TEMPERATURE", "0.0"))
USE_RULE_ROUTER = os.getenv("USE_RULE_ROUTER", "true").lower() == "true"
USE_HANDYMAN_VERIFY = os.getenv("USE_HANDYMAN_VERIFY", "true").lower() == "true"

_GENERATOR = None
_GENERATOR_ERROR = None

NOISE_PATTERNS = [
    r"(?im)^.*(privacy policy|terms of use|terms and conditions).*$",
    r"(?im)^.*(sign in|log in|create account|my account).*$",
    r"(?im)^.*(shopping cart|view cart|checkout).*$",
    r"(?im)^.*(customer service|contact us|call us).*$",
    r"(?im)^.*(returns|shipping information|free shipping).*$",
    r"(?im)^.*(newsletter|subscribe).*$",
]


def is_handyman_enabled() -> bool:
    return USE_HANDYMAN


def is_verify_enabled() -> bool:
    return USE_HANDYMAN and USE_HANDYMAN_VERIFY


def _collapse_blank_lines(text: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", text).strip()


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


def _load_generator():
    global _GENERATOR, _GENERATOR_ERROR
    if _GENERATOR is not None or _GENERATOR_ERROR is not None:
        return _GENERATOR

    try:
        from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
    except Exception as exc:
        _GENERATOR_ERROR = f"transformers import failed: {exc}"
        return None

    model_kwargs: dict[str, Any] = {}
    if HANDYMAN_DTYPE != "auto":
        model_kwargs["torch_dtype"] = HANDYMAN_DTYPE

    try:
        tokenizer = AutoTokenizer.from_pretrained(HANDYMAN_MODEL)
        model = AutoModelForCausalLM.from_pretrained(
            HANDYMAN_MODEL,
            device_map=HANDYMAN_DEVICE if HANDYMAN_DEVICE != "auto" else "auto",
            **model_kwargs,
        )
        _GENERATOR = pipeline("text-generation", model=model, tokenizer=tokenizer)
        return _GENERATOR
    except Exception as exc:
        _GENERATOR_ERROR = f"local model load failed: {exc}"
        return None


def _generate_json(prompt: str) -> dict[str, Any] | None:
    if HANDYMAN_BACKEND != "local_llm":
        return None

    generator = _load_generator()
    if generator is None:
        return None

    try:
        outputs = generator(
            prompt,
            max_new_tokens=HANDYMAN_MAX_NEW_TOKENS,
            temperature=HANDYMAN_TEMPERATURE,
            return_full_text=False,
        )
        text = outputs[0]["generated_text"].strip()
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return None
        return json.loads(match.group(0))
    except Exception:
        return None


def handyman_route(url: str, markdown_text: str) -> HandymanRouteDecision:
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
        if HANDYMAN_BACKEND != "local_llm":
            return baseline
    else:
        baseline = HandymanRouteDecision(label="uncertain", confidence=0.5, reason="rules router disabled")

    payload = _generate_json(
        "Classify this Safco page as product, category, other, or uncertain. "
        "Return strict JSON with keys label, confidence, reason.\n"
        f"URL: {url}\n"
        f"PAGE:\n{markdown_text[:6000]}"
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


def handyman_prune(url: str, markdown_text: str) -> str:
    cleaned = rules_prune(markdown_text)
    if HANDYMAN_BACKEND != "local_llm":
        return cleaned

    payload = _generate_json(
        "Clean this scraped markdown while preserving all product facts. "
        "Return strict JSON with one key cleaned_markdown.\n"
        f"URL: {url}\n"
        f"PAGE:\n{cleaned[:6000]}"
    )
    if payload is None:
        return cleaned

    maybe_cleaned = payload.get("cleaned_markdown")
    if isinstance(maybe_cleaned, str) and maybe_cleaned.strip():
        return maybe_cleaned.strip()
    return cleaned


def handyman_verify_extraction(url: str, markdown_text: str, product: DentalProduct) -> HandymanVerifyResult:
    page = (markdown_text or "").lower()
    issues: list[str] = []

    if product.product_name and product.product_name.lower() not in page:
        issues.append("product_name not directly found in page text")
    if not product.variations:
        issues.append("no variations extracted")
    if not product.image_urls:
        issues.append("no image urls extracted")
    if not product.category_hierarchy:
        issues.append("missing category hierarchy")

    for variation in product.variations:
        if variation.sku and variation.sku.lower() not in page:
            issues.append(f"sku {variation.sku} not directly found in page text")

    if issues:
        baseline = HandymanVerifyResult(
            decision="warn" if len(issues) <= 2 else "fail",
            confidence=0.7 if len(issues) <= 2 else 0.82,
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

    if HANDYMAN_BACKEND != "local_llm":
        return baseline

    payload = _generate_json(
        "Compare this extracted product JSON against the page text. "
        "Return strict JSON with keys decision, confidence, issues, notes. "
        "Decision must be pass, warn, or fail.\n"
        f"URL: {url}\n"
        f"PAGE:\n{markdown_text[:5000]}\n"
        f"EXTRACTED_JSON:\n{product.model_dump_json(indent=2)}"
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


def handyman_backend_status() -> str:
    if HANDYMAN_BACKEND != "local_llm":
        return "rules"
    _load_generator()
    if _GENERATOR_ERROR:
        return f"rules_fallback ({_GENERATOR_ERROR})"
    return "local_llm"
