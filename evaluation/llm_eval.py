import json
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()


QUALITY_EVAL_PROMPT = (
    "You are evaluating the quality of a structured web-scraping extraction.\n"
    "Judge the product JSON for semantic plausibility, fidelity to the provided evidence, and completeness.\n"
    "Be strict about invented SKUs, invented brands, or implausible variation rows.\n"
    "A fake SKU derived from the product title, such as '<title>-Q1' or 'Q1-Large', should be treated as a failure.\n"
    "If the page evidence does not explicitly show a field, missing data is acceptable and should reduce completeness, not fidelity.\n"
    "Return strict JSON only.\n"
)


class LLMQualityResult(BaseModel):
    decision: str = Field(description="One of pass, warn, fail")
    confidence: float = Field(description="Confidence from 0.0 to 1.0")
    fidelity_score: float = Field(description="How faithful the JSON is to the provided evidence, from 0.0 to 1.0")
    completeness_score: float = Field(description="How complete the extraction is relative to visible evidence, from 0.0 to 1.0")
    issues: list[str] = Field(default_factory=list)
    notes: str = Field(default="")


def _get_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "your_openai_key_here":
        raise ValueError("Missing valid OPENAI_API_KEY in .env")
    from openai import AsyncOpenAI

    return AsyncOpenAI(api_key=api_key)


async def evaluate_product_llm(product: dict, page_evidence: Optional[str] = None) -> dict:
    client = _get_client()
    model = os.getenv("API_MODEL", "gpt-4o-mini")
    evidence = page_evidence or "No original page markdown provided. Judge plausibility and internal consistency from the JSON alone."

    completion = await client.beta.chat.completions.parse(
        model=model,
        messages=[
            {"role": "system", "content": QUALITY_EVAL_PROMPT},
            {
                "role": "user",
                "content": (
                    "SOURCE EVIDENCE:\n"
                    f"{evidence}\n\n"
                    "EXTRACTED PRODUCT JSON:\n"
                    f"{json.dumps(product, indent=2, ensure_ascii=False)}"
                ),
            },
        ],
        response_format=LLMQualityResult,
        temperature=0.0,
    )

    parsed = completion.choices[0].message.parsed
    if parsed is None:
        return LLMQualityResult(
            decision="warn",
            confidence=0.5,
            fidelity_score=0.5,
            completeness_score=0.5,
            issues=["llm evaluation returned no parsed response"],
            notes="fallback result",
        ).model_dump()

    return parsed.model_dump()

