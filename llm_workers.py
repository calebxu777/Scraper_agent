import os

from dotenv import load_dotenv

from pydantic import BaseModel, Field

from models import DentalProduct, DentalProductExtraction, ProductVariation
from prompts import EXTRACTOR_PROMPT, FIXER_PROMPT, VARIANT_RECOVERY_PROMPT

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
API_MODEL = os.getenv("API_MODEL", "gpt-4o-mini")


class VariationRecoveryPayload(BaseModel):
    variations: list[ProductVariation] = Field(default_factory=list)


def get_cloud_client():
    if not OPENAI_API_KEY or OPENAI_API_KEY == "your_openai_key_here":
        raise ValueError("Missing valid OPENAI_API_KEY in .env")
    from openai import AsyncOpenAI
    return AsyncOpenAI(api_key=OPENAI_API_KEY)


async def api_extract_product(cleaned_markdown: str, source_url: str) -> DentalProduct:
    """
    Uses OpenAI's structured outputs to extract the Pydantic schema.
    """
    client = get_cloud_client()
    completion = await client.beta.chat.completions.parse(
        model=API_MODEL,
        messages=[
            {
                "role": "system",
                "content": EXTRACTOR_PROMPT,
            },
            {"role": "user", "content": f"URL: {source_url}\n\nExtract the product information from this page:\n\n{cleaned_markdown}"},
        ],
        response_format=DentalProductExtraction,
        temperature=0.0,
    )

    parsed_data = completion.choices[0].message.parsed
    if parsed_data is None:
        raise ValueError("Extractor returned no parsed product payload.")

    return DentalProduct.model_validate({
        **parsed_data.model_dump(),
        "source_url": source_url,
    })


async def api_fix_product(cleaned_markdown: str, current_product: DentalProduct, issues: list[str], source_url: str) -> DentalProduct:
    """
    Uses one additional API pass to repair a previously extracted product payload.
    """
    client = get_cloud_client()
    completion = await client.beta.chat.completions.parse(
        model=API_MODEL,
        messages=[
            {
                "role": "system",
                "content": FIXER_PROMPT,
            },
            {
                "role": "user",
                "content": (
                    f"URL: {source_url}\n\n"
                    f"ISSUES IDENTIFIED:\n{' | '.join(issues)}\n\n"
                    f"PREVIOUS BROKEN EXTRACTION:\n{current_product.model_dump_json(indent=2)}\n\n"
                    f"ORIGINAL PAGE:\n{cleaned_markdown}"
                ),
            },
        ],
        response_format=DentalProductExtraction,
        temperature=0.0,
    )

    parsed_data = completion.choices[0].message.parsed
    if parsed_data is None:
        raise ValueError("API fixer returned no parsed product payload.")

    return DentalProduct.model_validate({
        **parsed_data.model_dump(),
        "source_url": source_url,
    })


async def api_recover_variations(cleaned_markdown: str, current_product: DentalProduct, source_url: str) -> DentalProduct:
    """
    Uses a focused API pass to recover only the variations list for a product.
    """
    client = get_cloud_client()
    completion = await client.beta.chat.completions.parse(
        model=API_MODEL,
        messages=[
            {
                "role": "system",
                "content": VARIANT_RECOVERY_PROMPT,
            },
            {
                "role": "user",
                "content": (
                    f"URL: {source_url}\n\n"
                    f"CURRENT PRODUCT JSON:\n{current_product.model_dump_json(indent=2)}\n\n"
                    f"ORIGINAL PAGE:\n{cleaned_markdown}"
                ),
            },
        ],
        response_format=VariationRecoveryPayload,
        temperature=0.0,
    )

    parsed_data = completion.choices[0].message.parsed
    if parsed_data is None:
        raise ValueError("API variant recovery returned no parsed payload.")

    updated_payload = current_product.model_dump()
    updated_payload["variations"] = [variation.model_dump() for variation in parsed_data.variations]
    return DentalProduct.model_validate(updated_payload)


async def api_verify_product(cleaned_markdown: str, product: DentalProduct) -> "HandymanVerifyResult":
    """
    Uses GPT-4o-mini to validate extracted product data against page text.
    Used as the API-mode verifier.
    """
    from models import HandymanVerifyResult
    from prompts import VALIDATOR_PROMPT

    client = get_cloud_client()
    completion = await client.beta.chat.completions.parse(
        model=API_MODEL,
        messages=[
            {
                "role": "system",
                "content": VALIDATOR_PROMPT,
            },
            {
                "role": "user",
                "content": (
                    f"PAGE:\n{cleaned_markdown[:5000]}\n"
                    f"EXTRACTED_JSON:\n{product.model_dump_json(indent=2)}"
                ),
            },
        ],
        response_format=HandymanVerifyResult,
        temperature=0.0,
    )

    parsed = completion.choices[0].message.parsed
    if parsed is None:
        return HandymanVerifyResult(
            decision="warn", confidence=0.5,
            issues=["API validator returned no response"], notes="fallback",
        )
    return parsed
