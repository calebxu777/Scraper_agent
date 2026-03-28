import os

from dotenv import load_dotenv

from models import DentalProduct, DentalProductExtraction

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
API_MODEL = os.getenv("API_MODEL", "gpt-4o-mini")


def get_cloud_client():
    if not OPENAI_API_KEY or OPENAI_API_KEY == "your_openai_key_here":
        raise ValueError("Missing valid OPENAI_API_KEY in .env")
    import openai
    return openai.OpenAI(api_key=OPENAI_API_KEY)


def api_extract_product(cleaned_markdown: str, source_url: str) -> DentalProduct:
    """
    Uses OpenAI's structured outputs to extract the Pydantic schema.
    """
    completion = get_cloud_client().beta.chat.completions.parse(
        model=API_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a specialized Web Scraping Agent for Safco Dental Supply. Your task is to extract product data precisely according to the provided JSON schema. Ensure you capture all variations (SKUs, packaging, sizes). If a field like price is not available, leave it null. Do not invent data. The source URL is provided separately and should not be inferred from the page text.",
            },
            {"role": "user", "content": f"Extract the product information from this page:\n\n{cleaned_markdown}"},
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
