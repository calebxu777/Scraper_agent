import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ClassificationResult:
    is_product: bool
    reason: str
    product_score: int
    category_score: int


def looks_like_product_page(url: str, markdown_text: str) -> ClassificationResult:
    text = (markdown_text or "").lower()
    url_text = (url or "").lower()

    product_signals = {
        "has_price": bool(re.search(r"\$\s?\d", markdown_text or "")),
        "has_sku": any(token in text for token in ["sku", "item #", "item number", "product code"]),
        "has_cart_cta": "add to cart" in text,
        "has_brand": any(token in text for token in ["manufacturer", "brand"]),
        "has_variation_terms": any(token in text for token in ["box of", "pack of", "size", "gauge", "length"]),
        "has_related_products": any(token in text for token in ["related products", "you may also like", "alternative products"]),
        "productish_url": any(token in url_text for token in ["/product", "/item", "/sku"]),
    }

    category_signals = {
        "catalog_url": "/catalog/" in url_text,
        "has_filters": any(token in text for token in ["filter by", "narrow results", "shop by", "sort by"]),
        "has_result_count": bool(re.search(r"\b\d+\s+results\b", text)),
        "has_pagination": any(token in text for token in ["next page", "previous page", "page 1", "showing"]),
        "has_category_grid": text.count("add to cart") >= 3 or text.count("$") >= 3,
    }

    product_score = sum(product_signals.values())
    category_score = sum(category_signals.values())

    if product_score >= 2 and product_score > category_score:
        reasons = [name for name, matched in product_signals.items() if matched]
        return ClassificationResult(
            is_product=True,
            reason=f"product signals: {', '.join(reasons[:3])}",
            product_score=product_score,
            category_score=category_score,
        )

    reasons = [name for name, matched in category_signals.items() if matched]
    if not reasons:
        reasons = ["insufficient product signals"]

    return ClassificationResult(
        is_product=False,
        reason=f"skipped due to {', '.join(reasons[:3])}",
        product_score=product_score,
        category_score=category_score,
    )
