from pydantic import BaseModel, Field
from typing import List, Optional

class ProductVariation(BaseModel):
    sku: str = Field(description="SKU, item number, or product code")
    size: Optional[str] = Field(description="Size, gauge, length, or dimension if applicable", default=None)
    package_count: Optional[str] = Field(description="Quantity per box/pack (e.g., 'Box of 100')", default=None)
    price: Optional[float] = Field(description="Price as a float. Null if not available.", default=None)
    availability: Optional[bool] = Field(description="True if in stock, False if backordered or out of stock", default=True)

class DentalProductExtraction(BaseModel):
    product_name: str = Field(description="Name of the product")
    brand: Optional[str] = Field(description="Brand or Manufacturer name", default=None)
    category_hierarchy: List[str] = Field(description="List representing the breadcrumb navigation path or category hierarchy")
    description: str = Field(description="Main description or summary of the product")
    variations: List[ProductVariation] = Field(description="List of all SKUs/options available for this product")
    image_urls: List[str] = Field(description="List of URLs pointing to product images")
    alternative_products: List[str] = Field(description="List of related/alternative products (text or SKUs) suggested on the page", default_factory=list)

class DentalProduct(DentalProductExtraction):
    source_url: str = Field(description="The Safco URL where this product was found")


class HandymanRouteDecision(BaseModel):
    label: str = Field(description="One of product, category, other, uncertain")
    confidence: float = Field(description="Confidence from 0.0 to 1.0")
    reason: str = Field(description="Short explanation for the routing decision")


class HandymanVerifyResult(BaseModel):
    decision: str = Field(description="One of pass, warn, fail")
    confidence: float = Field(description="Confidence from 0.0 to 1.0")
    issues: List[str] = Field(description="Potential problems found in the extracted payload", default_factory=list)
    notes: str = Field(description="Short verification note", default="")
