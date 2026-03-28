import asyncio
import unittest
from unittest.mock import patch

import handyman
from handyman import handyman_prune, handyman_route, handyman_verify_extraction
from models import DentalProduct, ProductVariation


class HandymanTests(unittest.TestCase):
    def test_rules_route_and_prune(self):
        async def run():
            markdown = """
            # SurgiSuture Chromic Gut
            Brand: Example Dental
            Item #: 12-345
            Add to Cart
            Privacy Policy
            Price: $29.99
            """
            with patch.object(handyman, "HANDYMAN_BACKEND", "rules"):
                route = await handyman_route("https://www.safcodental.com/item/surgisuture", markdown)
                cleaned = await handyman_prune("https://www.safcodental.com/item/surgisuture", markdown)
                self.assertEqual(route.label, "product")
                self.assertNotIn("Privacy Policy", cleaned)

        asyncio.run(run())

    def test_rules_verification_warns_for_missing_support(self):
        async def run():
            product = DentalProduct(
                product_name="Not On Page",
                brand="Example Dental",
                category_hierarchy=["Gloves"],
                description="desc",
                specifications=[],
                variations=[ProductVariation(sku="XYZ-1")],
                image_urls=[],
                alternative_products=[],
                source_url="https://www.safcodental.com/item/example",
            )
            with patch.object(handyman, "HANDYMAN_BACKEND", "rules"):
                result = await handyman_verify_extraction("https://www.safcodental.com/item/example", "Plain page text", product)
                self.assertIn(result.decision, {"warn", "fail"})

        asyncio.run(run())

    def test_rules_verification_uses_normalized_matching(self):
        async def run():
            product = DentalProduct(
                product_name="Aurelia Absolute",
                brand="Aurelia",
                category_hierarchy=["Gloves", "Nitrile Gloves"],
                description="desc",
                specifications=[],
                variations=[ProductVariation(sku="102-5801")],
                image_urls=[],
                alternative_products=[],
                source_url="https://www.safcodental.com/item/example",
            )
            page = "AURELIA ABSOLUTE item 1025801 available now"
            with patch.object(handyman, "HANDYMAN_BACKEND", "rules"):
                result = await handyman_verify_extraction("https://www.safcodental.com/item/example", page, product)
                self.assertEqual(result.decision, "pass")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
