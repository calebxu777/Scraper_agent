import unittest

from handyman import handyman_prune, handyman_route, handyman_verify_extraction
from models import DentalProduct, ProductVariation


class HandymanTests(unittest.TestCase):
    def test_rules_route_and_prune(self):
        markdown = """
        # SurgiSuture Chromic Gut
        Brand: Example Dental
        Item #: 12-345
        Add to Cart
        Privacy Policy
        Price: $29.99
        """
        route = handyman_route("https://www.safcodental.com/item/surgisuture", markdown)
        cleaned = handyman_prune("https://www.safcodental.com/item/surgisuture", markdown)
        self.assertEqual(route.label, "product")
        self.assertNotIn("Privacy Policy", cleaned)

    def test_rules_verification_warns_for_missing_support(self):
        product = DentalProduct(
            product_name="Not On Page",
            brand="Example Dental",
            category_hierarchy=["Gloves"],
            description="desc",
            variations=[ProductVariation(sku="XYZ-1")],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/item/example",
        )
        result = handyman_verify_extraction("https://www.safcodental.com/item/example", "Plain page text", product)
        self.assertIn(result.decision, {"warn", "fail"})


if __name__ == "__main__":
    unittest.main()
