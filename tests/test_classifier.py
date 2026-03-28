import unittest

from classifier import looks_like_product_page


class ClassifierTests(unittest.TestCase):
    def test_detects_product_page(self):
        markdown = """
        # SurgiSuture Chromic Gut
        Brand: Example Dental
        Item #: 12-345
        Box of 12
        Add to Cart
        Price: $29.99
        Related Products
        """
        result = looks_like_product_page("https://www.safcodental.com/item/surgisuture-chromic-gut", markdown)
        self.assertTrue(result.is_product)
        self.assertGreaterEqual(result.product_score, 2)

    def test_rejects_category_page(self):
        markdown = """
        # Gloves
        Filter By
        Sort By
        124 results
        Showing 1-24
        Add to Cart
        Add to Cart
        Add to Cart
        """
        result = looks_like_product_page("https://www.safcodental.com/catalog/gloves", markdown)
        self.assertFalse(result.is_product)
        self.assertGreaterEqual(result.category_score, 2)


if __name__ == "__main__":
    unittest.main()
