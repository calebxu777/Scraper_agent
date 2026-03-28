import unittest

from navigator import _normalize_discovered_url


class NavigatorUnitTests(unittest.TestCase):
    def test_preserves_catalog_pagination_query(self):
        self.assertEqual(
            _normalize_discovered_url("https://www.safcodental.com/catalog/gloves?page=2#top"),
            "https://www.safcodental.com/catalog/gloves?page=2",
        )

    def test_drops_non_pagination_query_from_catalog(self):
        self.assertEqual(
            _normalize_discovered_url("https://www.safcodental.com/catalog/gloves?sort=name"),
            "https://www.safcodental.com/catalog/gloves",
        )

    def test_drops_query_from_product_urls(self):
        self.assertEqual(
            _normalize_discovered_url("https://www.safcodental.com/product/example?variant=large"),
            "https://www.safcodental.com/product/example",
        )


if __name__ == "__main__":
    unittest.main()
