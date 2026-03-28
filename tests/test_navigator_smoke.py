import unittest

from navigator import fetch_page


class NavigatorSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_gloves_category_renders_product_links(self):
        result = await fetch_page("https://www.safcodental.com/catalog/gloves")

        self.assertTrue(
            result.markdown,
            msg=f"Expected rendered markdown, got error_type={result.error_type} error={result.error}",
        )
        self.assertIsNotNone(result.final_url)
        self.assertEqual(result.http_status, 200)

        product_links = [link for link in result.links if "/product/" in link]
        self.assertGreaterEqual(
            len(product_links),
            10,
            msg=f"Expected rendered product links from category grid, got {len(product_links)} links",
        )


if __name__ == "__main__":
    unittest.main()
