import unittest

from main import (
    build_seed_category_prefixes,
    build_fix_issue_list,
    build_seed_scope_terms,
    collect_incomplete_issues,
    collect_rejection_issues,
    decide_page_with_rules,
    find_suspicious_variation_issues,
    is_product_within_seed_scope,
    mark_product_quality,
    should_enqueue_link,
    should_attempt_variant_recovery,
    should_reject_product_record,
)
from models import DentalProduct, ProductVariation


class MainFilterTests(unittest.TestCase):
    def test_rejects_compare_page_record(self):
        product = DentalProduct(
            product_name="Products Comparison List - Dental Products & Equipment",
            brand=None,
            category_hierarchy=[],
            description="You have no items to compare.",
            variations=[],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/catalog/product_compare/index",
        )
        reason = should_reject_product_record(product.source_url, "You have no items to compare.", product)
        self.assertIsNotNone(reason)

    def test_rejects_catalog_listing_masquerading_as_product(self):
        product = DentalProduct(
            product_name="Dental Dam",
            brand=None,
            category_hierarchy=["Dental Dam"],
            description="",
            variations=[ProductVariation(sku="item-1", price=12.49)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/catalog/category/view/s/rubber-dam/id/913",
        )
        cleaned_md = "Filter By\nSort By\n15 results\nAdd to Cart\nAdd to Cart\n"
        reason = should_reject_product_record(product.source_url, cleaned_md, product)
        self.assertIsNotNone(reason)

    def test_allows_real_product_detail_record(self):
        product = DentalProduct(
            product_name="Black Maxx",
            brand="Safco",
            category_hierarchy=["Gloves", "Nitrile Gloves"],
            description="Powder-free nitrile exam gloves.",
            variations=[ProductVariation(sku="BMNT200XS", size="Extra Small", package_count="Box of 100", price=24.99)],
            image_urls=["https://www.safcodental.com/media/catalog/product/example.jpg"],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/black-maxx-sup-reg-sup",
        )
        reason = should_reject_product_record(product.source_url, "Brand Safco\nSKU BMNT200XS\nAdd to Cart\n$24.99", product)
        self.assertIsNone(reason)

    def test_rule_routing_accepts_clear_product(self):
        decision = decide_page_with_rules(
            "https://www.safcodental.com/product/black-maxx-sup-reg-sup",
            "Black Maxx\nBrand: Safco\nSKU BMNT200XS\nAdd to Cart\n$24.99\nBox of 100",
        )
        self.assertEqual(decision.label, "product")

    def test_rule_routing_rejects_clear_non_product(self):
        decision = decide_page_with_rules(
            "https://www.safcodental.com/catalog/product_compare/index",
            "Products Comparison List\nYou have no items to compare.",
        )
        self.assertEqual(decision.label, "other")

    def test_rule_routing_marks_borderline_page_uncertain(self):
        decision = decide_page_with_rules(
            "https://www.safcodental.com/catalog/gloves/sample-page",
            "Gloves\nBrand: Example\nAdd to Cart\nLearn more",
        )
        self.assertEqual(decision.label, "uncertain")

    def test_scope_allows_seed_descendant_category(self):
        prefixes = build_seed_category_prefixes([
            "https://www.safcodental.com/catalog/gloves",
            "https://www.safcodental.com/catalog/sutures-surgical-products",
        ])
        self.assertTrue(
            should_enqueue_link(
                "https://www.safcodental.com/catalog/gloves",
                "https://www.safcodental.com/catalog/gloves/glove-holder",
                prefixes,
            )
        )

    def test_scope_blocks_unrelated_category(self):
        prefixes = build_seed_category_prefixes([
            "https://www.safcodental.com/catalog/gloves",
            "https://www.safcodental.com/catalog/sutures-surgical-products",
        ])
        self.assertFalse(
            should_enqueue_link(
                "https://www.safcodental.com/catalog/gloves",
                "https://www.safcodental.com/catalog/bone-grafting-products",
                prefixes,
            )
        )

    def test_scope_allows_product_from_seed_category_page(self):
        prefixes = build_seed_category_prefixes([
            "https://www.safcodental.com/catalog/gloves",
        ])
        self.assertTrue(
            should_enqueue_link(
                "https://www.safcodental.com/catalog/gloves?page=2",
                "https://www.safcodental.com/product/aurelia-reg-absolute-trade",
                prefixes,
            )
        )

    def test_scope_blocks_product_from_unscoped_page(self):
        prefixes = build_seed_category_prefixes([
            "https://www.safcodental.com/catalog/gloves",
        ])
        self.assertFalse(
            should_enqueue_link(
                "https://www.safcodental.com/product/ossif-i-sem-trade",
                "https://www.safcodental.com/product/sterile-water-for-injection-b-braun",
                prefixes,
            )
        )

    def test_seed_scope_accepts_matching_category_hierarchy(self):
        product = DentalProduct(
            product_name="Aurelia Absolute",
            brand="Aurelia",
            category_hierarchy=["Gloves", "Nitrile Gloves"],
            description="Powder-free nitrile exam gloves.",
            variations=[ProductVariation(sku="98995", size="XS", package_count="Box of 200", price=16.49)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/aurelia-reg-absolute-trade",
        )
        seed_scope_terms = build_seed_scope_terms([
            "https://www.safcodental.com/catalog/gloves",
            "https://www.safcodental.com/catalog/sutures-surgical-products",
        ])
        self.assertTrue(is_product_within_seed_scope(product, seed_scope_terms))

    def test_seed_scope_rejects_broad_parent_without_target_leaf(self):
        product = DentalProduct(
            product_name="R.T.R.+ Syringe",
            brand="Septodont",
            category_hierarchy=["Sutures & Surgical Products", "Bone Grafting"],
            description="Synthetic bone grafting material.",
            variations=[ProductVariation(sku="01S0530", price=152.99)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/r-t-r-syringe",
        )
        seed_scope_terms = build_seed_scope_terms([
            "https://www.safcodental.com/catalog/gloves",
            "https://www.safcodental.com/catalog/sutures-surgical-products",
        ])
        self.assertFalse(is_product_within_seed_scope(product, seed_scope_terms))

    def test_seed_scope_rejects_unrelated_category_hierarchy(self):
        product = DentalProduct(
            product_name="OSSIF-i sem",
            brand="Surgical Esthetics",
            category_hierarchy=["Bone Grafting Products", "Surgical Esthetics"],
            description="Mineralized cancellous bone allograft.",
            variations=[ProductVariation(sku="BG-010050", price=41.49)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/ossif-i-sem-trade",
        )
        seed_scope_terms = build_seed_scope_terms([
            "https://www.safcodental.com/catalog/gloves",
            "https://www.safcodental.com/catalog/sutures-surgical-products",
        ])
        self.assertFalse(is_product_within_seed_scope(product, seed_scope_terms))
        self.assertEqual(
            should_reject_product_record(
                product.source_url,
                "Bone Grafting Products\nSurgical Esthetics\nBG-010050",
                product,
                seed_scope_terms=seed_scope_terms,
            ),
            "product category hierarchy falls outside seed scope",
        )

    def test_flags_placeholder_or_short_skus(self):
        product = DentalProduct(
            product_name="Example Gloves",
            brand="Example",
            category_hierarchy=["Gloves"],
            description="Example description.",
            variations=[ProductVariation(sku="Q1", package_count="Box of 100", price=8.29)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/example-gloves",
        )
        issues = find_suspicious_variation_issues(product)
        self.assertIn("variation has suspiciously short sku: Q1", issues)

    def test_flags_title_derived_q1_skus(self):
        product = DentalProduct(
            product_name="Transcend",
            brand="Safco Dental",
            category_hierarchy=["Gloves", "Nitrile Gloves"],
            description="Powder-free nitrile exam gloves.",
            variations=[ProductVariation(sku="Transcend-Q1", package_count="Box of 300", price=15.99)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/transcend-trade",
        )
        issues = find_suspicious_variation_issues(product)
        self.assertIn("variation sku looks like a synthetic Q1 placeholder: Transcend-Q1", issues)
        self.assertIn("variation sku appears derived from product title instead of page sku: Transcend-Q1", issues)

    def test_flags_q1_size_placeholder_skus(self):
        product = DentalProduct(
            product_name="BeeSure UltraSlim Nitrile",
            brand="BeeSure",
            category_hierarchy=["Gloves", "Nitrile Gloves"],
            description="Powder-free nitrile exam gloves.",
            variations=[ProductVariation(sku="Q1-Large", size="Large", package_count="Box of 300", price=25.49)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/beesure-ultraslim-nitrile",
        )
        issues = find_suspicious_variation_issues(product)
        self.assertIn("variation sku looks like a synthetic Q1 placeholder: Q1-Large", issues)

    def test_flags_duplicate_sku_conflicts(self):
        product = DentalProduct(
            product_name="Example Gloves",
            brand="Example",
            category_hierarchy=["Gloves"],
            description="Example description.",
            variations=[
                ProductVariation(sku="DRCCZ", size="X-Large", package_count="Box of 180", price=19.99),
                ProductVariation(sku="DRCCZ", size=None, package_count="Box of 200", price=19.99),
            ],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/example-gloves",
        )
        issues = find_suspicious_variation_issues(product)
        self.assertIn("duplicate sku with conflicting variation details: DRCCZ", issues)

    def test_reject_product_record_on_suspicious_variations(self):
        product = DentalProduct(
            product_name="Example Gloves",
            brand="Example",
            category_hierarchy=["Gloves"],
            description="Example description.",
            variations=[ProductVariation(sku="N/A", package_count="Box of 100", price=8.29)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/example-gloves",
        )
        self.assertEqual(
            should_reject_product_record(
                product.source_url,
                "Example page",
                product,
                seed_scope_terms=("gloves",),
            ),
            "variation has placeholder sku: N/A",
        )

    def test_collect_rejection_issues_combines_scope_and_variation_problems(self):
        product = DentalProduct(
            product_name="OSSIF-i sem",
            brand="Surgical Esthetics",
            category_hierarchy=["Bone Grafting Products"],
            description="Mineralized cancellous bone allograft.",
            variations=[ProductVariation(sku="Q1", price=41.49)],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/ossif-i-sem-trade",
        )
        issues = collect_rejection_issues(
            product.source_url,
            "Bone Grafting Products\nQ1",
            product,
            seed_scope_terms=("gloves", "sutures"),
        )
        self.assertIn("product category hierarchy falls outside seed scope", issues)
        self.assertIn("variation has suspiciously short sku: Q1", issues)

    def test_build_fix_issue_list_deduplicates_issues(self):
        issues = build_fix_issue_list(
            ["missing brand", "variation has suspiciously short sku: Q1"],
            ["variation has suspiciously short sku: Q1", "product category hierarchy falls outside seed scope"],
        )
        self.assertEqual(
            issues,
            [
                "missing brand",
                "variation has suspiciously short sku: Q1",
                "product category hierarchy falls outside seed scope",
            ],
        )

    def test_collect_incomplete_issues_flags_missing_glove_variations(self):
        product = DentalProduct(
            product_name="Aurelia Bold",
            brand="Aurelia",
            category_hierarchy=["Gloves", "Nitrile Gloves"],
            description="Powder-free nitrile exam gloves.",
            variations=[],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/aurelia-reg-bold-reg",
        )
        cleaned_md = "Sizes available: XS S M L XL\n100 gloves per box\n"
        issues = collect_incomplete_issues(cleaned_md, product)
        self.assertIn("glove size run visible but no variations were extracted", issues)
        self.assertTrue(should_attempt_variant_recovery(cleaned_md, product))

    def test_mark_product_quality_marks_incomplete(self):
        product = DentalProduct(
            product_name="Aurelia Bold",
            brand="Aurelia",
            category_hierarchy=["Gloves", "Nitrile Gloves"],
            description="Powder-free nitrile exam gloves.",
            variations=[],
            image_urls=[],
            alternative_products=[],
            source_url="https://www.safcodental.com/product/aurelia-reg-bold-reg",
        )
        mark_product_quality(product, ["glove size run visible but no variations were extracted"])
        self.assertEqual(product.quality_status, "incomplete")
        self.assertEqual(product.quality_notes, ["glove size run visible but no variations were extracted"])


if __name__ == "__main__":
    unittest.main()
