from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    Exterior,
    ItemCatalog,
    ItemDefinition,
    MultiMarketPriceManager,
    PriceQuote,
    Rarity,
    TradeUpScanner,
    WatchlistTarget,
    calculate_formula_ev,
    export_scan_results_csv,
    find_optimal_materials,
    format_scan_results,
    load_watchlist,
)


class StaticMarketClient:
    def __init__(self, market_name: str, quotes):
        self.market_name = market_name
        self.quotes = dict(quotes)
        self.calls = []

    def get_item_price(self, item_name: str, exterior: str) -> PriceQuote:
        self.calls.append((item_name, exterior))
        return self.quotes[(item_name, exterior)]


class BatchMarketClient(StaticMarketClient):
    def __init__(self, market_name: str, quotes):
        super().__init__(market_name, quotes)
        self.batch_calls = []

    def get_item_prices(self, item_name: str, exteriors) -> dict[str, PriceQuote]:
        normalized = tuple(exteriors)
        self.batch_calls.append((item_name, normalized))
        return {
            exterior: self.quotes[(item_name, exterior)]
            for exterior in normalized
            if (item_name, exterior) in self.quotes
        }


class ScannerTests(unittest.TestCase):
    def build_catalog(self) -> ItemCatalog:
        items = [
            ItemDefinition("Alpha Input 1", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Alpha Input 2", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Beta Input 1", "Beta", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Beta Input 2", "Beta", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Gamma Input 1", "Gamma", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Target Gun", "Alpha", Rarity.RESTRICTED, 0.00, 0.70),
            ItemDefinition("Alpha Sidearm", "Alpha", Rarity.RESTRICTED, 0.10, 0.60),
            ItemDefinition("Beta Outcome", "Beta", Rarity.RESTRICTED, 0.00, 0.25),
            ItemDefinition("Gamma Outcome 1", "Gamma", Rarity.RESTRICTED, 0.00, 0.50),
            ItemDefinition("Gamma Outcome 2", "Gamma", Rarity.RESTRICTED, 0.05, 0.45),
        ]
        return ItemCatalog(items)

    def build_scanner(self):
        buff_quotes = {
            ("Alpha Input 1", "Minimal Wear"): PriceQuote(5.0),
            ("Alpha Input 2", "Minimal Wear"): PriceQuote(3.0),
            ("Beta Input 1", "Minimal Wear"): PriceQuote(1.0),
            ("Beta Input 2", "Minimal Wear"): PriceQuote(1.4),
            ("Gamma Input 1", "Minimal Wear"): PriceQuote(1.8),
            ("Target Gun", "Factory New"): PriceQuote(100.0),
            ("Alpha Sidearm", "Minimal Wear"): PriceQuote(10.0),
            ("Beta Outcome", "Factory New"): PriceQuote(20.0),
            ("Gamma Outcome 1", "Factory New"): PriceQuote(15.0),
            ("Gamma Outcome 2", "Minimal Wear"): PriceQuote(16.0),
        }
        uu_quotes = {
            ("Alpha Input 1", "Minimal Wear"): PriceQuote(4.5),
            ("Alpha Input 2", "Minimal Wear"): PriceQuote(3.2),
            ("Beta Input 1", "Minimal Wear"): PriceQuote(1.2),
            ("Beta Input 2", "Minimal Wear"): PriceQuote(1.1),
            ("Gamma Input 1", "Minimal Wear"): PriceQuote(2.0),
            ("Target Gun", "Factory New"): PriceQuote(120.0),
            ("Alpha Sidearm", "Minimal Wear"): PriceQuote(12.0),
            ("Beta Outcome", "Factory New"): PriceQuote(18.0),
            ("Gamma Outcome 1", "Factory New"): PriceQuote(18.0),
            ("Gamma Outcome 2", "Minimal Wear"): PriceQuote(14.0),
        }
        buff = StaticMarketClient("BUFF", buff_quotes)
        uu = StaticMarketClient("UU", uu_quotes)
        price_manager = MultiMarketPriceManager({"BUFF": buff, "UU": uu}, max_workers=6)
        scanner = TradeUpScanner(
            catalog=self.build_catalog(),
            price_manager=price_manager,
            fee_rate=0.025,
            max_workers=4,
        )
        return scanner, buff, uu

    def test_scan_target_filters_and_sorts_by_roi(self) -> None:
        scanner, buff, uu = self.build_scanner()
        target = WatchlistTarget(
            item_name="Target Gun",
            exterior=Exterior.FACTORY_NEW,
            formula_options={
                "min_target_count": 2,
                "max_target_count": 2,
                "max_auxiliary_collections": 1,
                "allowed_auxiliary_collections": ("Beta", "Gamma"),
                "max_formulas": None,
            },
        )

        results = scanner.scan_target(target, roi_threshold=1.5, formula_limit=10)

        self.assertEqual(len(results), 1)
        best = results[0]
        self.assertEqual(best.formula.collection_counts, {"Alpha": 2, "Beta": 8})
        self.assertAlmostEqual(best.total_cost, 14.0)
        self.assertAlmostEqual(best.expected_revenue, 28.47)
        self.assertAlmostEqual(best.expected_profit, 14.47)
        self.assertAlmostEqual(best.roi, 28.47 / 14.0)
        self.assertEqual(
            [(pricing.item.name, pricing.unit_price, pricing.market_name) for pricing in best.material_pricings],
            [
                ("Alpha Input 2", 3.0, "BUFF"),
                ("Beta Input 1", 1.0, "BUFF"),
            ],
        )
        self.assertEqual(best.outcome_pricings[0].item.name, "Beta Outcome")
        self.assertEqual(best.outcome_pricings[0].market_name, "BUFF")
        self.assertIn("Target Gun [Factory New]", format_scan_results(results))
        self.assertGreaterEqual(len(buff.calls), 10)
        self.assertGreaterEqual(len(uu.calls), 10)
        self.assertIn(("Target Gun", "Factory New"), buff.calls)
        self.assertIn(("Beta Outcome", "Factory New"), buff.calls)

    def test_scan_targets_returns_multiple_results_and_exports_csv(self) -> None:
        scanner, _, _ = self.build_scanner()
        targets = [
            WatchlistTarget(
                item_name="Target Gun",
                exterior=Exterior.FACTORY_NEW,
                formula_options={
                    "min_target_count": 2,
                    "max_target_count": 2,
                    "max_auxiliary_collections": 1,
                    "allowed_auxiliary_collections": ("Beta", "Gamma"),
                    "max_formulas": None,
                },
            )
        ]

        results = scanner.scan_targets(targets, roi_threshold=1.05, formula_limit_per_target=10)

        self.assertEqual(len(results), 2)
        self.assertGreater(results[0].roi, results[1].roi)
        self.assertEqual(results[1].formula.collection_counts, {"Alpha": 2, "Gamma": 8})

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = export_scan_results_csv(results, Path(temp_dir) / "scan.csv")
            csv_text = csv_path.read_text(encoding="utf-8-sig")

        self.assertIn("target_item,target_exterior,float_validation_status,roi", csv_text)
        self.assertIn("Target Gun", csv_text)

    def test_load_watchlist_coerces_csv_formula_options(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "watchlist.csv"
            csv_path.write_text(
                "item_name,exterior,min_target_count,allowed_auxiliary_collections\n"
                'Target Gun,Factory New,2,"[""Beta"", ""Gamma""]"\n',
                encoding="utf-8",
            )

            targets = load_watchlist(csv_path)

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].item_name, "Target Gun")
        self.assertEqual(targets[0].exterior, Exterior.FACTORY_NEW)
        self.assertEqual(targets[0].formula_options["min_target_count"], 2)
        self.assertEqual(targets[0].formula_options["allowed_auxiliary_collections"], ["Beta", "Gamma"])

    def test_price_manager_prefetch_batches_same_item_exteriors_when_supported(self) -> None:
        client = BatchMarketClient(
            "SteamDTCache",
            {
                ("AK-47 | Slate", "Factory New"): PriceQuote(11.0),
                ("AK-47 | Slate", "Field-Tested"): PriceQuote(7.0),
            },
        )
        manager = MultiMarketPriceManager({"SteamDTCache": client}, max_workers=4)

        manager.prefetch(
            [
                ("AK-47 | Slate", "Factory New"),
                ("AK-47 | Slate", "Field-Tested"),
            ]
        )

        self.assertEqual(
            client.batch_calls,
            [("AK-47 | Slate", ("Factory New", "Field-Tested"))],
        )
        self.assertEqual(client.calls, [])
        self.assertAlmostEqual(
            manager.get_best_quote("AK-47 | Slate", "Factory New").quote.lowest_price,
            11.0,
        )

    def test_calculate_formula_ev_uses_adjusted_float_and_outcome_specific_ranges(self) -> None:
        short_input = ItemDefinition("Short Input", "Alpha", Rarity.MIL_SPEC, 0.00, 0.08)
        wide_input = ItemDefinition("Wide Input", "Alpha", Rarity.MIL_SPEC, 0.10, 0.90)
        outcome_alpha = ItemDefinition("Outcome Alpha", "Alpha", Rarity.RESTRICTED, 0.00, 0.80)
        outcome_beta = ItemDefinition("Outcome Beta", "Beta", Rarity.RESTRICTED, 0.10, 0.30)

        result = calculate_formula_ev(
            materials_list=[
                {
                    "item": short_input,
                    "actual_float": 0.04,
                    "count": 5,
                    "unit_price": 2.0,
                },
                {
                    "item": wide_input,
                    "actual_float": 0.50,
                    "count": 5,
                    "unit_price": 2.0,
                },
            ],
            possible_outcomes=[
                {"item": outcome_alpha, "probability": 0.6},
                {"item": outcome_beta, "probability": 0.4},
            ],
            db_prices={
                ("Outcome Alpha", "Well-Worn"): 100.0,
                ("Outcome Beta", "Field-Tested"): 50.0,
            },
        )

        self.assertAlmostEqual(result.average_adjusted, 0.5)
        self.assertAlmostEqual(result.total_cost, 20.0)
        self.assertAlmostEqual(result.expected_revenue, 78.0)
        self.assertAlmostEqual(result.expected_profit, 58.0)
        self.assertAlmostEqual(result.roi, 3.9)
        self.assertEqual(
            [(pricing.item.name, pricing.exterior.value) for pricing in result.outcome_pricings],
            [("Outcome Alpha", "Well-Worn"), ("Outcome Beta", "Field-Tested")],
        )

    def test_find_optimal_materials_uses_cheapest_valid_exterior_under_adjusted_cap(self) -> None:
        catalog = ItemCatalog(
            [
                ItemDefinition(
                    "Alpha Input",
                    "Alpha",
                    Rarity.MIL_SPEC,
                    0.00,
                    1.00,
                    available_exteriors=(Exterior.FACTORY_NEW, Exterior.MINIMAL_WEAR),
                ),
                ItemDefinition(
                    "Target Gun",
                    "Alpha",
                    Rarity.RESTRICTED,
                    0.00,
                    0.70,
                    available_exteriors=(Exterior.FACTORY_NEW,),
                ),
                ItemDefinition(
                    "Alpha Sidearm",
                    "Alpha",
                    Rarity.RESTRICTED,
                    0.10,
                    0.60,
                    available_exteriors=(Exterior.MINIMAL_WEAR,),
                ),
            ]
        )

        results = find_optimal_materials(
            "Target Gun",
            Exterior.FACTORY_NEW,
            catalog,
            {
                ("Alpha Input", "Factory New"): PriceQuote(2.0),
                ("Alpha Input", "Minimal Wear"): PriceQuote(1.0),
                ("Target Gun", "Factory New"): PriceQuote(100.0),
                ("Alpha Sidearm", "Minimal Wear"): PriceQuote(10.0),
            },
            min_target_count=10,
            max_target_count=10,
            max_auxiliary_collections=0,
            max_formulas=5,
        )

        self.assertEqual(len(results), 1)
        best = results[0]
        self.assertAlmostEqual(best.total_cost, 10.0)
        self.assertAlmostEqual(best.planned_average_metric, 0.1, places=4)
        self.assertEqual(
            [(pricing.requested_exterior.value, pricing.count) for pricing in best.material_pricings],
            [("Minimal Wear", 10)],
        )
        self.assertEqual(best.material_pricings[0].float_source, "requirement_cap")
        self.assertTrue(best.material_pricings[0].requires_float_verification)
        self.assertAlmostEqual(best.material_pricings[0].min_float, 0.07, places=4)
        self.assertAlmostEqual(best.material_pricings[0].max_float, 0.1, places=4)
        self.assertEqual(best.outcome_pricings[0].item.name, "Target Gun")

    def test_find_optimal_materials_conservative_mode_uses_midpoint_estimate(self) -> None:
        catalog = ItemCatalog(
            [
                ItemDefinition(
                    "Alpha Input",
                    "Alpha",
                    Rarity.MIL_SPEC,
                    0.00,
                    1.00,
                    available_exteriors=(Exterior.FACTORY_NEW, Exterior.MINIMAL_WEAR),
                ),
                ItemDefinition(
                    "Target Gun",
                    "Alpha",
                    Rarity.RESTRICTED,
                    0.00,
                    0.70,
                    available_exteriors=(Exterior.FACTORY_NEW,),
                ),
                ItemDefinition(
                    "Alpha Sidearm",
                    "Alpha",
                    Rarity.RESTRICTED,
                    0.10,
                    0.60,
                    available_exteriors=(Exterior.MINIMAL_WEAR,),
                ),
            ]
        )

        results = find_optimal_materials(
            "Target Gun",
            Exterior.FACTORY_NEW,
            catalog,
            {
                ("Alpha Input", "Factory New"): PriceQuote(2.0),
                ("Alpha Input", "Minimal Wear"): PriceQuote(1.0),
                ("Target Gun", "Factory New"): PriceQuote(100.0),
                ("Alpha Sidearm", "Minimal Wear"): PriceQuote(10.0),
            },
            min_target_count=10,
            max_target_count=10,
            max_auxiliary_collections=0,
            max_formulas=5,
            conservative_float_mode=True,
        )

        self.assertEqual(len(results), 1)
        best = results[0]
        self.assertAlmostEqual(best.total_cost, 12.0)
        self.assertEqual(
            [(pricing.requested_exterior.value, pricing.count) for pricing in best.material_pricings],
            [("Factory New", 2), ("Minimal Wear", 8)],
        )
        self.assertTrue(
            all(pricing.float_source == "exterior_midpoint" for pricing in best.material_pricings)
        )
        self.assertTrue(
            all(not pricing.requires_float_verification for pricing in best.material_pricings)
        )
        self.assertAlmostEqual(best.material_pricings[0].min_float, 0.0, places=4)
        self.assertAlmostEqual(best.material_pricings[0].estimated_float, 0.035, places=4)


if __name__ == "__main__":
    unittest.main()
