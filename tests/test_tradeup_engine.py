from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    ContractItem,
    FloatCalculator,
    ItemCatalog,
    ItemDefinition,
    MissingUpgradePathError,
    PriceQuote,
    Rarity,
    TradeUpContract,
    TradeUpEngine,
)


class TradeUpEngineTests(unittest.TestCase):
    def sample_catalog(self) -> ItemCatalog:
        items = [
            ItemDefinition("Alpha Input", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Beta Input", "Beta", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Gamma Input", "Gamma", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Alpha Output 1", "Alpha", Rarity.RESTRICTED, 0.00, 0.70),
            ItemDefinition("Alpha Output 2", "Alpha", Rarity.RESTRICTED, 0.10, 0.60),
            ItemDefinition("Beta Output 1", "Beta", Rarity.RESTRICTED, 0.00, 0.20),
        ]
        return ItemCatalog(items)

    def test_legacy_float_formula_matches_requested_equation(self) -> None:
        calculator = FloatCalculator(formula="legacy")
        input_item = ItemDefinition("Input", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00)
        output_item = ItemDefinition("Output", "Alpha", Rarity.RESTRICTED, 0.10, 0.50)
        inputs = [
            ContractItem(input_item, 0.20),
            ContractItem(input_item, 0.30),
        ]

        result = calculator.calculate_output_float(inputs, output_item)

        self.assertAlmostEqual(result, 0.20)

    def test_normalized_formula_supports_post_range_normalization(self) -> None:
        calculator = FloatCalculator(formula="normalized")
        short_range = ItemDefinition("Short Range", "Alpha", Rarity.MIL_SPEC, 0.00, 0.08)
        wide_range = ItemDefinition("Wide Range", "Alpha", Rarity.MIL_SPEC, 0.10, 0.90)
        output_item = ItemDefinition("Output", "Alpha", Rarity.RESTRICTED, 0.00, 0.80)
        inputs = [
            ContractItem(short_range, 0.04),
            ContractItem(wide_range, 0.50),
        ]

        result = calculator.calculate_output_float(inputs, output_item)

        self.assertAlmostEqual(result, 0.40)
        self.assertAlmostEqual(calculator.required_average_metric(output_item, 0.40), 0.50)

    def test_probability_distribution_for_mixed_collections(self) -> None:
        catalog = self.sample_catalog()
        engine = TradeUpEngine(catalog)
        alpha = catalog.get_item("Alpha Input")
        beta = catalog.get_item("Beta Input")
        contract = TradeUpContract(
            tuple(
                [ContractItem(alpha, 0.10) for _ in range(6)]
                + [ContractItem(beta, 0.20) for _ in range(4)]
            )
        )

        outcomes = engine.calculate_outcomes(contract)
        probabilities = {outcome.item.name: outcome.probability for outcome in outcomes}

        self.assertAlmostEqual(probabilities["Alpha Output 1"], 0.30)
        self.assertAlmostEqual(probabilities["Alpha Output 2"], 0.30)
        self.assertAlmostEqual(probabilities["Beta Output 1"], 0.40)
        self.assertAlmostEqual(sum(probabilities.values()), 1.0)
        self.assertAlmostEqual(outcomes[0].output_float, 0.098)

    def test_evaluate_contract_ev_and_roi(self) -> None:
        catalog = self.sample_catalog()
        engine = TradeUpEngine(catalog)
        alpha = catalog.get_item("Alpha Input")
        beta = catalog.get_item("Beta Input")
        contract = TradeUpContract(
            tuple(
                [ContractItem(alpha, 0.10, price_paid=3.0) for _ in range(6)]
                + [ContractItem(beta, 0.20, price_paid=3.0) for _ in range(4)]
            )
        )
        market_prices = {
            "Alpha Output 1": PriceQuote(lowest_price=100.0, recent_average_price=95.0),
            "Alpha Output 2": 50.0,
            "Beta Output 1": 20.0,
        }

        evaluation = engine.evaluate(contract, market_prices=market_prices)

        self.assertAlmostEqual(evaluation.total_cost, 30.0)
        self.assertAlmostEqual(evaluation.expected_revenue, 51.675)
        self.assertAlmostEqual(evaluation.expected_profit, 21.675)
        self.assertAlmostEqual(evaluation.roi, 1.7225)
        self.assertAlmostEqual(evaluation.roi_percent, 172.25)

    def test_missing_upgrade_path_raises_clear_error(self) -> None:
        catalog = self.sample_catalog()
        engine = TradeUpEngine(catalog)
        gamma = catalog.get_item("Gamma Input")
        contract = TradeUpContract(tuple(ContractItem(gamma, 0.15) for _ in range(10)))

        with self.assertRaises(MissingUpgradePathError):
            engine.calculate_outcomes(contract)

    def test_contract_requires_same_rarity(self) -> None:
        mil_spec = ItemDefinition("Mil", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00)
        restricted = ItemDefinition("Restricted", "Alpha", Rarity.RESTRICTED, 0.00, 1.00)

        with self.assertRaises(ValueError):
            TradeUpContract(
                tuple(
                    [ContractItem(mil_spec, 0.10) for _ in range(9)]
                    + [ContractItem(restricted, 0.10)]
                )
            )


if __name__ == "__main__":
    unittest.main()
