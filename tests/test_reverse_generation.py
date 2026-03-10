from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    Exterior,
    ImpossibleExteriorError,
    ItemCatalog,
    ItemDefinition,
    Rarity,
    TradeUpFormulaGenerator,
    generate_trade_up_formulas,
)


class ReverseGenerationTests(unittest.TestCase):
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
            ItemDefinition("Omega Input", "Omega", Rarity.MIL_SPEC, 0.00, 1.00),
            ItemDefinition("Impossible Target", "Omega", Rarity.RESTRICTED, 0.10, 0.80),
        ]
        return ItemCatalog(items)

    def test_generate_trade_up_formulas_prefers_cheaper_auxiliary_collection(self) -> None:
        catalog = self.build_catalog()
        generator = TradeUpFormulaGenerator(catalog)

        formulas = generator.generate_trade_up_formulas(
            target_item="Target Gun",
            target_exterior="Factory New",
            min_target_count=2,
            max_target_count=2,
            max_auxiliary_collections=1,
            allowed_auxiliary_collections=("Beta", "Gamma"),
            max_formulas=None,
            collection_costs={"Alpha": 4.0, "Beta": 1.0, "Gamma": 2.0},
        )

        self.assertEqual(len(formulas), 2)
        best_formula = formulas[0]
        self.assertEqual(best_formula.collection_counts, {"Alpha": 2, "Beta": 8})
        self.assertAlmostEqual(best_formula.estimated_cost, 16.0)
        self.assertAlmostEqual(best_formula.target_probability, 0.1)
        self.assertAlmostEqual(
            best_formula.exterior_requirement.required_average_metric_max,
            0.1,
        )
        self.assertEqual(
            [(outcome.item.name, round(outcome.probability, 3)) for outcome in best_formula.outcome_probabilities],
            [
                ("Beta Outcome", 0.8),
                ("Alpha Sidearm", 0.1),
                ("Target Gun", 0.1),
            ],
        )

    def test_generate_trade_up_formulas_enumerates_split_auxiliary_counts(self) -> None:
        catalog = self.build_catalog()

        formulas = generate_trade_up_formulas(
            "Target Gun",
            Exterior.FACTORY_NEW,
            catalog,
            min_target_count=2,
            max_target_count=2,
            max_auxiliary_collections=2,
            allowed_auxiliary_collections=("Beta", "Gamma"),
            max_formulas=None,
        )

        self.assertEqual(len(formulas), 9)
        split_formula_counts = [
            formula.collection_counts
            for formula in formulas
            if len(formula.collection_components) == 3
        ]
        self.assertIn({"Alpha": 2, "Beta": 1, "Gamma": 7}, split_formula_counts)
        self.assertIn({"Alpha": 2, "Beta": 7, "Gamma": 1}, split_formula_counts)

    def test_factory_new_requirement_raises_when_target_cannot_spawn_fn(self) -> None:
        catalog = self.build_catalog()
        generator = TradeUpFormulaGenerator(catalog)

        with self.assertRaises(ImpossibleExteriorError):
            generator.generate_trade_up_formulas(
                target_item="Impossible Target",
                target_exterior="Factory New",
            )


if __name__ == "__main__":
    unittest.main()
