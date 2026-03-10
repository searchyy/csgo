from __future__ import annotations

import math
from typing import Mapping

from .catalog import ItemCatalog
from .exceptions import InvalidContractError, MissingPriceError, MissingUpgradePathError
from .models import (
    ContractEvaluation,
    ContractItem,
    Exterior,
    ItemDefinition,
    PriceQuote,
    PricedTradeUpOutcome,
    TradeUpContract,
    TradeUpOutcome,
)

DEFAULT_FEE_RATE = 0.025


class FloatCalculator:
    SUPPORTED_FORMULAS = {"legacy", "normalized"}

    def __init__(self, formula: str = "normalized") -> None:
        if formula not in self.SUPPORTED_FORMULAS:
            raise ValueError(f"Unsupported formula: {formula!r}")
        self.formula = formula

    def average_input_metric(self, inputs: tuple[ContractItem, ...] | list[ContractItem]) -> float:
        if not inputs:
            raise ValueError("At least one input item is required")
        if self.formula == "legacy":
            return sum(item.float_value for item in inputs) / len(inputs)
        return sum(
            item.definition.wear_position(item.float_value) for item in inputs
        ) / len(inputs)

    def output_float_from_metric(self, average_metric: float, output_item: ItemDefinition) -> float:
        return average_metric * output_item.float_range + output_item.min_float

    def calculate_output_float(
        self, inputs: tuple[ContractItem, ...] | list[ContractItem], output_item: ItemDefinition
    ) -> float:
        average_metric = self.average_input_metric(inputs)
        return self.output_float_from_metric(average_metric, output_item)

    def required_average_metric(
        self, output_item: ItemDefinition, desired_output_float: float
    ) -> float:
        if not output_item.min_float <= desired_output_float <= output_item.max_float:
            raise ValueError(
                f"Desired float {desired_output_float} is outside output range "
                f"[{output_item.min_float}, {output_item.max_float}]"
            )
        if output_item.float_range == 0:
            return 0.0
        return (desired_output_float - output_item.min_float) / output_item.float_range


class ProbabilityCalculator:
    def __init__(self, float_calculator: FloatCalculator | None = None) -> None:
        self.float_calculator = float_calculator or FloatCalculator()

    def calculate_outcomes(
        self, contract: TradeUpContract, catalog: ItemCatalog
    ) -> tuple[TradeUpOutcome, ...]:
        try:
            next_rarity = contract.input_rarity.next_rarity()
        except ValueError as error:
            raise InvalidContractError(str(error)) from error

        collection_counts = contract.collection_counts()
        missing_collections: list[str] = []
        outcomes: list[TradeUpOutcome] = []

        for collection, count in sorted(collection_counts.items()):
            candidates = catalog.get_items(collection, next_rarity)
            if not candidates:
                missing_collections.append(collection)
                continue
            probability = count / contract.required_item_count / len(candidates)
            for candidate in sorted(candidates, key=lambda item: item.name):
                outcomes.append(
                    TradeUpOutcome(
                        item=candidate,
                        probability=probability,
                        output_float=self.float_calculator.calculate_output_float(
                            contract.inputs, candidate
                        ),
                    )
                )

        if missing_collections:
            joined = ", ".join(missing_collections)
            raise MissingUpgradePathError(
                f"No upgrade candidates found for collection(s): {joined}"
            )

        total_probability = sum(outcome.probability for outcome in outcomes)
        if not math.isclose(total_probability, 1.0, rel_tol=0.0, abs_tol=1e-9):
            raise InvalidContractError(
                f"Outcome probability must sum to 1.0, got {total_probability}"
            )

        return tuple(outcomes)


class EconomicsCalculator:
    def evaluate(
        self,
        outcomes: tuple[TradeUpOutcome, ...] | list[TradeUpOutcome],
        total_cost: float,
        market_prices: Mapping[object, float | PriceQuote],
        fee_rate: float = DEFAULT_FEE_RATE,
        price_source: str = "lowest",
    ) -> ContractEvaluation:
        if total_cost < 0:
            raise ValueError("total_cost cannot be negative")
        if not 0.0 <= fee_rate < 1.0:
            raise ValueError("fee_rate must be within [0, 1)")

        priced_outcomes: list[PricedTradeUpOutcome] = []
        expected_revenue = 0.0

        for outcome in outcomes:
            quote = self._resolve_price_quote(outcome, market_prices)
            market_price = quote.resolve(price_source=price_source)
            net_sale_price = market_price * (1.0 - fee_rate)
            contribution = outcome.probability * net_sale_price
            expected_revenue += contribution
            priced_outcomes.append(
                PricedTradeUpOutcome(
                    outcome=outcome,
                    market_price=market_price,
                    net_sale_price=net_sale_price,
                    expected_revenue_contribution=contribution,
                )
            )

        priced_outcomes.sort(
            key=lambda value: value.expected_revenue_contribution, reverse=True
        )

        expected_profit = expected_revenue - total_cost
        roi = math.inf if total_cost == 0 else expected_revenue / total_cost

        return ContractEvaluation(
            total_cost=total_cost,
            fee_rate=fee_rate,
            expected_revenue=expected_revenue,
            expected_profit=expected_profit,
            roi=roi,
            priced_outcomes=tuple(priced_outcomes),
        )

    def _resolve_price_quote(
        self,
        outcome: TradeUpOutcome,
        market_prices: Mapping[object, float | PriceQuote],
    ) -> PriceQuote:
        lookup_keys = (
            (outcome.item.name, outcome.exterior),
            (outcome.item.name, outcome.exterior.value),
            outcome.item.name,
        )
        for key in lookup_keys:
            if key in market_prices:
                value = market_prices[key]
                if isinstance(value, PriceQuote):
                    return value
                return PriceQuote(lowest_price=float(value))
        raise MissingPriceError(
            f"Missing market price for {outcome.item.name} ({outcome.exterior.value})"
        )


class TradeUpEngine:
    def __init__(
        self,
        catalog: ItemCatalog,
        float_calculator: FloatCalculator | None = None,
        probability_calculator: ProbabilityCalculator | None = None,
        economics_calculator: EconomicsCalculator | None = None,
    ) -> None:
        self.catalog = catalog
        self.float_calculator = float_calculator or FloatCalculator()
        self.probability_calculator = probability_calculator or ProbabilityCalculator(
            self.float_calculator
        )
        self.economics_calculator = economics_calculator or EconomicsCalculator()

    def calculate_outcomes(self, contract: TradeUpContract) -> tuple[TradeUpOutcome, ...]:
        return self.probability_calculator.calculate_outcomes(contract, self.catalog)

    def evaluate(
        self,
        contract: TradeUpContract,
        market_prices: Mapping[object, float | PriceQuote],
        total_cost: float | None = None,
        fee_rate: float = DEFAULT_FEE_RATE,
        price_source: str = "lowest",
    ) -> ContractEvaluation:
        outcomes = self.calculate_outcomes(contract)
        resolved_cost = contract.total_cost if total_cost is None else total_cost
        return self.economics_calculator.evaluate(
            outcomes=outcomes,
            total_cost=resolved_cost,
            market_prices=market_prices,
            fee_rate=fee_rate,
            price_source=price_source,
        )
