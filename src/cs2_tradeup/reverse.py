from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import combinations
from typing import Iterable, Mapping

from .catalog import ItemCatalog
from .engine import FloatCalculator
from .exceptions import FormulaGenerationError, ImpossibleExteriorError
from .models import Exterior, ItemDefinition, Rarity

_EXTERIOR_FLOAT_BOUNDS: dict[Exterior, tuple[float, float]] = {
    Exterior.FACTORY_NEW: (0.00, 0.07),
    Exterior.MINIMAL_WEAR: (0.07, 0.15),
    Exterior.FIELD_TESTED: (0.15, 0.38),
    Exterior.WELL_WORN: (0.38, 0.45),
    Exterior.BATTLE_SCARRED: (0.45, 1.00),
}


@dataclass(frozen=True, slots=True)
class ExteriorRequirement:
    exterior: Exterior
    output_float_min: float
    output_float_max: float
    required_average_metric_min: float
    required_average_metric_max: float
    metric_name: str

    @property
    def max_average_input_float(self) -> float | None:
        if self.metric_name != "average_input_float":
            return None
        return self.required_average_metric_max


@dataclass(frozen=True, slots=True)
class FormulaCollectionComponent:
    collection: str
    count: int
    input_items: tuple[ItemDefinition, ...]
    output_items: tuple[ItemDefinition, ...]
    estimated_unit_cost: float | None = None

    @property
    def share(self) -> float:
        return self.count / 10.0


@dataclass(frozen=True, slots=True)
class FormulaOutcome:
    item: ItemDefinition
    probability: float
    is_target: bool = False


@dataclass(frozen=True, slots=True)
class TradeUpFormula:
    target_item: ItemDefinition
    target_exterior: Exterior
    input_rarity: Rarity
    collection_components: tuple[FormulaCollectionComponent, ...]
    target_probability: float
    outcome_probabilities: tuple[FormulaOutcome, ...]
    exterior_requirement: ExteriorRequirement
    estimated_cost: float | None = None

    @property
    def target_collection_count(self) -> int:
        for component in self.collection_components:
            if component.collection == self.target_item.collection:
                return component.count
        return 0

    @property
    def collection_counts(self) -> dict[str, int]:
        return {
            component.collection: component.count
            for component in self.collection_components
        }


class TradeUpFormulaGenerator:
    def __init__(
        self,
        catalog: ItemCatalog,
        float_calculator: FloatCalculator | None = None,
        contract_size: int = 10,
    ) -> None:
        self.catalog = catalog
        self.float_calculator = float_calculator or FloatCalculator()
        self.contract_size = contract_size

    def generate_trade_up_formulas(
        self,
        target_item: str | ItemDefinition,
        target_exterior: Exterior | str,
        *,
        min_target_count: int = 1,
        max_target_count: int | None = None,
        max_auxiliary_collections: int = 2,
        max_auxiliary_collection_candidates: int | None = 12,
        allowed_auxiliary_collections: Iterable[str] | None = None,
        collection_costs: Mapping[str, float] | None = None,
        max_formulas: int | None = 50,
        min_target_probability: float = 0.0,
    ) -> tuple[TradeUpFormula, ...]:
        resolved_target = self._resolve_target_item(target_item)
        resolved_exterior = self._resolve_target_exterior(target_exterior)
        input_rarity = self._resolve_input_rarity(resolved_target)
        target_inputs = self.catalog.get_items(resolved_target.collection, input_rarity)
        if not target_inputs:
            raise FormulaGenerationError(
                f"Collection '{resolved_target.collection}' has no {input_rarity.name} inputs "
                f"for target '{resolved_target.name}'"
            )

        target_outputs = self.catalog.get_items(resolved_target.collection, resolved_target.rarity)
        if not target_outputs:
            raise FormulaGenerationError(
                f"Collection '{resolved_target.collection}' has no outputs for '{resolved_target.name}'"
            )

        requirement = self._build_exterior_requirement(resolved_target, resolved_exterior)
        max_target_count = self.contract_size if max_target_count is None else max_target_count
        self._validate_generation_bounds(
            min_target_count=min_target_count,
            max_target_count=max_target_count,
            max_auxiliary_collections=max_auxiliary_collections,
        )

        auxiliary_collections = self._eligible_auxiliary_collections(
            input_rarity=input_rarity,
            target_rarity=resolved_target.rarity,
            target_collection=resolved_target.collection,
            allowed_auxiliary_collections=allowed_auxiliary_collections,
            collection_costs=collection_costs,
            max_candidates=max_auxiliary_collection_candidates,
        )

        formulas: list[TradeUpFormula] = []
        for target_count in range(min_target_count, max_target_count + 1):
            remaining_slots = self.contract_size - target_count
            max_aux_parts = min(max_auxiliary_collections, remaining_slots)

            if remaining_slots == 0:
                formula = self._build_formula(
                    target_item=resolved_target,
                    target_exterior=resolved_exterior,
                    input_rarity=input_rarity,
                    requirement=requirement,
                    collection_counts=((resolved_target.collection, target_count),),
                    collection_costs=collection_costs,
                )
                if formula.target_probability >= min_target_probability:
                    formulas.append(formula)
                continue

            for aux_parts in range(1, max_aux_parts + 1):
                for chosen_collections in combinations(auxiliary_collections, aux_parts):
                    for counts in self._positive_compositions(remaining_slots, aux_parts):
                        collection_counts = [(resolved_target.collection, target_count)]
                        collection_counts.extend(zip(chosen_collections, counts, strict=True))
                        formula = self._build_formula(
                            target_item=resolved_target,
                            target_exterior=resolved_exterior,
                            input_rarity=input_rarity,
                            requirement=requirement,
                            collection_counts=tuple(collection_counts),
                            collection_costs=collection_costs,
                        )
                        if formula.target_probability >= min_target_probability:
                            formulas.append(formula)

        formulas.sort(key=self._build_sort_key(collection_costs is not None))
        if max_formulas is not None:
            formulas = formulas[:max_formulas]
        return tuple(formulas)

    def _resolve_target_item(self, target_item: str | ItemDefinition) -> ItemDefinition:
        if isinstance(target_item, ItemDefinition):
            return target_item
        try:
            return self.catalog.get_item(target_item)
        except KeyError as error:
            raise FormulaGenerationError(f"Unknown target item: {target_item}") from error

    def _resolve_target_exterior(self, exterior: Exterior | str) -> Exterior:
        if isinstance(exterior, Exterior):
            return exterior
        return Exterior.from_label(exterior)

    def _resolve_input_rarity(self, target_item: ItemDefinition) -> Rarity:
        try:
            return Rarity(target_item.rarity - 1)
        except ValueError as error:
            raise FormulaGenerationError(
                f"Item '{target_item.name}' does not have a valid lower rarity tier"
            ) from error

    def _build_exterior_requirement(
        self,
        target_item: ItemDefinition,
        target_exterior: Exterior,
    ) -> ExteriorRequirement:
        lower_bound, upper_bound = _EXTERIOR_FLOAT_BOUNDS[target_exterior]
        output_float_min = max(target_item.min_float, lower_bound)
        output_float_max = target_item.max_float
        if target_exterior is not Exterior.BATTLE_SCARRED:
            output_float_max = min(
                target_item.max_float,
                math.nextafter(upper_bound, -math.inf),
            )
        if output_float_min > output_float_max:
            raise ImpossibleExteriorError(
                f"{target_item.name} cannot be generated as {target_exterior.value}"
            )

        metric_name = (
            "average_input_float"
            if self.float_calculator.formula == "legacy"
            else "average_input_wear_metric"
        )
        metric_min = self.float_calculator.required_average_metric(
            target_item, output_float_min
        )
        metric_max = self.float_calculator.required_average_metric(
            target_item, output_float_max
        )
        return ExteriorRequirement(
            exterior=target_exterior,
            output_float_min=output_float_min,
            output_float_max=output_float_max,
            required_average_metric_min=metric_min,
            required_average_metric_max=metric_max,
            metric_name=metric_name,
        )

    def _validate_generation_bounds(
        self,
        *,
        min_target_count: int,
        max_target_count: int,
        max_auxiliary_collections: int,
    ) -> None:
        if not 1 <= min_target_count <= self.contract_size:
            raise ValueError("min_target_count must be within [1, contract_size]")
        if not min_target_count <= max_target_count <= self.contract_size:
            raise ValueError(
                "max_target_count must be within [min_target_count, contract_size]"
            )
        if max_auxiliary_collections < 0:
            raise ValueError("max_auxiliary_collections cannot be negative")

    def _eligible_auxiliary_collections(
        self,
        *,
        input_rarity: Rarity,
        target_rarity: Rarity,
        target_collection: str,
        allowed_auxiliary_collections: Iterable[str] | None,
        collection_costs: Mapping[str, float] | None,
        max_candidates: int | None,
    ) -> tuple[str, ...]:
        allowed = set(allowed_auxiliary_collections or ())
        collections = [
            collection
            for collection in self.catalog.get_collections_with_upgrade_path(
                input_rarity=input_rarity,
                target_rarity=target_rarity,
            )
            if collection != target_collection
            and (not allowed or collection in allowed)
        ]
        collections.sort(
            key=lambda collection: (
                math.inf if collection_costs is None else collection_costs.get(collection, math.inf),
                collection,
            )
        )
        if max_candidates is not None:
            collections = collections[:max_candidates]
        return tuple(collections)

    def _build_formula(
        self,
        *,
        target_item: ItemDefinition,
        target_exterior: Exterior,
        input_rarity: Rarity,
        requirement: ExteriorRequirement,
        collection_counts: tuple[tuple[str, int], ...],
        collection_costs: Mapping[str, float] | None,
    ) -> TradeUpFormula:
        components: list[FormulaCollectionComponent] = []
        outcomes: list[FormulaOutcome] = []
        estimated_cost = 0.0 if collection_costs is not None else None
        target_probability = 0.0

        for collection, count in sorted(collection_counts):
            input_items = tuple(
                sorted(
                    self.catalog.get_items(collection, input_rarity),
                    key=lambda item: item.name,
                )
            )
            output_items = tuple(
                sorted(
                    self.catalog.get_items(collection, target_item.rarity),
                    key=lambda item: item.name,
                )
            )
            if not input_items or not output_items:
                raise FormulaGenerationError(
                    f"Collection '{collection}' is missing input or output items"
                )

            unit_cost = None if collection_costs is None else collection_costs.get(collection)
            if estimated_cost is not None and unit_cost is not None:
                estimated_cost += unit_cost * count
            elif estimated_cost is not None and unit_cost is None:
                estimated_cost = None

            components.append(
                FormulaCollectionComponent(
                    collection=collection,
                    count=count,
                    input_items=input_items,
                    output_items=output_items,
                    estimated_unit_cost=unit_cost,
                )
            )

            probability = count / self.contract_size / len(output_items)
            for output_item in output_items:
                is_target = output_item.name == target_item.name
                if is_target:
                    target_probability += probability
                outcomes.append(
                    FormulaOutcome(
                        item=output_item,
                        probability=probability,
                        is_target=is_target,
                    )
                )

        outcomes.sort(key=lambda outcome: (-outcome.probability, outcome.item.name))
        return TradeUpFormula(
            target_item=target_item,
            target_exterior=target_exterior,
            input_rarity=input_rarity,
            collection_components=tuple(components),
            target_probability=target_probability,
            outcome_probabilities=tuple(outcomes),
            exterior_requirement=requirement,
            estimated_cost=estimated_cost,
        )

    def _build_sort_key(self, use_cost: bool):
        if use_cost:
            return lambda formula: (
                math.inf if formula.estimated_cost is None else formula.estimated_cost,
                -formula.target_probability,
                len(formula.collection_components),
                -formula.target_collection_count,
                self._formula_signature(formula),
            )
        return lambda formula: (
            -formula.target_probability,
            len(formula.collection_components),
            self._formula_signature(formula),
        )

    def _formula_signature(self, formula: TradeUpFormula) -> tuple[tuple[str, int], ...]:
        return tuple(
            (component.collection, component.count)
            for component in formula.collection_components
        )

    def _positive_compositions(self, total: int, parts: int):
        if parts == 0:
            if total == 0:
                yield ()
            return
        if parts == 1:
            if total >= 1:
                yield (total,)
            return
        max_first = total - parts + 1
        for first in range(1, max_first + 1):
            for remainder in self._positive_compositions(total - first, parts - 1):
                yield (first, *remainder)


def generate_trade_up_formulas(
    target_item: str | ItemDefinition,
    target_exterior: Exterior | str,
    db: ItemCatalog,
    **kwargs,
) -> tuple[TradeUpFormula, ...]:
    return TradeUpFormulaGenerator(db).generate_trade_up_formulas(
        target_item=target_item,
        target_exterior=target_exterior,
        **kwargs,
    )
