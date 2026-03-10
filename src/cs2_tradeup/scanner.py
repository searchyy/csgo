from __future__ import annotations

import csv
import json
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .catalog import ItemCatalog
from .engine import DEFAULT_FEE_RATE, FloatCalculator
from .exceptions import FormulaGenerationError, PriceLookupError
from .market import normalize_exterior_label
from .models import Exterior, ItemDefinition, PriceQuote
from .reverse import FormulaCollectionComponent, TradeUpFormula, TradeUpFormulaGenerator

FLOAT_STATUS_VERIFIED = "verified_float"
FLOAT_STATUS_NEEDS_VERIFICATION = "needs_float_verification"
FLOAT_STATUS_CONSERVATIVE = "conservative_estimate"

FLOAT_STATUS_LABELS = {
    FLOAT_STATUS_VERIFIED: "已验浮",
    FLOAT_STATUS_NEEDS_VERIFICATION: "需验浮",
    FLOAT_STATUS_CONSERVATIVE: "保守估算",
}

FLOAT_SOURCE_VERIFIED = "verified_listing"
FLOAT_SOURCE_REQUIREMENT_CAP = "requirement_cap"
FLOAT_SOURCE_EXTERIOR_MIDPOINT = "exterior_midpoint"

# When the float cap restricts less than this fraction of the exterior range, the
# cheapest listings (near the exterior's upper bound) won't satisfy the cap.  Using
# lowest_price would silently underestimate material cost, so we downgrade to
# FLOAT_SOURCE_REQUIREMENT_CAP and flag requires_float_verification=True instead.
_FLOAT_CAP_USABLE_FRACTION_THRESHOLD = 0.75

FLOAT_SOURCE_LABELS = {
    FLOAT_SOURCE_VERIFIED: "已验浮",
    FLOAT_SOURCE_REQUIREMENT_CAP: "需验浮",
    FLOAT_SOURCE_EXTERIOR_MIDPOINT: "外观中位估算",
}


@dataclass(frozen=True, slots=True)
class WatchlistTarget:
    item_name: str
    exterior: Exterior
    formula_options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SelectedMarketQuote:
    market_name: str
    quote: PriceQuote


@dataclass(frozen=True, slots=True)
class MaterialPricing:
    item: ItemDefinition
    count: int
    min_float: float
    max_float: float
    estimated_float: float
    adjusted_float: float
    requested_exterior: Exterior
    market_name: str
    unit_price: float
    total_price: float
    float_source: str = FLOAT_SOURCE_REQUIREMENT_CAP
    float_source_label: str = FLOAT_SOURCE_LABELS[FLOAT_SOURCE_REQUIREMENT_CAP]
    float_verified: bool = False
    requires_float_verification: bool = True


@dataclass(frozen=True, slots=True)
class OutcomePricing:
    item: ItemDefinition
    probability: float
    output_float: float
    exterior: Exterior
    market_name: str
    market_price: float
    net_sale_price: float
    expected_revenue_contribution: float


@dataclass(frozen=True, slots=True)
class TradeUpScanResult:
    target_item: ItemDefinition
    target_exterior: Exterior
    formula: TradeUpFormula
    planned_average_metric: float
    total_cost: float
    expected_revenue: float
    expected_profit: float
    roi: float
    fee_rate: float
    material_pricings: tuple[MaterialPricing, ...]
    outcome_pricings: tuple[OutcomePricing, ...]

    @property
    def roi_percent(self) -> float:
        return self.roi * 100.0

    @property
    def target_probability(self) -> float:
        return self.formula.target_probability

    @property
    def formula_signature(self) -> str:
        return ", ".join(
            f"{collection}:{count}"
            for collection, count in sorted(self.formula.collection_counts.items())
        )


@dataclass(frozen=True, slots=True)
class FormulaMaterialInput:
    item: ItemDefinition
    actual_float: float
    count: int = 1
    unit_price: float = 0.0
    total_price: float | None = None
    requested_exterior: Exterior | None = None
    market_name: str = ""
    float_source: str = FLOAT_SOURCE_REQUIREMENT_CAP
    float_verified: bool = False
    requires_float_verification: bool = True


@dataclass(frozen=True, slots=True)
class FormulaEVResult:
    average_adjusted: float
    total_cost: float
    expected_revenue: float
    expected_profit: float
    roi: float
    outcome_pricings: tuple[OutcomePricing, ...]


@dataclass(frozen=True, slots=True)
class _MaterialCandidate:
    item: ItemDefinition
    requested_exterior: Exterior
    actual_float: float
    adjusted_float: float
    market_name: str
    unit_price: float
    adjusted_units: int
    float_source: str
    float_verified: bool
    requires_float_verification: bool


@dataclass(frozen=True, slots=True)
class _OptimizedFormulaMaterials:
    average_adjusted: float
    total_cost: float
    material_inputs: tuple[FormulaMaterialInput, ...]
    material_pricings: tuple[MaterialPricing, ...]


class IdentityPriceAdjustmentStrategy:
    def adjust_material_quote(
        self,
        *,
        item: ItemDefinition,
        max_float: float,
        requested_exterior: Exterior,
        selected_quote: SelectedMarketQuote,
    ) -> SelectedMarketQuote:
        return selected_quote

    def adjust_outcome_quote(
        self,
        *,
        item: ItemDefinition,
        output_float: float,
        exterior: Exterior,
        selected_quote: SelectedMarketQuote,
    ) -> SelectedMarketQuote:
        return selected_quote


class MultiMarketPriceManager:
    def __init__(
        self,
        clients: Mapping[str, Any] | Sequence[Any],
        *,
        max_workers: int = 8,
    ) -> None:
        if isinstance(clients, Mapping):
            self._clients = tuple(clients.items())
        else:
            self._clients = tuple(
                (
                    getattr(client, "market_name", client.__class__.__name__),
                    client,
                )
                for client in clients
            )
        if not self._clients:
            raise ValueError("At least one market client is required")
        self.max_workers = max_workers
        self.requires_serial_execution = any(
            getattr(client, "thread_affine", False)
            for _, client in self._clients
        )
        self._cache: dict[tuple[str, str], tuple[SelectedMarketQuote, ...]] = {}
        self._lock = threading.Lock()

    def get_best_quote(
        self,
        item_name: str,
        exterior: Exterior | str,
        *,
        prefer: str = "lowest",
    ) -> SelectedMarketQuote:
        normalized_exterior = normalize_exterior_label(exterior)
        cache_key = (item_name, normalized_exterior)
        market_quotes = self._fetch_market_quotes(cache_key)
        if prefer == "lowest":
            return min(market_quotes, key=lambda entry: entry.quote.lowest_price)
        if prefer == "highest":
            return max(market_quotes, key=lambda entry: entry.quote.lowest_price)
        raise ValueError("prefer must be either 'lowest' or 'highest'")

    def prefetch(self, requests_to_prefetch: Iterable[tuple[str, Exterior | str]]) -> None:
        unique_requests = {
            (item_name, normalize_exterior_label(exterior))
            for item_name, exterior in requests_to_prefetch
        }
        if not unique_requests:
            return

        uncached_requests = []
        with self._lock:
            for request in sorted(unique_requests):
                if request not in self._cache:
                    uncached_requests.append(request)

        if not uncached_requests:
            return

        requests_by_item: dict[str, list[str]] = {}
        for item_name, exterior in uncached_requests:
            requests_by_item.setdefault(item_name, []).append(exterior)

        batched_quotes: dict[tuple[str, str], list[SelectedMarketQuote]] = {
            request: [] for request in uncached_requests
        }
        if self.requires_serial_execution:
            results_iterable = (
                self._fetch_client_quotes_for_item(
                    market_name,
                    client,
                    item_name,
                    tuple(exteriors),
                )
                for market_name, client in self._clients
                for item_name, exteriors in requests_by_item.items()
            )
            for market_name, item_name, resolved_quotes in results_iterable:
                for exterior, quote in resolved_quotes.items():
                    cache_key = (item_name, exterior)
                    if cache_key in batched_quotes:
                        batched_quotes[cache_key].append(
                            SelectedMarketQuote(market_name=market_name, quote=quote)
                        )
        else:
            worker_count = min(self.max_workers, max(len(requests_by_item) * len(self._clients), 1))
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(
                        self._fetch_client_quotes_for_item,
                        market_name,
                        client,
                        item_name,
                        tuple(exteriors),
                    )
                    for market_name, client in self._clients
                    for item_name, exteriors in requests_by_item.items()
                ]
                for future in as_completed(futures):
                    market_name, item_name, resolved_quotes = future.result()
                    for exterior, quote in resolved_quotes.items():
                        cache_key = (item_name, exterior)
                        if cache_key in batched_quotes:
                            batched_quotes[cache_key].append(
                                SelectedMarketQuote(market_name=market_name, quote=quote)
                            )

        with self._lock:
            for cache_key, quotes in batched_quotes.items():
                if quotes:
                    self._cache[cache_key] = tuple(quotes)

    def _fetch_market_quotes(
        self,
        cache_key: tuple[str, str],
    ) -> tuple[SelectedMarketQuote, ...]:
        with self._lock:
            cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        item_name, normalized_exterior = cache_key
        market_quotes: list[SelectedMarketQuote] = []
        errors: list[str] = []
        for market_name, client in self._clients:
            try:
                quote = client.get_item_price(item_name, normalized_exterior)
            except Exception as error:
                errors.append(f"{market_name}: {error}")
                continue
            market_quotes.append(SelectedMarketQuote(market_name=market_name, quote=quote))

        if not market_quotes:
            message = "; ".join(errors) if errors else "no quotes returned"
            raise PriceLookupError(
                f"Could not resolve market price for {item_name} ({normalized_exterior}): {message}"
            )

        resolved_quotes = tuple(market_quotes)
        with self._lock:
            self._cache[cache_key] = resolved_quotes
        return resolved_quotes

    def _fetch_client_quotes_for_item(
        self,
        market_name: str,
        client: Any,
        item_name: str,
        exteriors: Sequence[str],
    ) -> tuple[str, str, dict[str, PriceQuote]]:
        normalized_exteriors = tuple(normalize_exterior_label(exterior) for exterior in exteriors)
        quotes: dict[str, PriceQuote] = {}
        batch_getter = getattr(client, "get_item_prices", None)
        if callable(batch_getter):
            try:
                batch_quotes = batch_getter(item_name, normalized_exteriors)
            except Exception:
                batch_quotes = {}
            else:
                quotes.update(
                    {
                        normalize_exterior_label(exterior): quote
                        for exterior, quote in dict(batch_quotes).items()
                    }
                )

        for exterior in normalized_exteriors:
            if exterior in quotes:
                continue
            try:
                quotes[exterior] = client.get_item_price(item_name, exterior)
            except Exception:
                continue
        return market_name, item_name, quotes


class TradeUpScanner:
    def __init__(
        self,
        catalog: ItemCatalog,
        price_manager: MultiMarketPriceManager,
        *,
        formula_generator: TradeUpFormulaGenerator | None = None,
        float_calculator: FloatCalculator | None = None,
        price_adjustment_strategy: IdentityPriceAdjustmentStrategy | None = None,
        fee_rate: float = DEFAULT_FEE_RATE,
        max_workers: int = 8,
        conservative_float_mode: bool = False,
    ) -> None:
        self.catalog = catalog
        self.price_manager = price_manager
        self.float_calculator = float_calculator or FloatCalculator()
        self.formula_generator = formula_generator or TradeUpFormulaGenerator(
            catalog=catalog,
            float_calculator=self.float_calculator,
        )
        self.price_adjustment_strategy = (
            price_adjustment_strategy or IdentityPriceAdjustmentStrategy()
        )
        self.fee_rate = fee_rate
        self.max_workers = max_workers
        self.conservative_float_mode = conservative_float_mode

    def scan_targets(
        self,
        targets: Iterable[WatchlistTarget | Mapping[str, Any] | tuple[str, str] | str],
        *,
        roi_threshold: float = 1.05,
        formula_limit_per_target: int = 25,
    ) -> tuple[TradeUpScanResult, ...]:
        resolved_targets = [self._normalize_target(target) for target in targets]
        all_results: list[TradeUpScanResult] = []
        for target in resolved_targets:
            target_results = self.scan_target(
                target,
                roi_threshold=roi_threshold,
                formula_limit=formula_limit_per_target,
            )
            all_results.extend(target_results)
        all_results.sort(key=lambda result: (-result.roi, -result.expected_profit, result.total_cost))
        return tuple(all_results)

    def scan_target(
        self,
        target: WatchlistTarget,
        *,
        roi_threshold: float = 1.05,
        formula_limit: int = 25,
    ) -> tuple[TradeUpScanResult, ...]:
        formula_options = dict(target.formula_options)
        formula_options.setdefault("max_formulas", formula_limit)
        formulas = self.formula_generator.generate_trade_up_formulas(
            target_item=target.item_name,
            target_exterior=target.exterior,
            **formula_options,
        )
        self.price_manager.prefetch(self._collect_prefetch_requests(formulas))

        results: list[TradeUpScanResult] = []
        should_run_serial = (
            self.max_workers <= 1
            or len(formulas) <= 1
            or getattr(self.price_manager, "requires_serial_execution", False)
        )
        if should_run_serial:
            for formula in formulas:
                try:
                    result = self._evaluate_formula(formula)
                except (FormulaGenerationError, PriceLookupError, ValueError):
                    continue
                if result is not None and result.roi >= roi_threshold:
                    results.append(result)
        else:
            with ThreadPoolExecutor(max_workers=min(self.max_workers, max(len(formulas), 1))) as executor:
                futures = [executor.submit(self._evaluate_formula, formula) for formula in formulas]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except (FormulaGenerationError, PriceLookupError, ValueError):
                        continue
                    if result is not None and result.roi >= roi_threshold:
                        results.append(result)

        results.sort(key=lambda result: (-result.roi, -result.expected_profit, result.total_cost))
        return tuple(results)

    def find_optimal_materials(
        self,
        target_weapon: str | ItemDefinition,
        target_exterior: Exterior | str,
        *,
        roi_threshold: float = 0.0,
        result_limit: int | None = None,
        conservative_float_mode: bool | None = None,
        **formula_options: Any,
    ) -> tuple[TradeUpScanResult, ...]:
        resolved_conservative_float_mode = (
            self.conservative_float_mode
            if conservative_float_mode is None
            else conservative_float_mode
        )
        return find_optimal_materials(
            target_weapon=target_weapon,
            target_exterior=target_exterior,
            db_items=self.catalog,
            db_prices=self.price_manager,
            roi_threshold=roi_threshold,
            result_limit=result_limit,
            formula_generator=self.formula_generator,
            float_calculator=self.float_calculator,
            price_adjustment_strategy=self.price_adjustment_strategy,
            fee_rate=self.fee_rate,
            conservative_float_mode=resolved_conservative_float_mode,
            **formula_options,
        )

    def _collect_prefetch_requests(
        self,
        formulas: Iterable[TradeUpFormula],
    ) -> set[tuple[str, Exterior]]:
        requests_to_prefetch: set[tuple[str, Exterior]] = set()
        for formula in formulas:
            planned_metric = self._planned_metric_for_pricing(formula)
            for component in formula.collection_components:
                for input_item in component.input_items:
                    for exterior in _iter_candidate_exteriors(input_item):
                        requests_to_prefetch.add((input_item.name, exterior))
            for outcome in formula.outcome_probabilities:
                output_float = self.float_calculator.output_float_from_metric(
                    planned_metric,
                    outcome.item,
                )
                requests_to_prefetch.add(
                    (outcome.item.name, self._exterior_from_output_float(output_float))
                )
        return requests_to_prefetch

    def _evaluate_formula(self, formula: TradeUpFormula) -> TradeUpScanResult | None:
        optimized_materials = _select_optimal_materials_for_formula(
            formula,
            db_prices=self.price_manager,
            price_adjustment_strategy=self.price_adjustment_strategy,
            conservative_float_mode=self.conservative_float_mode,
        )
        if optimized_materials is None:
            return None
        ev_result = calculate_formula_ev(
            optimized_materials.material_inputs,
            formula.outcome_probabilities,
            self.price_manager,
            fee_rate=self.fee_rate,
            outcome_quote_adjuster=self.price_adjustment_strategy.adjust_outcome_quote,
        )
        return TradeUpScanResult(
            target_item=formula.target_item,
            target_exterior=formula.target_exterior,
            formula=formula,
            planned_average_metric=optimized_materials.average_adjusted,
            total_cost=ev_result.total_cost,
            expected_revenue=ev_result.expected_revenue,
            expected_profit=ev_result.expected_profit,
            roi=ev_result.roi,
            fee_rate=self.fee_rate,
            material_pricings=optimized_materials.material_pricings,
            outcome_pricings=ev_result.outcome_pricings,
        )

    def _planned_metric_for_pricing(self, formula: TradeUpFormula) -> float:
        metric_min = formula.exterior_requirement.required_average_metric_min
        metric_max = formula.exterior_requirement.required_average_metric_max
        if metric_max <= metric_min:
            return metric_max
        adjusted_metric = math.nextafter(metric_max, -math.inf)
        return max(metric_min, adjusted_metric)

    def _resolve_max_input_float(
        self,
        item: ItemDefinition,
        average_metric_limit: float,
    ) -> float | None:
        if self.float_calculator.formula == "legacy":
            max_float = min(item.max_float, average_metric_limit)
        else:
            max_float = item.min_float + item.float_range * average_metric_limit
            max_float = min(item.max_float, max_float)
        if max_float < item.min_float:
            return None
        return max_float

    def _exterior_from_float_cap(self, max_float: float) -> Exterior:
        if max_float <= 0:
            return Exterior.FACTORY_NEW
        if max_float >= 1.0:
            return Exterior.BATTLE_SCARRED
        return Exterior.from_float(math.nextafter(max_float, -math.inf))

    def _exterior_from_output_float(self, output_float: float) -> Exterior:
        if output_float <= 0:
            return Exterior.FACTORY_NEW
        if output_float >= 1.0:
            return Exterior.BATTLE_SCARRED
        return Exterior.from_float(math.nextafter(output_float, -math.inf))

    def _normalize_target(
        self,
        target: WatchlistTarget | Mapping[str, Any] | tuple[str, str] | str,
    ) -> WatchlistTarget:
        if isinstance(target, WatchlistTarget):
            return target
        if isinstance(target, str):
            raise ValueError(
                "String targets are ambiguous; pass WatchlistTarget or a mapping with exterior"
            )
        if isinstance(target, tuple):
            item_name, exterior = target
            return WatchlistTarget(item_name=item_name, exterior=Exterior.from_label(exterior))
        item_name = target["item_name"]
        exterior = target.get("exterior") or target.get("target_exterior")
        if exterior is None:
            raise ValueError("Target mapping must include 'exterior' or 'target_exterior'")
        raw_formula_options = target.get("formula_options", {})
        formula_options = {
            key: _coerce_watchlist_value(value)
            for key, value in dict(raw_formula_options).items()
        }
        for key, value in target.items():
            if key not in {"item_name", "exterior", "target_exterior", "formula_options"}:
                formula_options.setdefault(key, _coerce_watchlist_value(value))
        return WatchlistTarget(
            item_name=item_name,
            exterior=Exterior.from_label(str(exterior)),
            formula_options=formula_options,
        )


def calculate_formula_ev(
    materials_list: Sequence[Any],
    possible_outcomes: Sequence[Any],
    db_prices: Any,
    *,
    fee_rate: float = DEFAULT_FEE_RATE,
    outcome_quote_adjuster: Any | None = None,
) -> FormulaEVResult:
    if not 0.0 <= fee_rate < 1.0:
        raise ValueError("fee_rate must be within [0, 1)")

    resolved_materials = tuple(_coerce_formula_material_input(entry) for entry in materials_list)
    total_count = sum(material.count for material in resolved_materials)
    if total_count != 10:
        raise ValueError(f"CS2 trade-up requires exactly 10 materials, got {total_count}")

    adjusted_total = 0.0
    total_cost = 0.0
    for material in resolved_materials:
        adjusted_total += material.item.wear_position(material.actual_float) * material.count
        total_cost += material.total_price if material.total_price is not None else material.unit_price * material.count
    average_adjusted = adjusted_total / total_count

    outcome_pricings: list[OutcomePricing] = []
    expected_revenue = 0.0
    total_probability = 0.0
    for raw_outcome in possible_outcomes:
        outcome_item, probability = _coerce_formula_outcome(raw_outcome)
        total_probability += probability
        output_float = outcome_item.min_float + average_adjusted * outcome_item.float_range
        output_float = max(outcome_item.min_float, min(outcome_item.max_float, output_float))
        exterior = _exterior_from_actual_float(output_float)
        selected_quote = _resolve_selected_quote(
            db_prices,
            outcome_item.name,
            exterior,
            prefer="highest",
        )
        if outcome_quote_adjuster is not None:
            selected_quote = outcome_quote_adjuster(
                item=outcome_item,
                output_float=output_float,
                exterior=exterior,
                selected_quote=selected_quote,
            )
        market_price = selected_quote.quote.lowest_price
        net_sale_price = market_price * (1.0 - fee_rate)
        contribution = probability * net_sale_price
        expected_revenue += contribution
        outcome_pricings.append(
            OutcomePricing(
                item=outcome_item,
                probability=probability,
                output_float=output_float,
                exterior=exterior,
                market_name=selected_quote.market_name,
                market_price=market_price,
                net_sale_price=net_sale_price,
                expected_revenue_contribution=contribution,
            )
        )

    if not math.isclose(total_probability, 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(f"Outcome probability must sum to 1.0, got {total_probability}")

    expected_profit = expected_revenue - total_cost
    roi = math.inf if total_cost == 0 else expected_revenue / total_cost
    outcome_pricings.sort(
        key=lambda pricing: (-pricing.expected_revenue_contribution, pricing.item.name)
    )
    return FormulaEVResult(
        average_adjusted=average_adjusted,
        total_cost=total_cost,
        expected_revenue=expected_revenue,
        expected_profit=expected_profit,
        roi=roi,
        outcome_pricings=tuple(outcome_pricings),
    )


def find_optimal_materials(
    target_weapon: str | ItemDefinition,
    target_exterior: Exterior | str,
    db_items: ItemCatalog | str | Path,
    db_prices: Any,
    *,
    roi_threshold: float = 0.0,
    result_limit: int | None = None,
    formula_generator: TradeUpFormulaGenerator | None = None,
    float_calculator: FloatCalculator | None = None,
    price_adjustment_strategy: IdentityPriceAdjustmentStrategy | None = None,
    fee_rate: float = DEFAULT_FEE_RATE,
    conservative_float_mode: bool = False,
    **formula_options: Any,
) -> tuple[TradeUpScanResult, ...]:
    catalog = db_items if isinstance(db_items, ItemCatalog) else ItemCatalog.from_path(db_items)
    resolved_float_calculator = (
        float_calculator
        or (formula_generator.float_calculator if formula_generator is not None else None)
        or FloatCalculator(formula="normalized")
    )
    resolved_formula_generator = formula_generator or TradeUpFormulaGenerator(
        catalog=catalog,
        float_calculator=resolved_float_calculator,
    )
    resolved_adjustment_strategy = price_adjustment_strategy or IdentityPriceAdjustmentStrategy()

    formulas = resolved_formula_generator.generate_trade_up_formulas(
        target_item=target_weapon,
        target_exterior=target_exterior,
        **formula_options,
    )

    if hasattr(db_prices, "prefetch"):
        try:
            db_prices.prefetch(_collect_optimizer_prefetch_requests(formulas))
        except Exception:
            pass

    results: list[TradeUpScanResult] = []
    for formula in formulas:
        try:
            result = _evaluate_formula_with_price_source(
                formula,
                db_prices=db_prices,
                fee_rate=fee_rate,
                price_adjustment_strategy=resolved_adjustment_strategy,
                conservative_float_mode=conservative_float_mode,
            )
        except (PriceLookupError, ValueError):
            continue
        if result is not None and result.roi >= roi_threshold:
            results.append(result)

    results.sort(key=lambda result: (-result.roi, -result.expected_profit, result.total_cost))
    if result_limit is not None:
        return tuple(results[: max(0, result_limit)])
    return tuple(results)


def _evaluate_formula_with_price_source(
    formula: TradeUpFormula,
    *,
    db_prices: Any,
    fee_rate: float,
    price_adjustment_strategy: IdentityPriceAdjustmentStrategy,
    conservative_float_mode: bool,
) -> TradeUpScanResult | None:
    optimized_materials = _select_optimal_materials_for_formula(
        formula,
        db_prices=db_prices,
        price_adjustment_strategy=price_adjustment_strategy,
        conservative_float_mode=conservative_float_mode,
    )
    if optimized_materials is None:
        return None

    ev_result = calculate_formula_ev(
        optimized_materials.material_inputs,
        formula.outcome_probabilities,
        db_prices,
        fee_rate=fee_rate,
        outcome_quote_adjuster=price_adjustment_strategy.adjust_outcome_quote,
    )
    return TradeUpScanResult(
        target_item=formula.target_item,
        target_exterior=formula.target_exterior,
        formula=formula,
        planned_average_metric=optimized_materials.average_adjusted,
        total_cost=ev_result.total_cost,
        expected_revenue=ev_result.expected_revenue,
        expected_profit=ev_result.expected_profit,
        roi=ev_result.roi,
        fee_rate=fee_rate,
        material_pricings=optimized_materials.material_pricings,
        outcome_pricings=ev_result.outcome_pricings,
    )


def _select_optimal_materials_for_formula(
    formula: TradeUpFormula,
    *,
    db_prices: Any,
    price_adjustment_strategy: IdentityPriceAdjustmentStrategy,
    conservative_float_mode: bool,
    scale: int = 10_000,
) -> _OptimizedFormulaMaterials | None:
    # required_average_metric_max is already computed from nextafter(exterior_upper, -inf),
    # so applying nextafter again would create a 1-unit rounding discrepancy with candidates
    # that are capped at this exact value.
    max_average_adjusted = max(0.0, min(1.0, formula.exterior_requirement.required_average_metric_max))
    max_total_units = _metric_to_units(
        max_average_adjusted * sum(component.count for component in formula.collection_components),
        scale=scale,
        round_up=False,
    )

    component_candidates: list[tuple[_MaterialCandidate, ...]] = []
    component_frontiers: list[dict[int, tuple[float, tuple[int, ...]]]] = []
    for component in formula.collection_components:
        candidates = _build_component_material_candidates(
            component,
            db_prices=db_prices,
            price_adjustment_strategy=price_adjustment_strategy,
            average_metric_cap=max_average_adjusted,
            conservative_float_mode=conservative_float_mode,
            scale=scale,
        )
        if not candidates:
            return None
        frontier = _build_component_frontier(
            count=component.count,
            candidates=candidates,
            max_total_units=max_total_units,
        )
        if not frontier:
            return None
        component_candidates.append(candidates)
        component_frontiers.append(frontier)

    combined_frontier = _combine_component_frontiers(
        component_frontiers,
        max_total_units=max_total_units,
    )
    if not combined_frontier:
        return None

    best_units, (best_cost, selection_plan) = min(
        combined_frontier.items(),
        key=lambda entry: (entry[1][0], entry[0]),
    )

    aggregated: dict[tuple[str, str, str, float, float, str, bool, bool], dict[str, Any]] = {}
    for candidates, selected_indices in zip(component_candidates, selection_plan):
        for option_index in selected_indices:
            candidate = candidates[option_index]
            key = (
                candidate.item.name,
                candidate.requested_exterior.value,
                candidate.market_name,
                round(candidate.actual_float, 8),
                round(candidate.unit_price, 8),
                candidate.float_source,
                candidate.float_verified,
                candidate.requires_float_verification,
            )
            if key not in aggregated:
                aggregated[key] = {"candidate": candidate, "count": 0}
            aggregated[key]["count"] += 1

    material_inputs: list[FormulaMaterialInput] = []
    material_pricings: list[MaterialPricing] = []
    adjusted_total = 0.0
    total_cost = 0.0
    total_adjusted_budget = max_average_adjusted * 10.0
    group_rows: list[dict[str, Any]] = []
    for entry in aggregated.values():
        candidate = entry["candidate"]
        count = int(entry["count"])
        total_price = candidate.unit_price * count
        adjusted_total += candidate.adjusted_float * count
        total_cost += total_price
        group_rows.append(
            {
                "candidate": candidate,
                "count": count,
                "total_price": total_price,
                "group_adjusted_total": candidate.adjusted_float * count,
            }
        )

    for group in group_rows:
        candidate = group["candidate"]
        count = int(group["count"])
        total_price = float(group["total_price"])
        group_adjusted_total = float(group["group_adjusted_total"])
        remaining_budget = total_adjusted_budget - (adjusted_total - group_adjusted_total)
        allowed_adjusted_per_item = max(0.0, min(1.0, remaining_budget / count))
        allowed_max_float = _adjusted_metric_to_float(
            candidate.item,
            allowed_adjusted_per_item,
        )
        allowed_max_float = min(
            _max_float_for_exterior(candidate.item, candidate.requested_exterior) or allowed_max_float,
            allowed_max_float,
        )
        allowed_min_float = (
            candidate.actual_float
            if candidate.float_verified
            else (_min_float_for_exterior(candidate.item, candidate.requested_exterior) or candidate.item.min_float)
        )
        material_inputs.append(
            FormulaMaterialInput(
                item=candidate.item,
                actual_float=candidate.actual_float,
                count=count,
                unit_price=candidate.unit_price,
                total_price=total_price,
                requested_exterior=candidate.requested_exterior,
                market_name=candidate.market_name,
                float_source=candidate.float_source,
                float_verified=candidate.float_verified,
                requires_float_verification=candidate.requires_float_verification,
            )
        )
        material_pricings.append(
            MaterialPricing(
                item=candidate.item,
                count=count,
                min_float=allowed_min_float,
                max_float=allowed_max_float,
                estimated_float=candidate.actual_float,
                adjusted_float=candidate.adjusted_float,
                requested_exterior=candidate.requested_exterior,
                market_name=candidate.market_name,
                unit_price=candidate.unit_price,
                total_price=total_price,
                float_source=candidate.float_source,
                float_source_label=_float_source_label(candidate.float_source),
                float_verified=candidate.float_verified,
                requires_float_verification=candidate.requires_float_verification,
            )
        )

    material_inputs.sort(
        key=lambda entry: (
            entry.item.collection,
            entry.item.name,
            entry.requested_exterior.value if entry.requested_exterior else "",
            entry.unit_price,
        )
    )
    material_pricings.sort(
        key=lambda pricing: (
            pricing.item.collection,
            pricing.item.name,
            pricing.requested_exterior.value,
            pricing.unit_price,
        )
    )
    average_adjusted = adjusted_total / 10.0
    if best_units > max_total_units:
        return None
    return _OptimizedFormulaMaterials(
        average_adjusted=average_adjusted,
        total_cost=total_cost,
        material_inputs=tuple(material_inputs),
        material_pricings=tuple(material_pricings),
    )


def _build_component_material_candidates(
    component: FormulaCollectionComponent,
    *,
    db_prices: Any,
    price_adjustment_strategy: IdentityPriceAdjustmentStrategy,
    average_metric_cap: float,
    conservative_float_mode: bool,
    scale: int,
) -> tuple[_MaterialCandidate, ...]:
    candidates: list[_MaterialCandidate] = []
    for input_item in sorted(component.input_items, key=lambda item: item.name):
        for exterior in _iter_candidate_exteriors(input_item):
            try:
                selected_quote = _resolve_selected_quote(
                    db_prices,
                    input_item.name,
                    exterior,
                    prefer="lowest",
                )
            except PriceLookupError:
                continue
            selected_quote = price_adjustment_strategy.adjust_material_quote(
                item=input_item,
                max_float=_candidate_float_for_exterior(
                    input_item,
                    exterior,
                    average_metric_cap=average_metric_cap,
                ) or (_max_float_for_exterior(input_item, exterior) or input_item.max_float),
                requested_exterior=exterior,
                selected_quote=selected_quote,
            )
            float_selection = _resolve_material_float_selection(
                input_item,
                exterior,
                average_metric_cap=average_metric_cap,
                selected_quote=selected_quote,
                conservative_float_mode=conservative_float_mode,
            )
            if float_selection is None:
                continue
            actual_float, float_source, float_verified, requires_float_verification = float_selection
            unit_price = selected_quote.quote.lowest_price
            adjusted_float = input_item.wear_position(actual_float)
            candidates.append(
                _MaterialCandidate(
                    item=input_item,
                    requested_exterior=exterior,
                    actual_float=actual_float,
                    adjusted_float=adjusted_float,
                    market_name=selected_quote.market_name,
                    unit_price=unit_price,
                    adjusted_units=_metric_to_units(adjusted_float, scale=scale, round_up=True),
                    float_source=float_source,
                    float_verified=float_verified,
                    requires_float_verification=requires_float_verification,
                )
            )
    candidates.sort(
        key=lambda candidate: (
            candidate.unit_price,
            candidate.adjusted_float,
            candidate.item.name,
            candidate.requested_exterior.value,
        )
    )
    return tuple(candidates)


def _build_component_frontier(
    *,
    count: int,
    candidates: Sequence[_MaterialCandidate],
    max_total_units: int,
) -> dict[int, tuple[float, tuple[int, ...]]]:
    frontier: dict[int, tuple[float, tuple[int, ...]]] = {0: (0.0, ())}
    for _ in range(count):
        next_frontier: dict[int, tuple[float, tuple[int, ...]]] = {}
        for total_units, (total_cost, selected_indices) in frontier.items():
            for option_index, candidate in enumerate(candidates):
                new_units = total_units + candidate.adjusted_units
                if new_units > max_total_units:
                    continue
                new_cost = total_cost + candidate.unit_price
                new_selection = selected_indices + (option_index,)
                current = next_frontier.get(new_units)
                if current is None or new_cost < current[0] or (
                    math.isclose(new_cost, current[0], rel_tol=0.0, abs_tol=1e-12)
                    and new_selection < current[1]
                ):
                    next_frontier[new_units] = (new_cost, new_selection)
        frontier = _pareto_prune_frontier(next_frontier)
        if not frontier:
            break
    return frontier


def _combine_component_frontiers(
    frontiers: Sequence[Mapping[int, tuple[float, tuple[int, ...]]]],
    *,
    max_total_units: int,
) -> dict[int, tuple[float, tuple[tuple[int, ...], ...]]]:
    combined: dict[int, tuple[float, tuple[tuple[int, ...], ...]]] = {0: (0.0, ())}
    for frontier in frontiers:
        next_combined: dict[int, tuple[float, tuple[tuple[int, ...], ...]]] = {}
        for total_units, (total_cost, total_plan) in combined.items():
            for component_units, (component_cost, component_plan) in frontier.items():
                new_units = total_units + component_units
                if new_units > max_total_units:
                    continue
                new_cost = total_cost + component_cost
                new_plan = total_plan + (component_plan,)
                current = next_combined.get(new_units)
                if current is None or new_cost < current[0] or (
                    math.isclose(new_cost, current[0], rel_tol=0.0, abs_tol=1e-12)
                    and new_plan < current[1]
                ):
                    next_combined[new_units] = (new_cost, new_plan)
        combined = _pareto_prune_frontier(next_combined)
        if not combined:
            break
    return combined


def _pareto_prune_frontier(frontier: Mapping[int, tuple[float, Any]]) -> dict[int, tuple[float, Any]]:
    pruned: dict[int, tuple[float, Any]] = {}
    best_cost = math.inf
    tolerance = 1e-12
    for total_units, payload in sorted(frontier.items(), key=lambda entry: entry[0]):
        total_cost = payload[0]
        if total_cost + tolerance < best_cost:
            best_cost = total_cost
            pruned[total_units] = payload
    return pruned


def _collect_optimizer_prefetch_requests(
    formulas: Iterable[TradeUpFormula],
) -> set[tuple[str, Exterior]]:
    requests_to_prefetch: set[tuple[str, Exterior]] = set()
    for formula in formulas:
        for component in formula.collection_components:
            for input_item in component.input_items:
                for exterior in _iter_candidate_exteriors(input_item):
                    requests_to_prefetch.add((input_item.name, exterior))
        for outcome in formula.outcome_probabilities:
            requests_to_prefetch.add((outcome.item.name, formula.target_exterior))
    return requests_to_prefetch


def _coerce_formula_material_input(entry: Any) -> FormulaMaterialInput:
    if isinstance(entry, FormulaMaterialInput):
        return entry
    if isinstance(entry, MaterialPricing):
        return FormulaMaterialInput(
            item=entry.item,
            actual_float=entry.estimated_float,
            count=entry.count,
            unit_price=entry.unit_price,
            total_price=entry.total_price,
            requested_exterior=entry.requested_exterior,
            market_name=entry.market_name,
            float_source=entry.float_source,
            float_verified=entry.float_verified,
            requires_float_verification=entry.requires_float_verification,
        )
    if not isinstance(entry, Mapping):
        raise TypeError(f"Unsupported material entry: {type(entry).__name__}")
    item = _coerce_item_definition(entry.get("item") or entry.get("definition") or entry)
    actual_float = entry.get("actual_float", entry.get("float_value", entry.get("max_float")))
    if actual_float is None:
        raise ValueError("Material entry must include actual_float, float_value, or max_float")
    count = int(entry.get("count", 1))
    unit_price = float(entry.get("unit_price", entry.get("price", entry.get("cost", 0.0))))
    total_price = entry.get("total_price", entry.get("total_cost"))
    requested_exterior = entry.get("requested_exterior") or entry.get("exterior")
    return FormulaMaterialInput(
        item=item,
        actual_float=float(actual_float),
        count=count,
        unit_price=unit_price,
        total_price=(float(total_price) if total_price is not None else None),
        requested_exterior=_coerce_exterior(requested_exterior),
        market_name=str(entry.get("market_name", "")),
        float_source=str(entry.get("float_source", FLOAT_SOURCE_REQUIREMENT_CAP)),
        float_verified=bool(entry.get("float_verified", False)),
        requires_float_verification=bool(entry.get("requires_float_verification", False)),
    )


def _coerce_formula_outcome(entry: Any) -> tuple[ItemDefinition, float]:
    if hasattr(entry, "item") and hasattr(entry, "probability"):
        return _coerce_item_definition(entry.item), float(entry.probability)
    if not isinstance(entry, Mapping):
        raise TypeError(f"Unsupported outcome entry: {type(entry).__name__}")
    item = _coerce_item_definition(entry.get("item") or entry)
    if "probability" not in entry:
        raise ValueError("Outcome entry must include probability")
    return item, float(entry["probability"])


def _coerce_item_definition(value: Any) -> ItemDefinition:
    if isinstance(value, ItemDefinition):
        return value
    if isinstance(value, Mapping):
        return ItemDefinition.from_dict(dict(value))
    raise TypeError(f"Unsupported item definition payload: {type(value).__name__}")


def _resolve_selected_quote(
    db_prices: Any,
    item_name: str,
    exterior: Exterior | str,
    *,
    prefer: str,
) -> SelectedMarketQuote:
    if hasattr(db_prices, "get_best_quote"):
        return db_prices.get_best_quote(item_name, exterior, prefer=prefer)
    if hasattr(db_prices, "get_item_price"):
        quote = _coerce_price_quote(db_prices.get_item_price(item_name, exterior))
        market_name = getattr(db_prices, "market_name", db_prices.__class__.__name__)
        return SelectedMarketQuote(str(market_name), quote)
    if hasattr(db_prices, "get_latest_snapshot"):
        snapshot = db_prices.get_latest_snapshot(
            item_name=item_name,
            exterior=exterior,
            max_age_seconds=None,
            prefer_cleaned=True,
            require_valid=True,
            require_tradeup_compatible_normal=True,
        )
        if snapshot is None:
            raise PriceLookupError(f"Missing price for {item_name} ({_coerce_exterior(exterior).value})")
        market_name = (
            snapshot.selected_platform_name
            or snapshot.selected_platform
            or snapshot.source
            or db_prices.__class__.__name__
        )
        return SelectedMarketQuote(str(market_name), snapshot.quote)
    if isinstance(db_prices, Mapping):
        quote = _resolve_quote_from_mapping(db_prices, item_name, exterior)
        return SelectedMarketQuote("db_prices", quote)
    raise TypeError(f"Unsupported price source: {type(db_prices).__name__}")


def _resolve_quote_from_mapping(
    db_prices: Mapping[Any, Any],
    item_name: str,
    exterior: Exterior | str,
) -> PriceQuote:
    resolved_exterior = _coerce_exterior(exterior)
    lookup_keys = (
        (item_name, resolved_exterior),
        (item_name, resolved_exterior.value),
        item_name,
    )
    for key in lookup_keys:
        if key not in db_prices:
            continue
        value = db_prices[key]
        if key == item_name and isinstance(value, Mapping):
            nested_keys = (
                resolved_exterior,
                resolved_exterior.value,
                normalize_exterior_label(resolved_exterior.value),
            )
            for nested_key in nested_keys:
                if nested_key in value:
                    return _coerce_price_quote(value[nested_key])
        return _coerce_price_quote(value)
    raise PriceLookupError(f"Missing price for {item_name} ({resolved_exterior.value})")


def _coerce_price_quote(value: Any) -> PriceQuote:
    if isinstance(value, PriceQuote):
        return value
    if isinstance(value, SelectedMarketQuote):
        return value.quote
    if isinstance(value, Mapping):
        if "quote" in value:
            return _coerce_price_quote(value["quote"])
        if "safe_price" in value and value["safe_price"] not in (None, ""):
            lowest_price = float(value["safe_price"])
        else:
            lowest_price = float(value.get("lowest_price", value.get("price", 0.0)))
        recent_average = value.get("recent_average_price")
        return PriceQuote(
            lowest_price=lowest_price,
            recent_average_price=(
                None if recent_average in (None, "") else float(recent_average)
            ),
        )
    return PriceQuote(lowest_price=float(value))


def _iter_candidate_exteriors(item: ItemDefinition) -> tuple[Exterior, ...]:
    available = item.available_exteriors or tuple(
        exterior
        for exterior in Exterior.ordered()
        if exterior.overlaps_float_range(item.min_float, item.max_float)
    )
    return tuple(
        exterior
        for exterior in Exterior.ordered()
        if exterior in available and exterior.overlaps_float_range(item.min_float, item.max_float)
    )


def _max_float_for_exterior(item: ItemDefinition, exterior: Exterior) -> float | None:
    lower_bound, upper_bound = exterior.float_bounds
    effective_lower = max(item.min_float, lower_bound)
    effective_upper = item.max_float
    if exterior is not Exterior.BATTLE_SCARRED:
        effective_upper = min(item.max_float, math.nextafter(upper_bound, lower_bound))
    if effective_lower > effective_upper:
        return None
    return effective_upper


def _min_float_for_exterior(item: ItemDefinition, exterior: Exterior) -> float | None:
    lower_bound, _ = exterior.float_bounds
    minimum = max(item.min_float, lower_bound)
    maximum = _max_float_for_exterior(item, exterior)
    if maximum is None or minimum > maximum:
        return None
    return minimum


def _candidate_float_for_exterior(
    item: ItemDefinition,
    exterior: Exterior,
    *,
    average_metric_cap: float,
) -> float | None:
    exterior_upper = _max_float_for_exterior(item, exterior)
    if exterior_upper is None:
        return None
    lower_bound, _ = exterior.float_bounds
    effective_lower = max(item.min_float, lower_bound)
    capped_float = item.min_float + item.float_range * max(0.0, min(1.0, average_metric_cap))
    actual_float = min(exterior_upper, capped_float)
    if actual_float < effective_lower:
        return None
    return actual_float


def _midpoint_float_for_exterior(item: ItemDefinition, exterior: Exterior) -> float | None:
    upper = _max_float_for_exterior(item, exterior)
    if upper is None:
        return None
    lower_bound, _ = exterior.float_bounds
    lower = max(item.min_float, lower_bound)
    if lower > upper:
        return None
    return (lower + upper) / 2.0


def _float_cap_usable_fraction(
    item: ItemDefinition, exterior: Exterior, average_metric_cap: float
) -> float:
    """Return the fraction of the exterior float range that satisfies the metric cap (0–1).

    For example, if the exterior runs from 0.00 to 0.07 (FN) and the cap allows floats
    up to 0.044, only 63% of listings in that exterior are usable — the cheapest
    listings near 0.07 are excluded, so lowest_price understates the true cost.
    """
    exterior_upper = _max_float_for_exterior(item, exterior)
    if exterior_upper is None:
        return 1.0
    lower_bound, _ = exterior.float_bounds
    exterior_lower = max(item.min_float, lower_bound)
    exterior_range = exterior_upper - exterior_lower
    if exterior_range <= 0:
        return 1.0
    cap_float = item.min_float + item.float_range * average_metric_cap
    usable_range = min(cap_float, exterior_upper) - exterior_lower
    return max(0.0, usable_range) / exterior_range


def _resolve_material_float_selection(
    item: ItemDefinition,
    exterior: Exterior,
    *,
    average_metric_cap: float,
    selected_quote: SelectedMarketQuote,
    conservative_float_mode: bool,
) -> tuple[float, str, bool, bool] | None:
    verified_float = _extract_verified_float(selected_quote)
    if verified_float is not None:
        if not item.min_float <= verified_float <= item.max_float:
            return None
        exterior_upper = _max_float_for_exterior(item, exterior)
        if exterior_upper is None:
            return None
        lower_bound, _ = exterior.float_bounds
        if not max(item.min_float, lower_bound) <= verified_float <= exterior_upper:
            return None
        return verified_float, FLOAT_SOURCE_VERIFIED, True, False

    if conservative_float_mode:
        midpoint_float = _midpoint_float_for_exterior(item, exterior)
        if midpoint_float is None:
            return None
        midpoint_metric = item.wear_position(midpoint_float)
        if midpoint_metric <= average_metric_cap:
            # Only use the exterior midpoint price when the cap still covers a large
            # enough fraction of the exterior range.  A tight cap means the cheapest
            # listings (near exterior_max) would be excluded at purchase time, so
            # lowest_price silently underestimates the actual material cost.
            usable_fraction = _float_cap_usable_fraction(item, exterior, average_metric_cap)
            if usable_fraction >= _FLOAT_CAP_USABLE_FRACTION_THRESHOLD:
                return midpoint_float, FLOAT_SOURCE_EXTERIOR_MIDPOINT, False, False
        # Midpoint exceeds cap, or cap is tight (cuts off too many cheap listings).
        # Fall back to capped float and require float verification at purchase time so
        # the caller knows the displayed price may be lower than actual market cost.
        capped_float = _candidate_float_for_exterior(
            item,
            exterior,
            average_metric_cap=average_metric_cap,
        )
        if capped_float is None:
            return None
        return capped_float, FLOAT_SOURCE_REQUIREMENT_CAP, False, True

    capped_float = _candidate_float_for_exterior(
        item,
        exterior,
        average_metric_cap=average_metric_cap,
    )
    if capped_float is None:
        return None
    return capped_float, FLOAT_SOURCE_REQUIREMENT_CAP, False, True


def _extract_verified_float(selected_quote: SelectedMarketQuote) -> float | None:
    candidate_values = [
        getattr(selected_quote, "listing_float", None),
        getattr(selected_quote, "float_value", None),
        getattr(selected_quote.quote, "listing_float", None),
        getattr(selected_quote.quote, "float_value", None),
    ]
    for value in candidate_values:
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _adjusted_metric_to_float(item: ItemDefinition, adjusted_metric: float) -> float:
    metric = max(0.0, min(1.0, adjusted_metric))
    return item.min_float + item.float_range * metric


def _float_source_label(source: str) -> str:
    return FLOAT_SOURCE_LABELS.get(source, source)


def summarize_float_validation(materials: Sequence[MaterialPricing | Mapping[str, Any]]) -> dict[str, Any]:
    entries = []
    for material in materials:
        if isinstance(material, MaterialPricing):
            entries.append(
                {
                    "float_verified": material.float_verified,
                    "float_source": material.float_source,
                    "requires_float_verification": material.requires_float_verification,
                }
            )
        else:
            entries.append(
                {
                    "float_verified": bool(material.get("float_verified", False)),
                    "float_source": str(material.get("float_source", "")),
                    "requires_float_verification": bool(
                        material.get("requires_float_verification", False)
                    ),
                }
            )

    total = len(entries)
    verified_count = sum(1 for entry in entries if entry["float_verified"])
    conservative_count = sum(
        1 for entry in entries if entry["float_source"] == FLOAT_SOURCE_EXTERIOR_MIDPOINT
    )
    requires_verification_count = sum(
        1 for entry in entries if entry["requires_float_verification"]
    )

    if total > 0 and verified_count == total:
        status = FLOAT_STATUS_VERIFIED
    elif conservative_count > 0:
        status = FLOAT_STATUS_CONSERVATIVE
    else:
        status = FLOAT_STATUS_NEEDS_VERIFICATION

    return {
        "float_validation_status": status,
        "float_validation_status_zh": FLOAT_STATUS_LABELS.get(status, status),
        "verified_material_count": verified_count,
        "conservative_material_count": conservative_count,
        "requires_float_verification_count": requires_verification_count,
    }


def _coerce_exterior(value: Exterior | str | None) -> Exterior | None:
    if value is None:
        return None
    return value if isinstance(value, Exterior) else Exterior.from_label(str(value))


def _exterior_from_actual_float(float_value: float) -> Exterior:
    if float_value <= 0.0:
        return Exterior.FACTORY_NEW
    if float_value >= 1.0:
        return Exterior.BATTLE_SCARRED
    return Exterior.from_float(math.nextafter(float_value, -math.inf))


def _metric_to_units(metric: float, *, scale: int, round_up: bool) -> int:
    clamped = max(0.0, metric)
    scaled = clamped * scale
    if round_up:
        return int(math.ceil(scaled - 1e-12))
    return int(math.floor(scaled + 1e-12))


def _normalize_watchlist_target(
    target: WatchlistTarget | Mapping[str, Any] | tuple[str, str] | str,
) -> WatchlistTarget:
    return TradeUpScanner._normalize_target(TradeUpScanner, target)


def _coerce_watchlist_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped == "":
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def load_watchlist(path: str | Path) -> tuple[WatchlistTarget, ...]:
    file_path = Path(path)
    suffix = file_path.suffix.lower()
    if suffix == ".json":
        raw_data = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(raw_data, list):
            raise ValueError("JSON watchlist must be an array")
        targets = [_normalize_watchlist_target(entry) for entry in raw_data]
        return tuple(targets)

    if suffix == ".csv":
        with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            targets = [
                _normalize_watchlist_target(row)
                for row in reader
                if row.get("item_name")
            ]
        return tuple(targets)

    targets: list[WatchlistTarget] = []
    for line in file_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        item_name, exterior = [part.strip() for part in stripped.rsplit(",", 1)]
        targets.append(WatchlistTarget(item_name=item_name, exterior=Exterior.from_label(exterior)))
    return tuple(targets)


def export_scan_results_csv(
    results: Iterable[TradeUpScanResult],
    path: str | Path,
) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "target_item",
                "target_exterior",
                "float_validation_status",
                "roi",
                "roi_percent",
                "expected_profit",
                "expected_revenue",
                "total_cost",
                "target_probability",
                "planned_average_metric",
                "formula_signature",
                "materials",
                "outcomes",
            ],
        )
        writer.writeheader()
        for result in results:
            float_validation = summarize_float_validation(result.material_pricings)
            writer.writerow(
                {
                    "target_item": result.target_item.name,
                    "target_exterior": result.target_exterior.value,
                    "float_validation_status": float_validation["float_validation_status_zh"],
                    "roi": f"{result.roi:.6f}",
                    "roi_percent": f"{result.roi_percent:.2f}",
                    "expected_profit": f"{result.expected_profit:.2f}",
                    "expected_revenue": f"{result.expected_revenue:.2f}",
                    "total_cost": f"{result.total_cost:.2f}",
                    "target_probability": f"{result.target_probability:.4f}",
                    "planned_average_metric": f"{result.planned_average_metric:.6f}",
                    "formula_signature": result.formula_signature,
                    "materials": "; ".join(
                        (
                            f"{pricing.item.name} x{pricing.count} "
                            f"[{pricing.requested_exterior.value} {pricing.min_float:.5f}~{pricing.max_float:.5f}, "
                            f"est {pricing.estimated_float:.5f}, {pricing.float_source_label}] "
                            f"@ {pricing.unit_price:.2f} ({pricing.market_name})"
                        )
                        for pricing in result.material_pricings
                    ),
                    "outcomes": "; ".join(
                        (
                            f"{pricing.item.name} {pricing.exterior.value} "
                            f"p={pricing.probability:.4f} "
                            f"@ {pricing.market_price:.2f} ({pricing.market_name})"
                        )
                        for pricing in result.outcome_pricings
                    ),
                }
            )
    return output_path


def format_scan_results(results: Iterable[TradeUpScanResult]) -> str:
    lines: list[str] = []
    for result in results:
        float_validation = summarize_float_validation(result.material_pricings)
        lines.append(
            (
                f"{result.target_item.name} [{result.target_exterior.value}] | "
                f"{float_validation['float_validation_status_zh']} | "
                f"ROI {result.roi_percent:.2f}% | EV {result.expected_profit:.2f} | "
                f"Cost {result.total_cost:.2f} | Target P {result.target_probability:.2%} | "
                f"Formula {result.formula_signature}"
            )
        )
    return "\n".join(lines)
