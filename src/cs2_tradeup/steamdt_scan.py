from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .catalog import ItemCatalog
from .exceptions import FormulaGenerationError
from .market import split_item_variant_name
from .models import Exterior, ItemVariant, Rarity
from .scanner import (
    DEFAULT_FEE_RATE,
    MultiMarketPriceManager,
    TradeUpScanResult,
    TradeUpScanner,
    WatchlistTarget,
    export_scan_results_csv,
    load_watchlist,
)
from .steamdt_market import CachedSteamDTMarketAPI, SteamDTMarketAPI, SteamDTPriceSnapshotStore


DEFAULT_SCAN_TARGET_RARITIES = (
    Rarity.RESTRICTED,
    Rarity.CLASSIFIED,
    Rarity.COVERT,
)


@dataclass(frozen=True, slots=True)
class SteamDTTradeUpScanSummary:
    catalog_path: str
    snapshot_store_path: str
    targets_scanned: int
    results_found: int
    cache_only: bool
    live_fetch_enabled: bool
    output_csv_path: str | None
    target_names: tuple[str, ...]
    results: tuple[TradeUpScanResult, ...]


def build_steamdt_tradeup_scanner(
    *,
    catalog: ItemCatalog | str | Path = Path("data") / "items.sqlite",
    snapshot_store: SteamDTPriceSnapshotStore | str | Path = Path("data") / "steamdt_prices.sqlite",
    steamdt_client: SteamDTMarketAPI | None = None,
    cache_only: bool = False,
    max_cache_age_seconds: float | None = None,
    write_back_on_fetch: bool = True,
    fee_rate: float = DEFAULT_FEE_RATE,
    scanner_max_workers: int = 8,
    price_max_workers: int = 8,
    prefer_safe_price: bool = True,
    require_valid_prices: bool = True,
    normal_tradeup_only: bool = True,
    conservative_float_mode: bool = True,
) -> tuple[TradeUpScanner, CachedSteamDTMarketAPI, ItemCatalog, SteamDTPriceSnapshotStore]:
    resolved_catalog = catalog if isinstance(catalog, ItemCatalog) else ItemCatalog.from_path(catalog)
    resolved_store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    cached_client = CachedSteamDTMarketAPI(
        resolved_store,
        steamdt_client=steamdt_client,
        max_age_seconds=max_cache_age_seconds,
        write_back_on_fetch=write_back_on_fetch,
        allow_live_fetch=not cache_only,
        prefer_safe_price=prefer_safe_price,
        require_valid_prices=require_valid_prices,
        normal_tradeup_only=normal_tradeup_only,
    )
    price_manager = MultiMarketPriceManager(
        {"SteamDTCache": cached_client},
        max_workers=price_max_workers,
    )
    scanner = TradeUpScanner(
        catalog=resolved_catalog,
        price_manager=price_manager,
        fee_rate=fee_rate,
        max_workers=scanner_max_workers,
        conservative_float_mode=conservative_float_mode,
    )
    return scanner, cached_client, resolved_catalog, resolved_store


def build_watchlist_from_steamdt_cache(
    *,
    catalog: ItemCatalog | str | Path,
    snapshot_store: SteamDTPriceSnapshotStore | str | Path,
    item_names: Iterable[str] | None = None,
    target_rarities: Iterable[Rarity | int | str] = DEFAULT_SCAN_TARGET_RARITIES,
    include_stattrak: bool = False,
    cached_exteriors_only: bool = True,
    default_exteriors: Iterable[Exterior | str] | None = None,
    formula_options: Mapping[str, Any] | None = None,
    max_targets: int | None = None,
    prefer_safe_price: bool = True,
    require_valid_prices: bool = True,
    normal_tradeup_only: bool = True,
) -> tuple[WatchlistTarget, ...]:
    resolved_catalog = catalog if isinstance(catalog, ItemCatalog) else ItemCatalog.from_path(catalog)
    resolved_store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    resolved_rarities = {Rarity.from_value(rarity) for rarity in target_rarities}
    requested_names = tuple(dict.fromkeys(name.strip() for name in (item_names or ()) if str(name).strip()))
    candidate_names = requested_names or resolved_store.list_item_families()
    default_target_exteriors = _resolve_exterior_sequence(default_exteriors)

    targets: list[WatchlistTarget] = []
    seen: set[tuple[str, Exterior]] = set()
    for item_name in candidate_names:
        try:
            item_definition = resolved_catalog.get_item(item_name)
        except KeyError:
            continue
        if item_definition.rarity not in resolved_rarities:
            continue

        cached_exteriors = _resolve_cached_target_exteriors(
            resolved_store.get_latest_snapshots_for_item_family(
                item_name,
                prefer_cleaned=prefer_safe_price,
                require_valid=require_valid_prices,
                require_tradeup_compatible_normal=normal_tradeup_only,
            ),
            include_stattrak=include_stattrak,
            allowed_exteriors=item_definition.available_exteriors,
        )
        if cached_exteriors_only:
            target_exteriors = cached_exteriors
        else:
            target_exteriors = cached_exteriors or default_target_exteriors or item_definition.available_exteriors

        for exterior in target_exteriors:
            key = (item_name, exterior)
            if key in seen:
                continue
            seen.add(key)
            targets.append(
                WatchlistTarget(
                    item_name=item_name,
                    exterior=exterior,
                    formula_options=dict(formula_options or {}),
                )
            )

    targets.sort(key=_watchlist_sort_key)
    if max_targets is not None:
        targets = targets[:max_targets]
    return tuple(targets)


def scan_steamdt_tradeup_candidates(
    *,
    catalog: ItemCatalog | str | Path = Path("data") / "items.sqlite",
    snapshot_store: SteamDTPriceSnapshotStore | str | Path = Path("data") / "steamdt_prices.sqlite",
    steamdt_client: SteamDTMarketAPI | None = None,
    targets: Iterable[WatchlistTarget | Mapping[str, Any] | tuple[str, str] | str] | None = None,
    watchlist_path: str | Path | None = None,
    item_names: Iterable[str] | None = None,
    target_rarities: Iterable[Rarity | int | str] = DEFAULT_SCAN_TARGET_RARITIES,
    include_stattrak_targets: bool = False,
    cached_exteriors_only: bool = True,
    default_exteriors: Iterable[Exterior | str] | None = None,
    cache_only: bool = False,
    max_cache_age_seconds: float | None = None,
    write_back_on_fetch: bool = True,
    fee_rate: float = DEFAULT_FEE_RATE,
    roi_threshold: float = 1.05,
    formula_limit_per_target: int = 25,
    formula_options: Mapping[str, Any] | None = None,
    max_targets: int | None = None,
    output_csv_path: str | Path | None = Path("output") / "steamdt_tradeup_candidates.csv",
    scanner_max_workers: int = 8,
    price_max_workers: int = 8,
    progress_callback: Callable[[int, int, str], None] | None = None,
    prefer_safe_price: bool = True,
    require_valid_prices: bool = True,
    normal_tradeup_only: bool = True,
    conservative_float_mode: bool = True,
) -> SteamDTTradeUpScanSummary:
    scanner, cached_client, resolved_catalog, resolved_store = build_steamdt_tradeup_scanner(
        catalog=catalog,
        snapshot_store=snapshot_store,
        steamdt_client=steamdt_client,
        cache_only=cache_only,
        max_cache_age_seconds=max_cache_age_seconds,
        write_back_on_fetch=write_back_on_fetch,
        fee_rate=fee_rate,
        scanner_max_workers=scanner_max_workers,
        price_max_workers=price_max_workers,
        prefer_safe_price=prefer_safe_price,
        require_valid_prices=require_valid_prices,
        normal_tradeup_only=normal_tradeup_only,
        conservative_float_mode=conservative_float_mode,
    )
    try:
        resolved_targets = _resolve_scan_targets(
            catalog=resolved_catalog,
            snapshot_store=resolved_store,
            targets=targets,
            watchlist_path=watchlist_path,
            item_names=item_names,
            target_rarities=target_rarities,
            include_stattrak_targets=include_stattrak_targets,
            cached_exteriors_only=cached_exteriors_only,
            default_exteriors=default_exteriors,
            formula_options=formula_options,
            max_targets=max_targets,
            prefer_safe_price=prefer_safe_price,
            require_valid_prices=require_valid_prices,
            normal_tradeup_only=normal_tradeup_only,
        )
        results_list: list[TradeUpScanResult] = []
        total_targets = len(resolved_targets)
        for index, target in enumerate(resolved_targets, start=1):
            if progress_callback is not None:
                progress_callback(
                    index,
                    total_targets,
                    f"扫描 {target.item_name} [{target.exterior.value}]",
                )
            try:
                target_results = scanner.scan_target(
                    target,
                    roi_threshold=roi_threshold,
                    formula_limit=formula_limit_per_target,
                )
            except FormulaGenerationError:
                continue
            results_list.extend(target_results)
        results_list.sort(
            key=lambda result: (-result.roi, -result.expected_profit, result.total_cost)
        )
        results = tuple(results_list)
        exported_path = None
        if output_csv_path is not None:
            exported_path = str(export_scan_results_csv(results, output_csv_path))
        return SteamDTTradeUpScanSummary(
            catalog_path=str(_as_path(catalog)),
            snapshot_store_path=str(_as_path(snapshot_store)),
            targets_scanned=len(resolved_targets),
            results_found=len(results),
            cache_only=cache_only,
            live_fetch_enabled=not cache_only,
            output_csv_path=exported_path,
            target_names=tuple(target.item_name for target in resolved_targets),
            results=tuple(results),
        )
    finally:
        cached_client.close()


def _resolve_scan_targets(
    *,
    catalog: ItemCatalog,
    snapshot_store: SteamDTPriceSnapshotStore,
    targets: Iterable[WatchlistTarget | Mapping[str, Any] | tuple[str, str] | str] | None,
    watchlist_path: str | Path | None,
    item_names: Iterable[str] | None,
    target_rarities: Iterable[Rarity | int | str],
    include_stattrak_targets: bool,
    cached_exteriors_only: bool,
    default_exteriors: Iterable[Exterior | str] | None,
    formula_options: Mapping[str, Any] | None,
    max_targets: int | None,
    prefer_safe_price: bool,
    require_valid_prices: bool,
    normal_tradeup_only: bool,
) -> tuple[WatchlistTarget, ...]:
    if targets is not None:
        resolved_targets = [
            _coerce_target_formula_options(target, formula_options)
            for target in targets
        ]
        if max_targets is not None:
            resolved_targets = resolved_targets[:max_targets]
        return tuple(resolved_targets)

    if watchlist_path is not None:
        loaded_targets = [
            _merge_formula_options(target, formula_options)
            for target in load_watchlist(watchlist_path)
        ]
        if max_targets is not None:
            loaded_targets = loaded_targets[:max_targets]
        return tuple(loaded_targets)

    return build_watchlist_from_steamdt_cache(
        catalog=catalog,
        snapshot_store=snapshot_store,
        item_names=item_names,
        target_rarities=target_rarities,
        include_stattrak=include_stattrak_targets,
        cached_exteriors_only=cached_exteriors_only,
        default_exteriors=default_exteriors,
        formula_options=formula_options,
        max_targets=max_targets,
        prefer_safe_price=prefer_safe_price,
        require_valid_prices=require_valid_prices,
        normal_tradeup_only=normal_tradeup_only,
    )


def _coerce_target_formula_options(
    target: WatchlistTarget | Mapping[str, Any] | tuple[str, str] | str,
    formula_options: Mapping[str, Any] | None,
) -> WatchlistTarget:
    normalized_target = TradeUpScanner._normalize_target(TradeUpScanner, target)
    return _merge_formula_options(normalized_target, formula_options)


def _merge_formula_options(
    target: WatchlistTarget,
    formula_options: Mapping[str, Any] | None,
) -> WatchlistTarget:
    if not formula_options:
        return target
    merged_formula_options = dict(formula_options)
    merged_formula_options.update(target.formula_options)
    return WatchlistTarget(
        item_name=target.item_name,
        exterior=target.exterior,
        formula_options=merged_formula_options,
    )


def _resolve_cached_target_exteriors(
    snapshots: Sequence[Any],
    *,
    include_stattrak: bool,
    allowed_exteriors: Iterable[Exterior],
) -> tuple[Exterior, ...]:
    allowed_lookup = set(allowed_exteriors)
    exteriors: list[Exterior] = []
    for snapshot in snapshots:
        base_item_name, variant = split_item_variant_name(snapshot.item_name)
        if not include_stattrak and variant is ItemVariant.STATTRAK:
            continue
        if snapshot.exterior in (None, ""):
            continue
        try:
            exterior = Exterior.from_label(snapshot.exterior)
        except ValueError:
            continue
        if exterior not in allowed_lookup or exterior in exteriors:
            continue
        exteriors.append(exterior)
    return tuple(
        exterior
        for exterior in Exterior.ordered()
        if exterior in exteriors
    )


def _resolve_exterior_sequence(
    exteriors: Iterable[Exterior | str] | None,
) -> tuple[Exterior, ...]:
    if exteriors is None:
        return ()
    resolved: list[Exterior] = []
    for exterior in exteriors:
        normalized = exterior if isinstance(exterior, Exterior) else Exterior.from_label(exterior)
        if normalized not in resolved:
            resolved.append(normalized)
    return tuple(
        exterior
        for exterior in Exterior.ordered()
        if exterior in resolved
    )


def _watchlist_sort_key(target: WatchlistTarget) -> tuple[str, int]:
    exterior_rank = {exterior: index for index, exterior in enumerate(Exterior.ordered())}
    return (target.item_name, exterior_rank.get(target.exterior, len(exterior_rank)))


def _as_path(value: ItemCatalog | SteamDTPriceSnapshotStore | str | Path) -> Path:
    if isinstance(value, ItemCatalog):
        return Path("<memory-catalog>")
    if isinstance(value, SteamDTPriceSnapshotStore):
        return value.path
    return Path(value)


__all__ = [
    "DEFAULT_SCAN_TARGET_RARITIES",
    "SteamDTTradeUpScanSummary",
    "build_steamdt_tradeup_scanner",
    "build_watchlist_from_steamdt_cache",
    "scan_steamdt_tradeup_candidates",
]
