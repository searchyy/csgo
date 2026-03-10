from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from .catalog import ItemCatalog
from .localization import DEFAULT_LOCALIZATION_PATH, sync_bymykel_localization_cache
from .models import Exterior, ItemDefinition, ItemVariant, Rarity


BYMYKEL_CSGO_API_SKINS_URL = (
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/skins.json"
)
TRADEUP_FIREARM_CATEGORY_NAMES = frozenset({"Rifles", "Pistols", "SMGs", "Heavy"})
_BYMYKEL_RARITY_MAP = {
    "Consumer Grade": Rarity.CONSUMER_GRADE,
    "Industrial Grade": Rarity.INDUSTRIAL_GRADE,
    "Mil-Spec Grade": Rarity.MIL_SPEC,
    "Restricted": Rarity.RESTRICTED,
    "Classified": Rarity.CLASSIFIED,
    "Covert": Rarity.COVERT,
}
_SKIPPED_RARITY_NAMES = frozenset({"Contraband", "Extraordinary"})
_ITEM_VARIANT_ORDER = (ItemVariant.NORMAL, ItemVariant.STATTRAK)
_EXTERIOR_ORDER = Exterior.ordered()


@dataclass(frozen=True, slots=True)
class StaticCatalogSyncSummary:
    source_url: str
    items_fetched: int
    items_written: int
    collections_written: int
    json_path: str | None = None
    sqlite_path: str | None = None
    localization_path: str | None = None


def fetch_bymykel_skins_payload(
    *,
    source_url: str = BYMYKEL_CSGO_API_SKINS_URL,
    timeout: float = 30.0,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    owns_session = session is None
    resolved_session = session or requests.Session()
    try:
        response = resolved_session.get(
            source_url,
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "cs2-tradeup-static-sync/0.1",
            },
        )
        response.raise_for_status()
        payload = response.json()
    finally:
        if owns_session:
            resolved_session.close()

    if not isinstance(payload, list):
        raise ValueError("ByMykel skins payload must be a JSON list")
    return [dict(row) for row in payload if isinstance(row, Mapping)]


def build_catalog_from_bymykel_api(
    skins_payload: Iterable[Mapping[str, Any]],
    *,
    allowed_categories: Iterable[str] = TRADEUP_FIREARM_CATEGORY_NAMES,
) -> ItemCatalog:
    allowed_category_names = {str(name) for name in allowed_categories}
    items_by_name: dict[str, ItemDefinition] = {}

    for row in skins_payload:
        item = _build_item_definition_from_bymykel_row(
            row,
            allowed_categories=allowed_category_names,
        )
        if item is None:
            continue
        existing_item = items_by_name.get(item.name)
        if existing_item is None:
            items_by_name[item.name] = item
            continue
        items_by_name[item.name] = _merge_duplicate_item_definitions(
            existing_item,
            item,
        )

    ordered_items = sorted(
        items_by_name.values(),
        key=lambda item: (item.collection, int(item.rarity), item.name),
    )
    return ItemCatalog(ordered_items)


def sync_bymykel_static_catalog(
    *,
    skins_payload: Iterable[Mapping[str, Any]] | None = None,
    source_url: str = BYMYKEL_CSGO_API_SKINS_URL,
    timeout: float = 30.0,
    session: requests.Session | None = None,
    output_json_path: str | Path | None = Path("data") / "items.json",
    output_sqlite_path: str | Path | None = Path("data") / "items.sqlite",
    output_localization_path: str | Path | None = DEFAULT_LOCALIZATION_PATH,
) -> StaticCatalogSyncSummary:
    resolved_payload = (
        [dict(row) for row in skins_payload]
        if skins_payload is not None
        else fetch_bymykel_skins_payload(
            source_url=source_url,
            timeout=timeout,
            session=session,
        )
    )
    catalog = build_catalog_from_bymykel_api(resolved_payload)

    json_path_str = None
    sqlite_path_str = None
    if output_json_path is not None:
        json_path_str = str(catalog.to_json(output_json_path))
    if output_sqlite_path is not None:
        sqlite_path_str = str(catalog.to_sqlite(output_sqlite_path))
    localization_path_str = None
    if output_localization_path is not None and skins_payload is None:
        localization_path_str = str(
            sync_bymykel_localization_cache(
                output_path=output_localization_path,
                session=session,
                timeout=timeout,
            )
        )

    return StaticCatalogSyncSummary(
        source_url=source_url if skins_payload is None else "inline_payload",
        items_fetched=len(resolved_payload),
        items_written=len(catalog.all_items()),
        collections_written=len({item.collection for item in catalog.all_items()}),
        json_path=json_path_str,
        sqlite_path=sqlite_path_str,
        localization_path=localization_path_str,
    )


def _build_item_definition_from_bymykel_row(
    row: Mapping[str, Any],
    *,
    allowed_categories: set[str],
) -> ItemDefinition | None:
    item_name = _extract_name(row.get("name"))
    if not item_name or " | " not in item_name:
        return None

    category_name = _extract_name(row.get("category"))
    if category_name not in allowed_categories:
        return None

    rarity_name = _extract_name(row.get("rarity"))
    if not rarity_name or rarity_name in _SKIPPED_RARITY_NAMES:
        return None
    rarity = _BYMYKEL_RARITY_MAP.get(rarity_name)
    if rarity is None:
        return None

    collection_name = _extract_collection_name(row.get("collections"))
    if not collection_name:
        return None

    try:
        min_float = float(row["min_float"])
        max_float = float(row["max_float"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Invalid float bounds for {item_name}") from error

    available_exteriors = _resolve_available_exteriors(
        row.get("wears"),
        min_float=min_float,
        max_float=max_float,
    )
    if not available_exteriors:
        return None

    available_variants = (
        (ItemVariant.NORMAL, ItemVariant.STATTRAK)
        if bool(row.get("stattrak"))
        else (ItemVariant.NORMAL,)
    )
    return ItemDefinition(
        name=item_name,
        collection=collection_name,
        rarity=rarity,
        min_float=min_float,
        max_float=max_float,
        available_variants=available_variants,
        available_exteriors=available_exteriors,
    )


def _extract_name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        nested_name = value.get("name")
        if nested_name in (None, ""):
            return None
        return str(nested_name).strip()
    if value in (None, ""):
        return None
    return str(value).strip()


def _extract_collection_name(value: Any) -> str | None:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, Mapping)):
        return _extract_name(value)

    for entry in value:
        collection_name = _extract_name(entry)
        if collection_name:
            return collection_name
    return None


def _resolve_available_exteriors(
    wears: Any,
    *,
    min_float: float,
    max_float: float,
) -> tuple[Exterior, ...]:
    explicit_exteriors: list[Exterior] = []
    if isinstance(wears, Iterable) and not isinstance(wears, (str, bytes, Mapping)):
        for entry in wears:
            wear_name = _extract_name(entry)
            if not wear_name:
                continue
            try:
                exterior = Exterior.from_label(wear_name)
            except ValueError:
                continue
            if exterior not in explicit_exteriors:
                explicit_exteriors.append(exterior)

    inferred_exteriors = [
        exterior
        for exterior in _EXTERIOR_ORDER
        if exterior.overlaps_float_range(min_float, max_float)
    ]
    if explicit_exteriors:
        explicit_lookup = set(explicit_exteriors)
        ordered_explicit = tuple(
            exterior
            for exterior in _EXTERIOR_ORDER
            if exterior in explicit_lookup and exterior in inferred_exteriors
        )
        if ordered_explicit:
            return ordered_explicit
        return tuple(
            exterior for exterior in _EXTERIOR_ORDER if exterior in explicit_lookup
        )
    return tuple(inferred_exteriors)


def _merge_duplicate_item_definitions(
    left: ItemDefinition,
    right: ItemDefinition,
) -> ItemDefinition:
    if left.collection != right.collection:
        raise ValueError(
            f"Duplicate item '{left.name}' appears in multiple collections: "
            f"{left.collection!r} vs {right.collection!r}"
        )
    if left.rarity != right.rarity:
        raise ValueError(
            f"Duplicate item '{left.name}' appears with multiple rarities: "
            f"{left.rarity!r} vs {right.rarity!r}"
        )
    if abs(left.min_float - right.min_float) > 1e-9 or abs(left.max_float - right.max_float) > 1e-9:
        raise ValueError(
            f"Duplicate item '{left.name}' appears with multiple float ranges: "
            f"[{left.min_float}, {left.max_float}] vs [{right.min_float}, {right.max_float}]"
        )

    variant_lookup = set(left.available_variants) | set(right.available_variants)
    exterior_lookup = set(left.available_exteriors) | set(right.available_exteriors)
    return ItemDefinition(
        name=left.name,
        collection=left.collection,
        rarity=left.rarity,
        min_float=left.min_float,
        max_float=left.max_float,
        available_variants=tuple(
            variant for variant in _ITEM_VARIANT_ORDER if variant in variant_lookup
        ),
        available_exteriors=tuple(
            exterior for exterior in _EXTERIOR_ORDER if exterior in exterior_lookup
        ),
    )


__all__ = [
    "BYMYKEL_CSGO_API_SKINS_URL",
    "TRADEUP_FIREARM_CATEGORY_NAMES",
    "StaticCatalogSyncSummary",
    "build_catalog_from_bymykel_api",
    "fetch_bymykel_skins_payload",
    "sync_bymykel_static_catalog",
]
