from __future__ import annotations

import csv
import json
import html
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from .catalog import ItemCatalog
from .localization import (
    translate_collection_zh_cn as _dynamic_translate_collection_zh_cn,
    translate_exterior_zh_cn as _dynamic_translate_exterior_zh_cn,
    translate_item_name_zh_cn as _dynamic_translate_item_name_zh_cn,
    translate_rarity_zh_cn as _dynamic_translate_rarity_zh_cn,
    translate_variant_zh_cn as _dynamic_translate_variant_zh_cn,
)
from .market import build_item_variant_name, split_item_variant_name
from .models import Exterior, ItemDefinition, ItemVariant, Rarity
from .steamdt_market import (
    CachedSteamDTMarketAPI,
    SteamDTMarketAPI,
    SteamDTPriceSnapshot,
    SteamDTPriceSnapshotStore,
    split_steamdt_market_hash_name,
)


MARKET_DERIVED_COLLECTION = "MarketDerived"
STEAMDT_PLATFORM_EXPORT_ORDER = (
    ("buff", "BUFF"),
    ("youpin", "悠悠"),
    ("steam", "Steam"),
    ("haloskins", "HaloSkins"),
)
WEAPON_NAME_ZH_CN_MAP = {
    "AK-47": "AK-47",
    "AWP": "AWP",
    "Glock-18": "格洛克 18 型",
    "M4A1-S": "M4A1 消音型",
    "M4A4": "M4A4",
    "USP-S": "USP 消音版",
}
FINISH_NAME_ZH_CN_MAP = {
    "Black Laminate": "黑色层压板",
    "Bullet Rain": "弹雨",
    "Chantico's Fire": "女火神之炽焰",
    "Containment Breach": "冲出重围",
    "Daybreak": "破晓",
    "Flashback": "闪回",
    "Green Laminate": "绿色层压板",
    "Hyper Beast": "暴怒野兽",
    "Leet Museo": "抽象派 1337",
    "Monster Mashup": "小绿怪",
    "Oni Taiji": "鬼退治",
    "Para Green": "绿色伞兵",
    "Point Disarray": "混沌点阵",
    "Safari Mesh": "狩猎网格",
    "The Coalition": "合纵",
    "The Empress": "皇后",
    "Wasteland Rebel": "荒野反叛",
    "X-Ray": "X 射线",
}
RARITY_ZH_CN_MAP = {
    "CONSUMER_GRADE": "消费级",
    "INDUSTRIAL_GRADE": "工业级",
    "MIL_SPEC": "军规级",
    "RESTRICTED": "受限",
    "CLASSIFIED": "保密",
    "COVERT": "隐秘",
}
EXTERIOR_ZH_CN_MAP = {
    "Factory New": "崭新出厂",
    "Minimal Wear": "略有磨损",
    "Field-Tested": "久经沙场",
    "Well-Worn": "破损不堪",
    "Battle-Scarred": "战痕累累",
}
VARIANT_ZH_CN_MAP = {
    "Normal": "普通",
    "StatTrak": "暗金",
}
COLLECTION_ZH_CN_MAP = {
    MARKET_DERIVED_COLLECTION: "市场派生",
}
PLATFORM_HTML_LABELS_ZH_CN = {
    "BUFF": "网易BUFF",
    "悠悠": "悠悠有品",
    "Steam": "Steam社区",
    "HaloSkins": "HaloSkins",
}
FIREARM_WEAPON_NAMES = frozenset(
    {
        "AK-47",
        "AUG",
        "AWP",
        "CZ75-Auto",
        "Desert Eagle",
        "Dual Berettas",
        "FAMAS",
        "Five-SeveN",
        "G3SG1",
        "Galil AR",
        "Glock-18",
        "M4A1-S",
        "M4A4",
        "MAC-10",
        "MAG-7",
        "MP5-SD",
        "MP7",
        "MP9",
        "M249",
        "Negev",
        "Nova",
        "P2000",
        "P250",
        "P90",
        "PP-Bizon",
        "R8 Revolver",
        "SCAR-20",
        "SG 553",
        "SSG 08",
        "Sawed-Off",
        "Tec-9",
        "UMP-45",
        "USP-S",
        "XM1014",
        "Zeus x27",
    }
)


@dataclass(frozen=True, slots=True)
class SteamDTCatalogSyncSummary:
    items_discovered: int
    items_synced: int
    snapshots_inserted: int
    json_path: str | None
    sqlite_path: str | None
    item_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SteamDTItemPriceDetailRow:
    base_item_name: str
    variant: str
    item_name: str
    market_hash_name: str
    exterior: str | None
    collection: str
    rarity: int
    rarity_name: str
    min_float: float
    max_float: float
    lowest_price: float
    safe_price: float | None
    is_valid: bool
    risk_level: str | None
    anomaly_flags: tuple[str, ...]
    anomaly_notes: str | None
    recent_average_price: float | None
    highest_buy_price: float | None
    selected_platform: str | None
    selected_platform_name: str | None
    selected_buy_platform: str | None
    selected_buy_platform_name: str | None
    sell_num: int | None
    fetched_at: str
    source: str


@dataclass(frozen=True, slots=True)
class SteamDTPlatformPriceView:
    platform: str
    platform_name: str
    price: float | None
    link: str | None


@dataclass(frozen=True, slots=True)
class SteamDTItemPlatformDetailRow:
    base_item_name: str
    variant: str
    item_name: str
    market_hash_name: str
    exterior: str | None
    collection: str
    rarity: int
    rarity_name: str
    min_float: float
    max_float: float
    lowest_price: float
    safe_price: float | None
    is_valid: bool
    risk_level: str | None
    anomaly_flags: tuple[str, ...]
    anomaly_notes: str | None
    recent_average_price: float | None
    highest_buy_price: float | None
    sell_num: int | None
    fetched_at: str
    source: str
    platform_prices: tuple[SteamDTPlatformPriceView, ...]


def discover_steamdt_firearm_item_names(
    steamdt_client: SteamDTMarketAPI,
    *,
    limit: int = 20,
    query_name: str = "",
    max_pages: int = 10,
    scroll_pause_ms: int = 2_500,
    idle_scroll_limit: int = 3,
) -> tuple[str, ...]:
    discovered: list[str] = []
    seen: set[str] = set()
    pages = steamdt_client.crawl_market_pages(
        query_name=query_name,
        max_pages=max_pages,
        scroll_pause_ms=scroll_pause_ms,
        idle_scroll_limit=idle_scroll_limit,
    )
    for page in pages:
        for market_item in page.items:
            item_name, _ = split_steamdt_market_hash_name(market_item.market_hash_name)
            base_item_name, _ = split_item_variant_name(item_name)
            if not is_firearm_item_name(base_item_name):
                continue
            if base_item_name in seen:
                continue
            seen.add(base_item_name)
            discovered.append(base_item_name)
            if len(discovered) >= limit:
                return tuple(discovered)
    return tuple(discovered)


def is_firearm_item_name(item_name: str) -> bool:
    normalized_name, _ = split_item_variant_name(item_name)
    if "|" not in normalized_name:
        return False
    weapon_name = normalized_name.split("|", 1)[0].strip()
    return weapon_name in FIREARM_WEAPON_NAMES


def build_item_definition_from_steamdt_snapshots(
    base_item_name: str,
    snapshots: Iterable[SteamDTPriceSnapshot],
    *,
    existing_item: ItemDefinition | None = None,
    placeholder_collection: str = MARKET_DERIVED_COLLECTION,
    default_rarity: Rarity = Rarity.MIL_SPEC,
) -> ItemDefinition:
    relevant_snapshots = [
        snapshot
        for snapshot in snapshots
        if split_item_variant_name(snapshot.item_name)[0] == base_item_name
    ]
    if not relevant_snapshots:
        raise ValueError(f"No snapshots available for {base_item_name}")

    observed_variants: list[ItemVariant] = []
    observed_exteriors: list[Exterior] = []
    resolved_rarity = existing_item.rarity if existing_item is not None else None

    for snapshot in relevant_snapshots:
        _, variant = split_item_variant_name(snapshot.item_name)
        if variant not in observed_variants:
            observed_variants.append(variant)
        if snapshot.exterior:
            exterior = Exterior.from_label(snapshot.exterior)
            if exterior not in observed_exteriors:
                observed_exteriors.append(exterior)
        if resolved_rarity is None:
            resolved_rarity = _extract_rarity_from_snapshot(snapshot)

    if existing_item is not None:
        merged_variants = _merge_variants(existing_item.available_variants, observed_variants)
        merged_exteriors = _merge_exteriors(existing_item.available_exteriors, observed_exteriors)
        return ItemDefinition(
            name=existing_item.name,
            collection=existing_item.collection,
            rarity=existing_item.rarity,
            min_float=existing_item.min_float,
            max_float=existing_item.max_float,
            available_variants=merged_variants,
            available_exteriors=merged_exteriors,
        )

    min_float, max_float = infer_float_bounds_from_exteriors(observed_exteriors)
    return ItemDefinition(
        name=base_item_name,
        collection=placeholder_collection,
        rarity=resolved_rarity or default_rarity,
        min_float=min_float,
        max_float=max_float,
        available_variants=_merge_variants((), observed_variants),
        available_exteriors=_merge_exteriors((), observed_exteriors),
    )


def infer_float_bounds_from_exteriors(
    exteriors: Iterable[Exterior | str],
) -> tuple[float, float]:
    normalized_exteriors = [
        exterior if isinstance(exterior, Exterior) else Exterior.from_label(exterior)
        for exterior in exteriors
    ]
    if not normalized_exteriors:
        return 0.0, 1.0
    lower_bounds = [exterior.float_bounds[0] for exterior in normalized_exteriors]
    upper_bounds = [exterior.float_bounds[1] for exterior in normalized_exteriors]
    return min(lower_bounds), max(upper_bounds)


def sync_steamdt_items_to_catalog(
    *,
    snapshot_store: SteamDTPriceSnapshotStore | str | Path,
    steamdt_client: SteamDTMarketAPI | None = None,
    target_item_names: Sequence[str] | None = None,
    item_limit: int = 20,
    base_catalog: ItemCatalog | str | Path | None = None,
    output_json_path: str | Path | None = Path("data") / "items.json",
    output_sqlite_path: str | Path | None = Path("data") / "items.sqlite",
    placeholder_collection: str = MARKET_DERIVED_COLLECTION,
    discovery_query_name: str = "",
    discovery_max_pages: int = 10,
    scroll_pause_ms: int = 2_500,
    idle_scroll_limit: int = 3,
) -> SteamDTCatalogSyncSummary:
    store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    owns_client = steamdt_client is None
    client = steamdt_client or SteamDTMarketAPI()
    cached_client = CachedSteamDTMarketAPI(
        store,
        steamdt_client=client,
        max_age_seconds=None,
        write_back_on_fetch=True,
    )
    try:
        working_catalog = _load_base_catalog(
            base_catalog=base_catalog,
            output_json_path=output_json_path,
            output_sqlite_path=output_sqlite_path,
        )
        item_map = {item.name: item for item in working_catalog.all_items()}
        if target_item_names is None:
            item_names = discover_steamdt_firearm_item_names(
                client,
                limit=item_limit,
                query_name=discovery_query_name,
                max_pages=discovery_max_pages,
                scroll_pause_ms=scroll_pause_ms,
                idle_scroll_limit=idle_scroll_limit,
            )
        else:
            item_names = tuple(_dedupe_preserve_order(target_item_names))[:item_limit]

        synced_names: list[str] = []
        snapshots_inserted = 0
        for item_name in item_names:
            snapshots = list(store.get_latest_snapshots_for_item_family(item_name))
            snapshots.extend(
                _warm_missing_variants(
                    cached_client,
                    item_name,
                    existing_snapshots=snapshots,
                )
            )
            if not snapshots:
                continue
            item_map[item_name] = build_item_definition_from_steamdt_snapshots(
                item_name,
                snapshots,
                existing_item=item_map.get(item_name),
                placeholder_collection=placeholder_collection,
            )
            synced_names.append(item_name)
            snapshots_inserted += len(
                [
                    snapshot
                    for snapshot in snapshots
                    if snapshot.source == "steamdt_private_batch"
                ]
            )

        catalog = ItemCatalog(item_map.values())
        json_path_str = None
        sqlite_path_str = None
        if output_json_path is not None:
            json_path = catalog.to_json(output_json_path)
            json_path_str = str(json_path)
        if output_sqlite_path is not None:
            sqlite_path = catalog.to_sqlite(output_sqlite_path)
            sqlite_path_str = str(sqlite_path)
        return SteamDTCatalogSyncSummary(
            items_discovered=len(item_names),
            items_synced=len(synced_names),
            snapshots_inserted=snapshots_inserted,
            json_path=json_path_str,
            sqlite_path=sqlite_path_str,
            item_names=tuple(synced_names),
        )
    finally:
        if owns_client:
            client.close()


def _load_base_catalog(
    *,
    base_catalog: ItemCatalog | str | Path | None,
    output_json_path: str | Path | None,
    output_sqlite_path: str | Path | None,
) -> ItemCatalog:
    if isinstance(base_catalog, ItemCatalog):
        return base_catalog
    if base_catalog is not None:
        return ItemCatalog.from_path(base_catalog)

    for candidate in (output_json_path, output_sqlite_path):
        if candidate is None:
            continue
        candidate_path = Path(candidate)
        if candidate_path.exists():
            return ItemCatalog.from_path(candidate_path)
    return ItemCatalog()


def _extract_rarity_from_snapshot(snapshot: SteamDTPriceSnapshot) -> Rarity | None:
    if not snapshot.raw_json:
        return None
    try:
        payload = json.loads(snapshot.raw_json)
    except ValueError:
        return None
    rarity_name = payload.get("rarityName")
    if rarity_name in (None, ""):
        return None
    try:
        return Rarity.from_value(str(rarity_name))
    except ValueError:
        return None


def _merge_variants(
    left: Iterable[ItemVariant | str],
    right: Iterable[ItemVariant | str],
) -> tuple[ItemVariant, ...]:
    ordered = [ItemVariant.NORMAL, ItemVariant.STATTRAK]
    merged = {ItemVariant.from_value(item) for item in left}
    merged.update(ItemVariant.from_value(item) for item in right)
    return tuple(variant for variant in ordered if variant in merged)


def _merge_exteriors(
    left: Iterable[Exterior | str],
    right: Iterable[Exterior | str],
) -> tuple[Exterior, ...]:
    merged = {
        item if isinstance(item, Exterior) else Exterior.from_label(item)
        for item in left
    }
    merged.update(
        item if isinstance(item, Exterior) else Exterior.from_label(item)
        for item in right
    )
    return tuple(exterior for exterior in Exterior.ordered() if exterior in merged)


def _dedupe_preserve_order(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return tuple(ordered)


def _warm_missing_variants(
    cached_client: CachedSteamDTMarketAPI,
    item_name: str,
    *,
    existing_snapshots: Sequence[SteamDTPriceSnapshot],
) -> tuple[SteamDTPriceSnapshot, ...]:
    base_item_name, _ = split_item_variant_name(item_name)
    have_normal = False
    have_stattrak = False
    for snapshot in existing_snapshots:
        _, variant = split_item_variant_name(snapshot.item_name)
        if variant is ItemVariant.STATTRAK:
            have_stattrak = True
        else:
            have_normal = True

    warmed: list[SteamDTPriceSnapshot] = []
    if not have_normal:
        warmed.extend(cached_client.warm_item_cache(base_item_name))
    if not have_stattrak:
        warmed.extend(
            cached_client.warm_item_cache(
                build_item_variant_name(base_item_name, ItemVariant.STATTRAK),
            )
        )
    return tuple(warmed)


def export_steamdt_item_price_details_csv(
    *,
    snapshot_store: SteamDTPriceSnapshotStore | str | Path,
    catalog: ItemCatalog | str | Path,
    output_csv_path: str | Path,
    item_names: Sequence[str] | None = None,
) -> Path:
    store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    resolved_catalog = catalog if isinstance(catalog, ItemCatalog) else ItemCatalog.from_path(catalog)
    rows = build_steamdt_item_price_detail_rows(
        snapshot_store=store,
        catalog=resolved_catalog,
        item_names=item_names,
    )
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "base_item_name",
                "variant",
                "item_name",
                "market_hash_name",
                "exterior",
                "collection",
                "rarity",
                "rarity_name",
                "min_float",
                "max_float",
                "lowest_price",
                "recent_average_price",
                "selected_platform",
                "selected_platform_name",
                "sell_num",
                "fetched_at",
                "source",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.base_item_name,
                    row.variant,
                    row.item_name,
                    row.market_hash_name,
                    row.exterior,
                    row.collection,
                    row.rarity,
                    row.rarity_name,
                    row.min_float,
                    row.max_float,
                    row.lowest_price,
                    row.recent_average_price,
                    row.selected_platform,
                    row.selected_platform_name,
                    row.sell_num,
                    row.fetched_at,
                    row.source,
                ]
            )
    return output_path


def export_steamdt_item_platform_prices_csv(
    *,
    snapshot_store: SteamDTPriceSnapshotStore | str | Path,
    catalog: ItemCatalog | str | Path,
    output_csv_path: str | Path,
    item_names: Sequence[str] | None = None,
) -> Path:
    store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    resolved_catalog = catalog if isinstance(catalog, ItemCatalog) else ItemCatalog.from_path(catalog)
    rows = build_steamdt_item_platform_detail_rows(
        snapshot_store=store,
        catalog=resolved_catalog,
        item_names=item_names,
    )
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    platform_columns = [platform_name for _, platform_name in STEAMDT_PLATFORM_EXPORT_ORDER]
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "base_item_name",
                "variant",
                "item_name",
                "market_hash_name",
                "exterior",
                "collection",
                "rarity",
                "rarity_name",
                "min_float",
                "max_float",
                "lowest_price",
                "recent_average_price",
                "sell_num",
                "fetched_at",
                "source",
                *[f"{column}_price" for column in platform_columns],
                *[f"{column}_link" for column in platform_columns],
            ]
        )
        for row in rows:
            price_lookup = {entry.platform_name: entry for entry in row.platform_prices}
            writer.writerow(
                [
                    row.base_item_name,
                    row.variant,
                    row.item_name,
                    row.market_hash_name,
                    row.exterior,
                    row.collection,
                    row.rarity,
                    row.rarity_name,
                    row.min_float,
                    row.max_float,
                    row.lowest_price,
                    row.recent_average_price,
                    row.sell_num,
                    row.fetched_at,
                    row.source,
                    *[price_lookup.get(column).price if price_lookup.get(column) else None for column in platform_columns],
                    *[price_lookup.get(column).link if price_lookup.get(column) else None for column in platform_columns],
                ]
            )
    return output_path


def export_steamdt_item_platform_prices_html(
    *,
    snapshot_store: SteamDTPriceSnapshotStore | str | Path,
    catalog: ItemCatalog | str | Path,
    output_html_path: str | Path,
    item_names: Sequence[str] | None = None,
    title: str = "CS2 饰品多平台价格看板",
) -> Path:
    store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    resolved_catalog = catalog if isinstance(catalog, ItemCatalog) else ItemCatalog.from_path(catalog)
    rows = build_steamdt_item_platform_detail_rows(
        snapshot_store=store,
        catalog=resolved_catalog,
        item_names=item_names,
    )
    payload = []
    for row in rows:
        payload.append(
            {
                "base_item_name": row.base_item_name,
                "base_item_name_zh": translate_item_name_zh_cn(row.base_item_name),
                "variant": row.variant,
                "variant_zh": translate_variant_zh_cn(row.variant),
                "item_name": row.item_name,
                "item_name_zh": translate_item_name_zh_cn(row.item_name),
                "market_hash_name": row.market_hash_name,
                "exterior": row.exterior,
                "exterior_zh": translate_exterior_zh_cn(row.exterior),
                "collection": row.collection,
                "collection_zh": translate_collection_zh_cn(row.collection),
                "rarity": row.rarity,
                "rarity_name": row.rarity_name,
                "rarity_name_zh": translate_rarity_zh_cn(row.rarity_name),
                "min_float": row.min_float,
                "max_float": row.max_float,
                "lowest_price": row.lowest_price,
                "recent_average_price": row.recent_average_price,
                "sell_num": row.sell_num,
                "fetched_at": row.fetched_at,
                "source": row.source,
                "platform_prices": [
                    {
                        "platform": entry.platform,
                        "platform_name": entry.platform_name,
                        "price": entry.price,
                        "link": entry.link,
                    }
                    for entry in row.platform_prices
                ],
            }
        )
    html_text = _build_platform_dashboard_html(
        rows=payload,
        title=title,
    )
    output_path = Path(output_html_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_text, encoding="utf-8")
    return output_path


def build_steamdt_item_price_detail_rows(
    *,
    snapshot_store: SteamDTPriceSnapshotStore,
    catalog: ItemCatalog,
    item_names: Sequence[str] | None = None,
    prefer_cleaned: bool = False,
    valid_only: bool = False,
) -> tuple[SteamDTItemPriceDetailRow, ...]:
    resolved_item_names = (
        tuple(_dedupe_preserve_order(item_names))
        if item_names is not None
        else tuple(item.name for item in catalog.all_items())
    )
    rows: list[SteamDTItemPriceDetailRow] = []
    for base_item_name in resolved_item_names:
        try:
            item_definition = catalog.get_item(base_item_name)
        except KeyError:
            continue
        snapshots = snapshot_store.get_latest_snapshots_for_item_family(
            base_item_name,
            prefer_cleaned=prefer_cleaned,
            require_valid=valid_only,
        )
        for snapshot in _sort_snapshots_for_export(snapshots):
            _, variant = split_item_variant_name(snapshot.item_name)
            rows.append(
                SteamDTItemPriceDetailRow(
                    base_item_name=base_item_name,
                    variant=variant.value,
                    item_name=snapshot.item_name,
                    market_hash_name=snapshot.market_hash_name,
                    exterior=snapshot.exterior,
                    collection=item_definition.collection,
                    rarity=int(item_definition.rarity),
                    rarity_name=item_definition.rarity.name,
                    min_float=item_definition.min_float,
                    max_float=item_definition.max_float,
                    lowest_price=snapshot.lowest_price,
                    safe_price=snapshot.safe_price,
                    is_valid=snapshot.is_valid,
                    risk_level=snapshot.risk_level,
                    anomaly_flags=snapshot.anomaly_flags,
                    anomaly_notes=snapshot.anomaly_notes,
                    recent_average_price=snapshot.recent_average_price,
                    highest_buy_price=snapshot.highest_buy_price,
                    selected_platform=snapshot.selected_platform,
                    selected_platform_name=snapshot.selected_platform_name,
                    selected_buy_platform=snapshot.selected_buy_platform,
                    selected_buy_platform_name=snapshot.selected_buy_platform_name,
                    sell_num=snapshot.sell_num,
                    fetched_at=snapshot.fetched_at,
                    source=snapshot.source,
                )
            )
    return tuple(rows)


def build_steamdt_item_platform_detail_rows(
    *,
    snapshot_store: SteamDTPriceSnapshotStore,
    catalog: ItemCatalog,
    item_names: Sequence[str] | None = None,
    prefer_cleaned: bool = False,
    valid_only: bool = False,
) -> tuple[SteamDTItemPlatformDetailRow, ...]:
    resolved_item_names = (
        tuple(_dedupe_preserve_order(item_names))
        if item_names is not None
        else tuple(item.name for item in catalog.all_items())
    )
    rows: list[SteamDTItemPlatformDetailRow] = []
    for base_item_name in resolved_item_names:
        try:
            item_definition = catalog.get_item(base_item_name)
        except KeyError:
            continue
        snapshots = snapshot_store.get_latest_snapshots_for_item_family(
            base_item_name,
            prefer_cleaned=prefer_cleaned,
            require_valid=valid_only,
        )
        for snapshot in _sort_snapshots_for_export(snapshots):
            _, variant = split_item_variant_name(snapshot.item_name)
            rows.append(
                SteamDTItemPlatformDetailRow(
                    base_item_name=base_item_name,
                    variant=variant.value,
                    item_name=snapshot.item_name,
                    market_hash_name=snapshot.market_hash_name,
                    exterior=snapshot.exterior,
                    collection=item_definition.collection,
                    rarity=int(item_definition.rarity),
                    rarity_name=item_definition.rarity.name,
                    min_float=item_definition.min_float,
                    max_float=item_definition.max_float,
                    lowest_price=snapshot.lowest_price,
                    safe_price=snapshot.safe_price,
                    is_valid=snapshot.is_valid,
                    risk_level=snapshot.risk_level,
                    anomaly_flags=snapshot.anomaly_flags,
                    anomaly_notes=snapshot.anomaly_notes,
                    recent_average_price=snapshot.recent_average_price,
                    highest_buy_price=snapshot.highest_buy_price,
                    sell_num=snapshot.sell_num,
                    fetched_at=snapshot.fetched_at,
                    source=snapshot.source,
                    platform_prices=_extract_platform_price_views(snapshot),
                )
            )
    return tuple(rows)


def translate_item_name_zh_cn(item_name: str) -> str:
    translated_dynamic = _dynamic_translate_item_name_zh_cn(item_name)
    if translated_dynamic != str(item_name).strip():
        return translated_dynamic
    normalized_item_name = str(item_name).strip()
    if not normalized_item_name:
        return normalized_item_name
    base_item_name, variant = split_item_variant_name(normalized_item_name)
    if " | " not in base_item_name:
        translated = base_item_name
    else:
        weapon_name, finish_name = base_item_name.split(" | ", 1)
        translated_weapon_name = WEAPON_NAME_ZH_CN_MAP.get(weapon_name, weapon_name)
        translated_finish_name = FINISH_NAME_ZH_CN_MAP.get(finish_name, finish_name)
        translated = f"{translated_weapon_name} | {translated_finish_name}"
    if variant is ItemVariant.STATTRAK:
        return f"{translate_variant_zh_cn(ItemVariant.STATTRAK.value)} {translated}"
    return translated


def translate_exterior_zh_cn(exterior: str | None) -> str:
    translated_dynamic = _dynamic_translate_exterior_zh_cn(exterior)
    if exterior is None or translated_dynamic != exterior:
        return translated_dynamic
    return EXTERIOR_ZH_CN_MAP.get(exterior, exterior)


def translate_variant_zh_cn(variant: str) -> str:
    translated_dynamic = _dynamic_translate_variant_zh_cn(variant)
    if translated_dynamic != variant:
        return translated_dynamic
    return VARIANT_ZH_CN_MAP.get(variant, variant)


def translate_rarity_zh_cn(rarity_name: str) -> str:
    translated_dynamic = _dynamic_translate_rarity_zh_cn(rarity_name)
    if translated_dynamic != rarity_name:
        return translated_dynamic
    return RARITY_ZH_CN_MAP.get(rarity_name, rarity_name)


def translate_collection_zh_cn(collection_name: str) -> str:
    translated_dynamic = _dynamic_translate_collection_zh_cn(collection_name)
    if translated_dynamic != collection_name:
        return translated_dynamic
    return COLLECTION_ZH_CN_MAP.get(collection_name, collection_name)


def _sort_snapshots_for_export(
    snapshots: Sequence[SteamDTPriceSnapshot],
) -> tuple[SteamDTPriceSnapshot, ...]:
    variant_rank = {
        ItemVariant.NORMAL.value: 0,
        ItemVariant.STATTRAK.value: 1,
    }
    exterior_rank = {exterior.value: index for index, exterior in enumerate(Exterior.ordered())}
    return tuple(
        sorted(
            snapshots,
            key=lambda snapshot: (
                variant_rank.get(
                    split_item_variant_name(snapshot.item_name)[1].value,
                    len(variant_rank),
                ),
                exterior_rank.get(snapshot.exterior or "", len(exterior_rank)),
                snapshot.item_name,
            ),
        )
    )


def _extract_platform_price_views(
    snapshot: SteamDTPriceSnapshot,
) -> tuple[SteamDTPlatformPriceView, ...]:
    raw_payload = {}
    if snapshot.raw_json:
        try:
            raw_payload = json.loads(snapshot.raw_json)
        except ValueError:
            raw_payload = {}
    raw_prices = raw_payload.get("sellingPriceList")
    by_platform: dict[str, SteamDTPlatformPriceView] = {}
    if isinstance(raw_prices, list):
        for entry in raw_prices:
            if not isinstance(entry, dict):
                continue
            platform = str(entry.get("platform") or "").strip().lower()
            if not platform:
                continue
            platform_name = str(entry.get("platformName") or platform).strip()
            price_value = entry.get("price")
            price = float(price_value) if isinstance(price_value, (int, float)) else None
            if price is None and price_value not in (None, ""):
                try:
                    price = float(str(price_value))
                except ValueError:
                    price = None
            by_platform[platform] = SteamDTPlatformPriceView(
                platform=platform,
                platform_name=platform_name,
                price=price,
                link=(str(entry["link"]) if entry.get("link") else None),
            )
    ordered_views: list[SteamDTPlatformPriceView] = []
    consumed: set[str] = set()
    for platform, platform_name in STEAMDT_PLATFORM_EXPORT_ORDER:
        view = by_platform.get(platform)
        if view is None:
            ordered_views.append(
                SteamDTPlatformPriceView(
                    platform=platform,
                    platform_name=platform_name,
                    price=None,
                    link=None,
                )
            )
        else:
            ordered_views.append(view)
            consumed.add(platform)
    for platform, view in sorted(by_platform.items()):
        if platform in consumed:
            continue
        ordered_views.append(view)
    return tuple(ordered_views)


def _build_platform_dashboard_html(
    *,
    rows: Sequence[dict[str, object]],
    title: str,
) -> str:
    payload_json = json.dumps(rows, ensure_ascii=False)
    safe_title = html.escape(title)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{safe_title}</title>
  <style>
    :root {{
      --bg: #0b1020;
      --panel: #131a2b;
      --panel-2: #182033;
      --text: #e8edf7;
      --muted: #9aa7bd;
      --accent: #4da3ff;
      --accent-2: #7cf29a;
      --border: #24304a;
      --danger: #ff7676;
      --warning: #ffd166;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      background: linear-gradient(180deg, #0a0f1d 0%, #111828 100%);
      color: var(--text);
    }}
    .wrap {{
      max-width: 1680px;
      margin: 0 auto;
      padding: 20px;
    }}
    .hero {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 18px;
    }}
    .hero h1 {{
      margin: 0;
      font-size: 28px;
    }}
    .hero p {{
      margin: 6px 0 0;
      color: var(--muted);
    }}
    .stats {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .stat {{
      background: rgba(255,255,255,0.04);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 14px;
      min-width: 120px;
    }}
    .stat .label {{
      color: var(--muted);
      font-size: 12px;
    }}
    .stat .value {{
      font-size: 22px;
      font-weight: 700;
      margin-top: 4px;
    }}
    .toolbar {{
      display: grid;
      grid-template-columns: 2fr 1fr 1fr 1fr;
      gap: 12px;
      margin-bottom: 14px;
    }}
    .toolbar input, .toolbar select {{
      width: 100%;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: var(--panel);
      color: var(--text);
      outline: none;
    }}
    .table-shell {{
      background: rgba(19, 26, 43, 0.88);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 10px 30px rgba(0,0,0,0.25);
    }}
    .table-scroll {{
      overflow: auto;
      max-height: 78vh;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 1540px;
    }}
    thead th {{
      position: sticky;
      top: 0;
      background: #11192a;
      color: #cfe0ff;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      z-index: 2;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid rgba(255,255,255,0.06);
      white-space: nowrap;
      text-align: left;
      font-size: 13px;
    }}
    tbody tr:hover {{
      background: rgba(255,255,255,0.03);
    }}
    .item {{
      font-weight: 600;
      color: #ffffff;
    }}
    .sub {{
      color: var(--muted);
      font-size: 12px;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(77,163,255,0.12);
      border: 1px solid rgba(77,163,255,0.24);
      color: #cfe0ff;
      font-size: 12px;
    }}
    .chip.variant-stattrak {{
      background: rgba(255, 209, 102, 0.12);
      border-color: rgba(255, 209, 102, 0.22);
      color: #ffe39b;
    }}
    .price {{
      font-variant-numeric: tabular-nums;
    }}
    .price.best {{
      color: var(--accent-2);
      font-weight: 700;
    }}
    .price.zero {{
      color: #66738a;
    }}
    .small-link {{
      color: var(--accent);
      text-decoration: none;
      font-size: 11px;
    }}
    .footer {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 12px;
    }}
    @media (max-width: 960px) {{
      .toolbar {{
        grid-template-columns: 1fr;
      }}
      .hero {{
        flex-direction: column;
        align-items: flex-start;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <h1>{safe_title}</h1>
        <p>按中文枪名、品质、磨损和平台价格查看。绿色价格表示该行当前已知最低非零报价。</p>
      </div>
      <div class="stats">
        <div class="stat"><div class="label">行数</div><div class="value" id="stat-rows">0</div></div>
        <div class="stat"><div class="label">枪械数</div><div class="value" id="stat-items">0</div></div>
        <div class="stat"><div class="label">普通数</div><div class="value" id="stat-normal">0</div></div>
        <div class="stat"><div class="label">暗金数</div><div class="value" id="stat-st">0</div></div>
      </div>
    </div>

    <div class="toolbar">
      <input id="search" type="text" placeholder="搜索中文枪名，例如 皇后 / 暴怒野兽 / 久经沙场" />
      <select id="variant">
        <option value="">全部变体</option>
        <option value="Normal">普通</option>
        <option value="StatTrak">暗金</option>
      </select>
      <select id="exterior">
        <option value="">全部外观</option>
        <option value="Factory New">崭新出厂</option>
        <option value="Minimal Wear">略有磨损</option>
        <option value="Field-Tested">久经沙场</option>
        <option value="Well-Worn">破损不堪</option>
        <option value="Battle-Scarred">战痕累累</option>
      </select>
      <select id="sort">
        <option value="item">按中文枪名排序</option>
        <option value="lowest">按最低价从低到高</option>
      </select>
    </div>

    <div class="table-shell">
      <div class="table-scroll">
        <table>
          <thead id="thead"></thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>
    </div>
    <div class="footer">本页面为本地静态 HTML，可直接双击打开；数据来自当前 SQLite 快照。</div>
  </div>

  <script>
    const rows = {payload_json};
    const platformOrder = {json.dumps([name for _, name in STEAMDT_PLATFORM_EXPORT_ORDER], ensure_ascii=False)};
    const platformLabels = {json.dumps(PLATFORM_HTML_LABELS_ZH_CN, ensure_ascii=False)};

    const searchInput = document.getElementById('search');
    const variantSelect = document.getElementById('variant');
    const exteriorSelect = document.getElementById('exterior');
    const sortSelect = document.getElementById('sort');
    const thead = document.getElementById('thead');
    const tbody = document.getElementById('tbody');

    function formatPrice(value) {{
      if (value === null || value === undefined) return '-';
      return Number(value).toLocaleString('zh-CN', {{ minimumFractionDigits: 0, maximumFractionDigits: 2 }});
    }}

    function getVisiblePlatforms(sourceRows) {{
      return platformOrder.filter((platformName) => {{
        return sourceRows.some((row) => (row.platform_prices || []).some((entry) => {{
          return entry.platform_name === platformName && entry.price !== null && entry.price !== undefined && Number(entry.price) > 0;
        }}));
      }});
    }}

    function renderHeader(visiblePlatforms) {{
      const platformHeaders = visiblePlatforms.map((platformName) => `<th>${{platformLabels[platformName] || platformName}}</th>`).join('');
      thead.innerHTML = `
        <tr>
          <th>饰品</th>
          <th>品质</th>
          <th>磨损</th>
          <th>最低价</th>
          ${{platformHeaders}}
          <th>近均价</th>
          <th>在售数</th>
          <th>更新时间</th>
        </tr>
      `;
    }}

    const visiblePlatforms = getVisiblePlatforms(rows);

    function filterRows() {{
        const keyword = searchInput.value.trim().toLowerCase();
        const variant = variantSelect.value;
        const exterior = exteriorSelect.value;
        const sort = sortSelect.value;

        let filtered = rows.filter((row) => {{
          const haystack = [
            row.base_item_name,
            row.base_item_name_zh,
            row.item_name,
            row.item_name_zh,
            row.market_hash_name,
            row.exterior,
            row.exterior_zh,
            row.variant,
            row.variant_zh,
            row.rarity_name,
            row.rarity_name_zh,
          ].join(' ').toLowerCase();
        if (keyword && !haystack.includes(keyword)) return false;
        if (variant && row.variant !== variant) return false;
        if (exterior && row.exterior !== exterior) return false;
        return true;
      }});

      if (sort === 'lowest') {{
        filtered.sort((a, b) => (a.lowest_price ?? Number.MAX_VALUE) - (b.lowest_price ?? Number.MAX_VALUE));
      }} else {{
        filtered.sort((a, b) => {{
          return a.base_item_name_zh.localeCompare(b.base_item_name_zh, 'zh-CN') ||
                 a.variant_zh.localeCompare(b.variant_zh, 'zh-CN') ||
                 String(a.exterior_zh || '').localeCompare(String(b.exterior_zh || ''), 'zh-CN');
        }});
      }}

      renderRows(filtered, visiblePlatforms);
      updateStats(filtered);
    }}

    function updateStats(filtered) {{
      document.getElementById('stat-rows').textContent = filtered.length;
      document.getElementById('stat-items').textContent = new Set(filtered.map((row) => row.base_item_name)).size;
      document.getElementById('stat-normal').textContent = filtered.filter((row) => row.variant === 'Normal').length;
      document.getElementById('stat-st').textContent = filtered.filter((row) => row.variant === 'StatTrak').length;
    }}

    function renderPlatformCell(platformEntry, bestPrice) {{
      if (!platformEntry || platformEntry.price === null || platformEntry.price === 0) {{
        return '<td class=\"price zero\">-</td>';
      }}
      const bestClass = platformEntry.price === bestPrice ? 'best' : '';
      const text = formatPrice(platformEntry.price);
      const link = platformEntry.link ? `<div><a class=\"small-link\" href=\"${{platformEntry.link}}\" target=\"_blank\">打开</a></div>` : '';
      return `<td class=\"price ${{bestClass}}\">${{text}}${{link}}</td>`;
    }}

    function renderRows(filtered, visiblePlatforms) {{
        const htmlRows = filtered.map((row) => {{
        const platformLookup = Object.fromEntries((row.platform_prices || []).map((entry) => [entry.platform_name, entry]));
        const positivePrices = (row.platform_prices || [])
          .map((entry) => entry.price)
          .filter((price) => price !== null && price !== undefined && Number(price) > 0);
        const bestPrice = positivePrices.length ? Math.min(...positivePrices) : null;
            const variantClass = row.variant === 'StatTrak' ? 'variant-stattrak' : '';
            const platformCells = visiblePlatforms.map((platformName) => renderPlatformCell(platformLookup[platformName], bestPrice)).join('');
            return `
          <tr>
            <td>
              <div class=\"item\" title=\"${{row.base_item_name}}\">${{row.base_item_name_zh}}</div>
              <div class=\"sub\">${{row.collection_zh}} · ${{row.rarity_name_zh}}</div>
            </td>
            <td><span class=\"chip ${{variantClass}}\">${{row.variant_zh}}</span></td>
            <td>${{row.exterior_zh || '-'}}</td>
            <td class=\"price\">${{formatPrice(row.lowest_price)}}</td>
            ${{platformCells}}
            <td class=\"price\">${{formatPrice(row.recent_average_price)}}</td>
            <td>${{row.sell_num ?? '-'}}</td>
            <td>${{row.fetched_at || '-'}}</td>
          </tr>
        `;
      }}).join('');
      const colspan = 7 + visiblePlatforms.length;
      tbody.innerHTML = htmlRows || `<tr><td colspan="${{colspan}}" style="text-align:center;color:#9aa7bd;padding:24px;">没有匹配数据</td></tr>`;
    }}

    [searchInput, variantSelect, exteriorSelect, sortSelect].forEach((element) => {{
      element.addEventListener('input', filterRows);
      element.addEventListener('change', filterRows);
    }});

    renderHeader(visiblePlatforms);
    filterRows();
  </script>
</body>
</html>"""


__all__ = [
    "FIREARM_WEAPON_NAMES",
    "MARKET_DERIVED_COLLECTION",
    "SteamDTCatalogSyncSummary",
    "SteamDTItemPriceDetailRow",
    "SteamDTItemPlatformDetailRow",
    "SteamDTPlatformPriceView",
    "build_item_definition_from_steamdt_snapshots",
    "build_steamdt_item_price_detail_rows",
    "build_steamdt_item_platform_detail_rows",
    "discover_steamdt_firearm_item_names",
    "export_steamdt_item_price_details_csv",
    "export_steamdt_item_platform_prices_csv",
    "export_steamdt_item_platform_prices_html",
    "infer_float_bounds_from_exteriors",
    "is_firearm_item_name",
    "sync_steamdt_items_to_catalog",
    "translate_exterior_zh_cn",
    "translate_item_name_zh_cn",
    "translate_rarity_zh_cn",
    "translate_variant_zh_cn",
]
