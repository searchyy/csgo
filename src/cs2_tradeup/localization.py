from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests

from .market import split_item_variant_name
from .models import ItemVariant


BYMYKEL_EN_SKINS_URL = (
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/skins.json"
)
BYMYKEL_EN_COLLECTIONS_URL = (
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/collections.json"
)
BYMYKEL_ZH_SKINS_URL = (
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/zh-CN/skins.json"
)
BYMYKEL_ZH_COLLECTIONS_URL = (
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/zh-CN/collections.json"
)
DEFAULT_LOCALIZATION_PATH = Path("data") / "localization.zh-CN.json"

_RARITY_ZH_FALLBACK = {
    "CONSUMER_GRADE": "消费级",
    "Consumer Grade": "消费级",
    "INDUSTRIAL_GRADE": "工业级",
    "Industrial Grade": "工业级",
    "MIL_SPEC": "军规级",
    "Mil-Spec Grade": "军规级",
    "RESTRICTED": "受限",
    "Restricted": "受限",
    "CLASSIFIED": "保密",
    "Classified": "保密",
    "COVERT": "隐秘",
    "Covert": "隐秘",
}
_EXTERIOR_ZH_FALLBACK = {
    "Factory New": "崭新出厂",
    "Minimal Wear": "略有磨损",
    "Field-Tested": "久经沙场",
    "Well-Worn": "破损不堪",
    "Battle-Scarred": "战痕累累",
}
_VARIANT_ZH_FALLBACK = {
    "Normal": "普通",
    "StatTrak": "暗金",
}

_DEFAULT_INDEX: "LocalizationIndex | None" = None


@dataclass(frozen=True, slots=True)
class LocalizationIndex:
    item_names: dict[str, str]
    collection_names: dict[str, str]
    rarity_names: dict[str, str]
    exterior_names: dict[str, str]
    variant_names: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_names": self.item_names,
            "collection_names": self.collection_names,
            "rarity_names": self.rarity_names,
            "exterior_names": self.exterior_names,
            "variant_names": self.variant_names,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "LocalizationIndex":
        return cls(
            item_names=_normalize_string_mapping(payload.get("item_names")),
            collection_names=_normalize_string_mapping(payload.get("collection_names")),
            rarity_names={**_RARITY_ZH_FALLBACK, **_normalize_string_mapping(payload.get("rarity_names"))},
            exterior_names={**_EXTERIOR_ZH_FALLBACK, **_normalize_string_mapping(payload.get("exterior_names"))},
            variant_names={**_VARIANT_ZH_FALLBACK, **_normalize_string_mapping(payload.get("variant_names"))},
        )


def fetch_bymykel_localization_payloads(
    *,
    timeout: float = 30.0,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    owns_session = session is None
    resolved_session = session or requests.Session()
    try:
        en_skins = _fetch_json_list(resolved_session, BYMYKEL_EN_SKINS_URL, timeout)
        zh_skins = _fetch_json_list(resolved_session, BYMYKEL_ZH_SKINS_URL, timeout)
        en_collections = _fetch_json_list(resolved_session, BYMYKEL_EN_COLLECTIONS_URL, timeout)
        zh_collections = _fetch_json_list(resolved_session, BYMYKEL_ZH_COLLECTIONS_URL, timeout)
        return en_skins, zh_skins, en_collections, zh_collections
    finally:
        if owns_session:
            resolved_session.close()


def build_localization_index(
    en_skins: Iterable[Mapping[str, Any]],
    zh_skins: Iterable[Mapping[str, Any]],
    en_collections: Iterable[Mapping[str, Any]],
    zh_collections: Iterable[Mapping[str, Any]],
) -> LocalizationIndex:
    en_skins_by_id = {
        str(row["id"]): row
        for row in en_skins
        if isinstance(row, Mapping) and row.get("id")
    }
    zh_skins_by_id = {
        str(row["id"]): row
        for row in zh_skins
        if isinstance(row, Mapping) and row.get("id")
    }
    item_names: dict[str, str] = {}
    rarity_names = dict(_RARITY_ZH_FALLBACK)
    exterior_names = dict(_EXTERIOR_ZH_FALLBACK)
    variant_names = dict(_VARIANT_ZH_FALLBACK)

    for skin_id, en_row in en_skins_by_id.items():
        zh_row = zh_skins_by_id.get(skin_id)
        if zh_row is None:
            continue
        en_name = _extract_name(en_row.get("name"))
        zh_name = _extract_name(zh_row.get("name"))
        if en_name and zh_name:
            item_names[en_name] = zh_name

        en_rarity = _extract_name(en_row.get("rarity"))
        zh_rarity = _extract_name(zh_row.get("rarity"))
        if en_rarity and zh_rarity:
            rarity_names[en_rarity] = zh_rarity

        for en_wear, zh_wear in zip(
            _extract_name_list(en_row.get("wears")),
            _extract_name_list(zh_row.get("wears")),
            strict=False,
        ):
            if en_wear and zh_wear:
                exterior_names[en_wear] = zh_wear

    en_collections_by_id = {
        str(row["id"]): row
        for row in en_collections
        if isinstance(row, Mapping) and row.get("id")
    }
    zh_collections_by_id = {
        str(row["id"]): row
        for row in zh_collections
        if isinstance(row, Mapping) and row.get("id")
    }
    collection_names: dict[str, str] = {}
    for collection_id, en_row in en_collections_by_id.items():
        zh_row = zh_collections_by_id.get(collection_id)
        if zh_row is None:
            continue
        en_name = _extract_name(en_row.get("name"))
        zh_name = _extract_name(zh_row.get("name"))
        if en_name and zh_name:
            collection_names[en_name] = zh_name

    return LocalizationIndex(
        item_names=item_names,
        collection_names=collection_names,
        rarity_names=rarity_names,
        exterior_names=exterior_names,
        variant_names=variant_names,
    )


def sync_bymykel_localization_cache(
    *,
    output_path: str | Path = DEFAULT_LOCALIZATION_PATH,
    timeout: float = 30.0,
    session: requests.Session | None = None,
) -> Path:
    index = build_localization_index(*fetch_bymykel_localization_payloads(timeout=timeout, session=session))
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(index.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_localization_index(
    path: str | Path = DEFAULT_LOCALIZATION_PATH,
) -> LocalizationIndex:
    file_path = Path(path)
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Localization file must be a JSON object")
    return LocalizationIndex.from_dict(payload)


def get_default_localization_index(
    *,
    path: str | Path = DEFAULT_LOCALIZATION_PATH,
    auto_sync_if_missing: bool = False,
) -> LocalizationIndex:
    global _DEFAULT_INDEX
    if _DEFAULT_INDEX is not None:
        return _DEFAULT_INDEX

    file_path = Path(path)
    if not file_path.exists() and auto_sync_if_missing:
        try:
            sync_bymykel_localization_cache(output_path=file_path)
        except Exception:
            _DEFAULT_INDEX = LocalizationIndex(
                item_names={},
                collection_names={},
                rarity_names=dict(_RARITY_ZH_FALLBACK),
                exterior_names=dict(_EXTERIOR_ZH_FALLBACK),
                variant_names=dict(_VARIANT_ZH_FALLBACK),
            )
            return _DEFAULT_INDEX

    if file_path.exists():
        try:
            _DEFAULT_INDEX = load_localization_index(file_path)
        except Exception:
            _DEFAULT_INDEX = LocalizationIndex(
                item_names={},
                collection_names={},
                rarity_names=dict(_RARITY_ZH_FALLBACK),
                exterior_names=dict(_EXTERIOR_ZH_FALLBACK),
                variant_names=dict(_VARIANT_ZH_FALLBACK),
            )
    else:
        _DEFAULT_INDEX = LocalizationIndex(
            item_names={},
            collection_names={},
            rarity_names=dict(_RARITY_ZH_FALLBACK),
            exterior_names=dict(_EXTERIOR_ZH_FALLBACK),
            variant_names=dict(_VARIANT_ZH_FALLBACK),
        )
    return _DEFAULT_INDEX


def translate_item_name_zh_cn(
    item_name: str,
    *,
    index: LocalizationIndex | None = None,
) -> str:
    localized_index = index or get_default_localization_index()
    normalized_item_name = str(item_name).strip()
    if not normalized_item_name:
        return normalized_item_name

    base_item_name, variant = split_item_variant_name(normalized_item_name)
    translated_base = localized_index.item_names.get(base_item_name, base_item_name)
    if variant is ItemVariant.STATTRAK:
        return f"{localized_index.variant_names.get(ItemVariant.STATTRAK.value, '暗金')} {translated_base}"
    return translated_base


def translate_collection_zh_cn(
    collection_name: str,
    *,
    index: LocalizationIndex | None = None,
) -> str:
    localized_index = index or get_default_localization_index()
    normalized = str(collection_name).strip()
    if not normalized:
        return normalized
    return localized_index.collection_names.get(normalized, normalized)


def translate_rarity_zh_cn(
    rarity_name: str,
    *,
    index: LocalizationIndex | None = None,
) -> str:
    localized_index = index or get_default_localization_index()
    normalized = str(rarity_name).strip()
    if not normalized:
        return normalized
    return localized_index.rarity_names.get(normalized, normalized)


def translate_exterior_zh_cn(
    exterior_name: str | None,
    *,
    index: LocalizationIndex | None = None,
) -> str:
    if exterior_name is None:
        return "-"
    localized_index = index or get_default_localization_index()
    normalized = str(exterior_name).strip()
    if not normalized:
        return "-"
    return localized_index.exterior_names.get(normalized, normalized)


def translate_variant_zh_cn(
    variant_name: str,
    *,
    index: LocalizationIndex | None = None,
) -> str:
    localized_index = index or get_default_localization_index()
    normalized = str(variant_name).strip()
    if not normalized:
        return normalized
    return localized_index.variant_names.get(normalized, normalized)


def _fetch_json_list(session: requests.Session, url: str, timeout: float) -> list[dict[str, Any]]:
    response = session.get(
        url,
        timeout=timeout,
        headers={
            "Accept": "application/json",
            "User-Agent": "cs2-tradeup-localization-sync/0.1",
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON list from {url}")
    return [dict(entry) for entry in payload if isinstance(entry, Mapping)]


def _extract_name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        nested = value.get("name")
        if nested in (None, ""):
            return None
        return str(nested).strip()
    if value in (None, ""):
        return None
    return str(value).strip()


def _extract_name_list(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, Mapping)):
        return ()
    result: list[str] = []
    for entry in value:
        name = _extract_name(entry)
        if name:
            result.append(name)
    return tuple(result)


def _normalize_string_mapping(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, str] = {}
    for key, item in value.items():
        if key in (None, "") or item in (None, ""):
            continue
        result[str(key)] = str(item)
    return result


__all__ = [
    "BYMYKEL_EN_COLLECTIONS_URL",
    "BYMYKEL_EN_SKINS_URL",
    "BYMYKEL_ZH_COLLECTIONS_URL",
    "BYMYKEL_ZH_SKINS_URL",
    "DEFAULT_LOCALIZATION_PATH",
    "LocalizationIndex",
    "build_localization_index",
    "fetch_bymykel_localization_payloads",
    "get_default_localization_index",
    "load_localization_index",
    "sync_bymykel_localization_cache",
    "translate_collection_zh_cn",
    "translate_exterior_zh_cn",
    "translate_item_name_zh_cn",
    "translate_rarity_zh_cn",
    "translate_variant_zh_cn",
]
