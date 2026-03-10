from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Iterable


class Rarity(IntEnum):
    CONSUMER_GRADE = 1
    INDUSTRIAL_GRADE = 2
    MIL_SPEC = 3
    RESTRICTED = 4
    CLASSIFIED = 5
    COVERT = 6

    @classmethod
    def from_value(cls, value: "Rarity | int | str") -> "Rarity":
        if isinstance(value, cls):
            return value
        if isinstance(value, int):
            return cls(value)

        text = str(value).strip()
        if text.isdigit():
            return cls(int(text))
        return cls.from_string(text)

    @classmethod
    def from_string(cls, value: str) -> "Rarity":
        normalized = value.strip().lower().replace("-", " ").replace("_", " ")
        aliases = {
            "consumer": cls.CONSUMER_GRADE,
            "consumer grade": cls.CONSUMER_GRADE,
            "white": cls.CONSUMER_GRADE,
            "消费级": cls.CONSUMER_GRADE,
            "industrial": cls.INDUSTRIAL_GRADE,
            "industrial grade": cls.INDUSTRIAL_GRADE,
            "light blue": cls.INDUSTRIAL_GRADE,
            "工业级": cls.INDUSTRIAL_GRADE,
            "mil spec": cls.MIL_SPEC,
            "mil spec grade": cls.MIL_SPEC,
            "milspec": cls.MIL_SPEC,
            "blue": cls.MIL_SPEC,
            "军规级": cls.MIL_SPEC,
            "restricted": cls.RESTRICTED,
            "purple": cls.RESTRICTED,
            "受限": cls.RESTRICTED,
            "classified": cls.CLASSIFIED,
            "pink": cls.CLASSIFIED,
            "保密": cls.CLASSIFIED,
            "covert": cls.COVERT,
            "red": cls.COVERT,
            "隐秘": cls.COVERT,
        }
        if normalized not in aliases:
            raise ValueError(f"Unknown rarity: {value!r}")
        return aliases[normalized]

    def next_rarity(self) -> "Rarity":
        try:
            return Rarity(self + 1)
        except ValueError as error:
            raise ValueError(f"{self.name} does not have a higher rarity tier") from error


class Exterior(str, Enum):
    FACTORY_NEW = "Factory New"
    MINIMAL_WEAR = "Minimal Wear"
    FIELD_TESTED = "Field-Tested"
    WELL_WORN = "Well-Worn"
    BATTLE_SCARRED = "Battle-Scarred"

    @classmethod
    def from_label(cls, label: str) -> "Exterior":
        normalized = label.strip().lower().replace("_", " ").replace("-", " ")
        aliases = {
            "factory new": cls.FACTORY_NEW,
            "fn": cls.FACTORY_NEW,
            "崭新出厂": cls.FACTORY_NEW,
            "minimal wear": cls.MINIMAL_WEAR,
            "mw": cls.MINIMAL_WEAR,
            "略有磨损": cls.MINIMAL_WEAR,
            "field tested": cls.FIELD_TESTED,
            "ft": cls.FIELD_TESTED,
            "久经沙场": cls.FIELD_TESTED,
            "well worn": cls.WELL_WORN,
            "ww": cls.WELL_WORN,
            "破损不堪": cls.WELL_WORN,
            "battle scarred": cls.BATTLE_SCARRED,
            "bs": cls.BATTLE_SCARRED,
            "战痕累累": cls.BATTLE_SCARRED,
        }
        if normalized not in aliases:
            raise ValueError(f"Unknown exterior label: {label!r}")
        return aliases[normalized]

    @classmethod
    def from_float(cls, float_value: float) -> "Exterior":
        if not 0.0 <= float_value <= 1.0:
            raise ValueError(f"Exterior float must be within [0, 1], got {float_value}")
        if float_value < 0.07:
            return cls.FACTORY_NEW
        if float_value < 0.15:
            return cls.MINIMAL_WEAR
        if float_value < 0.38:
            return cls.FIELD_TESTED
        if float_value < 0.45:
            return cls.WELL_WORN
        return cls.BATTLE_SCARRED

    @classmethod
    def ordered(cls) -> tuple["Exterior", ...]:
        return (
            cls.FACTORY_NEW,
            cls.MINIMAL_WEAR,
            cls.FIELD_TESTED,
            cls.WELL_WORN,
            cls.BATTLE_SCARRED,
        )

    @property
    def float_bounds(self) -> tuple[float, float]:
        bounds = {
            self.FACTORY_NEW: (0.00, 0.07),
            self.MINIMAL_WEAR: (0.07, 0.15),
            self.FIELD_TESTED: (0.15, 0.38),
            self.WELL_WORN: (0.38, 0.45),
            self.BATTLE_SCARRED: (0.45, 1.00),
        }
        return bounds[self]

    def overlaps_float_range(self, min_float: float, max_float: float) -> bool:
        lower_bound, upper_bound = self.float_bounds
        effective_upper = (
            upper_bound
            if self is Exterior.BATTLE_SCARRED
            else math.nextafter(upper_bound, lower_bound)
        )
        return max_float >= lower_bound and min_float <= effective_upper


class ItemVariant(str, Enum):
    NORMAL = "Normal"
    STATTRAK = "StatTrak"

    @classmethod
    def from_value(cls, value: "ItemVariant | str") -> "ItemVariant":
        if isinstance(value, cls):
            return value
        normalized = (
            str(value)
            .strip()
            .lower()
            .replace("™", "")
            .replace("_", " ")
            .replace("-", " ")
        )
        aliases = {
            "normal": cls.NORMAL,
            "default": cls.NORMAL,
            "standard": cls.NORMAL,
            "ordinary": cls.NORMAL,
            "common": cls.NORMAL,
            "普通": cls.NORMAL,
            "stattrak": cls.STATTRAK,
            "stat trak": cls.STATTRAK,
            "stattrack": cls.STATTRAK,
            "startrrk": cls.STATTRAK,
            "st": cls.STATTRAK,
        }
        if normalized not in aliases:
            raise ValueError(f"Unknown item variant: {value!r}")
        return aliases[normalized]


@dataclass(frozen=True, slots=True)
class ItemDefinition:
    name: str
    collection: str
    rarity: Rarity
    min_float: float
    max_float: float
    available_variants: tuple[ItemVariant, ...] = ()
    available_exteriors: tuple[Exterior, ...] = ()

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Item name cannot be empty")
        if not self.collection:
            raise ValueError("Collection cannot be empty")
        if not 0.0 <= self.min_float <= self.max_float <= 1.0:
            raise ValueError(
                "Float bounds must satisfy 0.0 <= min_float <= max_float <= 1.0"
            )
        normalized_variants = self._normalize_variants(self.available_variants)
        normalized_exteriors = self._normalize_exteriors(self.available_exteriors)
        object.__setattr__(self, "available_variants", normalized_variants)
        object.__setattr__(self, "available_exteriors", normalized_exteriors)

    @property
    def float_range(self) -> float:
        return self.max_float - self.min_float

    @property
    def supports_stattrak(self) -> bool:
        return ItemVariant.STATTRAK in self.available_variants

    def wear_position(self, float_value: float) -> float:
        if not self.min_float <= float_value <= self.max_float:
            raise ValueError(
                f"Float {float_value} is outside {self.name} range "
                f"[{self.min_float}, {self.max_float}]"
            )
        if self.float_range == 0:
            return 0.0
        return (float_value - self.min_float) / self.float_range

    def supports_variant(self, variant: ItemVariant | str) -> bool:
        resolved_variant = ItemVariant.from_value(variant)
        return resolved_variant in self.available_variants

    def supports_exterior(self, exterior: Exterior | str) -> bool:
        resolved_exterior = exterior if isinstance(exterior, Exterior) else Exterior.from_label(exterior)
        return resolved_exterior in self.available_exteriors

    def build_market_name(
        self,
        exterior: Exterior | str | None = None,
        *,
        variant: ItemVariant | str = ItemVariant.NORMAL,
    ) -> str:
        resolved_variant = ItemVariant.from_value(variant)
        if not self.supports_variant(resolved_variant):
            raise ValueError(f"{self.name} does not support variant {resolved_variant.value}")

        market_name = self.name
        if resolved_variant is ItemVariant.STATTRAK:
            market_name = f"StatTrak™ {market_name}"

        if exterior is None:
            return market_name

        resolved_exterior = exterior if isinstance(exterior, Exterior) else Exterior.from_label(exterior)
        if not self.supports_exterior(resolved_exterior):
            raise ValueError(
                f"{self.name} does not support exterior {resolved_exterior.value}"
            )
        return f"{market_name} ({resolved_exterior.value})"

    @classmethod
    def from_dict(cls, data: dict) -> "ItemDefinition":
        aliases = {
            "name": ("name", "item_name", "market_hash_name", "full_name", "weapon_name"),
            "collection": (
                "collection",
                "collection_name",
                "set_name",
                "crate",
                "case",
            ),
            "rarity": ("rarity", "rarity_name", "grade", "quality"),
            "min_float": (
                "min_float",
                "wear_min",
                "min_wear",
                "wearmin",
                "float_min",
            ),
            "max_float": (
                "max_float",
                "wear_max",
                "max_wear",
                "wearmax",
                "float_max",
            ),
        }

        def resolve(field_name: str):
            for key in aliases[field_name]:
                if key in data and data[key] not in (None, ""):
                    return data[key]
            raise KeyError(field_name)

        def resolve_optional(*keys: str) -> Any | None:
            for key in keys:
                if key in data and data[key] not in (None, ""):
                    return data[key]
            return None

        rarity = Rarity.from_value(resolve("rarity"))
        explicit_variants = cls._parse_variants(
            resolve_optional("variants", "available_variants", "item_variants")
        )
        supports_stattrak = cls._parse_optional_bool(
            resolve_optional("supports_stattrak", "has_stattrak", "stattrak")
        )
        available_variants = explicit_variants or cls._default_variants(supports_stattrak)
        available_exteriors = cls._parse_exteriors(
            resolve_optional("exteriors", "available_exteriors", "wears", "wear_tiers")
        )
        return cls(
            name=str(resolve("name")),
            collection=str(resolve("collection")),
            rarity=rarity,
            min_float=float(resolve("min_float")),
            max_float=float(resolve("max_float")),
            available_variants=available_variants,
            available_exteriors=available_exteriors,
        )

    def to_dict(self) -> dict[str, float | int | str | bool | list[str]]:
        return {
            "name": self.name,
            "collection": self.collection,
            "rarity": int(self.rarity),
            "rarity_name": self.rarity.name,
            "min_float": self.min_float,
            "max_float": self.max_float,
            "supports_stattrak": self.supports_stattrak,
            "available_variants": [variant.value for variant in self.available_variants],
            "available_exteriors": [exterior.value for exterior in self.available_exteriors],
        }

    @classmethod
    def _default_variants(
        cls, supports_stattrak: bool | None = None
    ) -> tuple[ItemVariant, ...]:
        if supports_stattrak is False:
            return (ItemVariant.NORMAL,)
        return (ItemVariant.NORMAL, ItemVariant.STATTRAK)

    def _normalize_variants(
        self,
        variants: Iterable[ItemVariant | str] | None,
    ) -> tuple[ItemVariant, ...]:
        raw_variants = tuple(variants or self._default_variants())
        normalized: list[ItemVariant] = []
        for value in raw_variants:
            variant = ItemVariant.from_value(value)
            if variant not in normalized:
                normalized.append(variant)
        if ItemVariant.NORMAL not in normalized:
            normalized.insert(0, ItemVariant.NORMAL)
        return tuple(normalized)

    def _normalize_exteriors(
        self,
        exteriors: Iterable[Exterior | str] | None,
    ) -> tuple[Exterior, ...]:
        if exteriors:
            normalized: list[Exterior] = []
            for value in exteriors:
                exterior = value if isinstance(value, Exterior) else Exterior.from_label(value)
                if exterior not in normalized:
                    normalized.append(exterior)
            ordered = [exterior for exterior in Exterior.ordered() if exterior in normalized]
            return tuple(ordered)
        return tuple(
            exterior
            for exterior in Exterior.ordered()
            if exterior.overlaps_float_range(self.min_float, self.max_float)
        )

    @classmethod
    def _parse_optional_bool(cls, value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
        raise ValueError(f"Cannot parse boolean flag from {value!r}")

    @classmethod
    def _parse_variants(cls, value: Any) -> tuple[ItemVariant, ...]:
        return tuple(ItemVariant.from_value(item) for item in cls._coerce_sequence(value))

    @classmethod
    def _parse_exteriors(cls, value: Any) -> tuple[Exterior, ...]:
        normalized: list[Exterior] = []
        for item in cls._coerce_sequence(value):
            exterior = item if isinstance(item, Exterior) else Exterior.from_label(item)
            if exterior not in normalized:
                normalized.append(exterior)
        return tuple(normalized)

    @classmethod
    def _coerce_sequence(cls, value: Any) -> tuple[Any, ...]:
        if value in (None, "", ()):
            return ()
        if isinstance(value, (list, tuple, set)):
            return tuple(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return ()
            if stripped.startswith("[") and stripped.endswith("]"):
                parsed = json.loads(stripped)
                if not isinstance(parsed, list):
                    raise ValueError(f"Expected JSON list, got {type(parsed).__name__}")
                return tuple(parsed)
            return tuple(
                token.strip()
                for token in stripped.replace("|", ",").replace("/", ",").split(",")
                if token.strip()
            )
        return (value,)


@dataclass(frozen=True, slots=True)
class ContractItem:
    definition: ItemDefinition
    float_value: float
    price_paid: float = 0.0

    def __post_init__(self) -> None:
        self.definition.wear_position(self.float_value)
        if self.price_paid < 0:
            raise ValueError("price_paid cannot be negative")

    @property
    def collection(self) -> str:
        return self.definition.collection

    @property
    def rarity(self) -> Rarity:
        return self.definition.rarity


@dataclass(frozen=True, slots=True)
class TradeUpContract:
    inputs: tuple[ContractItem, ...]
    required_item_count: int = 10

    def __post_init__(self) -> None:
        normalized_inputs = tuple(self.inputs)
        object.__setattr__(self, "inputs", normalized_inputs)

        if len(normalized_inputs) != self.required_item_count:
            raise ValueError(
                f"Trade-up requires exactly {self.required_item_count} items, "
                f"got {len(normalized_inputs)}"
            )
        rarities = {item.rarity for item in normalized_inputs}
        if len(rarities) != 1:
            raise ValueError("All trade-up inputs must have the same rarity")

    @property
    def input_rarity(self) -> Rarity:
        return self.inputs[0].rarity

    @property
    def average_input_float(self) -> float:
        return sum(item.float_value for item in self.inputs) / len(self.inputs)

    @property
    def total_cost(self) -> float:
        return sum(item.price_paid for item in self.inputs)

    def collection_counts(self) -> Counter[str]:
        return Counter(item.collection for item in self.inputs)


@dataclass(frozen=True, slots=True)
class TradeUpOutcome:
    item: ItemDefinition
    probability: float
    output_float: float

    @property
    def exterior(self) -> Exterior:
        return Exterior.from_float(self.output_float)


@dataclass(frozen=True, slots=True)
class PriceQuote:
    lowest_price: float
    recent_average_price: float | None = None

    def __post_init__(self) -> None:
        if self.lowest_price < 0:
            raise ValueError("lowest_price cannot be negative")
        if self.recent_average_price is not None and self.recent_average_price < 0:
            raise ValueError("recent_average_price cannot be negative")

    def resolve(self, price_source: str = "lowest") -> float:
        if price_source == "lowest":
            return self.lowest_price
        if price_source == "recent_average":
            if self.recent_average_price is None:
                raise ValueError("recent_average_price is not available")
            return self.recent_average_price
        raise ValueError(f"Unsupported price_source: {price_source!r}")


@dataclass(frozen=True, slots=True)
class PricedTradeUpOutcome:
    outcome: TradeUpOutcome
    market_price: float
    net_sale_price: float
    expected_revenue_contribution: float


@dataclass(frozen=True, slots=True)
class ContractEvaluation:
    total_cost: float
    fee_rate: float
    expected_revenue: float
    expected_profit: float
    roi: float
    priced_outcomes: tuple[PricedTradeUpOutcome, ...]

    @property
    def roi_percent(self) -> float:
        return self.roi * 100.0


def ensure_iterable_contract_items(items: Iterable[ContractItem]) -> tuple[ContractItem, ...]:
    return tuple(items)
