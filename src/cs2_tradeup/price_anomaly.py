from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .market import is_souvenir_item_name, split_item_variant_name
from .models import ItemVariant


_EXTERIOR_ALIASES = {
    "factory new": "FN",
    "minimal wear": "MW",
    "field-tested": "FT",
    "well-worn": "WW",
    "battle-scarred": "BS",
    "崭新出厂": "FN",
    "略有磨损": "MW",
    "久经沙场": "FT",
    "破损不堪": "WW",
    "战痕累累": "BS",
    "宕柊鍑哄巶": "FN",
    "鐣ユ湁纾ㄦ崯": "MW",
    "涔呯粡娌欏満": "FT",
    "鐮存崯涓嶅牚": "WW",
    "鎴樼棔绱疮": "BS",
}

_RISK_LEVELS = {"low": 0, "medium": 1, "high": 2}
_STAR_ITEM_MARKERS = ("★",)


@dataclass(frozen=True, slots=True)
class PriceAnomalyDetectorConfig:
    source_table: str = "price_snapshots"
    target_table: str = "price_snapshots_cleaned"
    item_name_column: str = "item_name"
    exterior_column: str = "exterior"
    sell_price_column: str = "sell_price"
    buy_price_column: str | None = "buy_price"
    volume_24h_column: str | None = "volume_24h"
    souvenir_column: str | None = "is_souvenir"
    tradeup_compatible_normal_column: str | None = "is_tradeup_compatible_normal"
    variant_filter_reason_column: str | None = "variant_filter_reason"
    minimum_valid_price_threshold: float = 0.1
    spread_ratio_threshold: float = 3.0
    spread_invalid_ratio_threshold: float = 3.0
    spread_safe_buy_multiplier: float = 1.1
    low_liquidity_threshold: int = 2
    low_liquidity_price_ratio_threshold: float = 3.0
    invalidate_on_low_liquidity_spike: bool = True
    exterior_overprice_threshold: float = 1.5
    invalidate_on_exterior_inversion: bool = False
    create_indexes: bool = True


@dataclass(frozen=True, slots=True)
class PriceCleaningSummary:
    source_table: str
    target_table: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    spread_flagged_rows: int
    low_liquidity_flagged_rows: int
    exterior_flagged_rows: int
    variant_excluded_rows: int
    skipped_spread_check_rows: int
    skipped_liquidity_check_rows: int
    cleaned_at: str


@dataclass(slots=True)
class _PriceRowState:
    raw: dict[str, Any]
    safe_price: float | None
    is_valid: bool
    is_tradeup_compatible_normal: bool = True
    variant_name: str = "Normal"
    variant_filter_reason: str | None = None
    risk_level: str = "low"
    anomaly_score: float = 0.0
    flags: set[str] = field(default_factory=set)
    notes: list[str] = field(default_factory=list)

    def add_flag(
        self,
        flag: str,
        note: str,
        *,
        risk_level: str = "medium",
        score: float = 10.0,
    ) -> None:
        self.flags.add(flag)
        self.notes.append(note)
        self.anomaly_score += score
        self.risk_level = _max_risk_level(self.risk_level, risk_level)


class PriceAnomalyDetector:
    """
    在进行 EV 计算前清洗价格快照，输出带 `safe_price` 的新表。

    默认假设源表结构包含：
    - item_name
    - exterior
    - sell_price
    - buy_price
    - volume_24h

    如需兼容其他表结构，可在初始化时覆盖字段名，例如：

    detector = PriceAnomalyDetector(
        PriceAnomalyDetectorConfig(
            source_table="steamdt_price_snapshots",
            target_table="steamdt_price_snapshots_cleaned",
            sell_price_column="lowest_price",
            buy_price_column="highest_buy_price",
            volume_24h_column="sell_num",
        )
    )
    """

    def __init__(self, config: PriceAnomalyDetectorConfig | None = None) -> None:
        self.config = config or PriceAnomalyDetectorConfig()

    def clean_database(self, database_path: str | Path) -> PriceCleaningSummary:
        with sqlite3.connect(database_path) as connection:
            summary = self.clean_prices(connection)
        return summary

    def clean_prices(self, db_connection: sqlite3.Connection) -> PriceCleaningSummary:
        source_columns = self._get_source_columns(db_connection)
        resolved_buy_column = self._resolve_optional_column(
            source_columns, self.config.buy_price_column
        )
        resolved_volume_column = self._resolve_optional_column(
            source_columns, self.config.volume_24h_column
        )
        resolved_souvenir_column = self._resolve_optional_column(
            source_columns, self.config.souvenir_column
        )
        resolved_tradeup_compatible_normal_column = self._resolve_optional_column(
            source_columns, self.config.tradeup_compatible_normal_column
        )
        resolved_variant_filter_reason_column = self._resolve_optional_column(
            source_columns, self.config.variant_filter_reason_column
        )
        rows = self._load_source_rows(db_connection)
        cleaned_at = dt.datetime.now(dt.timezone.utc).isoformat()

        skipped_spread_check_rows = 0
        skipped_liquidity_check_rows = 0
        states: list[_PriceRowState] = []

        for raw in rows:
            item_name = str(raw.get(self.config.item_name_column) or "")
            _, variant = split_item_variant_name(item_name)

            is_souvenir = (
                _to_optional_bool(raw.get(resolved_souvenir_column))
                if resolved_souvenir_column is not None
                else None
            )
            if is_souvenir is None:
                is_souvenir = is_souvenir_item_name(item_name)

            source_tradeup_compatible_normal = (
                _to_optional_bool(raw.get(resolved_tradeup_compatible_normal_column))
                if resolved_tradeup_compatible_normal_column is not None
                else None
            )
            source_variant_filter_reason = (
                str(raw.get(resolved_variant_filter_reason_column) or "").strip() or None
                if resolved_variant_filter_reason_column is not None
                else None
            )

            sell_price = _to_positive_float(raw.get(self.config.sell_price_column))
            buy_price = (
                _to_positive_float(raw.get(resolved_buy_column))
                if resolved_buy_column is not None
                else None
            )
            volume_24h = (
                _to_non_negative_float(raw.get(resolved_volume_column))
                if resolved_volume_column is not None
                else None
            )

            state = _PriceRowState(
                raw=dict(raw),
                safe_price=_pick_initial_safe_price(sell_price, buy_price),
                is_valid=sell_price is not None or buy_price is not None,
                variant_name=variant.value,
            )

            special_item_reason = _detect_special_tradeup_reason(
                item_name=item_name,
                is_souvenir=bool(is_souvenir),
                variant=variant,
            )
            if special_item_reason == "souvenir":
                state.is_tradeup_compatible_normal = False
                state.variant_filter_reason = "souvenir"
                state.is_valid = False
                state.add_flag(
                    "souvenir_excluded",
                    "纪念品 / Souvenir 饰品不可参与炼金，已直接标记为无效。",
                    risk_level="high",
                    score=40.0,
                )
            elif special_item_reason == "stattrak":
                state.is_tradeup_compatible_normal = False
                state.variant_filter_reason = "stattrak"
                state.is_valid = False
                state.add_flag(
                    "stattrak_excluded_for_normal",
                    "StatTrak™ / 暗金饰品不可参与普通炼金，已直接标记为无效。",
                    risk_level="high",
                    score=30.0,
                )
            elif special_item_reason == "star":
                state.is_tradeup_compatible_normal = False
                state.variant_filter_reason = "star"
                state.is_valid = False
                state.add_flag(
                    "star_item_excluded",
                    "★ 刀具或手套不可参与炼金，已直接标记为无效。",
                    risk_level="high",
                    score=40.0,
                )
            elif source_tradeup_compatible_normal is False:
                state.is_tradeup_compatible_normal = False
                state.variant_filter_reason = source_variant_filter_reason or "source_flag"
                state.is_valid = False
                state.add_flag(
                    "source_variant_excluded_for_normal",
                    "源数据已标记该饰品不可参与普通炼金，已直接标记为无效。",
                    risk_level="high",
                    score=25.0,
                )

            if sell_price is None and buy_price is None:
                state.is_valid = False
                state.add_flag(
                    "missing_price",
                    "缺少 sell_price 和 buy_price，无法作为 EV 计算输入。",
                    risk_level="high",
                    score=50.0,
                )
            elif (
                state.safe_price is not None
                and state.safe_price < self.config.minimum_valid_price_threshold
            ):
                state.is_valid = False
                state.add_flag(
                    "price_too_low",
                    (
                        f"抓取价格 {state.safe_price:.4f} 低于最小有效阈值 "
                        f"{self.config.minimum_valid_price_threshold:.2f}，判定为解析异常。"
                    ),
                    risk_level="high",
                    score=35.0,
                )

            abnormal_price = False
            if sell_price is not None and buy_price is not None and buy_price > 0:
                spread_ratio = sell_price / buy_price
                if spread_ratio > self.config.spread_ratio_threshold:
                    adjusted_price = buy_price * self.config.spread_safe_buy_multiplier
                    state.safe_price = _min_defined(state.safe_price, adjusted_price)
                    state.add_flag(
                        "spread",
                        (
                            f"买卖价差过大：sell/buy={spread_ratio:.2f}，"
                            f"safe_price 下修为 buy_price * {self.config.spread_safe_buy_multiplier:.2f}。"
                        ),
                        risk_level="medium",
                        score=25.0,
                    )
                    abnormal_price = True
                    if spread_ratio > self.config.spread_invalid_ratio_threshold:
                        state.is_valid = False
                        state.add_flag(
                            "spread_hard_stop",
                            (
                                f"买卖价差超过硬熔断阈值 "
                                f"{self.config.spread_invalid_ratio_threshold:.2f}，标记为无效。"
                            ),
                            risk_level="high",
                            score=35.0,
                        )
                elif spread_ratio >= self.config.low_liquidity_price_ratio_threshold:
                    abnormal_price = True
            else:
                skipped_spread_check_rows += 1

            if volume_24h is not None:
                if volume_24h < self.config.low_liquidity_threshold and abnormal_price:
                    state.add_flag(
                        "low_liquidity",
                        (
                            f"24h 成交量仅 {volume_24h:.0f}，且价格存在异常抬高迹象，"
                            "标记为高风险。"
                        ),
                        risk_level="high",
                        score=30.0,
                    )
                    if self.config.invalidate_on_low_liquidity_spike:
                        state.is_valid = False
                elif volume_24h < self.config.low_liquidity_threshold:
                    state.add_flag(
                        "low_liquidity_warning",
                        f"24h 成交量仅 {volume_24h:.0f}，流动性偏低。",
                        risk_level="medium",
                        score=10.0,
                    )
            else:
                skipped_liquidity_check_rows += 1

            states.append(state)

        self._apply_exterior_logical_check(states)
        self._write_target_table(db_connection, source_columns, states, cleaned_at)

        return PriceCleaningSummary(
            source_table=self.config.source_table,
            target_table=self.config.target_table,
            total_rows=len(states),
            valid_rows=sum(1 for state in states if state.is_valid),
            invalid_rows=sum(1 for state in states if not state.is_valid),
            spread_flagged_rows=sum(1 for state in states if "spread" in state.flags),
            low_liquidity_flagged_rows=sum(
                1
                for state in states
                if "low_liquidity" in state.flags or "low_liquidity_warning" in state.flags
            ),
            exterior_flagged_rows=sum(
                1 for state in states if "exterior_inversion" in state.flags
            ),
            variant_excluded_rows=sum(
                1 for state in states if not state.is_tradeup_compatible_normal
            ),
            skipped_spread_check_rows=skipped_spread_check_rows,
            skipped_liquidity_check_rows=skipped_liquidity_check_rows,
            cleaned_at=cleaned_at,
        )

    def _apply_exterior_logical_check(self, states: list[_PriceRowState]) -> None:
        grouped: dict[str, list[_PriceRowState]] = {}
        for state in states:
            item_name = str(state.raw.get(self.config.item_name_column) or "")
            grouped.setdefault(item_name, []).append(state)

        for item_states in grouped.values():
            reference_prices: list[float] = []
            for state in item_states:
                exterior = _normalize_exterior(state.raw.get(self.config.exterior_column))
                if exterior in {"FT", "MW"} and state.safe_price is not None and state.safe_price > 0:
                    reference_prices.append(float(state.safe_price))
            if not reference_prices:
                continue

            conservative_reference = min(reference_prices)
            for state in item_states:
                exterior = _normalize_exterior(state.raw.get(self.config.exterior_column))
                if exterior not in {"WW", "BS"}:
                    continue
                sell_price = _to_positive_float(state.raw.get(self.config.sell_price_column))
                if sell_price is None:
                    continue
                if sell_price <= conservative_reference * self.config.exterior_overprice_threshold:
                    continue

                state.safe_price = _min_defined(state.safe_price, conservative_reference)
                state.add_flag(
                    "exterior_inversion",
                    (
                        f"{state.raw.get(self.config.exterior_column)} 标价 {sell_price:.2f} "
                        f"高于 FT/MW 参考价 {conservative_reference:.2f} 的 "
                        f"{self.config.exterior_overprice_threshold:.2f} 倍，已下修。"
                    ),
                    risk_level="high",
                    score=35.0,
                )
                volume_24h = (
                    _to_non_negative_float(state.raw.get(self.config.volume_24h_column))
                    if self.config.volume_24h_column is not None
                    else None
                )
                if (
                    volume_24h is not None
                    and volume_24h < self.config.low_liquidity_threshold
                    and self.config.invalidate_on_low_liquidity_spike
                ):
                    state.is_valid = False
                if self.config.invalidate_on_exterior_inversion:
                    state.is_valid = False

    def _get_source_columns(self, db_connection: sqlite3.Connection) -> tuple[str, ...]:
        source_table = _quote_identifier(self.config.source_table)
        rows = db_connection.execute(f"PRAGMA table_info({source_table})").fetchall()
        columns = tuple(str(row[1]) for row in rows)
        required = {
            self.config.item_name_column,
            self.config.exterior_column,
            self.config.sell_price_column,
        }
        missing_required = sorted(column for column in required if column not in columns)
        if missing_required:
            missing_text = ", ".join(missing_required)
            raise ValueError(
                f"Source table '{self.config.source_table}' 缺少必要字段：{missing_text}"
            )
        return columns

    def _resolve_optional_column(
        self,
        source_columns: tuple[str, ...],
        column_name: str | None,
    ) -> str | None:
        if column_name is None:
            return None
        return column_name if column_name in source_columns else None

    def _load_source_rows(self, db_connection: sqlite3.Connection) -> list[dict[str, Any]]:
        db_connection.row_factory = sqlite3.Row
        source_table = _quote_identifier(self.config.source_table)
        rows = db_connection.execute(
            f'SELECT rowid AS "__rowid__", * FROM {source_table}'
        ).fetchall()
        return [dict(row) for row in rows]

    def _write_target_table(
        self,
        db_connection: sqlite3.Connection,
        source_columns: tuple[str, ...],
        states: list[_PriceRowState],
        cleaned_at: str,
    ) -> None:
        source_table = _quote_identifier(self.config.source_table)
        target_table = _quote_identifier(self.config.target_table)
        supplemental_columns = [
            "safe_price",
            "is_valid",
            "risk_level",
            "anomaly_flags",
            "anomaly_notes",
            "anomaly_score",
            "variant_name",
            "is_tradeup_compatible_normal",
            "variant_filter_reason",
            "cleaned_at",
        ]
        passthrough_columns = [
            column for column in source_columns if column not in set(supplemental_columns)
        ]
        db_connection.execute(f"DROP TABLE IF EXISTS {target_table}")
        db_connection.execute(
            f"""
            CREATE TABLE {target_table} AS
            SELECT
                {", ".join(_quote_identifier(column) for column in passthrough_columns)},
                CAST(NULL AS REAL) AS safe_price,
                CAST(1 AS INTEGER) AS is_valid,
                CAST('low' AS TEXT) AS risk_level,
                CAST('' AS TEXT) AS anomaly_flags,
                CAST('' AS TEXT) AS anomaly_notes,
                CAST(0 AS REAL) AS anomaly_score,
                CAST('Normal' AS TEXT) AS variant_name,
                CAST(1 AS INTEGER) AS is_tradeup_compatible_normal,
                CAST(NULL AS TEXT) AS variant_filter_reason,
                CAST('' AS TEXT) AS cleaned_at
            FROM {source_table}
            WHERE 0
            """
        )

        insert_columns = passthrough_columns + supplemental_columns
        placeholders = ", ".join("?" for _ in insert_columns)
        insert_sql = (
            f'INSERT INTO {target_table} '
            f'({", ".join(_quote_identifier(column) for column in insert_columns)}) '
            f"VALUES ({placeholders})"
        )
        values: list[tuple[Any, ...]] = []
        for state in states:
            row_values = [state.raw.get(column) for column in passthrough_columns]
            row_values.extend(
                [
                    state.safe_price,
                    1 if state.is_valid else 0,
                    state.risk_level,
                    ",".join(sorted(state.flags)),
                    " | ".join(state.notes),
                    state.anomaly_score,
                    state.variant_name,
                    1 if state.is_tradeup_compatible_normal else 0,
                    state.variant_filter_reason,
                    cleaned_at,
                ]
            )
            values.append(tuple(row_values))
        db_connection.executemany(insert_sql, values)

        if self.config.create_indexes:
            item_name_column = _quote_identifier(self.config.item_name_column)
            exterior_column = _quote_identifier(self.config.exterior_column)
            db_connection.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self._index_name('item_exterior')}
                ON {target_table} ({item_name_column}, {exterior_column})
                """
            )
            db_connection.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self._index_name('valid_safe_price')}
                ON {target_table} ("is_valid", "safe_price")
                """
            )
            db_connection.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self._index_name('tradeup_compatible')}
                ON {target_table} ("is_tradeup_compatible_normal", "is_valid", "safe_price")
                """
            )
        db_connection.commit()

    def _index_name(self, suffix: str) -> str:
        table_name = self.config.target_table.replace('"', "").replace(".", "_")
        return _quote_identifier(f"idx_{table_name}_{suffix}")


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _to_positive_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _to_non_negative_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _to_optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _detect_special_tradeup_reason(
    *,
    item_name: str,
    is_souvenir: bool,
    variant: ItemVariant,
) -> str | None:
    normalized_name = str(item_name or "").strip()
    lowered_name = normalized_name.lower()
    if is_souvenir or "souvenir" in lowered_name or "纪念品" in normalized_name:
        return "souvenir"
    if variant is ItemVariant.STATTRAK or "stattrak" in lowered_name:
        return "stattrak"
    if any(marker in normalized_name for marker in _STAR_ITEM_MARKERS):
        return "star"
    return None


def _pick_initial_safe_price(sell_price: float | None, buy_price: float | None) -> float | None:
    if sell_price is not None:
        return sell_price
    if buy_price is not None:
        return buy_price
    return None


def _min_defined(left: float | None, right: float | None) -> float | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _normalize_exterior(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return _EXTERIOR_ALIASES.get(str(value).strip().lower(), str(value).strip().upper())


def _max_risk_level(current: str, candidate: str) -> str:
    if _RISK_LEVELS.get(candidate, 0) > _RISK_LEVELS.get(current, 0):
        return candidate
    return current


__all__ = [
    "PriceAnomalyDetector",
    "PriceAnomalyDetectorConfig",
    "PriceCleaningSummary",
]
