from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .scanner import TradeUpScanResult


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class ScanRunRecord:
    id: int
    run_type: str
    status: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    parameters: dict[str, Any]
    summary: dict[str, Any]
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class ScanResultRecord:
    id: int
    run_id: int
    target_item: str
    target_exterior: str
    target_collection: str
    target_rarity: int
    target_rarity_name: str
    roi: float
    roi_percent: float
    expected_profit: float
    expected_revenue: float
    total_cost: float
    target_probability: float
    planned_average_metric: float
    formula_signature: str
    fee_rate: float
    collection_counts: dict[str, int]
    materials: list[dict[str, Any]]
    outcomes: list[dict[str, Any]]
    created_at: str


class TradeUpScanResultStore:
    default_runs_table = "scan_runs"
    default_results_table = "scan_results"

    def __init__(
        self,
        path: str | Path,
        *,
        runs_table: str = default_runs_table,
        results_table: str = default_results_table,
    ) -> None:
        self.path = Path(path)
        self.runs_table = runs_table
        self.results_table = results_table
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def create_run(
        self,
        *,
        run_type: str,
        parameters: Mapping[str, Any] | None = None,
        status: str = "running",
        started_at: str | None = None,
    ) -> int:
        created_at = _utc_now_iso()
        with closing(sqlite3.connect(self.path)) as connection:
            cursor = connection.execute(
                f'''
                INSERT INTO "{self.runs_table}" (
                    run_type, status, created_at, started_at, parameters_json, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (
                    run_type,
                    status,
                    created_at,
                    started_at or created_at,
                    json.dumps(dict(parameters or {}), ensure_ascii=False),
                    json.dumps({}, ensure_ascii=False),
                ),
            )
            connection.commit()
            return int(cursor.lastrowid)

    def complete_run(
        self,
        run_id: int,
        *,
        status: str = "completed",
        summary: Mapping[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                f'''
                UPDATE "{self.runs_table}"
                SET status = ?,
                    finished_at = ?,
                    summary_json = ?,
                    error_message = ?
                WHERE id = ?
                ''',
                (
                    status,
                    _utc_now_iso(),
                    json.dumps(dict(summary or {}), ensure_ascii=False),
                    error_message,
                    run_id,
                ),
            )
            connection.commit()

    def append_results(
        self,
        run_id: int,
        results: Iterable[TradeUpScanResult],
    ) -> int:
        serialized_results = [self._serialize_result(run_id, result) for result in results]
        if not serialized_results:
            return 0

        with closing(sqlite3.connect(self.path)) as connection:
            cursor = connection.executemany(
                f'''
                INSERT INTO "{self.results_table}" (
                    run_id,
                    target_item,
                    target_exterior,
                    target_collection,
                    target_rarity,
                    target_rarity_name,
                    roi,
                    roi_percent,
                    expected_profit,
                    expected_revenue,
                    total_cost,
                    target_probability,
                    planned_average_metric,
                    formula_signature,
                    fee_rate,
                    collection_counts_json,
                    materials_json,
                    outcomes_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                serialized_results,
            )
            connection.commit()
            return cursor.rowcount if cursor.rowcount != -1 else len(serialized_results)

    def get_run(self, run_id: int) -> ScanRunRecord:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                f'SELECT * FROM "{self.runs_table}" WHERE id = ?',
                (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return self._row_to_run(row)

    def list_runs(
        self,
        *,
        limit: int = 20,
        run_type: str | None = None,
        status: str | None = None,
    ) -> tuple[ScanRunRecord, ...]:
        where_clauses: list[str] = []
        params: list[Any] = []
        if run_type:
            where_clauses.append("run_type = ?")
            params.append(run_type)
        if status:
            where_clauses.append("status = ?")
            params.append(status)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                f'''
                SELECT *
                FROM "{self.runs_table}"
                {where_sql}
                ORDER BY id DESC
                LIMIT ?
                ''',
                (*params, limit),
            ).fetchall()
        return tuple(self._row_to_run(row) for row in rows)

    def list_results(
        self,
        *,
        limit: int = 100,
        min_roi: float | None = None,
        max_roi: float | None = None,
        min_expected_profit: float | None = None,
        max_total_cost: float | None = None,
        search: str | None = None,
        run_id: int | None = None,
        latest_run_only: bool = False,
        target_collection: str | None = None,
        target_exterior: str | None = None,
        target_rarity_name: str | None = None,
        sort_by: str = "roi",
        sort_dir: str = "desc",
    ) -> tuple[ScanResultRecord, ...]:
        where_clauses: list[str] = []
        params: list[Any] = []
        if min_roi is not None:
            where_clauses.append("roi >= ?")
            params.append(float(min_roi))
        if max_roi is not None:
            where_clauses.append("roi <= ?")
            params.append(float(max_roi))
        if min_expected_profit is not None:
            where_clauses.append("expected_profit >= ?")
            params.append(float(min_expected_profit))
        if max_total_cost is not None:
            where_clauses.append("total_cost <= ?")
            params.append(float(max_total_cost))
        if search:
            like = f"%{search.strip()}%"
            where_clauses.append(
                "(target_item LIKE ? OR target_collection LIKE ? OR formula_signature LIKE ?)"
            )
            params.extend((like, like, like))
        if run_id is not None:
            where_clauses.append("run_id = ?")
            params.append(int(run_id))
        elif latest_run_only:
            where_clauses.append(
                f'run_id = (SELECT MAX(id) FROM "{self.runs_table}" WHERE status = \'completed\')'
            )
        if target_collection:
            where_clauses.append("target_collection = ?")
            params.append(target_collection)
        if target_exterior:
            where_clauses.append("target_exterior = ?")
            params.append(target_exterior)
        if target_rarity_name:
            where_clauses.append("target_rarity_name = ?")
            params.append(target_rarity_name)
        sort_columns = {
            "roi": "roi",
            "roi_percent": "roi_percent",
            "expected_profit": "expected_profit",
            "expected_revenue": "expected_revenue",
            "total_cost": "total_cost",
            "target_probability": "target_probability",
            "created_at": "created_at",
            "target_item": "target_item",
        }
        resolved_sort_column = sort_columns.get(sort_by, "roi")
        resolved_sort_dir = "ASC" if str(sort_dir).lower() == "asc" else "DESC"
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                f'''
                SELECT *
                FROM "{self.results_table}"
                {where_sql}
                ORDER BY {resolved_sort_column} {resolved_sort_dir}, expected_profit DESC, total_cost ASC, id DESC
                LIMIT ?
                ''',
                (*params, limit),
            ).fetchall()
        return tuple(self._row_to_result(row) for row in rows)

    def delete_results_for_run(self, run_id: int) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                f'DELETE FROM "{self.results_table}" WHERE run_id = ?',
                (run_id,),
            )
            connection.commit()

    def _ensure_schema(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.runs_table}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    parameters_json TEXT NOT NULL DEFAULT '{{}}',
                    summary_json TEXT NOT NULL DEFAULT '{{}}',
                    error_message TEXT
                )
                '''
            )
            connection.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.results_table}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    target_item TEXT NOT NULL,
                    target_exterior TEXT NOT NULL,
                    target_collection TEXT NOT NULL,
                    target_rarity INTEGER NOT NULL,
                    target_rarity_name TEXT NOT NULL,
                    roi REAL NOT NULL,
                    roi_percent REAL NOT NULL,
                    expected_profit REAL NOT NULL,
                    expected_revenue REAL NOT NULL,
                    total_cost REAL NOT NULL,
                    target_probability REAL NOT NULL,
                    planned_average_metric REAL NOT NULL,
                    formula_signature TEXT NOT NULL,
                    fee_rate REAL NOT NULL,
                    collection_counts_json TEXT NOT NULL,
                    materials_json TEXT NOT NULL,
                    outcomes_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES "{self.runs_table}" (id)
                )
                '''
            )
            connection.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "idx_{self.results_table}_run_id"
                ON "{self.results_table}" (run_id)
                '''
            )
            connection.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "idx_{self.results_table}_roi"
                ON "{self.results_table}" (roi DESC, expected_profit DESC)
                '''
            )
            connection.commit()

    def _serialize_result(
        self,
        run_id: int,
        result: TradeUpScanResult,
    ) -> tuple[Any, ...]:
        materials = [
            {
                "item_name": pricing.item.name,
                "collection": pricing.item.collection,
                "count": pricing.count,
                "requested_exterior": pricing.requested_exterior.value,
                "min_float": pricing.min_float,
                "max_float": pricing.max_float,
                "estimated_float": pricing.estimated_float,
                "adjusted_float": pricing.adjusted_float,
                "market_name": pricing.market_name,
                "unit_price": pricing.unit_price,
                "total_price": pricing.total_price,
                "float_source": pricing.float_source,
                "float_source_label": pricing.float_source_label,
                "float_verified": pricing.float_verified,
                "requires_float_verification": pricing.requires_float_verification,
            }
            for pricing in result.material_pricings
        ]
        outcomes = [
            {
                "item_name": pricing.item.name,
                "collection": pricing.item.collection,
                "probability": pricing.probability,
                "output_float": pricing.output_float,
                "exterior": pricing.exterior.value,
                "market_name": pricing.market_name,
                "market_price": pricing.market_price,
                "net_sale_price": pricing.net_sale_price,
                "expected_revenue_contribution": pricing.expected_revenue_contribution,
            }
            for pricing in result.outcome_pricings
        ]
        return (
            run_id,
            result.target_item.name,
            result.target_exterior.value,
            result.target_item.collection,
            int(result.target_item.rarity),
            result.target_item.rarity.name,
            result.roi,
            result.roi_percent,
            result.expected_profit,
            result.expected_revenue,
            result.total_cost,
            result.target_probability,
            result.planned_average_metric,
            result.formula_signature,
            result.fee_rate,
            json.dumps(result.formula.collection_counts, ensure_ascii=False),
            json.dumps(materials, ensure_ascii=False),
            json.dumps(outcomes, ensure_ascii=False),
            _utc_now_iso(),
        )

    def _row_to_run(self, row: sqlite3.Row) -> ScanRunRecord:
        return ScanRunRecord(
            id=int(row["id"]),
            run_type=row["run_type"],
            status=row["status"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            parameters=_safe_load_json_object(row["parameters_json"]),
            summary=_safe_load_json_object(row["summary_json"]),
            error_message=row["error_message"],
        )

    def _row_to_result(self, row: sqlite3.Row) -> ScanResultRecord:
        return ScanResultRecord(
            id=int(row["id"]),
            run_id=int(row["run_id"]),
            target_item=row["target_item"],
            target_exterior=row["target_exterior"],
            target_collection=row["target_collection"],
            target_rarity=int(row["target_rarity"]),
            target_rarity_name=row["target_rarity_name"],
            roi=float(row["roi"]),
            roi_percent=float(row["roi_percent"]),
            expected_profit=float(row["expected_profit"]),
            expected_revenue=float(row["expected_revenue"]),
            total_cost=float(row["total_cost"]),
            target_probability=float(row["target_probability"]),
            planned_average_metric=float(row["planned_average_metric"]),
            formula_signature=row["formula_signature"],
            fee_rate=float(row["fee_rate"]),
            collection_counts=_safe_load_json_object(row["collection_counts_json"]),
            materials=_safe_load_json_list(row["materials_json"]),
            outcomes=_safe_load_json_list(row["outcomes_json"]),
            created_at=row["created_at"],
        )


def store_scan_results(
    store: TradeUpScanResultStore | str | Path,
    *,
    run_type: str,
    results: Sequence[TradeUpScanResult],
    parameters: Mapping[str, Any] | None = None,
    summary: Mapping[str, Any] | None = None,
) -> int:
    resolved_store = store if isinstance(store, TradeUpScanResultStore) else TradeUpScanResultStore(store)
    run_id = resolved_store.create_run(run_type=run_type, parameters=parameters)
    try:
        resolved_store.append_results(run_id, results)
        resolved_store.complete_run(
            run_id,
            status="completed",
            summary={
                **dict(summary or {}),
                "results_found": len(results),
            },
        )
    except Exception as error:
        resolved_store.complete_run(
            run_id,
            status="failed",
            summary=dict(summary or {}),
            error_message=str(error),
        )
        raise
    return run_id


def _safe_load_json_object(raw_value: str | None) -> dict[str, Any]:
    if not raw_value:
        return {}
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _safe_load_json_list(raw_value: str | None) -> list[dict[str, Any]]:
    if not raw_value:
        return []
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [dict(entry) for entry in value if isinstance(entry, Mapping)]


__all__ = [
    "ScanResultRecord",
    "ScanRunRecord",
    "TradeUpScanResultStore",
    "store_scan_results",
]
