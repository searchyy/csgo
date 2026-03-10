from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any, Iterable, Sequence

from .models import ItemDefinition, Rarity


DEFAULT_SQLITE_TABLE_NAME = "items"
SQLITE_TABLE_CANDIDATES = (
    "items",
    "item_definitions",
    "skins",
    "cs2_items",
    "csgo_items",
)


class ItemCatalog:
    def __init__(self, items: Iterable[ItemDefinition] | None = None) -> None:
        self._items_by_name: dict[str, ItemDefinition] = {}
        self._items_by_collection_and_rarity: dict[tuple[str, Rarity], list[ItemDefinition]] = {}
        for item in items or ():
            self.add_item(item)

    def add_item(self, item: ItemDefinition) -> None:
        if item.name in self._items_by_name:
            raise ValueError(f"Duplicate item definition: {item.name}")
        self._items_by_name[item.name] = item
        key = (item.collection, item.rarity)
        self._items_by_collection_and_rarity.setdefault(key, []).append(item)

    def get_item(self, name: str) -> ItemDefinition:
        return self._items_by_name[name]

    def get_items(self, collection: str, rarity: Rarity) -> tuple[ItemDefinition, ...]:
        return tuple(self._items_by_collection_and_rarity.get((collection, rarity), ()))

    def all_items(self) -> tuple[ItemDefinition, ...]:
        return tuple(self._items_by_name.values())

    def get_collections_for_rarity(self, rarity: Rarity) -> tuple[str, ...]:
        collections = {
            collection
            for collection, item_rarity in self._items_by_collection_and_rarity
            if item_rarity == rarity
        }
        return tuple(sorted(collections))

    def iter_items_by_rarity(self, rarity: Rarity) -> tuple[ItemDefinition, ...]:
        return tuple(
            item
            for item in self._items_by_name.values()
            if item.rarity == rarity
        )

    def get_upgrade_candidates(
        self, collection: str, input_rarity: Rarity
    ) -> tuple[ItemDefinition, ...]:
        return self.get_items(collection, input_rarity.next_rarity())

    def get_collections_with_upgrade_path(
        self,
        input_rarity: Rarity,
        target_rarity: Rarity | None = None,
    ) -> tuple[str, ...]:
        resolved_target_rarity = target_rarity or input_rarity.next_rarity()
        collections = [
            collection
            for collection in self.get_collections_for_rarity(input_rarity)
            if self.get_items(collection, resolved_target_rarity)
        ]
        return tuple(sorted(collections))

    def to_json(
        self,
        path: str | Path,
        *,
        root_key: str = "items",
        indent: int = 2,
    ) -> Path:
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {root_key: [item.to_dict() for item in self.all_items()]}
        file_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=indent),
            encoding="utf-8",
        )
        return file_path

    def to_sqlite(
        self,
        path: str | Path,
        *,
        table_name: str = DEFAULT_SQLITE_TABLE_NAME,
        if_exists: str = "replace",
    ) -> Path:
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        valid_if_exists = {"replace", "append", "fail"}
        if if_exists not in valid_if_exists:
            raise ValueError(f"if_exists must be one of {sorted(valid_if_exists)}")

        with closing(sqlite3.connect(file_path)) as connection:
            if if_exists == "replace":
                connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            elif if_exists == "fail" and self._table_exists(connection, table_name):
                raise ValueError(f"Table '{table_name}' already exists")

            self._create_sqlite_schema(connection, table_name)
            self._ensure_sqlite_columns(connection, table_name)
            rows = [
                (
                    item.name,
                    item.collection,
                    int(item.rarity),
                    item.rarity.name,
                    item.min_float,
                    item.max_float,
                    json.dumps(
                        [variant.value for variant in item.available_variants],
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        [exterior.value for exterior in item.available_exteriors],
                        ensure_ascii=False,
                    ),
                )
                for item in self.all_items()
            ]
            connection.executemany(
                (
                    f'INSERT OR REPLACE INTO "{table_name}" '
                    "("
                    "name, collection, rarity, rarity_name, min_float, max_float, "
                    "available_variants, available_exteriors"
                    ") "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                ),
                rows,
            )
            connection.commit()
        return file_path

    @classmethod
    def from_json(cls, path: str | Path) -> "ItemCatalog":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        items = cls._extract_records_from_json_payload(data)
        return cls(ItemDefinition.from_dict(item) for item in items)

    @classmethod
    def from_sqlite(
        cls,
        path: str | Path,
        *,
        table_name: str | None = None,
        where: str | None = None,
        params: Sequence[Any] | None = None,
    ) -> "ItemCatalog":
        file_path = Path(path)
        with closing(sqlite3.connect(file_path)) as connection:
            connection.row_factory = sqlite3.Row
            resolved_table_name = table_name or cls._detect_sqlite_table_name(connection)
            query = (
                f'SELECT * FROM "{resolved_table_name}"'
                + (f" WHERE {where}" if where else "")
            )
            rows = connection.execute(query, tuple(params or ())).fetchall()
        return cls(ItemDefinition.from_dict(dict(row)) for row in rows)

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        *,
        table_name: str | None = None,
        where: str | None = None,
        params: Sequence[Any] | None = None,
    ) -> "ItemCatalog":
        file_path = Path(path)
        suffix = file_path.suffix.lower()
        if suffix == ".json":
            return cls.from_json(file_path)
        if suffix in {".sqlite", ".sqlite3", ".db"}:
            return cls.from_sqlite(
                file_path,
                table_name=table_name,
                where=where,
                params=params,
            )
        raise ValueError(f"Unsupported catalog format for path: {file_path}")

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _create_sqlite_schema(connection: sqlite3.Connection, table_name: str) -> None:
        connection.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                name TEXT PRIMARY KEY,
                collection TEXT NOT NULL,
                rarity INTEGER NOT NULL,
                rarity_name TEXT NOT NULL,
                min_float REAL NOT NULL,
                max_float REAL NOT NULL,
                available_variants TEXT NOT NULL DEFAULT '["Normal", "StatTrak"]',
                available_exteriors TEXT NOT NULL DEFAULT '["Factory New", "Minimal Wear", "Field-Tested", "Well-Worn", "Battle-Scarred"]'
            )
            '''
        )
        connection.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "idx_{table_name}_collection_rarity"
            ON "{table_name}" (collection, rarity)
            '''
        )

    @staticmethod
    def _ensure_sqlite_columns(connection: sqlite3.Connection, table_name: str) -> None:
        rows = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        existing_columns = {row[1] for row in rows}
        column_definitions = {
            "available_variants": "TEXT NOT NULL DEFAULT '[\"Normal\", \"StatTrak\"]'",
            "available_exteriors": (
                "TEXT NOT NULL DEFAULT "
                "'[\"Factory New\", \"Minimal Wear\", \"Field-Tested\", \"Well-Worn\", \"Battle-Scarred\"]'"
            ),
        }
        for column_name, sql_definition in column_definitions.items():
            if column_name in existing_columns:
                continue
            connection.execute(
                f'ALTER TABLE "{table_name}" ADD COLUMN {column_name} {sql_definition}'
            )

    @classmethod
    def _detect_sqlite_table_name(cls, connection: sqlite3.Connection) -> str:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        table_names = {row[0] for row in rows}
        for candidate in SQLITE_TABLE_CANDIDATES:
            if candidate in table_names:
                return candidate
        if not table_names:
            raise ValueError("SQLite catalog does not contain any tables")
        return sorted(table_names)[0]

    @classmethod
    def _extract_records_from_json_payload(cls, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [record for record in payload if isinstance(record, dict)]
        if not isinstance(payload, dict):
            raise ValueError("JSON catalog payload must be a list or object")

        for key in ("items", "data", "records", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [record for record in value if isinstance(record, dict)]
            if isinstance(value, dict):
                nested_records = cls._extract_records_from_json_payload(value)
                if nested_records:
                    return nested_records

        dict_values = [value for value in payload.values() if isinstance(value, dict)]
        if dict_values and all(_looks_like_item_record(value) for value in dict_values):
            return dict_values
        if _looks_like_item_record(payload):
            return [payload]
        return []


def _looks_like_item_record(record: dict[str, Any]) -> bool:
    required_groups = (
        ("name", "item_name", "market_hash_name", "full_name", "weapon_name"),
        ("collection", "collection_name", "set_name"),
        ("rarity", "rarity_name", "grade", "quality"),
    )
    return all(any(key in record for key in group) for group in required_groups)
