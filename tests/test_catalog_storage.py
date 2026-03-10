from pathlib import Path
from contextlib import closing
import json
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import Exterior, ItemCatalog, ItemDefinition, ItemVariant, Rarity


class CatalogStorageTests(unittest.TestCase):
    def build_catalog(self) -> ItemCatalog:
        return ItemCatalog(
            [
                ItemDefinition("Alpha Input", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00),
                ItemDefinition("Target Gun", "Alpha", Rarity.RESTRICTED, 0.00, 0.70),
                ItemDefinition("Beta Outcome", "Beta", Rarity.RESTRICTED, 0.05, 0.45),
            ]
        )

    def test_item_definition_from_dict_supports_common_aliases(self) -> None:
        item = ItemDefinition.from_dict(
            {
                "market_hash_name": "AK-47 | Slate",
                "collection_name": "Snakebite",
                "rarity_name": "3",
                "wear_min": 0.0,
                "wear_max": 1.0,
            }
        )

        self.assertEqual(item.name, "AK-47 | Slate")
        self.assertEqual(item.collection, "Snakebite")
        self.assertEqual(item.rarity, Rarity.MIL_SPEC)
        self.assertAlmostEqual(item.min_float, 0.0)
        self.assertAlmostEqual(item.max_float, 1.0)
        self.assertEqual(item.available_variants, (ItemVariant.NORMAL, ItemVariant.STATTRAK))
        self.assertEqual(item.available_exteriors, tuple(Exterior.ordered()))

    def test_item_definition_tracks_variant_and_exterior_metadata(self) -> None:
        item = ItemDefinition.from_dict(
            {
                "name": "M4A4 | Desolate Space",
                "collection": "Gamma",
                "rarity": "classified",
                "min_float": 0.16,
                "max_float": 0.60,
                "supports_stattrak": False,
                "available_exteriors": '["Field-Tested", "Well-Worn", "Battle-Scarred"]',
            }
        )

        self.assertEqual(item.available_variants, (ItemVariant.NORMAL,))
        self.assertFalse(item.supports_stattrak)
        self.assertEqual(
            item.available_exteriors,
            (
                Exterior.FIELD_TESTED,
                Exterior.WELL_WORN,
                Exterior.BATTLE_SCARRED,
            ),
        )
        self.assertTrue(item.supports_exterior("Field-Tested"))
        self.assertFalse(item.supports_exterior("Factory New"))
        self.assertEqual(
            item.build_market_name(Exterior.FIELD_TESTED),
            "M4A4 | Desolate Space (Field-Tested)",
        )
        with self.assertRaises(ValueError):
            item.build_market_name(Exterior.FIELD_TESTED, variant=ItemVariant.STATTRAK)

    def test_catalog_round_trip_json_and_sqlite(self) -> None:
        catalog = self.build_catalog()

        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "items.json"
            sqlite_path = Path(temp_dir) / "items.sqlite"

            catalog.to_json(json_path)
            catalog.to_sqlite(sqlite_path)

            from_json = ItemCatalog.from_path(json_path)
            from_sqlite = ItemCatalog.from_path(sqlite_path)

        self.assertEqual(
            [item.name for item in from_json.all_items()],
            [item.name for item in catalog.all_items()],
        )
        self.assertEqual(
            [item.name for item in from_sqlite.all_items()],
            [item.name for item in catalog.all_items()],
        )
        self.assertEqual(from_sqlite.get_item("Target Gun").rarity, Rarity.RESTRICTED)
        self.assertEqual(
            from_sqlite.get_item("Target Gun").available_variants,
            (ItemVariant.NORMAL, ItemVariant.STATTRAK),
        )

    def test_catalog_from_json_handles_nested_payload(self) -> None:
        payload = {
            "data": {
                "records": [
                    {
                        "item_name": "M4A1-S | Basilisk",
                        "collection": "Operation Breakout",
                        "rarity": "restricted",
                        "min_float": 0.00,
                        "max_float": 1.00,
                    }
                ]
            }
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "nested.json"
            json_path.write_text(json.dumps(payload), encoding="utf-8")
            catalog = ItemCatalog.from_json(json_path)

        self.assertEqual(catalog.get_item("M4A1-S | Basilisk").collection, "Operation Breakout")

    def test_catalog_from_sqlite_detects_supported_table_name_and_alias_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "catalog.db"
            with closing(sqlite3.connect(sqlite_path)) as connection:
                connection.execute(
                    """
                    CREATE TABLE skins (
                        market_hash_name TEXT PRIMARY KEY,
                        collection_name TEXT NOT NULL,
                        rarity_name TEXT NOT NULL,
                        wear_min REAL NOT NULL,
                        wear_max REAL NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO skins (
                        market_hash_name,
                        collection_name,
                        rarity_name,
                        wear_min,
                        wear_max
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    ("USP-S | Cortex", "Prisma 2", "classified", 0.06, 0.80),
                )
                connection.commit()

            catalog = ItemCatalog.from_sqlite(sqlite_path)

        loaded = catalog.get_item("USP-S | Cortex")
        self.assertEqual(loaded.collection, "Prisma 2")
        self.assertEqual(loaded.rarity, Rarity.CLASSIFIED)
        self.assertAlmostEqual(loaded.min_float, 0.06)
        self.assertAlmostEqual(loaded.max_float, 0.80)
        self.assertEqual(loaded.available_variants, (ItemVariant.NORMAL, ItemVariant.STATTRAK))
        self.assertEqual(
            loaded.available_exteriors,
            (
                Exterior.FACTORY_NEW,
                Exterior.MINIMAL_WEAR,
                Exterior.FIELD_TESTED,
                Exterior.WELL_WORN,
                Exterior.BATTLE_SCARRED,
            ),
        )

    def test_catalog_to_sqlite_fail_mode_rejects_existing_table(self) -> None:
        catalog = self.build_catalog()

        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "items.sqlite"
            catalog.to_sqlite(sqlite_path, if_exists="replace")
            with self.assertRaises(ValueError):
                catalog.to_sqlite(sqlite_path, if_exists="fail")

    def test_catalog_to_sqlite_append_upgrades_legacy_schema(self) -> None:
        catalog = self.build_catalog()

        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "legacy.sqlite"
            with closing(sqlite3.connect(sqlite_path)) as connection:
                connection.execute(
                    """
                    CREATE TABLE items (
                        name TEXT PRIMARY KEY,
                        collection TEXT NOT NULL,
                        rarity INTEGER NOT NULL,
                        rarity_name TEXT NOT NULL,
                        min_float REAL NOT NULL,
                        max_float REAL NOT NULL
                    )
                    """
                )
                connection.commit()

            catalog.to_sqlite(sqlite_path, if_exists="append")
            loaded = ItemCatalog.from_sqlite(sqlite_path).get_item("Alpha Input")

        self.assertEqual(loaded.available_variants, (ItemVariant.NORMAL, ItemVariant.STATTRAK))
        self.assertEqual(loaded.available_exteriors, tuple(Exterior.ordered()))


if __name__ == "__main__":
    unittest.main()
