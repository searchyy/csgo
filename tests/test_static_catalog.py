from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    Exterior,
    ItemCatalog,
    ItemVariant,
    Rarity,
    build_catalog_from_bymykel_api,
    generate_trade_up_formulas,
    sync_bymykel_static_catalog,
)


def build_skin(
    name: str,
    *,
    weapon: str,
    category: str,
    collection: str,
    rarity: str,
    min_float: float,
    max_float: float,
    wears: list[str],
    stattrak: bool,
) -> dict:
    return {
        "name": name,
        "weapon": {"name": weapon},
        "category": {"name": category},
        "collections": [{"name": collection}],
        "rarity": {"name": rarity},
        "min_float": min_float,
        "max_float": max_float,
        "wears": [{"name": wear} for wear in wears],
        "stattrak": stattrak,
    }


class StaticCatalogTests(unittest.TestCase):
    def build_payload(self) -> list[dict]:
        return [
            build_skin(
                "MAC-10 | Heat",
                weapon="MAC-10",
                category="SMGs",
                collection="The Phoenix Collection",
                rarity="Mil-Spec Grade",
                min_float=0.00,
                max_float=1.00,
                wears=[
                    "Factory New",
                    "Minimal Wear",
                    "Field-Tested",
                    "Well-Worn",
                    "Battle-Scarred",
                ],
                stattrak=True,
            ),
            build_skin(
                "MAG-7 | Heaven Guard",
                weapon="MAG-7",
                category="Heavy",
                collection="The Phoenix Collection",
                rarity="Mil-Spec Grade",
                min_float=0.00,
                max_float=1.00,
                wears=[
                    "Factory New",
                    "Minimal Wear",
                    "Field-Tested",
                    "Well-Worn",
                    "Battle-Scarred",
                ],
                stattrak=True,
            ),
            build_skin(
                "AK-47 | Redline",
                weapon="AK-47",
                category="Rifles",
                collection="The Phoenix Collection",
                rarity="Restricted",
                min_float=0.10,
                max_float=0.70,
                wears=[
                    "Minimal Wear",
                    "Field-Tested",
                    "Well-Worn",
                    "Battle-Scarred",
                ],
                stattrak=True,
            ),
            build_skin(
                "Glock-18 | Gamma Doppler",
                weapon="Glock-18",
                category="Pistols",
                collection="The 2021 Train Collection",
                rarity="Covert",
                min_float=0.00,
                max_float=0.08,
                wears=["Factory New", "Minimal Wear"],
                stattrak=False,
            ),
            build_skin(
                "Glock-18 | Gamma Doppler",
                weapon="Glock-18",
                category="Pistols",
                collection="The 2021 Train Collection",
                rarity="Covert",
                min_float=0.00,
                max_float=0.08,
                wears=["Factory New", "Minimal Wear"],
                stattrak=False,
            ),
            build_skin(
                "★ Karambit | Doppler",
                weapon="Karambit",
                category="Knives",
                collection="The Chroma Collection",
                rarity="Extraordinary",
                min_float=0.00,
                max_float=0.08,
                wears=["Factory New"],
                stattrak=False,
            ),
            build_skin(
                "M4A4 | Howl",
                weapon="M4A4",
                category="Rifles",
                collection="The Huntsman Collection",
                rarity="Contraband",
                min_float=0.00,
                max_float=0.40,
                wears=[
                    "Factory New",
                    "Minimal Wear",
                    "Field-Tested",
                    "Well-Worn",
                ],
                stattrak=True,
            ),
        ]

    def test_build_catalog_from_bymykel_api_maps_real_fields(self) -> None:
        catalog = build_catalog_from_bymykel_api(self.build_payload())

        self.assertEqual(len(catalog.all_items()), 4)

        redline = catalog.get_item("AK-47 | Redline")
        self.assertEqual(redline.collection, "The Phoenix Collection")
        self.assertEqual(redline.rarity, Rarity.RESTRICTED)
        self.assertAlmostEqual(redline.min_float, 0.10)
        self.assertAlmostEqual(redline.max_float, 0.70)
        self.assertEqual(
            redline.available_variants,
            (ItemVariant.NORMAL, ItemVariant.STATTRAK),
        )
        self.assertEqual(
            redline.available_exteriors,
            (
                Exterior.MINIMAL_WEAR,
                Exterior.FIELD_TESTED,
                Exterior.WELL_WORN,
                Exterior.BATTLE_SCARRED,
            ),
        )

        gamma = catalog.get_item("Glock-18 | Gamma Doppler")
        self.assertEqual(gamma.collection, "The 2021 Train Collection")
        self.assertEqual(gamma.available_variants, (ItemVariant.NORMAL,))
        self.assertEqual(
            gamma.available_exteriors,
            (Exterior.FACTORY_NEW, Exterior.MINIMAL_WEAR),
        )

        self.assertNotIn("M4A4 | Howl", {item.name for item in catalog.all_items()})
        self.assertNotIn("★ Karambit | Doppler", {item.name for item in catalog.all_items()})

    def test_sync_bymykel_static_catalog_writes_catalog_and_supports_formulas(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "items.json"
            sqlite_path = Path(temp_dir) / "items.sqlite"
            summary = sync_bymykel_static_catalog(
                skins_payload=self.build_payload(),
                output_json_path=json_path,
                output_sqlite_path=sqlite_path,
            )

            self.assertTrue(json_path.exists())
            self.assertTrue(sqlite_path.exists())
            self.assertEqual(summary.items_fetched, 7)
            self.assertEqual(summary.items_written, 4)
            self.assertEqual(summary.collections_written, 2)

            catalog = ItemCatalog.from_path(sqlite_path)
            formulas = generate_trade_up_formulas(
                "AK-47 | Redline",
                Exterior.FIELD_TESTED,
                catalog,
                max_formulas=5,
            )

        self.assertEqual(len(formulas), 1)
        self.assertEqual(formulas[0].target_item.collection, "The Phoenix Collection")
        self.assertEqual(formulas[0].input_rarity, Rarity.MIL_SPEC)
        self.assertEqual(formulas[0].collection_counts, {"The Phoenix Collection": 10})
        self.assertAlmostEqual(formulas[0].target_probability, 1.0)


if __name__ == "__main__":
    unittest.main()
