from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    Exterior,
    ItemCatalog,
    ItemDefinition,
    ItemVariant,
    MARKET_DERIVED_COLLECTION,
    Rarity,
    SteamDTMarketAPI,
    SteamDTPriceSnapshot,
    SteamDTPriceSnapshotStore,
    build_item_definition_from_steamdt_snapshots,
    export_steamdt_item_price_details_csv,
    export_steamdt_item_platform_prices_html,
    export_steamdt_item_platform_prices_csv,
    discover_steamdt_firearm_item_names,
    sync_steamdt_items_to_catalog,
)


class FakeTransport:
    def __init__(self, payloads):
        self.payloads = dict(payloads)
        self.calls = []
        self.crawl_calls = []

    def fetch_market_payload(self, *, query_name: str = ""):
        self.calls.append(query_name)
        return self.payloads[query_name]

    def crawl_market_payloads(
        self,
        *,
        query_name: str = "",
        max_pages: int | None = None,
        scroll_pause_ms: int = 2500,
        idle_scroll_limit: int = 3,
    ):
        self.crawl_calls.append(
            {
                "query_name": query_name,
                "max_pages": max_pages,
                "scroll_pause_ms": scroll_pause_ms,
                "idle_scroll_limit": idle_scroll_limit,
            }
        )
        payload = self.payloads[query_name]
        if isinstance(payload, tuple):
            items = payload
        else:
            items = (payload,)
        return items[: max_pages or None]


def build_payload(items, *, total="1", next_id=""):
    return {
        "success": True,
        "data": {
            "pageNum": "1",
            "pageSize": "20",
            "total": total,
            "nextId": next_id,
            "systemTime": "1772900000000",
            "list": items,
        },
        "errorMsg": None,
    }


def build_item(
    market_hash_name: str,
    *,
    item_id: str = "1",
    rarity_name: str = "restricted",
):
    return {
        "id": item_id,
        "name": market_hash_name,
        "shortName": market_hash_name.split(" (")[0],
        "marketHashName": market_hash_name,
        "marketShortName": market_hash_name.split(" (")[0],
        "imageUrl": "https://example.com/item.png",
        "qualityName": "普通",
        "qualityColor": "#888888",
        "rarityName": rarity_name,
        "rarityColor": "#EB4B4B",
        "exteriorName": "Field-Tested",
        "exteriorColor": "#888888",
        "sellingPriceList": [
            {
                "platform": "buff",
                "platformName": "BUFF",
                "price": 488.0,
                "lastUpdate": 1772900000,
                "link": "https://buff.example/item",
            }
        ],
        "purchasePriceList": [],
        "increasePrice": 1.25,
        "trendList": [["1", 500.0], ["2", 490.0], ["3", 495.0]],
        "sellNum": 321,
    }


class CatalogSyncTests(unittest.TestCase):
    def test_discover_steamdt_firearm_item_names_filters_non_guns(self) -> None:
        transport = FakeTransport(
            {
                "": (
                    build_payload(
                        [
                            build_item("Sticker | Fluxo (Holo) | Austin 2025", item_id="1"),
                            build_item("AK-47 | Redline (Field-Tested)", item_id="2"),
                            build_item("StatTrak™ AK-47 | Redline (Minimal Wear)", item_id="3"),
                            build_item("USP-S | Cortex (Factory New)", item_id="4"),
                        ],
                        total="4",
                    ),
                )
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        names = discover_steamdt_firearm_item_names(client, limit=2, max_pages=1)

        self.assertEqual(names, ("AK-47 | Redline", "USP-S | Cortex"))

    def test_build_item_definition_from_steamdt_snapshots_bootstraps_market_derived_item(self) -> None:
        snapshots = (
            SteamDTPriceSnapshot(
                market_hash_name="AK-47 | Redline (Factory New)",
                item_name="AK-47 | Redline",
                exterior="Factory New",
                lowest_price=10.0,
                raw_json='{"rarityName":"restricted"}',
            ),
            SteamDTPriceSnapshot(
                market_hash_name="StatTrak™ AK-47 | Redline (Field-Tested)",
                item_name="StatTrak™ AK-47 | Redline",
                exterior="Field-Tested",
                lowest_price=20.0,
                raw_json='{"rarityName":"restricted"}',
            ),
        )

        item = build_item_definition_from_steamdt_snapshots("AK-47 | Redline", snapshots)

        self.assertEqual(item.collection, MARKET_DERIVED_COLLECTION)
        self.assertEqual(item.rarity, Rarity.RESTRICTED)
        self.assertEqual(item.available_variants, (ItemVariant.NORMAL, ItemVariant.STATTRAK))
        self.assertEqual(
            item.available_exteriors,
            (Exterior.FACTORY_NEW, Exterior.FIELD_TESTED),
        )
        self.assertAlmostEqual(item.min_float, 0.0)
        self.assertAlmostEqual(item.max_float, 0.38)

    def test_sync_steamdt_items_to_catalog_preserves_existing_static_fields(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [
                        build_item("AK-47 | Redline (Field-Tested)", item_id="1"),
                        build_item("AK-47 | Redline (Well-Worn)", item_id="2"),
                    ],
                    total="2",
                ),
                "StatTrak™ AK-47 | Redline": build_payload(
                    [
                        build_item("StatTrak™ AK-47 | Redline (Minimal Wear)", item_id="3"),
                        build_item("StatTrak™ AK-47 | Redline (Field-Tested)", item_id="4"),
                    ],
                    total="2",
                ),
            }
        )
        client = SteamDTMarketAPI(transport=transport)
        base_catalog = ItemCatalog(
            [
                ItemDefinition(
                    "AK-47 | Redline",
                    "Phoenix",
                    Rarity.RESTRICTED,
                    0.10,
                    0.70,
                    available_variants=(ItemVariant.NORMAL,),
                    available_exteriors=(
                        Exterior.FIELD_TESTED,
                        Exterior.WELL_WORN,
                        Exterior.BATTLE_SCARRED,
                    ),
                )
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "items.json"
            sqlite_path = Path(temp_dir) / "items.sqlite"
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            summary = sync_steamdt_items_to_catalog(
                snapshot_store=store,
                steamdt_client=client,
                target_item_names=("AK-47 | Redline",),
                base_catalog=base_catalog,
                output_json_path=json_path,
                output_sqlite_path=sqlite_path,
            )
            reloaded = ItemCatalog.from_path(sqlite_path).get_item("AK-47 | Redline")

            self.assertTrue(json_path.exists())
            self.assertTrue(sqlite_path.exists())

        self.assertEqual(summary.items_synced, 1)
        self.assertEqual(reloaded.collection, "Phoenix")
        self.assertEqual(reloaded.rarity, Rarity.RESTRICTED)
        self.assertAlmostEqual(reloaded.min_float, 0.10)
        self.assertAlmostEqual(reloaded.max_float, 0.70)
        self.assertEqual(reloaded.available_variants, (ItemVariant.NORMAL, ItemVariant.STATTRAK))
        self.assertEqual(
            reloaded.available_exteriors,
            (
                Exterior.MINIMAL_WEAR,
                Exterior.FIELD_TESTED,
                Exterior.WELL_WORN,
                Exterior.BATTLE_SCARRED,
            ),
        )

    def test_sync_steamdt_items_to_catalog_prefers_cached_family_variant_data(self) -> None:
        transport = FakeTransport(
            {
                "StatTrak™ AK-47 | Redline": build_payload(
                    [
                        build_item("StatTrak™ AK-47 | Redline (Minimal Wear)", item_id="3"),
                        build_item("StatTrak™ AK-47 | Redline (Field-Tested)", item_id="4"),
                    ],
                    total="2",
                ),
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="AK-47 | Redline (Field-Tested)",
                    item_name="AK-47 | Redline",
                    exterior="Field-Tested",
                    lowest_price=9.0,
                    raw_json='{"rarityName":"restricted"}',
                )
            )
            summary = sync_steamdt_items_to_catalog(
                snapshot_store=store,
                steamdt_client=client,
                target_item_names=("AK-47 | Redline",),
                output_json_path=Path(temp_dir) / "items.json",
                output_sqlite_path=Path(temp_dir) / "items.sqlite",
            )

        self.assertEqual(summary.items_synced, 1)
        self.assertEqual(transport.calls, ["StatTrak™ AK-47 | Redline"])

    def test_export_steamdt_item_price_details_csv_outputs_one_row_per_variant_exterior(self) -> None:
        catalog = ItemCatalog(
            [
                ItemDefinition(
                    "AK-47 | Redline",
                    "Phoenix",
                    Rarity.RESTRICTED,
                    0.10,
                    0.70,
                    available_variants=(ItemVariant.NORMAL, ItemVariant.STATTRAK),
                    available_exteriors=(Exterior.MINIMAL_WEAR, Exterior.FIELD_TESTED),
                )
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="AK-47 | Redline (Field-Tested)",
                    item_name="AK-47 | Redline",
                    exterior="Field-Tested",
                    lowest_price=9.0,
                    recent_average_price=9.5,
                    selected_platform="buff",
                    selected_platform_name="BUFF",
                    fetched_at="2026-03-08T00:00:00+00:00",
                )
            )
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="StatTrak™ AK-47 | Redline (Minimal Wear)",
                    item_name="StatTrak™ AK-47 | Redline",
                    exterior="Minimal Wear",
                    lowest_price=19.0,
                    recent_average_price=19.5,
                    selected_platform="buff",
                    selected_platform_name="BUFF",
                    fetched_at="2026-03-08T00:00:00+00:00",
                )
            )
            output_path = export_steamdt_item_price_details_csv(
                snapshot_store=store,
                catalog=catalog,
                output_csv_path=Path(temp_dir) / "details.csv",
                item_names=("AK-47 | Redline",),
            )
            csv_text = output_path.read_text(encoding="utf-8-sig")

        self.assertIn("base_item_name,variant,item_name,market_hash_name,exterior", csv_text)
        self.assertIn("AK-47 | Redline,Normal,AK-47 | Redline,AK-47 | Redline (Field-Tested),Field-Tested", csv_text)
        self.assertIn("AK-47 | Redline,StatTrak,StatTrak™ AK-47 | Redline,StatTrak™ AK-47 | Redline (Minimal Wear),Minimal Wear", csv_text)

    def test_export_steamdt_item_platform_prices_csv_outputs_platform_columns(self) -> None:
        catalog = ItemCatalog(
            [
                ItemDefinition(
                    "AK-47 | Redline",
                    "Phoenix",
                    Rarity.RESTRICTED,
                    0.10,
                    0.70,
                    available_variants=(ItemVariant.NORMAL,),
                    available_exteriors=(Exterior.FIELD_TESTED,),
                )
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="AK-47 | Redline (Field-Tested)",
                    item_name="AK-47 | Redline",
                    exterior="Field-Tested",
                    lowest_price=9.0,
                    recent_average_price=9.5,
                    selected_platform="buff",
                    selected_platform_name="BUFF",
                    fetched_at="2026-03-08T00:00:00+00:00",
                    raw_json=(
                        '{"sellingPriceList":['
                        '{"platform":"buff","platformName":"BUFF","price":9.0,"link":"https://buff.example"},'
                        '{"platform":"c5","platformName":"C5GAME","price":9.2,"link":"https://c5.example"},'
                        '{"platform":"steam","platformName":"Steam","price":12.3,"link":"https://steam.example"}'
                        ']}'
                    ),
                )
            )
            output_path = export_steamdt_item_platform_prices_csv(
                snapshot_store=store,
                catalog=catalog,
                output_csv_path=Path(temp_dir) / "platforms.csv",
                item_names=("AK-47 | Redline",),
            )
            csv_text = output_path.read_text(encoding="utf-8-sig")

        self.assertIn("BUFF_price,悠悠_price,Steam_price,HaloSkins_price", csv_text)
        self.assertIn("9.0,,12.3,", csv_text)

    def test_export_steamdt_item_platform_prices_html_uses_chinese_labels(self) -> None:
        catalog = ItemCatalog(
            [
                ItemDefinition(
                    "AK-47 | The Empress",
                    "MarketDerived",
                    Rarity.COVERT,
                    0.0,
                    1.0,
                    available_variants=(ItemVariant.NORMAL,),
                    available_exteriors=(Exterior.FACTORY_NEW,),
                )
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="AK-47 | The Empress (Factory New)",
                    item_name="AK-47 | The Empress",
                    exterior="Factory New",
                    lowest_price=100.0,
                    raw_json='{"sellingPriceList":[{"platform":"buff","platformName":"BUFF","price":100.0}]}',
                )
            )
            output_path = export_steamdt_item_platform_prices_html(
                snapshot_store=store,
                catalog=catalog,
                output_html_path=Path(temp_dir) / "dashboard.html",
                item_names=("AK-47 | The Empress",),
            )
            html_text = output_path.read_text(encoding="utf-8")

        self.assertIn("皇后", html_text)
        self.assertIn("崭新出厂", html_text)
        self.assertIn("市场派生", html_text)
        self.assertIn("暗金数", html_text)

    def test_export_steamdt_item_platform_prices_html_hides_empty_platform_columns(self) -> None:
        catalog = ItemCatalog(
            [
                ItemDefinition(
                    "AK-47 | The Empress",
                    "MarketDerived",
                    Rarity.COVERT,
                    0.0,
                    1.0,
                    available_variants=(ItemVariant.NORMAL,),
                    available_exteriors=(Exterior.FACTORY_NEW,),
                )
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="AK-47 | The Empress (Factory New)",
                    item_name="AK-47 | The Empress",
                    exterior="Factory New",
                    lowest_price=100.0,
                    raw_json=(
                        '{"sellingPriceList":['
                        '{"platform":"buff","platformName":"BUFF","price":100.0},'
                        '{"platform":"steam","platformName":"Steam","price":120.0},'
                        '{"platform":"dmarket","platformName":"DMarket","price":0},'
                        '{"platform":"csmoney","platformName":"CSMoney","price":0}'
                        ']}'
                    ),
                )
            )
            output_path = export_steamdt_item_platform_prices_html(
                snapshot_store=store,
                catalog=catalog,
                output_html_path=Path(temp_dir) / "dashboard.html",
                item_names=("AK-47 | The Empress",),
            )
            html_text = output_path.read_text(encoding="utf-8")

        self.assertIn("网易BUFF", html_text)
        self.assertIn("Steam社区", html_text)
        self.assertNotIn("<th>C5GAME</th>", html_text)
        self.assertNotIn("<th>DMarket</th>", html_text)
        self.assertNotIn("<th>CS.MONEY</th>", html_text)


if __name__ == "__main__":
    unittest.main()
