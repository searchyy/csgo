from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    Exterior,
    ItemCatalog,
    ItemDefinition,
    Rarity,
    SteamDTPriceSnapshot,
    SteamDTPriceSnapshotStore,
    build_watchlist_from_steamdt_cache,
    scan_steamdt_tradeup_candidates,
)
from cs2_tradeup.steamdt_market import CachedSteamDTMarketAPI


class FailingLiveClient:
    def __init__(self) -> None:
        self.calls = []

    def get_item_listings(self, item_name: str, exteriors=None):
        self.calls.append((item_name, tuple(exteriors or ())))
        raise AssertionError("cache-only mode should not hit live client")

    def close(self) -> None:
        return None


class SteamDTScanPipelineTests(unittest.TestCase):
    def build_catalog(self) -> ItemCatalog:
        return ItemCatalog(
            [
                ItemDefinition("Alpha Input 1", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00),
                ItemDefinition("Alpha Input 2", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00),
                ItemDefinition("Target Gun", "Alpha", Rarity.RESTRICTED, 0.00, 0.70),
                ItemDefinition("Alpha Sidearm", "Alpha", Rarity.RESTRICTED, 0.10, 0.60),
                ItemDefinition("Impossible Target", "Beta", Rarity.RESTRICTED, 0.00, 0.50),
            ]
        )

    def test_cached_steamdt_market_api_cache_only_uses_sqlite_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="AK-47 | Slate (Field-Tested)",
                    item_name="AK-47 | Slate",
                    exterior="Field-Tested",
                    lowest_price=11.5,
                    recent_average_price=11.8,
                    fetched_at_epoch=0.0,
                    fetched_at="2026-03-08T00:00:00+00:00",
                )
            )
            live_client = FailingLiveClient()
            cached_client = CachedSteamDTMarketAPI(
                store,
                steamdt_client=live_client,
                max_age_seconds=1.0,
                write_back_on_fetch=False,
                allow_live_fetch=False,
            )

            quote = cached_client.get_item_price("AK-47 | Slate", Exterior.FIELD_TESTED)

        self.assertAlmostEqual(quote.lowest_price, 11.5)
        self.assertAlmostEqual(quote.recent_average_price, 11.8)
        self.assertEqual(live_client.calls, [])

    def test_scan_steamdt_tradeup_candidates_runs_from_cache_and_exports_csv(self) -> None:
        catalog = self.build_catalog()
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "items.sqlite"
            catalog.to_sqlite(sqlite_path)

            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshots(
                [
                    SteamDTPriceSnapshot(
                        market_hash_name="Target Gun (Factory New)",
                        item_name="Target Gun",
                        exterior="Factory New",
                        lowest_price=100.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                    SteamDTPriceSnapshot(
                        market_hash_name="Alpha Sidearm (Minimal Wear)",
                        item_name="Alpha Sidearm",
                        exterior="Minimal Wear",
                        lowest_price=10.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                    SteamDTPriceSnapshot(
                        market_hash_name="Alpha Input 1 (Minimal Wear)",
                        item_name="Alpha Input 1",
                        exterior="Minimal Wear",
                        lowest_price=5.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                    SteamDTPriceSnapshot(
                        market_hash_name="Alpha Input 2 (Minimal Wear)",
                        item_name="Alpha Input 2",
                        exterior="Minimal Wear",
                        lowest_price=3.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                ]
            )

            auto_targets = build_watchlist_from_steamdt_cache(
                catalog=sqlite_path,
                snapshot_store=store,
                item_names=("Target Gun",),
                target_rarities=(Rarity.RESTRICTED,),
                cached_exteriors_only=True,
            )
            summary = scan_steamdt_tradeup_candidates(
                catalog=sqlite_path,
                snapshot_store=store,
                item_names=("Target Gun",),
                target_rarities=(Rarity.RESTRICTED,),
                cached_exteriors_only=True,
                cache_only=True,
                roi_threshold=1.05,
                formula_limit_per_target=5,
                formula_options={
                    "min_target_count": 10,
                    "max_target_count": 10,
                    "max_auxiliary_collections": 0,
                    "max_formulas": 5,
                },
                output_csv_path=Path(temp_dir) / "scan.csv",
            )

        self.assertEqual(len(auto_targets), 1)
        self.assertEqual(auto_targets[0].item_name, "Target Gun")
        self.assertEqual(auto_targets[0].exterior, Exterior.FACTORY_NEW)
        self.assertEqual(summary.targets_scanned, 1)
        self.assertEqual(summary.results_found, 1)
        self.assertTrue(summary.cache_only)
        self.assertFalse(summary.live_fetch_enabled)
        self.assertIsNotNone(summary.output_csv_path)
        self.assertAlmostEqual(summary.results[0].total_cost, 30.0)
        self.assertGreater(summary.results[0].roi, 1.7)
        self.assertEqual(summary.results[0].material_pricings[0].item.name, "Alpha Input 2")

    def test_scan_steamdt_tradeup_candidates_skips_targets_without_valid_inputs(self) -> None:
        catalog = self.build_catalog()
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "items.sqlite"
            catalog.to_sqlite(sqlite_path)

            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshots(
                [
                    SteamDTPriceSnapshot(
                        market_hash_name="Target Gun (Factory New)",
                        item_name="Target Gun",
                        exterior="Factory New",
                        lowest_price=100.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                    SteamDTPriceSnapshot(
                        market_hash_name="Impossible Target (Factory New)",
                        item_name="Impossible Target",
                        exterior="Factory New",
                        lowest_price=88.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                    SteamDTPriceSnapshot(
                        market_hash_name="Alpha Sidearm (Minimal Wear)",
                        item_name="Alpha Sidearm",
                        exterior="Minimal Wear",
                        lowest_price=10.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                    SteamDTPriceSnapshot(
                        market_hash_name="Alpha Input 1 (Minimal Wear)",
                        item_name="Alpha Input 1",
                        exterior="Minimal Wear",
                        lowest_price=5.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                    SteamDTPriceSnapshot(
                        market_hash_name="Alpha Input 2 (Minimal Wear)",
                        item_name="Alpha Input 2",
                        exterior="Minimal Wear",
                        lowest_price=3.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                ]
            )

            summary = scan_steamdt_tradeup_candidates(
                catalog=sqlite_path,
                snapshot_store=store,
                item_names=("Target Gun", "Impossible Target"),
                target_rarities=(Rarity.RESTRICTED,),
                cached_exteriors_only=True,
                cache_only=True,
                roi_threshold=1.05,
                formula_limit_per_target=5,
                formula_options={
                    "min_target_count": 10,
                    "max_target_count": 10,
                    "max_auxiliary_collections": 0,
                    "max_formulas": 5,
                },
                output_csv_path=None,
            )

        self.assertEqual(summary.targets_scanned, 2)
        self.assertEqual(summary.results_found, 1)
        self.assertEqual(len(summary.results), 1)
        self.assertEqual(summary.results[0].target_item.name, "Target Gun")


if __name__ == "__main__":
    unittest.main()
