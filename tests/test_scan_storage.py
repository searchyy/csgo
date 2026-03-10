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
    TradeUpScanResultStore,
    scan_steamdt_tradeup_candidates,
)


class ScanStorageTests(unittest.TestCase):
    def build_catalog(self) -> ItemCatalog:
        return ItemCatalog(
            [
                ItemDefinition("Alpha Input 1", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00),
                ItemDefinition("Alpha Input 2", "Alpha", Rarity.MIL_SPEC, 0.00, 1.00),
                ItemDefinition("Target Gun", "Alpha", Rarity.RESTRICTED, 0.00, 0.70),
                ItemDefinition("Alpha Sidearm", "Alpha", Rarity.RESTRICTED, 0.10, 0.60),
            ]
        )

    def test_scan_result_store_persists_and_queries_results(self) -> None:
        catalog = self.build_catalog()
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_path = Path(temp_dir) / "items.sqlite"
            scan_path = Path(temp_dir) / "scan_results.sqlite"
            catalog.to_sqlite(catalog_path)

            price_store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            price_store.insert_snapshots(
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
            summary = scan_steamdt_tradeup_candidates(
                catalog=catalog_path,
                snapshot_store=price_store,
                item_names=("Target Gun",),
                target_rarities=(Rarity.RESTRICTED,),
                cached_exteriors_only=True,
                cache_only=True,
                roi_threshold=1.0,
                formula_limit_per_target=5,
                formula_options={
                    "min_target_count": 10,
                    "max_target_count": 10,
                    "max_auxiliary_collections": 0,
                    "max_formulas": 5,
                },
                output_csv_path=None,
            )
            store = TradeUpScanResultStore(scan_path)
            run_id = store.create_run(run_type="ev_scan", parameters={"item_names": ["Target Gun"]})
            inserted = store.append_results(run_id, summary.results)
            store.complete_run(run_id, summary={"results_found": summary.results_found})

            runs = store.list_runs()
            results = store.list_results(search="Target Gun")

        self.assertEqual(inserted, 1)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].status, "completed")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].target_item, "Target Gun")
        self.assertGreater(results[0].roi, 1.7)


if __name__ == "__main__":
    unittest.main()
