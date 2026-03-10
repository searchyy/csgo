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
    Rarity,
    SteamDTPriceSnapshot,
    SteamDTPriceSnapshotStore,
    TradeUpScanResultStore,
    create_app,
    scan_steamdt_tradeup_candidates,
)


class WebAppTests(unittest.TestCase):
    def build_catalog(self) -> ItemCatalog:
        return ItemCatalog(
            [
                ItemDefinition(
                    "Alpha Input 1",
                    "Alpha",
                    Rarity.MIL_SPEC,
                    0.00,
                    1.00,
                    available_variants=(ItemVariant.NORMAL,),
                    available_exteriors=(Exterior.MINIMAL_WEAR,),
                ),
                ItemDefinition(
                    "Alpha Input 2",
                    "Alpha",
                    Rarity.MIL_SPEC,
                    0.00,
                    1.00,
                    available_variants=(ItemVariant.NORMAL,),
                    available_exteriors=(Exterior.MINIMAL_WEAR,),
                ),
                ItemDefinition(
                    "Target Gun",
                    "Alpha",
                    Rarity.RESTRICTED,
                    0.00,
                    0.70,
                    available_variants=(ItemVariant.NORMAL,),
                    available_exteriors=(Exterior.FACTORY_NEW,),
                ),
                ItemDefinition(
                    "Alpha Sidearm",
                    "Alpha",
                    Rarity.RESTRICTED,
                    0.10,
                    0.60,
                    available_variants=(ItemVariant.NORMAL,),
                    available_exteriors=(Exterior.MINIMAL_WEAR,),
                ),
            ]
        )

    def build_app(self):
        temp_dir = tempfile.TemporaryDirectory()
        catalog = self.build_catalog()
        catalog_path = Path(temp_dir.name) / "items.sqlite"
        catalog.to_sqlite(catalog_path)

        price_store = SteamDTPriceSnapshotStore(Path(temp_dir.name) / "steamdt.sqlite")
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
            conservative_float_mode=False,
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
        scan_store = TradeUpScanResultStore(Path(temp_dir.name) / "scan_results.sqlite")
        run_id = scan_store.create_run(run_type="ev_scan", parameters={"item_names": ["Target Gun"]})
        scan_store.append_results(run_id, summary.results)
        scan_store.complete_run(run_id, summary={"results_found": summary.results_found})

        crawl_log_path = Path(temp_dir.name) / "crawl.log"
        crawl_log_path.write_text(
            "\n".join(
                [
                    "START 2026-03-08 00:00:00",
                    "[2026-03-08 00:00:01] [0/2] 已启动 2 个独立浏览器抓取进程",
                ]
            ),
            encoding="utf-8",
        )
        worker_log_dir = Path(temp_dir.name) / "crawl_workers"
        worker_log_dir.mkdir()
        (worker_log_dir / "steamdt_worker_01.log").write_text(
            "\n".join(
                [
                    "START worker=1 items=2",
                    "[2026-03-08 00:00:05] [worker 1] [1/2] 抓取价格：Alpha Input 1",
                ]
            ),
            encoding="utf-8",
        )
        (worker_log_dir / "steamdt_worker_02.log").write_text(
            "\n".join(
                [
                    "START worker=2 items=2",
                    "SUMMARY worker=2 processed=1 skipped=0 failed=0 snapshots_inserted=1",
                ]
            ),
            encoding="utf-8",
        )

        app = create_app(
            catalog_path=catalog_path,
            price_snapshot_path=price_store.path,
            scan_result_path=scan_store.path,
            crawl_log_path=crawl_log_path,
            crawl_worker_log_dir=worker_log_dir,
        )
        app.config.update(TESTING=True)
        return temp_dir, app

    def test_web_app_core_pages_and_apis(self) -> None:
        temp_dir, app = self.build_app()
        self.addCleanup(temp_dir.cleanup)
        client = app.test_client()

        health = client.get("/api/health")
        prices = client.get("/api/prices?limit=20")
        ev_results = client.get("/api/ev/results?limit=20")
        crawl_progress = client.get("/api/crawl/progress?limit=20")
        optimizer = client.get(
            "/api/optimizer?item_name=Target%20Gun&exterior=Factory%20New&cache_only=true&persist=false&conservative_float_mode=false"
        )
        page = client.get("/prices")
        ev_page = client.get("/ev")
        crawl_page = client.get("/crawl-progress")

        self.assertEqual(health.status_code, 200)
        self.assertTrue(health.get_json()["ok"])
        self.assertEqual(prices.status_code, 200)
        self.assertGreaterEqual(prices.get_json()["total"], 1)
        self.assertIn("safe_price", prices.get_json()["rows"][0])
        self.assertIn("is_valid", prices.get_json()["rows"][0])
        self.assertIn("no-store", prices.headers.get("Cache-Control", ""))
        self.assertEqual(ev_results.status_code, 200)
        self.assertEqual(ev_results.get_json()["total"], 1)
        self.assertIn("float_validation_status", ev_results.get_json()["rows"][0])
        self.assertIn("min_float", ev_results.get_json()["rows"][0]["materials"][0])
        self.assertIn("estimated_float", ev_results.get_json()["rows"][0]["materials"][0])
        self.assertIn("no-store", ev_results.headers.get("Cache-Control", ""))
        self.assertEqual(crawl_progress.status_code, 200)
        crawl_payload = crawl_progress.get_json()
        self.assertEqual(crawl_payload["summary"]["total_families"], 4)
        self.assertEqual(crawl_payload["summary"]["complete_families"], 4)
        self.assertEqual(len(crawl_payload["workers"]), 2)
        self.assertEqual(crawl_payload["run"]["status"], "running")
        self.assertEqual(optimizer.status_code, 200)
        self.assertGreaterEqual(len(optimizer.get_json()["rows"]), 1)
        self.assertEqual(page.status_code, 200)
        self.assertIn("多平台价格看板", page.get_data(as_text=True))
        self.assertIn("id=\"auto-refresh\"", page.get_data(as_text=True))
        self.assertIn("no-store", page.headers.get("Cache-Control", ""))
        self.assertEqual(ev_page.status_code, 200)
        self.assertIn("全量炼金 EV 排行", ev_page.get_data(as_text=True))
        self.assertIn("id=\"auto-refresh\"", ev_page.get_data(as_text=True))
        self.assertNotIn("slice(0, 4)", ev_page.get_data(as_text=True))
        self.assertIn("no-store", ev_page.headers.get("Cache-Control", ""))
        self.assertEqual(crawl_page.status_code, 200)
        self.assertIn("价格抓取进度看板", crawl_page.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
