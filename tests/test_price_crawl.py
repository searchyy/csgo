from pathlib import Path
import sys
import tempfile
import unittest
from concurrent.futures.process import BrokenProcessPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    ItemCatalog,
    ItemDefinition,
    Rarity,
    SteamDTPriceSnapshot,
    SteamDTPriceSnapshotStore,
    build_steamdt_crawl_worker_profiles,
    crawl_catalog_item_prices_multiworker_to_sqlite,
    partition_catalog_item_names,
)
from cs2_tradeup.price_crawl import _is_steamdt_quota_error


class PriceCrawlTests(unittest.TestCase):
    def test_partition_catalog_item_names_round_robin(self) -> None:
        shards = partition_catalog_item_names(
            ("A", "B", "C", "D", "E"),
            2,
        )

        self.assertEqual(shards, (("A", "C", "E"), ("B", "D")))

    def test_build_worker_profiles_rotates_user_agents_and_proxies(self) -> None:
        profiles = build_steamdt_crawl_worker_profiles(
            3,
            user_agents=("ua-1", "ua-2"),
            proxy_servers=("http://proxy-1", "http://proxy-2", "http://proxy-3"),
            locales=("zh-CN", "en-US"),
            timezone_ids=("Asia/Taipei",),
            proxy_credentials=(("alice", "pw-1"), ("bob", "pw-2")),
            rate_limit_min_seconds=1.0,
            rate_limit_max_seconds=2.0,
            rate_limit_step_seconds=0.5,
        )

        self.assertEqual(len(profiles), 3)
        self.assertEqual(profiles[0].user_agent, "ua-1")
        self.assertEqual(profiles[1].user_agent, "ua-2")
        self.assertEqual(profiles[2].user_agent, "ua-1")
        self.assertEqual(profiles[0].proxy_server, "http://proxy-1")
        self.assertEqual(profiles[1].proxy_server, "http://proxy-2")
        self.assertEqual(profiles[2].proxy_server, "http://proxy-3")
        self.assertEqual(profiles[0].proxy_username, "alice")
        self.assertEqual(profiles[1].proxy_username, "bob")
        self.assertEqual(profiles[2].proxy_username, "alice")
        self.assertEqual(profiles[0].locale, "zh-CN")
        self.assertEqual(profiles[1].locale, "en-US")
        self.assertEqual(profiles[2].timezone_id, "Asia/Taipei")
        self.assertLess(profiles[0].rate_limit_min_seconds, profiles[1].rate_limit_min_seconds)
        self.assertLess(profiles[1].rate_limit_min_seconds, profiles[2].rate_limit_min_seconds)

    def test_multiworker_summary_aggregates_worker_results(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)

        catalog = ItemCatalog(
            [
                ItemDefinition("Item A", "Alpha", Rarity.MIL_SPEC, 0.0, 1.0),
                ItemDefinition("Item B", "Alpha", Rarity.MIL_SPEC, 0.0, 1.0),
                ItemDefinition("Item C", "Alpha", Rarity.MIL_SPEC, 0.0, 1.0),
                ItemDefinition("Item D", "Alpha", Rarity.MIL_SPEC, 0.0, 1.0),
            ]
        )
        snapshot_path = Path(temp_dir.name) / "steamdt.sqlite"
        store = SteamDTPriceSnapshotStore(snapshot_path)

        def fake_runner(task):
            task_store = SteamDTPriceSnapshotStore(task.snapshot_store_path)
            for item_name in task.item_names:
                task_store.insert_snapshot(
                    SteamDTPriceSnapshot(
                        market_hash_name=f"{item_name} (Factory New)",
                        item_name=item_name,
                        exterior="Factory New",
                        lowest_price=10.0,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    )
                )
            return type(
                "WorkerSummary",
                (),
                {
                    "worker_id": task.worker_id,
                    "assigned_items": len(task.item_names),
                    "processed_items": len(task.item_names),
                    "skipped_recent_items": 0,
                    "failed_items": (),
                    "snapshots_inserted": len(task.item_names),
                    "log_path": task.log_path,
                },
            )()

        summary = crawl_catalog_item_prices_multiworker_to_sqlite(
            catalog=catalog,
            snapshot_store=store,
            worker_count=2,
            worker_profiles=build_steamdt_crawl_worker_profiles(2, user_agents=("ua-1", "ua-2")),
            _worker_runner=fake_runner,
        )

        self.assertEqual(summary.worker_count, 2)
        self.assertEqual(summary.total_items, 4)
        self.assertEqual(summary.processed_items, 4)
        self.assertEqual(summary.snapshots_inserted, 4)
        self.assertEqual(summary.snapshots_after, 4)
        self.assertEqual(len(summary.worker_summaries), 2)
        self.assertFalse((snapshot_path.parent / ".multiworker_catalog.sqlite").exists())

    def test_multiworker_supervisor_retries_after_pool_failure(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)

        catalog = ItemCatalog(
            [
                ItemDefinition("Item A", "Alpha", Rarity.MIL_SPEC, 0.0, 1.0),
                ItemDefinition("Item B", "Alpha", Rarity.MIL_SPEC, 0.0, 1.0),
            ]
        )
        snapshot_path = Path(temp_dir.name) / "steamdt.sqlite"
        store = SteamDTPriceSnapshotStore(snapshot_path)
        call_count = {"value": 0}

        def fake_batch_runner(tasks):
            call_count["value"] += 1
            if call_count["value"] == 1:
                raise BrokenProcessPool("simulated crash")
            summaries = []
            for task in tasks:
                summaries.append(
                    type(
                        "WorkerSummary",
                        (),
                        {
                            "worker_id": task.worker_id,
                            "assigned_items": len(task.item_names),
                            "processed_items": len(task.item_names),
                            "skipped_recent_items": 0,
                            "failed_items": (),
                            "snapshots_inserted": len(task.item_names),
                            "log_path": task.log_path,
                        },
                    )()
                )
            return tuple(summaries)

        summary = crawl_catalog_item_prices_multiworker_to_sqlite(
            catalog=catalog,
            snapshot_store=store,
            worker_count=2,
            worker_profiles=build_steamdt_crawl_worker_profiles(2, user_agents=("ua-1", "ua-2")),
            supervisor_restart_limit=2,
            supervisor_backoff_base_seconds=0.0,
            _supervised_batch_runner=fake_batch_runner,
        )

        self.assertEqual(call_count["value"], 2)
        self.assertEqual(summary.worker_count, 2)
        self.assertEqual(summary.processed_items, 2)
        self.assertEqual(summary.failed_items, ())

    def test_quota_error_detection(self) -> None:
        self.assertTrue(_is_steamdt_quota_error(Exception("今日访问次数超限，请明日再试！")))
        self.assertFalse(_is_steamdt_quota_error(Exception("SteamDT did not return item")))

    def test_multiworker_supervisor_stops_on_quota_error(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)

        catalog = ItemCatalog(
            [
                ItemDefinition("Item A", "Alpha", Rarity.MIL_SPEC, 0.0, 1.0),
                ItemDefinition("Item B", "Alpha", Rarity.MIL_SPEC, 0.0, 1.0),
            ]
        )
        snapshot_path = Path(temp_dir.name) / "steamdt.sqlite"
        store = SteamDTPriceSnapshotStore(snapshot_path)
        call_count = {"value": 0}

        def fake_batch_runner(tasks):
            call_count["value"] += 1
            raise Exception("今日访问次数超限，请明日再试！")

        summary = crawl_catalog_item_prices_multiworker_to_sqlite(
            catalog=catalog,
            snapshot_store=store,
            worker_count=2,
            worker_profiles=build_steamdt_crawl_worker_profiles(2, user_agents=("ua-1", "ua-2")),
            supervisor_restart_limit=3,
            supervisor_backoff_base_seconds=0.0,
            _supervised_batch_runner=fake_batch_runner,
        )

        self.assertEqual(call_count["value"], 1)
        self.assertEqual(summary.processed_items, 0)
        self.assertEqual(len(summary.failed_items), 2)


if __name__ == "__main__":
    unittest.main()
