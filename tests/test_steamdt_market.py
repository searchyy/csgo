from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    CachedSteamDTMarketAPI,
    MarketParseError,
    SteamDTMarketAPI,
    SteamDTCrawlSummary,
    SteamDTPriceSnapshot,
    SteamDTPriceSnapshotStore,
    crawl_all_steamdt_market_to_sqlite,
    split_steamdt_market_hash_name,
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
    name: str | None = None,
    prices=None,
    trend=None,
    quality_name: str = "Normal",
):
    return {
        "id": item_id,
        "name": name or market_hash_name,
        "shortName": market_hash_name.split(" (")[0],
        "marketHashName": market_hash_name,
        "marketShortName": market_hash_name.split(" (")[0],
        "imageUrl": "https://example.com/item.png",
        "qualityName": quality_name,
        "qualityColor": "#888888",
        "rarityName": "Covert",
        "rarityColor": "#EB4B4B",
        "exteriorName": "Field-Tested",
        "exteriorColor": "#888888",
        "sellingPriceList": prices
        or [
            {
                "platform": "buff",
                "platformName": "BUFF",
                "price": 488.0,
                "lastUpdate": 1772900000,
                "link": "https://buff.example/item",
            },
            {
                "platform": "youpin",
                "platformName": "YouPin",
                "price": 487.5,
                "lastUpdate": 1772900000,
                "link": "https://youpin.example/item",
            },
            {
                "platform": "steam",
                "platformName": "Steam",
                "price": 650.0,
                "lastUpdate": 1772900000,
                "link": "https://steam.example/item",
            },
        ],
        "purchasePriceList": [],
        "increasePrice": 1.25,
        "trendList": trend or [["1", 500.0], ["2", 490.0], ["3", 495.0]],
        "sellNum": 321,
    }


class SteamDTMarketTests(unittest.TestCase):
    def test_split_market_hash_name_extracts_exterior(self) -> None:
        item_name, exterior = split_steamdt_market_hash_name("AK-47 | Redline (Field-Tested)")
        self.assertEqual(item_name, "AK-47 | Redline")
        self.assertEqual(exterior, "Field-Tested")

    def test_fetch_market_page_parses_items(self) -> None:
        transport = FakeTransport(
            {
                "": build_payload(
                    [build_item("AK-47 | Redline (Field-Tested)")],
                    total="8",
                    next_id="57,24726",
                )
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        page = client.fetch_market_page()

        self.assertEqual(page.total, 8)
        self.assertEqual(page.next_id, "57,24726")
        self.assertEqual(page.page_size, 20)
        self.assertEqual(page.items[0].market_hash_name, "AK-47 | Redline (Field-Tested)")

    def test_get_item_price_prefers_requested_platforms(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [
                        build_item("AK-47 | Redline (Minimal Wear)", item_id="2"),
                        build_item("AK-47 | Redline (Field-Tested)", item_id="1"),
                    ],
                    total="2",
                )
            }
        )
        client = SteamDTMarketAPI(
            transport=transport,
            preferred_platforms=("youpin", "buff", "steam"),
            trend_sample_size=3,
        )

        quote = client.get_item_price("AK-47 | Redline", "Field-Tested")

        self.assertAlmostEqual(quote.lowest_price, 487.5)
        self.assertAlmostEqual(quote.recent_average_price, 495.0)

    def test_get_item_price_falls_back_to_lowest_positive_platform(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [
                        build_item(
                            "AK-47 | Redline (Field-Tested)",
                            prices=[
                                {
                                    "platform": "buff",
                                    "platformName": "BUFF",
                                    "price": 0,
                                    "lastUpdate": 1,
                                    "link": "https://buff.example/item",
                                },
                                {
                                    "platform": "steam",
                                    "platformName": "Steam",
                                    "price": 700.0,
                                    "lastUpdate": 1,
                                    "link": "https://steam.example/item",
                                },
                                {
                                    "platform": "haloskins",
                                    "platformName": "HaloSkins",
                                    "price": 655.0,
                                    "lastUpdate": 1,
                                    "link": "https://haloskins.example/item",
                                },
                            ],
                        )
                    ]
                )
            }
        )
        client = SteamDTMarketAPI(
            transport=transport,
            preferred_platforms=("buff", "youpin"),
        )

        quote = client.get_item_price("AK-47 | Redline", "Field-Tested")

        self.assertAlmostEqual(quote.lowest_price, 655.0)

    def test_get_item_listing_falls_back_to_full_market_hash_name_search(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [build_item("AK-47 | Redline (Minimal Wear)", item_id="2")]
                ),
                "AK-47 | Redline (Field-Tested)": build_payload(
                    [build_item("AK-47 | Redline (Field-Tested)", item_id="1")]
                ),
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        listing = client.get_item_listing("AK-47 | Redline", "Field-Tested")

        self.assertEqual(listing.id, "1")
        self.assertEqual(
            transport.calls,
            ["AK-47 | Redline", "AK-47 | Redline (Field-Tested)"],
        )

    def test_get_item_prices_fetches_multiple_exteriors_in_single_variant_query(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [
                        build_item("AK-47 | Redline (Factory New)", item_id="1"),
                        build_item("AK-47 | Redline (Minimal Wear)", item_id="2"),
                        build_item("AK-47 | Redline (Field-Tested)", item_id="3"),
                    ],
                    total="3",
                )
            }
        )
        client = SteamDTMarketAPI(
            transport=transport,
            preferred_platforms=("buff", "steam"),
            trend_sample_size=3,
        )

        quotes = client.get_item_prices(
            "AK-47 | Redline",
            exteriors=("Factory New", "Field-Tested"),
        )

        self.assertEqual(sorted(quotes), ["Factory New", "Field-Tested"])
        self.assertEqual(transport.calls, ["AK-47 | Redline"])

    def test_get_item_family_prices_fetches_normal_and_stattrak_with_two_queries(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [
                        build_item("AK-47 | Redline (Factory New)", item_id="1"),
                        build_item("AK-47 | Redline (Field-Tested)", item_id="2"),
                    ],
                    total="2",
                ),
                "StatTrak™ AK-47 | Redline": build_payload(
                    [
                        build_item("StatTrak™ AK-47 | Redline (Factory New)", item_id="3"),
                        build_item("StatTrak™ AK-47 | Redline (Field-Tested)", item_id="4"),
                    ],
                    total="2",
                ),
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        quotes = client.get_item_family_prices(
            "AK-47 | Redline",
            exteriors=("Factory New", "Field-Tested"),
        )

        self.assertIn(("AK-47 | Redline", "Factory New"), quotes)
        self.assertIn(("StatTrak™ AK-47 | Redline", "Field-Tested"), quotes)
        self.assertEqual(
            transport.calls,
            ["AK-47 | Redline", "StatTrak™ AK-47 | Redline"],
        )

    def test_snapshot_store_persists_listing_and_reads_latest_quote(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [build_item("AK-47 | Redline (Field-Tested)", item_id="1")]
                )
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            listing = client.get_item_listing("AK-47 | Redline", "Field-Tested")
            store.insert_listing(
                listing,
                preferred_platforms=client.preferred_platforms,
                trend_sample_size=client.trend_sample_size,
                query="AK-47 | Redline",
            )
            snapshot = store.get_latest_snapshot("AK-47 | Redline", "Field-Tested")

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertAlmostEqual(snapshot.lowest_price, 488.0)
        self.assertAlmostEqual(snapshot.recent_average_price, 495.0)
        self.assertEqual(snapshot.selected_platform, "buff")

    def test_cached_client_prefers_sqlite_snapshot(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [build_item("AK-47 | Redline (Field-Tested)", item_id="1")]
                )
            }
        )
        live_client = SteamDTMarketAPI(transport=transport)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="AK-47 | Redline (Field-Tested)",
                    item_name="AK-47 | Redline",
                    exterior="Field-Tested",
                    lowest_price=466.0,
                    recent_average_price=470.0,
                    selected_platform="buff",
                    selected_platform_name="BUFF",
                    sell_num=200,
                    fetched_at_epoch=1772900000.0,
                    fetched_at="2026-03-08T00:00:00+00:00",
                )
            )
            cached_client = CachedSteamDTMarketAPI(
                store,
                steamdt_client=live_client,
                max_age_seconds=999999999,
            )
            quote = cached_client.get_item_price("AK-47 | Redline", "Field-Tested")

        self.assertAlmostEqual(quote.lowest_price, 466.0)
        self.assertEqual(transport.calls, [])

    def test_cached_client_fetches_and_backfills_sqlite_on_cache_miss(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [build_item("AK-47 | Redline (Field-Tested)", item_id="1")]
                )
            }
        )
        live_client = SteamDTMarketAPI(
            transport=transport,
            preferred_platforms=("buff", "youpin", "steam"),
            trend_sample_size=3,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            cached_client = CachedSteamDTMarketAPI(
                store,
                steamdt_client=live_client,
                max_age_seconds=3600,
            )
            quote = cached_client.get_item_price("AK-47 | Redline", "Field-Tested")
            snapshot = store.get_latest_snapshot("AK-47 | Redline", "Field-Tested")
            self.assertAlmostEqual(quote.lowest_price, 488.0)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertAlmostEqual(snapshot.lowest_price, 488.0)
            self.assertAlmostEqual(snapshot.recent_average_price, 495.0)
            self.assertEqual(store.count_snapshots(), 1)
            self.assertEqual(transport.calls, ["AK-47 | Redline"])

    def test_cached_client_falls_back_to_stale_snapshot_on_live_failure(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": {
                    "success": False,
                    "data": None,
                    "errorMsg": "当前环境异常",
                }
            }
        )
        live_client = SteamDTMarketAPI(transport=transport)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="AK-47 | Redline (Field-Tested)",
                    item_name="AK-47 | Redline",
                    exterior="Field-Tested",
                    lowest_price=472.0,
                    recent_average_price=480.0,
                    selected_platform="buff",
                    selected_platform_name="BUFF",
                    sell_num=200,
                    fetched_at_epoch=1772900000.0,
                    fetched_at="2026-03-08T00:00:00+00:00",
                )
            )
            cached_client = CachedSteamDTMarketAPI(
                store,
                steamdt_client=live_client,
                max_age_seconds=1,
            )
            quote = cached_client.get_item_price("AK-47 | Redline", "Field-Tested")

        self.assertAlmostEqual(quote.lowest_price, 472.0)
        self.assertEqual(transport.calls, ["AK-47 | Redline"])

    def test_warm_query_cache_backfills_all_search_results(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [
                        build_item("AK-47 | Redline (Field-Tested)", item_id="1"),
                        build_item("AK-47 | Redline (Minimal Wear)", item_id="2"),
                    ],
                    total="2",
                )
            }
        )
        live_client = SteamDTMarketAPI(transport=transport)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            cached_client = CachedSteamDTMarketAPI(store, steamdt_client=live_client)
            page = cached_client.warm_query_cache("AK-47 | Redline")

            ft_snapshot = store.get_latest_snapshot("AK-47 | Redline", "Field-Tested")
            mw_snapshot = store.get_latest_snapshot("AK-47 | Redline", "Minimal Wear")
            self.assertEqual(page.total, 2)
            self.assertEqual(store.count_snapshots(), 2)
            self.assertIsNotNone(ft_snapshot)
            self.assertIsNotNone(mw_snapshot)

    def test_cached_client_warm_item_family_cache_backfills_normal_and_stattrak(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": build_payload(
                    [
                        build_item("AK-47 | Redline (Factory New)", item_id="1"),
                        build_item("AK-47 | Redline (Field-Tested)", item_id="2"),
                    ],
                    total="2",
                ),
                "StatTrak™ AK-47 | Redline": build_payload(
                    [
                        build_item("StatTrak™ AK-47 | Redline (Factory New)", item_id="3"),
                        build_item("StatTrak™ AK-47 | Redline (Field-Tested)", item_id="4"),
                    ],
                    total="2",
                ),
            }
        )
        live_client = SteamDTMarketAPI(transport=transport)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            cached_client = CachedSteamDTMarketAPI(store, steamdt_client=live_client)
            snapshots = cached_client.warm_item_family_cache("AK-47 | Redline")

            normal_fn = store.get_latest_snapshot("AK-47 | Redline", "Factory New")
            stattrak_ft = store.get_latest_snapshot("StatTrak™ AK-47 | Redline", "Field-Tested")

        self.assertEqual(len(snapshots), 4)
        self.assertIsNotNone(normal_fn)
        self.assertIsNotNone(stattrak_ft)
        self.assertEqual(
            transport.calls,
            ["AK-47 | Redline", "StatTrak™ AK-47 | Redline"],
        )

    def test_snapshot_store_can_read_cleaned_safe_price(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshots(
                [
                    SteamDTPriceSnapshot(
                        market_hash_name="USP-S | Example (Field-Tested)",
                        item_name="USP-S | Example",
                        exterior="Field-Tested",
                        lowest_price=100.0,
                        sell_num=25,
                        fetched_at_epoch=1.0,
                        fetched_at="2026-03-08T00:00:00+00:00",
                    ),
                    SteamDTPriceSnapshot(
                        market_hash_name="USP-S | Example (Well-Worn)",
                        item_name="USP-S | Example",
                        exterior="Well-Worn",
                        lowest_price=170.0,
                        sell_num=20,
                        fetched_at_epoch=2.0,
                        fetched_at="2026-03-08T00:00:01+00:00",
                    ),
                ]
            )

            store.refresh_cleaned_prices()
            snapshot = store.get_latest_snapshot(
                "USP-S | Example",
                "Well-Worn",
                prefer_cleaned=True,
                require_valid=False,
            )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertAlmostEqual(snapshot.lowest_price, 170.0)
        self.assertAlmostEqual(snapshot.safe_price or 0.0, 100.0)
        self.assertEqual(snapshot.quote.lowest_price, 100.0)
        self.assertIn("exterior_inversion", snapshot.anomaly_flags)

    def test_cached_client_skips_invalid_cleaned_prices(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="CZ75-Auto | Victoria (Well-Worn)",
                    item_name="CZ75-Auto | Victoria",
                    exterior="Well-Worn",
                    lowest_price=16000.0,
                    highest_buy_price=5000.0,
                    sell_num=1,
                    fetched_at_epoch=1.0,
                    fetched_at="2026-03-08T00:00:00+00:00",
                )
            )
            store.refresh_cleaned_prices()
            cached_client = CachedSteamDTMarketAPI(
                store,
                allow_live_fetch=False,
                prefer_safe_price=True,
                require_valid_prices=True,
            )

            with self.assertRaises(MarketParseError):
                cached_client.get_item_price("CZ75-Auto | Victoria", "Well-Worn")

    def test_cached_client_ignores_souvenir_snapshot_flagged_only_in_sqlite_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="P90 | Facility Negative (Minimal Wear)",
                    item_name="P90 | Facility Negative",
                    exterior="Minimal Wear",
                    lowest_price=10.8,
                    highest_buy_price=10.2,
                    sell_num=40,
                    is_souvenir=False,
                    is_tradeup_compatible_normal=True,
                    fetched_at_epoch=1.0,
                    fetched_at="2026-03-08T00:00:00+00:00",
                )
            )
            store.insert_snapshot(
                SteamDTPriceSnapshot(
                    market_hash_name="P90 | Facility Negative (Minimal Wear)",
                    item_name="P90 | Facility Negative",
                    exterior="Minimal Wear",
                    lowest_price=1.53,
                    sell_num=999,
                    is_souvenir=True,
                    is_tradeup_compatible_normal=False,
                    variant_filter_reason="souvenir",
                    fetched_at_epoch=2.0,
                    fetched_at="2026-03-08T00:05:00+00:00",
                )
            )
            store.refresh_cleaned_prices()
            cached_client = CachedSteamDTMarketAPI(
                store,
                allow_live_fetch=False,
                prefer_safe_price=True,
                require_valid_prices=True,
                normal_tradeup_only=True,
            )

            quote = cached_client.get_item_price("P90 | Facility Negative", "Minimal Wear")

            self.assertAlmostEqual(quote.lowest_price, 10.8)

    def test_get_item_prices_filters_souvenir_records_even_if_market_name_matches(self) -> None:
        souvenir_like = build_item(
            "P90 | Facility Negative (Minimal Wear)",
            item_id="1",
            name="纪念品 P90 | 设施系列·底片图 (略有磨损)",
            prices=[
                {
                    "platform": "buff",
                    "platformName": "BUFF",
                    "price": 1.53,
                    "lastUpdate": 1772900000,
                    "link": "https://buff.example/item",
                }
            ],
            quality_name="纪念品",
        )
        normal_item = build_item(
            "P90 | Facility Negative (Minimal Wear)",
            item_id="2",
            name="P90 | 设施系列·底片图 (略有磨损)",
            prices=[
                {
                    "platform": "buff",
                    "platformName": "BUFF",
                    "price": 10.8,
                    "lastUpdate": 1772900000,
                    "link": "https://buff.example/item",
                }
            ],
            quality_name="普通",
        )
        transport = FakeTransport(
            {
                "P90 | Facility Negative": build_payload(
                    [souvenir_like, normal_item],
                    total="2",
                )
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        quote = client.get_item_price("P90 | Facility Negative", "Minimal Wear")

        self.assertAlmostEqual(quote.lowest_price, 10.8)

    def test_crawl_market_pages_uses_transport_bulk_crawl(self) -> None:
        transport = FakeTransport(
            {
                "": (
                    build_payload([build_item("AK-47 | Redline (Field-Tested)", item_id="1")], total="40", next_id="n1"),
                    build_payload([build_item("AK-47 | Redline (Minimal Wear)", item_id="2")], total="40", next_id="n2"),
                )
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        pages = client.crawl_market_pages(max_pages=2, scroll_pause_ms=1234, idle_scroll_limit=5)

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0].total, 40)
        self.assertEqual(transport.crawl_calls[0]["max_pages"], 2)
        self.assertEqual(transport.crawl_calls[0]["scroll_pause_ms"], 1234)
        self.assertEqual(transport.crawl_calls[0]["idle_scroll_limit"], 5)

    def test_crawl_all_steamdt_market_to_sqlite_persists_all_pages(self) -> None:
        transport = FakeTransport(
            {
                "": (
                    build_payload([build_item("AK-47 | Redline (Field-Tested)", item_id="1")], total="40", next_id="n1"),
                    build_payload([build_item("AK-47 | Redline (Minimal Wear)", item_id="2")], total="40", next_id="n2"),
                )
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamDTPriceSnapshotStore(Path(temp_dir) / "steamdt.sqlite")
            summary = crawl_all_steamdt_market_to_sqlite(
                store,
                steamdt_client=client,
                max_pages=2,
            )
            ft_snapshot = store.get_latest_snapshot("AK-47 | Redline", "Field-Tested")
            mw_snapshot = store.get_latest_snapshot("AK-47 | Redline", "Minimal Wear")

            self.assertIsInstance(summary, SteamDTCrawlSummary)
            self.assertEqual(summary.pages_crawled, 2)
            self.assertEqual(summary.items_seen, 2)
            self.assertEqual(summary.unique_items, 2)
            self.assertEqual(summary.snapshots_inserted, 2)
            self.assertEqual(summary.total_available, 40)
            self.assertEqual(summary.last_next_id, "n2")
            self.assertEqual(store.count_snapshots(), 2)
            self.assertIsNotNone(ft_snapshot)
            self.assertIsNotNone(mw_snapshot)

    def test_unsuccessful_payload_raises_parse_error(self) -> None:
        transport = FakeTransport(
            {
                "AK-47 | Redline": {
                    "success": False,
                    "data": None,
                    "errorMsg": "当前环境异常",
                }
            }
        )
        client = SteamDTMarketAPI(transport=transport)

        with self.assertRaises(MarketParseError):
            client.fetch_market_page(query_name="AK-47 | Redline")


if __name__ == "__main__":
    unittest.main()
