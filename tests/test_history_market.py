from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    BuffTransactionHistoryCrawler,
    C5TransactionHistoryCrawler,
    IGXECachedPriceAPI,
    IGXETransactionHistoryCrawler,
    RandomizedRateLimiter,
    TrackedGoods,
    TransactionHistoryPriceAPI,
    TransactionHistorySnapshotPriceAPI,
    TransactionHistoryStore,
    TransactionHistorySyncService,
    build_market_name,
    split_market_name,
)


class FakeResponse:
    def __init__(self, payload=None, text=None):
        self.payload = payload
        self.text = text if text is not None else ""

    def raise_for_status(self) -> None:
        return None

    def json(self):
        if self.payload is None:
            raise ValueError("No JSON payload")
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}
        self.cookies = {}
        self.proxies = {}

    def request(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class HistoryMarketTests(unittest.TestCase):
    def test_market_name_helpers(self) -> None:
        self.assertEqual(
            build_market_name("AK-47 | Slate", "Field-Tested"),
            "AK-47 | Slate (Field-Tested)",
        )
        self.assertEqual(
            build_market_name("AK-47 | Slate (Field-Tested)", "Field-Tested"),
            "AK-47 | Slate (Field-Tested)",
        )
        self.assertEqual(
            split_market_name("AK-47 | Slate (Field-Tested)"),
            ("AK-47 | Slate", "Field-Tested"),
        )

    def test_buff_transaction_history_parses_records(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "data": {
                            "items": [
                                {
                                    "goods_id": 42192,
                                    "price": "12.34",
                                    "transact_time": 1710000000,
                                    "asset_info": {
                                        "paintwear": "0.123456",
                                        "info": {
                                            "stickers": [
                                                {"name": "Crown", "wear": "0.01"}
                                            ]
                                        },
                                    },
                                }
                            ]
                        }
                    }
                )
            ]
        )
        crawler = BuffTransactionHistoryCrawler(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        records = crawler.fetch_transaction_history(42192, "AK-47 | Slate (Field-Tested)")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].platform, "BUFF")
        self.assertAlmostEqual(records[0].price, 12.34)
        self.assertEqual(records[0].paintwear, "0.123456")
        self.assertEqual(records[0].stickers, ("Crown|0.01",))

    def test_c5_transaction_history_parses_detail_page(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "data": [
                            {
                                "itemId": "553370868",
                                "price": "15.88",
                                "updateTime": 1710000100,
                                "productId": "998877",
                            }
                        ]
                    }
                ),
                FakeResponse(text="<center>磨损：0.0154 印花: Crown, Howling Dawn</center>"),
            ]
        )
        crawler = C5TransactionHistoryCrawler(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        records = crawler.fetch_transaction_history(553370868, "AK-47 | Slate (Factory New)")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].platform, "C5")
        self.assertEqual(records[0].paintwear, "0.0154")
        self.assertEqual(records[0].stickers, ("Crown", "Howling Dawn"))

    def test_igxe_transaction_history_parses_chinese_datetime(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "data": [
                            {
                                "product_id": 571769,
                                "unit_price": "19.99",
                                "last_updated": "2024年03月10日",
                                "exterior_wear": "0.1111",
                                "id": 778899,
                                "sticker": [
                                    {"sticker_title": "Crown", "wear": "0.02"}
                                ],
                            }
                        ]
                    }
                )
            ]
        )
        crawler = IGXETransactionHistoryCrawler(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        records = crawler.fetch_transaction_history(571769, "AK-47 | Slate (Field-Tested)")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].platform, "IGXE")
        self.assertEqual(records[0].external_record_id, "778899")
        self.assertEqual(records[0].stickers, ("Crown|0.02",))
        self.assertTrue(records[0].transact_time.startswith("2024-03-10"))

    def test_history_store_and_price_api_work_with_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = TransactionHistoryStore(Path(temp_dir) / "history.sqlite")
            store.upsert_tracked_goods(
                [
                    TrackedGoods(
                        item_name="AK-47 | Slate (Field-Tested)",
                        buff_goods_id=42192,
                        c5_goods_id=553370868,
                        igxe_goods_id=571769,
                    )
                ]
            )

            inserted = store.insert_records(
                BuffTransactionHistoryCrawler(
                    session=FakeSession(
                        [
                            FakeResponse(
                                {
                                    "data": {
                                        "items": [
                                            {
                                                "goods_id": 42192,
                                                "price": "10.00",
                                                "transact_time": 1710000000,
                                                "asset_info": {
                                                    "paintwear": "0.10",
                                                    "info": {"stickers": []},
                                                },
                                            },
                                            {
                                                "goods_id": 42192,
                                                "price": "12.00",
                                                "transact_time": 1710003600,
                                                "asset_info": {
                                                    "paintwear": "0.11",
                                                    "info": {"stickers": []},
                                                },
                                            },
                                        ]
                                    }
                                }
                            )
                        ]
                    ),
                    rate_limiter=RandomizedRateLimiter(0.0, 0.0),
                ).fetch_transaction_history(42192, "AK-47 | Slate (Field-Tested)")
            )
            quote = store.get_recent_price_quote(
                "AK-47 | Slate",
                "Field-Tested",
                platforms=["BUFF"],
                lookback_days=5000,
            )
            api = TransactionHistoryPriceAPI(store, platforms=["BUFF"], lookback_days=5000)
            api_quote = api.get_item_price("AK-47 | Slate", "Field-Tested")

        self.assertEqual(inserted, 2)
        self.assertIsNotNone(quote)
        assert quote is not None
        self.assertAlmostEqual(quote.lowest_price, 10.0)
        self.assertAlmostEqual(quote.recent_average_price, 11.0)
        self.assertAlmostEqual(api_quote.lowest_price, 10.0)

    def test_sync_service_writes_igxe_snapshot_into_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = TransactionHistoryStore(Path(temp_dir) / "history.sqlite")
            tracked = (
                TrackedGoods(
                    item_name="AK-47 | Slate (Field-Tested)",
                    igxe_goods_id=3,
                ),
            )
            igxe_crawler = IGXETransactionHistoryCrawler(
                session=FakeSession(
                    [
                        FakeResponse(
                            {
                                "data": [
                                    {
                                        "product_id": 3,
                                        "unit_price": "12.00",
                                        "last_updated": "2024年03月10日",
                                        "exterior_wear": "0.10",
                                        "id": 333,
                                        "sticker": [],
                                    },
                                    {
                                        "product_id": 3,
                                        "unit_price": "10.00",
                                        "last_updated": "2024年03月11日",
                                        "exterior_wear": "0.11",
                                        "id": 334,
                                        "sticker": [],
                                    },
                                ]
                            }
                        )
                    ]
                ),
                rate_limiter=RandomizedRateLimiter(0.0, 0.0),
            )

            service = TransactionHistorySyncService(store, igxe_crawler=igxe_crawler)
            summary = service.sync_tracked_goods(tracked, platforms=("IGXE",))
            snapshot = store.get_latest_price_snapshot(
                "AK-47 | Slate",
                "Field-Tested",
                platforms=["IGXE"],
            )
            snapshot_api = TransactionHistorySnapshotPriceAPI(
                store,
                platforms=["IGXE"],
                max_age_seconds=86400,
            )
            quote = snapshot_api.get_item_price("AK-47 | Slate", "Field-Tested")
            self.assertEqual(summary["tracked_goods"], 1)
            self.assertEqual(summary["records_inserted"], 2)
            self.assertEqual(summary["snapshots_inserted"], 1)
            self.assertEqual(store.count_price_snapshots(), 1)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.platform, "IGXE")
            self.assertEqual(snapshot.item_name, "AK-47 | Slate")
            self.assertEqual(snapshot.exterior, "Field-Tested")
            self.assertAlmostEqual(snapshot.lowest_price, 10.0)
            self.assertAlmostEqual(snapshot.recent_average_price, 11.0)
            self.assertEqual(snapshot.sample_count, 2)
            self.assertAlmostEqual(quote.lowest_price, 10.0)

    def test_igxe_cached_price_api_fetches_and_caches_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = TransactionHistoryStore(Path(temp_dir) / "history.sqlite")
            store.upsert_tracked_goods(
                [
                    TrackedGoods(
                        item_name="AK-47 | Slate (Field-Tested)",
                        igxe_goods_id=571769,
                    )
                ]
            )
            session = FakeSession(
                [
                    FakeResponse(
                        {
                            "data": [
                                {
                                    "product_id": 571769,
                                    "unit_price": "19.99",
                                    "last_updated": "2024年03月10日",
                                    "exterior_wear": "0.1111",
                                    "id": 778899,
                                    "sticker": [],
                                },
                                {
                                    "product_id": 571769,
                                    "unit_price": "18.50",
                                    "last_updated": "2024年03月11日",
                                    "exterior_wear": "0.1099",
                                    "id": 778900,
                                    "sticker": [],
                                },
                            ]
                        }
                    )
                ]
            )
            api = IGXECachedPriceAPI(
                store,
                crawler=IGXETransactionHistoryCrawler(
                    session=session,
                    rate_limiter=RandomizedRateLimiter(0.0, 0.0),
                ),
                max_age_seconds=86400,
            )

            quote_first = api.get_item_price("AK-47 | Slate", "Field-Tested")
            quote_second = api.get_item_price("AK-47 | Slate", "Field-Tested")
            snapshot = store.get_latest_price_snapshot(
                "AK-47 | Slate",
                "Field-Tested",
                platforms=["IGXE"],
            )

            self.assertAlmostEqual(quote_first.lowest_price, 18.5)
            self.assertAlmostEqual(quote_first.recent_average_price, 19.245)
            self.assertAlmostEqual(quote_second.lowest_price, 18.5)
            self.assertEqual(len(session.calls), 1)
            self.assertEqual(store.count_records(), 2)
            self.assertEqual(store.count_price_snapshots(), 1)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.source, "igxe_history")

    def test_sync_service_fetches_all_enabled_platforms(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = TransactionHistoryStore(Path(temp_dir) / "history.sqlite")
            tracked = (
                TrackedGoods(
                    item_name="AK-47 | Slate (Field-Tested)",
                    buff_goods_id=1,
                    c5_goods_id=2,
                    igxe_goods_id=3,
                ),
            )

            buff_crawler = BuffTransactionHistoryCrawler(
                session=FakeSession(
                    [
                        FakeResponse(
                            {
                                "data": {
                                    "items": [
                                        {
                                            "goods_id": 1,
                                            "price": "10.00",
                                            "transact_time": 1710000000,
                                            "asset_info": {"paintwear": "0.10", "info": {"stickers": []}},
                                        }
                                    ]
                                }
                            }
                        )
                    ]
                ),
                rate_limiter=RandomizedRateLimiter(0.0, 0.0),
            )
            c5_crawler = C5TransactionHistoryCrawler(
                session=FakeSession(
                    [
                        FakeResponse(
                            {
                                "data": [
                                    {
                                        "itemId": "2",
                                        "price": "11.00",
                                        "updateTime": 1710000100,
                                        "productId": "22",
                                    }
                                ]
                            }
                        ),
                        FakeResponse(text="<center>磨损：0.0150 印花: Crown</center>"),
                    ]
                ),
                rate_limiter=RandomizedRateLimiter(0.0, 0.0),
            )
            igxe_crawler = IGXETransactionHistoryCrawler(
                session=FakeSession(
                    [
                        FakeResponse(
                            {
                                "data": [
                                    {
                                        "product_id": 3,
                                        "unit_price": "12.00",
                                        "last_updated": "2024年03月10日",
                                        "exterior_wear": "0.10",
                                        "id": 333,
                                        "sticker": [],
                                    }
                                ]
                            }
                        )
                    ]
                ),
                rate_limiter=RandomizedRateLimiter(0.0, 0.0),
            )

            service = TransactionHistorySyncService(
                store,
                buff_crawler=buff_crawler,
                c5_crawler=c5_crawler,
                igxe_crawler=igxe_crawler,
            )
            summary = service.sync_tracked_goods(tracked, include_c5_detail=True)
            records = store.get_recent_records(
                "AK-47 | Slate",
                "Field-Tested",
                limit=10,
            )

        self.assertEqual(summary["tracked_goods"], 1)
        self.assertEqual(summary["records_inserted"], 3)
        self.assertEqual(summary["snapshots_inserted"], 3)
        self.assertEqual(len(records), 3)
        self.assertEqual({record.platform for record in records}, {"BUFF", "C5", "IGXE"})


if __name__ == "__main__":
    unittest.main()
