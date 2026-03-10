from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    CachedSteamMarketAPI,
    PriceQuote,
    RandomizedRateLimiter,
    SteamMarketAPI,
    SteamPriceSnapshotStore,
    split_market_hash_name,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
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


class BrowserFallbackStub:
    def __init__(self, quote: PriceQuote) -> None:
        self.quote = quote
        self.calls = []

    def get_item_price(self, item_name: str, exterior: str) -> PriceQuote:
        self.calls.append((item_name, exterior))
        return self.quote


class SteamMarketTests(unittest.TestCase):
    def test_split_market_hash_name_extracts_exterior(self) -> None:
        item_name, exterior = split_market_hash_name("AK-47 | Slate (Field-Tested)")
        self.assertEqual(item_name, "AK-47 | Slate")
        self.assertEqual(exterior, "Field-Tested")

    def test_get_item_price_uses_priceoverview(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "lowest_price": "$7.04",
                        "median_price": "$7.16",
                        "volume": "783",
                    }
                )
            ]
        )
        client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        quote = client.get_item_price("AK-47 | Slate", "Field-Tested")

        self.assertAlmostEqual(quote.lowest_price, 7.04)
        self.assertAlmostEqual(quote.recent_average_price, 7.16)
        self.assertEqual(
            session.calls[0]["params"]["market_hash_name"],
            "AK-47 | Slate (Field-Tested)",
        )

    def test_get_item_price_uses_browser_fallback_on_parse_failure(self) -> None:
        session = FakeSession([FakeResponse({"success": False})])
        fallback = BrowserFallbackStub(PriceQuote(lowest_price=6.5, recent_average_price=6.7))
        client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
            browser_fallback=fallback,
        )

        quote = client.get_item_price("AK-47 | Slate", "Field-Tested")

        self.assertAlmostEqual(quote.lowest_price, 6.5)
        self.assertEqual(fallback.calls, [("AK-47 | Slate", "Field-Tested")])

    def test_get_item_prices_fetches_multiple_exteriors_from_single_search(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "start": 0,
                        "pagesize": 3,
                        "total_count": 3,
                        "results": [
                            {
                                "hash_name": "AK-47 | Slate (Factory New)",
                                "sell_listings": 100,
                                "sell_price_text": "$11.04",
                                "sale_price_text": "$11.16",
                                "appid": 730,
                            },
                            {
                                "hash_name": "AK-47 | Slate (Field-Tested)",
                                "sell_listings": 783,
                                "sell_price_text": "$7.04",
                                "sale_price_text": "$7.16",
                                "appid": 730,
                            },
                            {
                                "hash_name": "AK-47 | Slate (Well-Worn)",
                                "sell_listings": 50,
                                "sell_price_text": "$5.04",
                                "sale_price_text": "$5.16",
                                "appid": 730,
                            },
                        ],
                    }
                )
            ]
        )
        client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        quotes = client.get_item_prices(
            "AK-47 | Slate",
            exteriors=("Factory New", "Field-Tested"),
        )

        self.assertEqual(sorted(quotes), ["Factory New", "Field-Tested"])
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(session.calls[0]["params"]["query"], "AK-47 | Slate")

    def test_get_item_family_prices_fetches_normal_and_stattrak(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "start": 0,
                        "pagesize": 2,
                        "total_count": 2,
                        "results": [
                            {
                                "hash_name": "AK-47 | Slate (Factory New)",
                                "sell_listings": 100,
                                "sell_price_text": "$11.04",
                                "sale_price_text": "$11.16",
                                "appid": 730,
                            },
                            {
                                "hash_name": "AK-47 | Slate (Field-Tested)",
                                "sell_listings": 783,
                                "sell_price_text": "$7.04",
                                "sale_price_text": "$7.16",
                                "appid": 730,
                            },
                        ],
                    }
                ),
                FakeResponse(
                    {
                        "success": True,
                        "start": 0,
                        "pagesize": 2,
                        "total_count": 2,
                        "results": [
                            {
                                "hash_name": "StatTrak™ AK-47 | Slate (Factory New)",
                                "sell_listings": 30,
                                "sell_price_text": "$31.04",
                                "sale_price_text": "$31.16",
                                "appid": 730,
                            },
                            {
                                "hash_name": "StatTrak™ AK-47 | Slate (Field-Tested)",
                                "sell_listings": 90,
                                "sell_price_text": "$21.04",
                                "sale_price_text": "$21.16",
                                "appid": 730,
                            },
                        ],
                    }
                ),
            ]
        )
        client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        quotes = client.get_item_family_prices(
            "AK-47 | Slate",
            exteriors=("Factory New", "Field-Tested"),
        )

        self.assertIn(("AK-47 | Slate", "Factory New"), quotes)
        self.assertIn(("StatTrak™ AK-47 | Slate", "Field-Tested"), quotes)
        self.assertEqual(len(session.calls), 2)

    def test_crawl_search_page_parses_market_rows(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "start": 0,
                        "pagesize": 2,
                        "total_count": 2500,
                        "results": [
                            {
                                "hash_name": "AK-47 | Slate (Field-Tested)",
                                "sell_listings": 783,
                                "sell_price": 704,
                                "sell_price_text": "$7.04",
                                "sale_price_text": "$7.16",
                                "appid": 730,
                            },
                            {
                                "hash_name": "M4A1-S | Basilisk (Minimal Wear)",
                                "sell_listings": "120",
                                "sell_price_text": "$2.33",
                                "sale_price_text": "$2.40",
                                "appid": 730,
                            },
                        ],
                    }
                )
            ]
        )
        client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        entries, total_count = client.crawl_search_page(query="", count=2)

        self.assertEqual(total_count, 2500)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].market_hash_name, "AK-47 | Slate (Field-Tested)")
        self.assertEqual(entries[0].sell_listings, 783)
        self.assertAlmostEqual(entries[0].sell_price, 7.04)
        self.assertEqual(entries[1].sell_listings, 120)
        self.assertEqual(session.calls[0]["params"]["norender"], 1)

    def test_crawl_search_results_paginates_and_exports(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "start": 0,
                        "pagesize": 2,
                        "total_count": 3,
                        "results": [
                            {
                                "hash_name": "Item A",
                                "sell_listings": 10,
                                "sell_price_text": "$1.00",
                                "appid": 730,
                            },
                            {
                                "hash_name": "Item B",
                                "sell_listings": 20,
                                "sell_price_text": "$2.00",
                                "appid": 730,
                            },
                        ],
                    }
                ),
                FakeResponse(
                    {
                        "success": True,
                        "start": 2,
                        "pagesize": 1,
                        "total_count": 3,
                        "results": [
                            {
                                "hash_name": "Item C",
                                "sell_listings": 30,
                                "sell_price_text": "$3.00",
                                "appid": 730,
                            }
                        ],
                    }
                ),
            ]
        )
        client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        entries = client.crawl_search_results(count=2)

        self.assertEqual([entry.market_hash_name for entry in entries], ["Item A", "Item B", "Item C"])
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = client.export_search_entries_json(entries, Path(temp_dir) / "steam.json")
            csv_path = client.export_search_entries_csv(entries, Path(temp_dir) / "steam.csv")
            self.assertIn("Item A", json_path.read_text(encoding="utf-8"))
            self.assertIn("market_hash_name", csv_path.read_text(encoding="utf-8-sig"))

    def test_snapshot_store_persists_search_entries_and_reads_latest_quote(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "start": 0,
                        "pagesize": 1,
                        "total_count": 1,
                        "results": [
                            {
                                "hash_name": "AK-47 | Slate (Field-Tested)",
                                "sell_listings": 783,
                                "sell_price_text": "$7.04",
                                "sale_price_text": "$7.16",
                                "appid": 730,
                            }
                        ],
                    }
                )
            ]
        )
        client = SteamMarketAPI(session=session, rate_limiter=RandomizedRateLimiter(0.0, 0.0))

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamPriceSnapshotStore(Path(temp_dir) / "steam.sqlite")
            entries = client.crawl_search_results(max_pages=1, count=1)
            inserted = store.insert_search_entries(entries, query="", currency=1, country="US")
            snapshot = store.get_latest_snapshot("AK-47 | Slate", "Field-Tested")

        self.assertEqual(inserted, 1)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertAlmostEqual(snapshot.lowest_price, 7.04)
        self.assertEqual(snapshot.sell_listings, 783)

    def test_cached_steam_market_prefers_local_sqlite_snapshot(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "lowest_price": "$7.04",
                        "median_price": "$7.16",
                        "volume": "783",
                    }
                )
            ]
        )
        live_client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamPriceSnapshotStore(Path(temp_dir) / "steam.sqlite")
            store.insert_price_quote(
                item_name="AK-47 | Slate",
                exterior="Field-Tested",
                quote=PriceQuote(lowest_price=6.5, recent_average_price=6.7),
                currency=1,
                country="US",
            )
            cached_client = CachedSteamMarketAPI(store, steam_client=live_client, max_age_seconds=3600)
            quote = cached_client.get_item_price("AK-47 | Slate", "Field-Tested")

        self.assertAlmostEqual(quote.lowest_price, 6.5)
        self.assertEqual(session.calls, [])

    def test_cached_steam_market_fetches_and_backfills_sqlite_on_cache_miss(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "lowest_price": "$7.04",
                        "median_price": "$7.16",
                        "volume": "783",
                    }
                )
            ]
        )
        live_client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
            country="US",
            currency=1,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamPriceSnapshotStore(Path(temp_dir) / "steam.sqlite")
            cached_client = CachedSteamMarketAPI(store, steam_client=live_client, max_age_seconds=3600)
            quote = cached_client.get_item_price("AK-47 | Slate", "Field-Tested")
            snapshot = store.get_latest_snapshot("AK-47 | Slate", "Field-Tested")

        self.assertAlmostEqual(quote.lowest_price, 7.04)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertAlmostEqual(snapshot.lowest_price, 7.04)
        self.assertEqual(len(session.calls), 1)

    def test_cached_steam_market_warm_item_family_cache_backfills_normal_and_stattrak(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "success": True,
                        "start": 0,
                        "pagesize": 2,
                        "total_count": 2,
                        "results": [
                            {
                                "hash_name": "AK-47 | Slate (Factory New)",
                                "sell_listings": 100,
                                "sell_price_text": "$11.04",
                                "sale_price_text": "$11.16",
                                "appid": 730,
                            },
                            {
                                "hash_name": "AK-47 | Slate (Field-Tested)",
                                "sell_listings": 783,
                                "sell_price_text": "$7.04",
                                "sale_price_text": "$7.16",
                                "appid": 730,
                            },
                        ],
                    }
                ),
                FakeResponse(
                    {
                        "success": True,
                        "start": 0,
                        "pagesize": 2,
                        "total_count": 2,
                        "results": [
                            {
                                "hash_name": "StatTrak™ AK-47 | Slate (Factory New)",
                                "sell_listings": 30,
                                "sell_price_text": "$31.04",
                                "sale_price_text": "$31.16",
                                "appid": 730,
                            },
                            {
                                "hash_name": "StatTrak™ AK-47 | Slate (Field-Tested)",
                                "sell_listings": 90,
                                "sell_price_text": "$21.04",
                                "sale_price_text": "$21.16",
                                "appid": 730,
                            },
                        ],
                    }
                ),
            ]
        )
        live_client = SteamMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
            country="US",
            currency=1,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SteamPriceSnapshotStore(Path(temp_dir) / "steam.sqlite")
            cached_client = CachedSteamMarketAPI(store, steam_client=live_client, max_age_seconds=3600)
            entries = cached_client.warm_item_family_cache("AK-47 | Slate")
            normal_fn = store.get_latest_snapshot("AK-47 | Slate", "Factory New")
            stattrak_ft = store.get_latest_snapshot("StatTrak™ AK-47 | Slate", "Field-Tested")

        self.assertEqual(len(entries), 4)
        self.assertIsNotNone(normal_fn)
        self.assertIsNotNone(stattrak_ft)
        self.assertEqual(len(session.calls), 2)


if __name__ == "__main__":
    unittest.main()
