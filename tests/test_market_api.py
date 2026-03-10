from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import (
    BuffMarketAPI,
    MarketParseError,
    PriceQuote,
    RandomizedRateLimiter,
    UUMarketAPI,
    parse_cookie_string,
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


class MarketAPITests(unittest.TestCase):
    def test_parse_cookie_string(self) -> None:
        cookies = parse_cookie_string("foo=bar; token=abc123; malformed; empty=")
        self.assertEqual(cookies["foo"], "bar")
        self.assertEqual(cookies["token"], "abc123")
        self.assertIn("empty", cookies)

    def test_rate_limiter_waits_until_next_window(self) -> None:
        sleep_calls = []
        timeline = iter([0.0, 0.5, 1.0])
        limiter = RandomizedRateLimiter(
            min_delay_seconds=1.0,
            max_delay_seconds=1.0,
            clock=lambda: next(timeline),
            sleeper=lambda value: sleep_calls.append(value),
            random_uniform=lambda start, end: 1.0,
        )

        limiter.acquire()
        limiter.acquire()

        self.assertEqual(sleep_calls, [0.5])

    def test_buff_market_api_parses_lowest_and_average_price(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "data": {
                            "items": [
                                {
                                    "goods_id": 123,
                                    "market_hash_name": "AK-47 | Case Hardened (Factory New)",
                                    "trans_price": "98.75",
                                }
                            ]
                        }
                    }
                ),
                FakeResponse(
                    {
                        "data": {
                            "items": [
                                {"price": "101.50"},
                                {"price": "103.00"},
                            ]
                        }
                    }
                ),
            ]
        )

        client = BuffMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        quote = client.get_item_price("AK-47 | Case Hardened", "Factory New")

        self.assertAlmostEqual(quote.lowest_price, 101.50)
        self.assertAlmostEqual(quote.recent_average_price, 98.75)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(session.calls[0]["params"]["search"], "AK-47 | Case Hardened (Factory New)")
        self.assertEqual(session.calls[1]["params"]["goods_id"], 123)

    def test_uu_market_api_uses_fallback_when_http_parsing_fails(self) -> None:
        session = FakeSession([FakeResponse({"data": {"list": []}})])
        fallback = BrowserFallbackStub(PriceQuote(lowest_price=88.0, recent_average_price=85.0))
        client = UUMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
            browser_fallback=fallback,
        )

        quote = client.get_item_price("AK-47 | Case Hardened", "Factory New")

        self.assertAlmostEqual(quote.lowest_price, 88.0)
        self.assertEqual(fallback.calls, [("AK-47 | Case Hardened", "Factory New")])

    def test_buff_market_api_raises_when_no_record_matches(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "data": {
                            "items": [
                                {
                                    "goods_id": 123,
                                    "market_hash_name": "M4A1-S | Hyper Beast (Field-Tested)",
                                }
                            ]
                        }
                    }
                )
            ]
        )
        client = BuffMarketAPI(
            session=session,
            rate_limiter=RandomizedRateLimiter(0.0, 0.0),
        )

        with self.assertRaises(MarketParseError):
            client.get_item_price("AK-47 | Case Hardened", "Factory New")


if __name__ == "__main__":
    unittest.main()
