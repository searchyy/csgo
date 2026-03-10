from __future__ import annotations

import csv
import json
import random
import re
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import quote_plus, urlparse

import requests

from .exceptions import MarketParseError, MarketRequestError
from .models import Exterior, ItemVariant, PriceQuote

DEFAULT_REQUEST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
}

STATTRAK_PREFIX = "StatTrak™ "
SOUVENIR_PREFIXES = ("Souvenir ", "纪念品 ")


def parse_cookie_string(cookie_string: str | None) -> dict[str, str]:
    if not cookie_string:
        return {}

    cookies: dict[str, str] = {}
    for fragment in cookie_string.split(";"):
        fragment = fragment.strip()
        if not fragment or "=" not in fragment:
            continue
        key, value = fragment.split("=", 1)
        key = key.strip()
        if key:
            cookies[key] = value.strip()
    return cookies


def normalize_exterior_label(exterior: Exterior | str) -> str:
    if isinstance(exterior, Exterior):
        return exterior.value
    try:
        return Exterior.from_label(str(exterior)).value
    except ValueError:
        return str(exterior).strip()


def split_item_variant_name(item_name: str) -> tuple[str, ItemVariant]:
    normalized_name = str(item_name).strip()
    for prefix in (STATTRAK_PREFIX, "StatTrak "):
        if normalized_name.startswith(prefix):
            return normalized_name[len(prefix) :].strip(), ItemVariant.STATTRAK
    return normalized_name, ItemVariant.NORMAL


def is_souvenir_item_name(item_name: str | None) -> bool:
    normalized_name = str(item_name or "").strip()
    if not normalized_name:
        return False
    lowered = normalized_name.lower()
    return any(
        lowered.startswith(prefix.lower())
        for prefix in SOUVENIR_PREFIXES
    )


def build_item_variant_name(
    item_name: str,
    variant: ItemVariant | str = ItemVariant.NORMAL,
) -> str:
    base_item_name, _ = split_item_variant_name(item_name)
    resolved_variant = ItemVariant.from_value(variant)
    if resolved_variant is ItemVariant.STATTRAK:
        return f"{STATTRAK_PREFIX}{base_item_name}"
    return base_item_name


def _normalize_text(value: Any) -> str:
    text = str(value).strip().lower()
    text = text.replace("（", "(").replace("）", ")")
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", text)


def _parse_price(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    match = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def _deep_get(container: Any, path: tuple[str, ...]) -> Any:
    current = container
    for part in path:
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _mapping_values_as_dicts(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [record for record in value if isinstance(record, dict)]
    if isinstance(value, Mapping):
        if all(isinstance(record, Mapping) for record in value.values()):
            return [dict(record) for record in value.values()]
        return [dict(value)]
    return []


@dataclass(slots=True)
class RandomizedRateLimiter:
    min_delay_seconds: float = 2.0
    max_delay_seconds: float = 5.0
    clock: Any = time.monotonic
    sleeper: Any = time.sleep
    random_uniform: Any = random.uniform
    _next_allowed_at: float = field(default=0.0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.min_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("Delay bounds cannot be negative")
        if self.min_delay_seconds > self.max_delay_seconds:
            raise ValueError("min_delay_seconds cannot exceed max_delay_seconds")

    def acquire(self) -> None:
        with self._lock:
            now = self.clock()
            if now < self._next_allowed_at:
                self.sleeper(self._next_allowed_at - now)
                now = self.clock()
            delay = self.random_uniform(self.min_delay_seconds, self.max_delay_seconds)
            self._next_allowed_at = now + delay


@dataclass(frozen=True, slots=True)
class BrowserExtractionConfig:
    search_url_template: str
    lowest_price_selector: str
    recent_average_selector: str | None = None
    wait_selector: str | None = None
    headless: bool = True


class BrowserPriceFallback(ABC):
    @abstractmethod
    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        raise NotImplementedError


class PlaywrightPriceFallback(BrowserPriceFallback):
    def __init__(
        self,
        config: BrowserExtractionConfig,
        *,
        base_url: str,
        headers: Mapping[str, str] | None = None,
        cookies: Mapping[str, str] | None = None,
        proxies: Mapping[str, str] | None = None,
        timeout_seconds: float = 20.0,
    ) -> None:
        self.config = config
        self.base_url = base_url.rstrip("/")
        self.headers = dict(headers or {})
        self.cookies = dict(cookies or {})
        self.proxies = dict(proxies or {})
        self.timeout_seconds = timeout_seconds

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as error:
            raise RuntimeError(
                "Playwright is not installed. Install with `pip install playwright` "
                "and run `playwright install`."
            ) from error

        exterior_label = normalize_exterior_label(exterior)
        url = self.config.search_url_template.format(
            query=quote_plus(f"{item_name} ({exterior_label})"),
            item_name=quote_plus(item_name),
            exterior=quote_plus(exterior_label),
        )

        launch_kwargs: dict[str, Any] = {"headless": self.config.headless}
        proxy_server = self.proxies.get("https") or self.proxies.get("http")
        if proxy_server:
            launch_kwargs["proxy"] = {"server": proxy_server}

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(**launch_kwargs)
            context = browser.new_context(
                user_agent=self.headers.get("User-Agent"),
                extra_http_headers=self.headers or None,
            )
            if self.cookies:
                context.add_cookies(self._playwright_cookies(url))
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=int(self.timeout_seconds * 1000))
            if self.config.wait_selector:
                page.wait_for_selector(
                    self.config.wait_selector, timeout=int(self.timeout_seconds * 1000)
                )
            lowest_text = page.locator(self.config.lowest_price_selector).first.text_content() or ""
            average_text = None
            if self.config.recent_average_selector:
                average_text = (
                    page.locator(self.config.recent_average_selector).first.text_content() or ""
                )
            browser.close()

        lowest_price = _parse_price(lowest_text)
        average_price = _parse_price(average_text)
        if lowest_price is None and average_price is None:
            raise MarketParseError("Browser fallback could not parse any price values")
        resolved_lowest_price = lowest_price if lowest_price is not None else average_price
        return PriceQuote(
            lowest_price=resolved_lowest_price,
            recent_average_price=average_price,
        )

    def _playwright_cookies(self, url: str) -> list[dict[str, Any]]:
        parsed = urlparse(url)
        target_url = f"{parsed.scheme}://{parsed.netloc}/"
        return [
            {"name": name, "value": value, "url": target_url}
            for name, value in self.cookies.items()
        ]


class SeleniumPriceFallback(BrowserPriceFallback):
    def __init__(
        self,
        config: BrowserExtractionConfig,
        *,
        base_url: str,
        headers: Mapping[str, str] | None = None,
        cookies: Mapping[str, str] | None = None,
        proxies: Mapping[str, str] | None = None,
        timeout_seconds: float = 20.0,
    ) -> None:
        self.config = config
        self.base_url = base_url.rstrip("/")
        self.headers = dict(headers or {})
        self.cookies = dict(cookies or {})
        self.proxies = dict(proxies or {})
        self.timeout_seconds = timeout_seconds

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.support.ui import WebDriverWait
        except ImportError as error:
            raise RuntimeError(
                "Selenium is not installed. Install with `pip install selenium` "
                "and ensure a compatible WebDriver is available."
            ) from error

        exterior_label = normalize_exterior_label(exterior)
        url = self.config.search_url_template.format(
            query=quote_plus(f"{item_name} ({exterior_label})"),
            item_name=quote_plus(item_name),
            exterior=quote_plus(exterior_label),
        )

        options = Options()
        if self.config.headless:
            options.add_argument("--headless=new")
        if self.headers.get("User-Agent"):
            options.add_argument(f"--user-agent={self.headers['User-Agent']}")
        proxy_server = self.proxies.get("https") or self.proxies.get("http")
        if proxy_server:
            options.add_argument(f"--proxy-server={proxy_server}")

        driver = webdriver.Chrome(options=options)
        try:
            driver.get(self.base_url)
            for name, value in self.cookies.items():
                driver.add_cookie({"name": name, "value": value, "path": "/"})
            driver.get(url)
            if self.config.wait_selector:
                WebDriverWait(driver, self.timeout_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, self.config.wait_selector))
                )
            lowest_text = driver.find_element(By.CSS_SELECTOR, self.config.lowest_price_selector).text
            average_text = None
            if self.config.recent_average_selector:
                average_text = driver.find_element(
                    By.CSS_SELECTOR, self.config.recent_average_selector
                ).text
        finally:
            driver.quit()

        lowest_price = _parse_price(lowest_text)
        average_price = _parse_price(average_text)
        if lowest_price is None and average_price is None:
            raise MarketParseError("Browser fallback could not parse any price values")
        resolved_lowest_price = lowest_price if lowest_price is not None else average_price
        return PriceQuote(
            lowest_price=resolved_lowest_price,
            recent_average_price=average_price,
        )


class BaseMarketAPI(ABC):
    market_name = "BaseMarket"
    default_base_url = ""
    search_endpoint = ""
    detail_endpoint = ""
    search_param_name = "search"
    detail_id_param_name = "item_id"
    search_record_paths: tuple[tuple[str, ...], ...] = ()
    detail_record_paths: tuple[tuple[str, ...], ...] = ()
    id_keys: tuple[str, ...] = ("id",)
    name_keys: tuple[str, ...] = (
        "name",
        "market_hash_name",
        "full_name",
        "short_name",
        "goods_name",
        "item_name",
        "commodity_name",
    )
    exterior_keys: tuple[str, ...] = ("exterior", "paintwear", "wear_name")
    lowest_price_keys: tuple[str, ...] = (
        "sell_min_price",
        "lowest_price",
        "min_price",
        "price",
        "current_price",
    )
    average_price_keys: tuple[str, ...] = (
        "trans_price",
        "avg_price",
        "average_price",
        "recent_average_price",
        "reference_price",
    )
    search_extra_params: Mapping[str, Any] = {"page_num": 1}
    detail_extra_params: Mapping[str, Any] = {"page_num": 1}

    def __init__(
        self,
        *,
        base_url: str | None = None,
        session: requests.Session | None = None,
        headers: Mapping[str, str] | None = None,
        cookies: Mapping[str, str] | None = None,
        cookie_string: str | None = None,
        proxies: Mapping[str, str] | None = None,
        timeout_seconds: float = 15.0,
        rate_limiter: RandomizedRateLimiter | None = None,
        browser_fallback: BrowserPriceFallback | None = None,
    ) -> None:
        self.base_url = (base_url or self.default_base_url).rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.browser_fallback = browser_fallback
        self.rate_limiter = rate_limiter or RandomizedRateLimiter()
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_REQUEST_HEADERS)
        if headers:
            self.session.headers.update(headers)
        if cookies:
            self.session.cookies.update(dict(cookies))
        if cookie_string:
            self.session.cookies.update(parse_cookie_string(cookie_string))
        if proxies:
            self.session.proxies.update(dict(proxies))

    def set_headers(self, headers: Mapping[str, str]) -> None:
        self.session.headers.update(dict(headers))

    def set_cookies(
        self,
        *,
        cookies: Mapping[str, str] | None = None,
        cookie_string: str | None = None,
    ) -> None:
        if cookies:
            self.session.cookies.update(dict(cookies))
        if cookie_string:
            self.session.cookies.update(parse_cookie_string(cookie_string))

    def set_proxies(self, proxies: Mapping[str, str]) -> None:
        self.session.proxies.update(dict(proxies))

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        try:
            return self._get_item_price_via_http(item_name=item_name, exterior=exterior)
        except (requests.RequestException, MarketRequestError, MarketParseError):
            if self.browser_fallback is None:
                raise
            return self.browser_fallback.get_item_price(item_name=item_name, exterior=exterior)

    @abstractmethod
    def _get_item_price_via_http(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        raise NotImplementedError

    def _request_json(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        self.rate_limiter.acquire()
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=dict(params or {}),
                headers=dict(headers or {}),
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            raise MarketRequestError(f"{self.market_name} request failed: {error}") from error
        except ValueError as error:
            raise MarketParseError(f"{self.market_name} returned non-JSON data") from error

    def _search_records(self, payload: Any, paths: tuple[tuple[str, ...], ...]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in paths:
            records.extend(_mapping_values_as_dicts(_deep_get(payload, path)))
        deduped: dict[str, dict[str, Any]] = {}
        for index, record in enumerate(records):
            key = self._extract_first_value(record, self.id_keys)
            deduped[str(key if key is not None else f"idx:{index}")] = record
        return list(deduped.values())

    def _match_record(
        self,
        records: list[dict[str, Any]],
        *,
        item_name: str,
        exterior: Exterior | str,
    ) -> dict[str, Any]:
        if not records:
            raise MarketParseError(f"{self.market_name} search returned no records")

        exterior_label = normalize_exterior_label(exterior)
        target_query = _normalize_text(f"{item_name} ({exterior_label})")
        target_name = _normalize_text(item_name)
        target_exterior = _normalize_text(exterior_label)

        ranked: list[tuple[int, dict[str, Any]]] = []
        for record in records:
            score = 0
            for candidate in self._record_string_candidates(record):
                normalized = _normalize_text(candidate)
                if normalized == target_query:
                    score = max(score, 100)
                elif target_query and target_query in normalized:
                    score = max(score, 90)
                elif normalized == target_name:
                    score = max(score, 75)
                elif target_name and target_name in normalized:
                    score = max(score, 60)
                if target_exterior and target_exterior in normalized:
                    score += 10
            for candidate in self._record_exterior_candidates(record):
                if _normalize_text(candidate) == target_exterior:
                    score += 10
            ranked.append((score, record))

        ranked.sort(key=lambda pair: pair[0], reverse=True)
        best_score, best_record = ranked[0]
        if best_score <= 0:
            raise MarketParseError(
                f"{self.market_name} could not match '{item_name} ({exterior_label})'"
            )
        return best_record

    def _record_string_candidates(self, record: Mapping[str, Any]) -> list[str]:
        candidates: list[str] = []
        for key in self.name_keys:
            value = record.get(key)
            if value:
                candidates.append(str(value))
        for nested in (record.get("goods_info"), record.get("commodity"), record.get("item")):
            if isinstance(nested, Mapping):
                for key in self.name_keys:
                    value = nested.get(key)
                    if value:
                        candidates.append(str(value))
        return candidates

    def _record_exterior_candidates(self, record: Mapping[str, Any]) -> list[str]:
        candidates: list[str] = []
        for key in self.exterior_keys:
            value = record.get(key)
            if value:
                candidates.append(str(value))
        for nested in (
            record.get("goods_info"),
            record.get("commodity"),
            record.get("item"),
            record.get("tags"),
        ):
            if isinstance(nested, Mapping):
                for key in self.exterior_keys:
                    value = nested.get(key)
                    if value:
                        candidates.append(str(value))
        return candidates

    def _extract_first_value(self, record: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
        for key in keys:
            if key in record and record[key] not in (None, ""):
                return record[key]
        for nested in (record.get("goods_info"), record.get("commodity"), record.get("item")):
            if isinstance(nested, Mapping):
                for key in keys:
                    if key in nested and nested[key] not in (None, ""):
                        return nested[key]
        return None

    def _extract_first_price(
        self,
        source: Mapping[str, Any] | list[dict[str, Any]],
        keys: tuple[str, ...],
    ) -> float | None:
        records = [source] if isinstance(source, Mapping) else source
        for record in records:
            value = self._extract_first_value(record, keys)
            price = _parse_price(value)
            if price is not None:
                return price
        return None


class BuffMarketAPI(BaseMarketAPI):
    market_name = "BUFF"
    default_base_url = "https://buff.163.com"
    search_endpoint = "/api/market/goods"
    detail_endpoint = "/api/market/goods/sell_order"
    search_param_name = "search"
    detail_id_param_name = "goods_id"
    id_keys = ("goods_id", "id")
    search_record_paths = (("data", "items"), ("data", "goods_infos"), ("items",))
    detail_record_paths = (("data", "items"), ("data", "goods_infos"), ("items",))
    search_extra_params = {"game": "csgo", "page_num": 1}
    detail_extra_params = {"game": "csgo", "page_num": 1}

    def _get_item_price_via_http(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        search_params = dict(self.search_extra_params)
        search_params[self.search_param_name] = f"{item_name} ({normalize_exterior_label(exterior)})"
        search_payload = self._request_json("GET", self.search_endpoint, params=search_params)
        matched = self._match_record(
            self._search_records(search_payload, self.search_record_paths),
            item_name=item_name,
            exterior=exterior,
        )

        goods_id = self._extract_first_value(matched, self.id_keys)
        if goods_id is None:
            raise MarketParseError("BUFF search result did not contain a goods_id")

        detail_params = dict(self.detail_extra_params)
        detail_params[self.detail_id_param_name] = goods_id
        detail_payload = self._request_json("GET", self.detail_endpoint, params=detail_params)
        detail_records = self._search_records(detail_payload, self.detail_record_paths)

        lowest_price = self._extract_first_price(detail_records, self.lowest_price_keys)
        if lowest_price is None:
            lowest_price = self._extract_first_price(matched, self.lowest_price_keys)
        average_price = self._extract_first_price(matched, self.average_price_keys)
        if average_price is None:
            average_price = self._extract_first_price(detail_records, self.average_price_keys)
        if lowest_price is None and average_price is None:
            raise MarketParseError("BUFF response did not include any usable price fields")
        resolved_lowest_price = lowest_price if lowest_price is not None else average_price
        return PriceQuote(
            lowest_price=resolved_lowest_price,
            recent_average_price=average_price,
        )


class UUMarketAPI(BaseMarketAPI):
    market_name = "UU"
    default_base_url = "https://www.youpin898.com"
    search_endpoint = "/api/home/search"
    detail_endpoint = "/api/goods/price"
    search_param_name = "keyword"
    detail_id_param_name = "item_id"
    id_keys = ("item_id", "commodity_id", "id")
    search_record_paths = (
        ("data", "list"),
        ("data", "items"),
        ("data", "results"),
        ("items",),
    )
    detail_record_paths = (
        ("data", "orders"),
        ("data", "items"),
        ("data", "commodity"),
        ("items",),
    )
    search_extra_params = {"game": "csgo"}
    detail_extra_params = {"game": "csgo"}

    def _get_item_price_via_http(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        search_params = dict(self.search_extra_params)
        search_params[self.search_param_name] = f"{item_name} ({normalize_exterior_label(exterior)})"
        search_payload = self._request_json("GET", self.search_endpoint, params=search_params)
        matched = self._match_record(
            self._search_records(search_payload, self.search_record_paths),
            item_name=item_name,
            exterior=exterior,
        )

        item_id = self._extract_first_value(matched, self.id_keys)
        if item_id is None:
            raise MarketParseError("UU search result did not contain an item identifier")

        detail_params = dict(self.detail_extra_params)
        detail_params[self.detail_id_param_name] = item_id
        detail_payload = self._request_json("GET", self.detail_endpoint, params=detail_params)
        detail_records = self._search_records(detail_payload, self.detail_record_paths)

        lowest_price = self._extract_first_price(detail_records, self.lowest_price_keys)
        if lowest_price is None:
            lowest_price = self._extract_first_price(matched, self.lowest_price_keys)
        average_price = self._extract_first_price(matched, self.average_price_keys)
        if average_price is None:
            average_price = self._extract_first_price(detail_records, self.average_price_keys)
        if lowest_price is None and average_price is None:
            raise MarketParseError("UU response did not include any usable price fields")
        resolved_lowest_price = lowest_price if lowest_price is not None else average_price
        return PriceQuote(
            lowest_price=resolved_lowest_price,
            recent_average_price=average_price,
        )
