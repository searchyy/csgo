from __future__ import annotations

import json
import random
import sqlite3
import statistics
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

from .exceptions import MarketParseError
from .market import (
    RandomizedRateLimiter,
    build_item_variant_name,
    is_souvenir_item_name,
    normalize_exterior_label,
    split_item_variant_name,
)
from .models import Exterior, ItemVariant, PriceQuote
from .price_anomaly import PriceAnomalyDetector, PriceAnomalyDetectorConfig


def _normalize_text(value: Any) -> str:
    return "".join(character for character in str(value).strip().lower() if character.isalnum())


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    digits = "".join(character for character in str(value) if character.isdigit())
    if not digits:
        return None
    return int(digits)


def _utc_now_parts() -> tuple[float, str]:
    timestamp = time.time()
    iso_value = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    return timestamp, iso_value


def split_steamdt_market_hash_name(market_hash_name: str) -> tuple[str, str | None]:
    normalized_name = market_hash_name.strip()
    exterior_labels = sorted((exterior.value for exterior in Exterior), key=len, reverse=True)
    for exterior_label in exterior_labels:
        suffix = f" ({exterior_label})"
        if normalized_name.endswith(suffix):
            return normalized_name[: -len(suffix)], exterior_label
    return normalized_name, None


def _ordered_exterior_labels(exteriors: Iterable[Exterior | str] | None) -> tuple[str, ...] | None:
    if exteriors is None:
        return None
    labels: list[str] = []
    for exterior in exteriors:
        label = normalize_exterior_label(exterior)
        if label and label not in labels:
            labels.append(label)
    return tuple(labels)


def _sort_market_items_by_exterior(items: Iterable["SteamDTMarketItem"]) -> tuple["SteamDTMarketItem", ...]:
    exterior_rank = {exterior.value: index for index, exterior in enumerate(Exterior.ordered())}
    return tuple(
        sorted(
            items,
            key=lambda item: (
                exterior_rank.get(
                    split_steamdt_market_hash_name(item.market_hash_name)[1] or "",
                    len(exterior_rank),
                ),
                item.market_hash_name,
            ),
        )
    )


def _is_souvenir_listing_record(record: Mapping[str, Any]) -> bool:
    quality_name = str(record.get("qualityName") or "").strip().lower()
    name = str(record.get("name") or "").strip()
    market_hash_name = str(record.get("marketHashName") or "").strip()
    market_short_name = str(record.get("marketShortName") or "").strip()
    bool_flags = (
        record.get("isSouvenir"),
        record.get("souvenir"),
        record.get("is_souvenir"),
    )
    if any(bool(value) for value in bool_flags if value is not None):
        return True
    if quality_name in {"souvenir", "纪念品"}:
        return True
    return any(
        is_souvenir_item_name(candidate)
        for candidate in (name, market_hash_name, market_short_name)
    )


@dataclass(frozen=True, slots=True)
class SteamDTPlatformPrice:
    platform: str
    platform_name: str | None
    price: float
    last_update: int | None = None
    link: str | None = None


@dataclass(frozen=True, slots=True)
class SteamDTMarketItem:
    id: str
    name: str
    short_name: str | None
    market_hash_name: str
    market_short_name: str | None
    image_url: str | None
    quality_name: str | None
    quality_color: str | None
    rarity_name: str | None
    rarity_color: str | None
    exterior_name: str | None
    exterior_color: str | None
    selling_price_list: tuple[SteamDTPlatformPrice, ...]
    purchase_price_list: tuple[SteamDTPlatformPrice, ...]
    increase_price: float | None
    trend_list: tuple[tuple[int, float], ...]
    sell_num: int | None
    is_souvenir: bool = False
    raw: dict[str, Any] | None = None

    def choose_selling_price(
        self,
        preferred_platforms: Sequence[str] | None = None,
    ) -> SteamDTPlatformPrice | None:
        positive_prices = tuple(price for price in self.selling_price_list if price.price > 0)
        if not positive_prices:
            return None
        if preferred_platforms:
            lookup = {price.platform.lower(): price for price in positive_prices}
            for platform in preferred_platforms:
                matched = lookup.get(platform.lower())
                if matched is not None:
                    return matched
        return min(positive_prices, key=lambda entry: entry.price)

    def choose_purchase_price(
        self,
        preferred_platforms: Sequence[str] | None = None,
    ) -> SteamDTPlatformPrice | None:
        positive_prices = tuple(price for price in self.purchase_price_list if price.price > 0)
        if not positive_prices:
            return None
        if preferred_platforms:
            lookup = {price.platform.lower(): price for price in positive_prices}
            for platform in preferred_platforms:
                matched = lookup.get(platform.lower())
                if matched is not None:
                    return matched
        return max(positive_prices, key=lambda entry: entry.price)

    def recent_average_price(self, sample_size: int = 7) -> float | None:
        prices = [price for _, price in self.trend_list if price > 0]
        if not prices:
            return None
        return float(statistics.fmean(prices[-sample_size:]))


@dataclass(frozen=True, slots=True)
class SteamDTMarketPage:
    page_num: int | None
    page_size: int | None
    total: int | None
    next_id: str | None
    system_time: int | None
    items: tuple[SteamDTMarketItem, ...]
    raw: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class SteamDTPriceSnapshot:
    market_hash_name: str
    item_name: str
    exterior: str | None
    lowest_price: float
    recent_average_price: float | None = None
    highest_buy_price: float | None = None
    selected_platform: str | None = None
    selected_platform_name: str | None = None
    selected_buy_platform: str | None = None
    selected_buy_platform_name: str | None = None
    sell_num: int | None = None
    is_souvenir: bool = False
    is_tradeup_compatible_normal: bool = True
    variant_filter_reason: str | None = None
    source: str = "steamdt_private"
    query: str | None = None
    fetched_at_epoch: float = 0.0
    fetched_at: str = ""
    raw_json: str | None = None
    safe_price: float | None = None
    is_valid: bool = True
    risk_level: str | None = None
    anomaly_flags: tuple[str, ...] = ()
    anomaly_notes: str | None = None
    anomaly_score: float | None = None
    cleaned_at: str | None = None

    @property
    def quote(self) -> PriceQuote:
        return PriceQuote(
            lowest_price=(
                self.safe_price
                if self.safe_price is not None and self.safe_price > 0
                else self.lowest_price
            ),
            recent_average_price=self.recent_average_price,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_hash_name": self.market_hash_name,
            "item_name": self.item_name,
            "exterior": self.exterior,
            "lowest_price": self.lowest_price,
            "recent_average_price": self.recent_average_price,
            "highest_buy_price": self.highest_buy_price,
            "selected_platform": self.selected_platform,
            "selected_platform_name": self.selected_platform_name,
            "selected_buy_platform": self.selected_buy_platform,
            "selected_buy_platform_name": self.selected_buy_platform_name,
            "sell_num": self.sell_num,
            "is_souvenir": self.is_souvenir,
            "is_tradeup_compatible_normal": self.is_tradeup_compatible_normal,
            "variant_filter_reason": self.variant_filter_reason,
            "source": self.source,
            "query": self.query,
            "fetched_at_epoch": self.fetched_at_epoch,
            "fetched_at": self.fetched_at,
            "raw_json": self.raw_json,
            "safe_price": self.safe_price,
            "is_valid": self.is_valid,
            "risk_level": self.risk_level,
            "anomaly_flags": list(self.anomaly_flags),
            "anomaly_notes": self.anomaly_notes,
            "anomaly_score": self.anomaly_score,
            "cleaned_at": self.cleaned_at,
        }


@dataclass(frozen=True, slots=True)
class SteamDTCapturedExchange:
    method: str
    url: str
    request_headers: dict[str, str]
    request_body: str | None
    status: int
    response_body: dict[str, Any] | list[Any] | str | None


@dataclass(frozen=True, slots=True)
class SteamDTCrawlSummary:
    query_name: str
    pages_crawled: int
    items_seen: int
    unique_items: int
    snapshots_inserted: int
    total_available: int | None
    last_next_id: str | None = None


def build_steamdt_price_snapshot(
    listing: SteamDTMarketItem,
    *,
    preferred_platforms: Sequence[str] | None = None,
    trend_sample_size: int = 7,
    query: str | None = None,
    source: str = "steamdt_private",
    fetched_at_epoch: float | None = None,
    fetched_at: str | None = None,
) -> SteamDTPriceSnapshot:
    selected_price = listing.choose_selling_price(preferred_platforms)
    if selected_price is None:
        raise MarketParseError(
            f"SteamDT did not include a positive selling price for {listing.market_hash_name}"
        )
    selected_buy_price = listing.choose_purchase_price(preferred_platforms)
    timestamp, iso_value = (
        _utc_now_parts()
        if fetched_at_epoch is None and fetched_at is None
        else (
            fetched_at_epoch
            if fetched_at_epoch is not None
            else datetime.fromisoformat(str(fetched_at)).timestamp(),
            fetched_at
            if fetched_at is not None
            else datetime.fromtimestamp(float(fetched_at_epoch), tz=timezone.utc).isoformat(),
        )
    )
    item_name, exterior = split_steamdt_market_hash_name(listing.market_hash_name)
    _, variant = split_item_variant_name(item_name)
    is_tradeup_compatible_normal = not listing.is_souvenir and variant is not ItemVariant.STATTRAK
    variant_filter_reason = None
    if listing.is_souvenir:
        variant_filter_reason = "souvenir"
    elif variant is ItemVariant.STATTRAK:
        variant_filter_reason = "stattrak"
    return SteamDTPriceSnapshot(
        market_hash_name=listing.market_hash_name,
        item_name=item_name,
        exterior=exterior,
        lowest_price=selected_price.price,
        recent_average_price=listing.recent_average_price(trend_sample_size),
        highest_buy_price=(
            selected_buy_price.price
            if selected_buy_price is not None and selected_buy_price.price > 0
            else None
        ),
        selected_platform=selected_price.platform,
        selected_platform_name=selected_price.platform_name,
        selected_buy_platform=(
            selected_buy_price.platform if selected_buy_price is not None else None
        ),
        selected_buy_platform_name=(
            selected_buy_price.platform_name if selected_buy_price is not None else None
        ),
        sell_num=listing.sell_num,
        is_souvenir=listing.is_souvenir,
        is_tradeup_compatible_normal=is_tradeup_compatible_normal,
        variant_filter_reason=variant_filter_reason,
        source=source,
        query=query,
        fetched_at_epoch=float(timestamp),
        fetched_at=str(iso_value),
        raw_json=json.dumps(listing.raw, ensure_ascii=False) if listing.raw is not None else None,
    )


class SteamDTMarketTransport(Protocol):
    def fetch_market_payload(self, *, query_name: str = "") -> Mapping[str, Any]:
        raise NotImplementedError

    def crawl_market_payloads(
        self,
        *,
        query_name: str = "",
        max_pages: int | None = None,
        scroll_pause_ms: int = 2_500,
        idle_scroll_limit: int = 3,
    ) -> tuple[Mapping[str, Any], ...]:
        raise NotImplementedError


class PlaywrightSteamDTTransport:
    market_page_url = "https://steamdt.com/mkt"
    market_page_api_path = "https://api.steamdt.com/skin/market/v3/page"

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout_ms: int = 120_000,
        warmup_wait_ms: int = 8_000,
        rate_limiter: RandomizedRateLimiter | None = None,
        viewport: Mapping[str, int] | None = None,
        user_agent: str | None = None,
        locale: str | None = None,
        timezone_id: str | None = None,
        extra_http_headers: Mapping[str, str] | None = None,
        proxy_server: str | None = None,
        proxy_username: str | None = None,
        proxy_password: str | None = None,
        browser_launch_kwargs: Mapping[str, Any] | None = None,
        browser_context_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        self.headless = headless
        self.timeout_ms = timeout_ms
        self.warmup_wait_ms = warmup_wait_ms
        self.rate_limiter = rate_limiter or RandomizedRateLimiter(1.0, 2.0)
        self.viewport = dict(viewport or {"width": 1600, "height": 1200})
        self.user_agent = user_agent
        self.locale = locale
        self.timezone_id = timezone_id
        self.extra_http_headers = dict(extra_http_headers or {})
        self.proxy_server = proxy_server
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.browser_launch_kwargs = dict(browser_launch_kwargs or {})
        self.browser_context_kwargs = dict(browser_context_kwargs or {})
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._initial_payload: dict[str, Any] | None = None

    def __enter__(self) -> "PlaywrightSteamDTTransport":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def start(self) -> None:
        if self._page is not None:
            return
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as error:
            raise RuntimeError(
                "Playwright is not installed. Install with `pip install playwright` "
                "and run `playwright install chromium`."
            ) from error

        self._playwright = sync_playwright().start()
        launch_kwargs: dict[str, Any] = {"headless": self.headless, **self.browser_launch_kwargs}
        if self.proxy_server:
            proxy_config: dict[str, Any] = {"server": self.proxy_server}
            if self.proxy_username:
                proxy_config["username"] = self.proxy_username
            if self.proxy_password:
                proxy_config["password"] = self.proxy_password
            launch_kwargs["proxy"] = proxy_config
        self._browser = self._playwright.chromium.launch(**launch_kwargs)

        context_kwargs: dict[str, Any] = {"viewport": self.viewport, **self.browser_context_kwargs}
        if self.user_agent:
            context_kwargs["user_agent"] = self.user_agent
        if self.locale:
            context_kwargs["locale"] = self.locale
        if self.timezone_id:
            context_kwargs["timezone_id"] = self.timezone_id
        if self.extra_http_headers:
            existing_headers = dict(context_kwargs.get("extra_http_headers", {}))
            existing_headers.update(self.extra_http_headers)
            context_kwargs["extra_http_headers"] = existing_headers
        self._context = self._browser.new_context(**context_kwargs)
        self._page = self._context.new_page()
        initial_exchange = self._capture_exchange(query_name="")
        response_body = initial_exchange.response_body
        if not isinstance(response_body, Mapping):
            raise MarketParseError("SteamDT initial market response was not JSON")
        self._initial_payload = dict(response_body)

    def close(self) -> None:
        if self._page is not None:
            self._page.close()
            self._page = None
        if self._context is not None:
            self._context.close()
            self._context = None
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None
        self._initial_payload = None

    def fetch_market_payload(self, *, query_name: str = "") -> Mapping[str, Any]:
        self.start()
        if not query_name:
            assert self._initial_payload is not None
            return dict(self._initial_payload)
        exchange = self._capture_exchange(query_name=query_name)
        response_body = exchange.response_body
        if not isinstance(response_body, Mapping):
            raise MarketParseError("SteamDT search response was not JSON")
        return dict(response_body)

    def sniff_market_exchange(self, *, query_name: str = "") -> SteamDTCapturedExchange:
        self.start()
        if not query_name and self._initial_payload is not None:
            return self._capture_exchange(query_name="")
        return self._capture_exchange(query_name=query_name)

    def crawl_market_payloads(
        self,
        *,
        query_name: str = "",
        max_pages: int | None = None,
        scroll_pause_ms: int = 2_500,
        idle_scroll_limit: int = 3,
    ) -> tuple[Mapping[str, Any], ...]:
        self.start()
        assert self._page is not None

        collected_payloads: list[dict[str, Any]] = []
        seen_keys: set[tuple[Any, ...]] = set()

        def on_response(response: Any) -> None:
            if not self._is_market_page_response(response):
                return
            try:
                payload = response.json()
            except Exception:
                return
            if not isinstance(payload, Mapping):
                return
            key = self._payload_key(payload)
            if key in seen_keys:
                return
            seen_keys.add(key)
            collected_payloads.append(dict(payload))

        self._page.on("response", on_response)
        try:
            if query_name:
                self._capture_exchange(query_name="")
                self._dismiss_overlays()
                collected_payloads.clear()
                seen_keys.clear()
                self._capture_exchange(query_name=query_name)
            else:
                self._capture_exchange(query_name="")

            self._page.wait_for_timeout(scroll_pause_ms)
            idle_scrolls = 0
            while idle_scrolls < idle_scroll_limit:
                if max_pages is not None and len(collected_payloads) >= max_pages:
                    break
                before_count = len(collected_payloads)
                self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                self._page.wait_for_timeout(max(500, scroll_pause_ms // 2))
                visible_wraps = self._visible_market_scroll_wraps()
                if not visible_wraps:
                    break
                for wrap in visible_wraps:
                    wrap.evaluate("(element) => { element.scrollTop = element.scrollHeight; }")
                self._page.wait_for_timeout(scroll_pause_ms)
                if len(collected_payloads) == before_count:
                    idle_scrolls += 1
                else:
                    idle_scrolls = 0
            payloads = collected_payloads[: max_pages or None]
        finally:
            self._page.remove_listener("response", on_response)
        return tuple(payloads)

    def _capture_exchange(self, *, query_name: str) -> SteamDTCapturedExchange:
        assert self._page is not None
        self.rate_limiter.acquire()
        if query_name:
            self._dismiss_overlays()
            with self._page.expect_response(self._is_market_page_response, timeout=self.timeout_ms) as info:
                search_input = self._page.locator("#search")
                search_input.fill(query_name)
                self._page.wait_for_timeout(300)
                search_button = self._page.locator(".iconfont.icon-sousuo1.text-20.text-white").first
                search_button.click(force=True)
            response = info.value
        else:
            with self._page.expect_response(self._is_market_page_response, timeout=self.timeout_ms) as info:
                self._page.goto(
                    self.market_page_url,
                    wait_until="domcontentloaded",
                    timeout=self.timeout_ms,
                )
            response = info.value
            self._page.wait_for_timeout(self.warmup_wait_ms)
            self._dismiss_overlays()

        request = response.request
        body: dict[str, Any] | list[Any] | str | None
        try:
            body = response.json()
        except Exception:
            body = response.text()
        return SteamDTCapturedExchange(
            method=request.method,
            url=response.url,
            request_headers=dict(request.headers),
            request_body=request.post_data,
            status=response.status,
            response_body=body,
        )

    def _dismiss_overlays(self) -> None:
        assert self._page is not None
        self._page.evaluate(
            """
            () => {
                document
                    .querySelectorAll('.el-overlay, .el-overlay-dialog, [role="dialog"]')
                    .forEach((element) => element.remove());
            }
            """
        )
        self._page.wait_for_timeout(500)

    def _is_market_page_response(self, response: Any) -> bool:
        return (
            response.request.method.upper() == "POST"
            and self.market_page_api_path in response.url
        )

    def _visible_market_scroll_wraps(self) -> tuple[Any, ...]:
        assert self._page is not None
        wraps = self._page.locator(".el-table__body-wrapper .el-scrollbar__wrap")
        visible: list[Any] = []
        for index in range(wraps.count()):
            wrap = wraps.nth(index)
            try:
                is_visible = wrap.evaluate(
                    "(element) => !!(element.offsetWidth || element.offsetHeight || element.getClientRects().length)"
                )
            except Exception:
                continue
            if is_visible:
                visible.append(wrap)
        return tuple(visible)

    def _payload_key(self, payload: Mapping[str, Any]) -> tuple[Any, ...]:
        data = payload.get("data")
        if not isinstance(data, Mapping):
            return ("no-data", payload.get("success"), payload.get("errorCode"))
        raw_items = data.get("list")
        item_ids: tuple[str, ...] = ()
        if isinstance(raw_items, list):
            item_ids = tuple(
                str(item.get("id"))
                for item in raw_items
                if isinstance(item, Mapping) and item.get("id") is not None
            )
        return (
            bool(payload.get("success")),
            str(data.get("nextId")),
            item_ids,
        )


class SteamDTMarketAPI:
    market_name = "SteamDTPrivate"
    thread_affine = True
    default_platform_preference = ("buff", "youpin", "c5", "steam", "skinport")

    def __init__(
        self,
        *,
        transport: SteamDTMarketTransport | None = None,
        preferred_platforms: Sequence[str] | None = None,
        trend_sample_size: int = 7,
    ) -> None:
        self.transport = transport or PlaywrightSteamDTTransport()
        self.preferred_platforms = tuple(preferred_platforms or self.default_platform_preference)
        self.trend_sample_size = trend_sample_size

    def __enter__(self) -> "SteamDTMarketAPI":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        close = getattr(self.transport, "close", None)
        if callable(close):
            close()

    def fetch_market_page(self, *, query_name: str = "") -> SteamDTMarketPage:
        payload = self.transport.fetch_market_payload(query_name=query_name)
        return self._parse_market_page(payload)

    def crawl_market_pages(
        self,
        *,
        query_name: str = "",
        max_pages: int | None = None,
        scroll_pause_ms: int = 2_500,
        idle_scroll_limit: int = 3,
    ) -> tuple[SteamDTMarketPage, ...]:
        crawl = getattr(self.transport, "crawl_market_payloads", None)
        if callable(crawl):
            payloads = crawl(
                query_name=query_name,
                max_pages=max_pages,
                scroll_pause_ms=scroll_pause_ms,
                idle_scroll_limit=idle_scroll_limit,
            )
            return tuple(self._parse_market_page(payload) for payload in payloads)

        page = self.fetch_market_page(query_name=query_name)
        return (page,)

    def search_items(self, query_name: str) -> tuple[SteamDTMarketItem, ...]:
        return self.fetch_market_page(query_name=query_name).items

    def get_item_listings(
        self,
        item_name: str,
        exteriors: Iterable[Exterior | str] | None = None,
    ) -> tuple[SteamDTMarketItem, ...]:
        requested_exteriors = _ordered_exterior_labels(exteriors)
        normalized_target_name = item_name.strip()
        page = self.fetch_market_page(query_name=normalized_target_name)
        matched_items = [
            item
            for item in page.items
            if self._listing_matches(
                item,
                item_name=normalized_target_name,
                allowed_exteriors=requested_exteriors,
            )
        ]
        return _sort_market_items_by_exterior(matched_items)

    def get_item_prices(
        self,
        item_name: str,
        exteriors: Iterable[Exterior | str] | None = None,
    ) -> dict[str, PriceQuote]:
        listings = self.get_item_listings(item_name, exteriors=exteriors)
        quotes: dict[str, PriceQuote] = {}
        for listing in listings:
            snapshot = build_steamdt_price_snapshot(
                listing,
                preferred_platforms=self.preferred_platforms,
                trend_sample_size=self.trend_sample_size,
            )
            if snapshot.exterior is None:
                continue
            quotes[snapshot.exterior] = snapshot.quote
        return quotes

    def get_item_family_prices(
        self,
        item_name: str,
        *,
        exteriors: Iterable[Exterior | str] | None = None,
        include_normal: bool = True,
        include_stattrak: bool = True,
    ) -> dict[tuple[str, str], PriceQuote]:
        base_item_name, _ = split_item_variant_name(item_name)
        variants: list[ItemVariant] = []
        if include_normal:
            variants.append(ItemVariant.NORMAL)
        if include_stattrak:
            variants.append(ItemVariant.STATTRAK)

        quotes: dict[tuple[str, str], PriceQuote] = {}
        for variant in variants:
            variant_item_name = build_item_variant_name(base_item_name, variant)
            for exterior_label, quote in self.get_item_prices(
                variant_item_name,
                exteriors=exteriors,
            ).items():
                quotes[(variant_item_name, exterior_label)] = quote
        return quotes

    def get_item_listing(self, item_name: str, exterior: Exterior | str) -> SteamDTMarketItem:
        target_exterior = normalize_exterior_label(exterior)
        listings = self.get_item_listings(item_name, exteriors=(target_exterior,))
        if listings:
            return listings[0]

        target_market_hash_name = f"{item_name.strip()} ({target_exterior})"
        page = self.fetch_market_page(query_name=target_market_hash_name)
        matched = self._find_exact_item(page.items, target_market_hash_name)
        if matched is not None:
            return matched
        raise MarketParseError(f"SteamDT did not return {target_market_hash_name}")

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        quotes = self.get_item_prices(item_name, exteriors=(exterior,))
        exterior_label = normalize_exterior_label(exterior)
        if exterior_label in quotes:
            return quotes[exterior_label]
        listing = self.get_item_listing(item_name, exterior)
        snapshot = build_steamdt_price_snapshot(
            listing,
            preferred_platforms=self.preferred_platforms,
            trend_sample_size=self.trend_sample_size,
        )
        return PriceQuote(
            lowest_price=snapshot.lowest_price,
            recent_average_price=listing.recent_average_price(self.trend_sample_size),
        )

    def _find_exact_item(
        self,
        items: Sequence[SteamDTMarketItem],
        target_market_hash_name: str,
    ) -> SteamDTMarketItem | None:
        normalized_target = _normalize_text(target_market_hash_name)
        for item in items:
            if _normalize_text(item.market_hash_name) == normalized_target:
                return item
        return None

    def _listing_matches(
        self,
        listing: SteamDTMarketItem,
        *,
        item_name: str,
        allowed_exteriors: Sequence[str] | None = None,
    ) -> bool:
        if listing.is_souvenir:
            return False
        listing_item_name, listing_exterior = split_steamdt_market_hash_name(
            listing.market_hash_name
        )
        if _normalize_text(listing_item_name) != _normalize_text(item_name):
            return False
        if allowed_exteriors is None:
            return True
        return listing_exterior in allowed_exteriors

    def _parse_market_page(self, payload: Mapping[str, Any]) -> SteamDTMarketPage:
        if not payload.get("success"):
            raise MarketParseError(payload.get("errorMsg") or "SteamDT private interface returned an error")

        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise MarketParseError("SteamDT private interface did not return a page object")

        raw_items = data.get("list")
        if not isinstance(raw_items, list):
            raise MarketParseError("SteamDT private interface did not return an item list")

        return SteamDTMarketPage(
            page_num=_parse_int(data.get("pageNum")),
            page_size=_parse_int(data.get("pageSize")),
            total=_parse_int(data.get("total")),
            next_id=str(data["nextId"]) if data.get("nextId") is not None else None,
            system_time=_parse_int(data.get("systemTime")),
            items=tuple(self._parse_market_item(record) for record in raw_items if isinstance(record, Mapping)),
            raw=dict(payload),
        )

    def _parse_market_item(self, record: Mapping[str, Any]) -> SteamDTMarketItem:
        selling_prices = self._parse_platform_prices(record.get("sellingPriceList"))
        purchase_prices = self._parse_platform_prices(record.get("purchasePriceList"))
        trend_list = self._parse_trend_list(record.get("trendList"))
        market_hash_name = record.get("marketHashName")
        name = record.get("name")
        if not market_hash_name or not name:
            raise MarketParseError("SteamDT item record is missing marketHashName or name")

        return SteamDTMarketItem(
            id=str(record.get("id", "")),
            name=str(name),
            short_name=str(record["shortName"]) if record.get("shortName") is not None else None,
            market_hash_name=str(market_hash_name),
            market_short_name=(
                str(record["marketShortName"]) if record.get("marketShortName") is not None else None
            ),
            image_url=str(record["imageUrl"]) if record.get("imageUrl") is not None else None,
            quality_name=str(record["qualityName"]) if record.get("qualityName") is not None else None,
            quality_color=str(record["qualityColor"]) if record.get("qualityColor") is not None else None,
            rarity_name=str(record["rarityName"]) if record.get("rarityName") is not None else None,
            rarity_color=str(record["rarityColor"]) if record.get("rarityColor") is not None else None,
            exterior_name=str(record["exteriorName"]) if record.get("exteriorName") is not None else None,
            exterior_color=str(record["exteriorColor"]) if record.get("exteriorColor") is not None else None,
            selling_price_list=selling_prices,
            purchase_price_list=purchase_prices,
            increase_price=_parse_float(record.get("increasePrice")),
            trend_list=trend_list,
            sell_num=_parse_int(record.get("sellNum")),
            is_souvenir=_is_souvenir_listing_record(record),
            raw=dict(record),
        )

    def _parse_platform_prices(self, value: Any) -> tuple[SteamDTPlatformPrice, ...]:
        if not isinstance(value, list):
            return ()
        entries: list[SteamDTPlatformPrice] = []
        for record in value:
            if not isinstance(record, Mapping):
                continue
            price = _parse_float(record.get("price"))
            if price is None:
                continue
            platform = record.get("platform")
            if not platform:
                continue
            entries.append(
                SteamDTPlatformPrice(
                    platform=str(platform),
                    platform_name=(
                        str(record["platformName"]) if record.get("platformName") is not None else None
                    ),
                    price=price,
                    last_update=_parse_int(record.get("lastUpdate")),
                    link=str(record["link"]) if record.get("link") is not None else None,
                )
            )
        return tuple(entries)

    def _parse_trend_list(self, value: Any) -> tuple[tuple[int, float], ...]:
        if not isinstance(value, list):
            return ()
        trend_points: list[tuple[int, float]] = []
        for row in value:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            timestamp = _parse_int(row[0])
            price = _parse_float(row[1])
            if timestamp is None or price is None:
                continue
            trend_points.append((timestamp, price))
        return tuple(trend_points)


class SteamDTPriceSnapshotStore:
    default_table_name = "steamdt_price_snapshots"

    def __init__(
        self,
        path: str | Path,
        *,
        table_name: str = default_table_name,
        busy_timeout_ms: int = 30_000,
        enable_wal: bool = True,
        synchronous_mode: str = "NORMAL",
    ) -> None:
        self.path = Path(path)
        self.table_name = table_name
        self.cleaned_table_name = f"{self.table_name}_cleaned"
        self.busy_timeout_ms = max(1_000, int(busy_timeout_ms))
        self.enable_wal = enable_wal
        self.synchronous_mode = synchronous_mode
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=max(1.0, self.busy_timeout_ms / 1000.0),
        )
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        if self.enable_wal:
            connection.execute("PRAGMA journal_mode = WAL")
        if self.synchronous_mode:
            connection.execute(f"PRAGMA synchronous = {self.synchronous_mode}")
        return connection

    def insert_snapshot(self, snapshot: SteamDTPriceSnapshot) -> SteamDTPriceSnapshot:
        self.insert_snapshots([snapshot])
        return snapshot

    def insert_snapshots(self, snapshots: Iterable[SteamDTPriceSnapshot]) -> int:
        rows = [
            (
                snapshot.market_hash_name,
                snapshot.item_name,
                snapshot.exterior,
                snapshot.lowest_price,
                snapshot.recent_average_price,
                snapshot.highest_buy_price,
                snapshot.selected_platform,
                snapshot.selected_platform_name,
                snapshot.selected_buy_platform,
                snapshot.selected_buy_platform_name,
                snapshot.sell_num,
                int(snapshot.is_souvenir),
                int(snapshot.is_tradeup_compatible_normal),
                snapshot.variant_filter_reason,
                snapshot.source,
                snapshot.query,
                snapshot.fetched_at_epoch,
                snapshot.fetched_at,
                snapshot.raw_json,
            )
            for snapshot in snapshots
        ]
        if not rows:
            return 0
        for attempt in range(4):
            try:
                with closing(self._connect()) as connection:
                    cursor = connection.executemany(
                        f'''
                        INSERT INTO "{self.table_name}" (
                            market_hash_name,
                            item_name,
                            exterior,
                            lowest_price,
                            recent_average_price,
                            highest_buy_price,
                            selected_platform,
                            selected_platform_name,
                            selected_buy_platform,
                            selected_buy_platform_name,
                            sell_num,
                            is_souvenir,
                            is_tradeup_compatible_normal,
                            variant_filter_reason,
                            source,
                            query,
                            fetched_at_epoch,
                            fetched_at,
                            raw_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        rows,
                    )
                    connection.commit()
                    return cursor.rowcount if cursor.rowcount != -1 else len(rows)
            except sqlite3.OperationalError as error:
                if "locked" not in str(error).lower() or attempt >= 3:
                    raise
                time.sleep(random.uniform(0.15, 0.45) * (attempt + 1))
        return 0

    def insert_listing(
        self,
        listing: SteamDTMarketItem,
        *,
        preferred_platforms: Sequence[str] | None = None,
        trend_sample_size: int = 7,
        query: str | None = None,
        source: str = "steamdt_private",
        fetched_at_epoch: float | None = None,
        fetched_at: str | None = None,
    ) -> SteamDTPriceSnapshot:
        snapshot = build_steamdt_price_snapshot(
            listing,
            preferred_platforms=preferred_platforms,
            trend_sample_size=trend_sample_size,
            query=query,
            source=source,
            fetched_at_epoch=fetched_at_epoch,
            fetched_at=fetched_at,
        )
        self.insert_snapshot(snapshot)
        return snapshot

    def insert_market_page(
        self,
        page: SteamDTMarketPage,
        *,
        preferred_platforms: Sequence[str] | None = None,
        trend_sample_size: int = 7,
        query: str | None = None,
        source: str = "steamdt_private",
        fetched_at_epoch: float | None = None,
        fetched_at: str | None = None,
    ) -> int:
        snapshots: list[SteamDTPriceSnapshot] = []
        for item in page.items:
            try:
                snapshots.append(
                    build_steamdt_price_snapshot(
                        item,
                        preferred_platforms=preferred_platforms,
                        trend_sample_size=trend_sample_size,
                        query=query,
                        source=source,
                        fetched_at_epoch=fetched_at_epoch,
                        fetched_at=fetched_at,
                    )
                )
            except MarketParseError:
                continue
        return self.insert_snapshots(snapshots)

    def get_latest_snapshot(
        self,
        item_name: str,
        exterior: Exterior | str,
        *,
        max_age_seconds: float | None = None,
        prefer_cleaned: bool = False,
        require_valid: bool = False,
        exclude_souvenir: bool = True,
        require_tradeup_compatible_normal: bool = False,
    ) -> SteamDTPriceSnapshot | None:
        exterior_label = normalize_exterior_label(exterior)
        params: list[Any] = [item_name.strip(), exterior_label]
        age_sql = ""
        if max_age_seconds is not None:
            age_sql = " AND fetched_at_epoch >= ?"
            params.append(time.time() - max_age_seconds)
        table_name = self._resolve_read_table_name(prefer_cleaned=prefer_cleaned)
        valid_sql = " AND is_valid = 1" if require_valid and table_name == self.cleaned_table_name else ""
        souvenir_sql = " AND COALESCE(is_souvenir, 0) = 0" if exclude_souvenir else ""
        compatible_sql = (
            " AND COALESCE(is_tradeup_compatible_normal, 1) = 1"
            if require_tradeup_compatible_normal and table_name == self.cleaned_table_name
            else ""
        )
        with closing(self._connect()) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                f'''
                SELECT *
                FROM "{table_name}"
                WHERE item_name = ?
                  AND exterior = ?
                  {age_sql}
                  {valid_sql}
                  {souvenir_sql}
                  {compatible_sql}
                ORDER BY fetched_at_epoch DESC, id DESC
                LIMIT 1
                ''',
                tuple(params),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_snapshot(row)

    def get_latest_snapshots_for_item_family(
        self,
        item_name: str,
        *,
        max_age_seconds: float | None = None,
        prefer_cleaned: bool = False,
        require_valid: bool = False,
        exclude_souvenir: bool = True,
        require_tradeup_compatible_normal: bool = False,
    ) -> tuple[SteamDTPriceSnapshot, ...]:
        base_item_name, _ = split_item_variant_name(item_name)
        candidate_names = (
            base_item_name,
            build_item_variant_name(base_item_name, ItemVariant.STATTRAK),
        )
        params: list[Any] = [*candidate_names]
        age_sql = ""
        if max_age_seconds is not None:
            age_sql = " AND fetched_at_epoch >= ?"
            params.append(time.time() - max_age_seconds)
        table_name = self._resolve_read_table_name(prefer_cleaned=prefer_cleaned)
        valid_sql = " AND is_valid = 1" if require_valid and table_name == self.cleaned_table_name else ""
        souvenir_sql = " AND COALESCE(is_souvenir, 0) = 0" if exclude_souvenir else ""
        compatible_sql = (
            " AND COALESCE(is_tradeup_compatible_normal, 1) = 1"
            if require_tradeup_compatible_normal and table_name == self.cleaned_table_name
            else ""
        )
        with closing(self._connect()) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                f'''
                SELECT *
                FROM "{table_name}"
                WHERE item_name IN (?, ?)
                {age_sql}
                {valid_sql}
                {souvenir_sql}
                {compatible_sql}
                ORDER BY item_name ASC, exterior ASC, fetched_at_epoch DESC, id DESC
                ''',
                tuple(params),
            ).fetchall()
        latest_by_key: dict[tuple[str, str | None], SteamDTPriceSnapshot] = {}
        for row in rows:
            snapshot = self._row_to_snapshot(row)
            key = (snapshot.item_name, snapshot.exterior)
            if key not in latest_by_key:
                latest_by_key[key] = snapshot
        return tuple(latest_by_key.values())

    def list_item_families(self) -> tuple[str, ...]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f'''
                SELECT DISTINCT item_name
                FROM "{self.table_name}"
                ORDER BY item_name ASC
                '''
            ).fetchall()
        families = {
            split_item_variant_name(str(row[0]))[0]
            for row in rows
            if row and row[0]
        }
        return tuple(sorted(families))

    def count_snapshots(self) -> int:
        with closing(self._connect()) as connection:
            row = connection.execute(f'SELECT COUNT(*) FROM "{self.table_name}"').fetchone()
        return int(row[0])

    def has_cleaned_prices(self) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = ?
                LIMIT 1
                """,
                (self.cleaned_table_name,),
            ).fetchone()
        return row is not None

    def refresh_cleaned_prices(self) -> Any:
        detector = PriceAnomalyDetector(
            PriceAnomalyDetectorConfig(
                source_table=self.table_name,
                target_table=self.cleaned_table_name,
                item_name_column="item_name",
                exterior_column="exterior",
                sell_price_column="lowest_price",
                buy_price_column="highest_buy_price",
                volume_24h_column="sell_num",
            )
        )
        with closing(self._connect()) as connection:
            return detector.clean_prices(connection)

    def ensure_cleaned_prices(self) -> Any | None:
        if self.has_cleaned_prices():
            return None
        return self.refresh_cleaned_prices()

    def _ensure_schema(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_hash_name TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    exterior TEXT,
                    lowest_price REAL NOT NULL,
                    recent_average_price REAL,
                    highest_buy_price REAL,
                    selected_platform TEXT,
                    selected_platform_name TEXT,
                    selected_buy_platform TEXT,
                    selected_buy_platform_name TEXT,
                    sell_num INTEGER,
                    is_souvenir INTEGER NOT NULL DEFAULT 0,
                    is_tradeup_compatible_normal INTEGER NOT NULL DEFAULT 1,
                    variant_filter_reason TEXT,
                    source TEXT NOT NULL,
                    query TEXT,
                    fetched_at_epoch REAL NOT NULL,
                    fetched_at TEXT NOT NULL,
                    raw_json TEXT
                )
                '''
            )
            connection.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "idx_{self.table_name}_lookup"
                ON "{self.table_name}" (item_name, exterior, fetched_at_epoch DESC)
                '''
            )
            existing_columns = {
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{self.table_name}")').fetchall()
            }
            optional_columns = {
                "highest_buy_price": 'REAL',
                "selected_buy_platform": 'TEXT',
                "selected_buy_platform_name": 'TEXT',
                "is_souvenir": 'INTEGER NOT NULL DEFAULT 0',
                "is_tradeup_compatible_normal": 'INTEGER NOT NULL DEFAULT 1',
                "variant_filter_reason": 'TEXT',
            }
            for column_name, column_type in optional_columns.items():
                if column_name in existing_columns:
                    continue
                connection.execute(
                    f'ALTER TABLE "{self.table_name}" ADD COLUMN "{column_name}" {column_type}'
                )
            connection.commit()

    def _row_to_snapshot(self, row: sqlite3.Row) -> SteamDTPriceSnapshot:
        row_keys = set(row.keys())
        anomaly_flags_value = row["anomaly_flags"] if "anomaly_flags" in row_keys else None
        anomaly_flags = tuple(
            flag for flag in str(anomaly_flags_value or "").split(",") if flag
        )
        return SteamDTPriceSnapshot(
            market_hash_name=row["market_hash_name"],
            item_name=row["item_name"],
            exterior=row["exterior"],
            lowest_price=float(row["lowest_price"]),
            recent_average_price=(
                float(row["recent_average_price"])
                if row["recent_average_price"] is not None
                else None
            ),
            highest_buy_price=(
                float(row["highest_buy_price"])
                if "highest_buy_price" in row_keys and row["highest_buy_price"] is not None
                else None
            ),
            selected_platform=row["selected_platform"],
            selected_platform_name=row["selected_platform_name"],
            selected_buy_platform=(
                row["selected_buy_platform"] if "selected_buy_platform" in row_keys else None
            ),
            selected_buy_platform_name=(
                row["selected_buy_platform_name"]
                if "selected_buy_platform_name" in row_keys
                else None
            ),
            sell_num=int(row["sell_num"]) if row["sell_num"] is not None else None,
            is_souvenir=bool(row["is_souvenir"]) if "is_souvenir" in row_keys else False,
            is_tradeup_compatible_normal=(
                bool(row["is_tradeup_compatible_normal"])
                if "is_tradeup_compatible_normal" in row_keys
                else True
            ),
            variant_filter_reason=(
                row["variant_filter_reason"] if "variant_filter_reason" in row_keys else None
            ),
            source=row["source"],
            query=row["query"],
            fetched_at_epoch=float(row["fetched_at_epoch"]),
            fetched_at=row["fetched_at"],
            raw_json=row["raw_json"],
            safe_price=(
                float(row["safe_price"])
                if "safe_price" in row_keys and row["safe_price"] is not None
                else None
            ),
            is_valid=bool(row["is_valid"]) if "is_valid" in row_keys else True,
            risk_level=row["risk_level"] if "risk_level" in row_keys else None,
            anomaly_flags=anomaly_flags,
            anomaly_notes=row["anomaly_notes"] if "anomaly_notes" in row_keys else None,
            anomaly_score=(
                float(row["anomaly_score"])
                if "anomaly_score" in row_keys and row["anomaly_score"] is not None
                else None
            ),
            cleaned_at=row["cleaned_at"] if "cleaned_at" in row_keys else None,
        )

    def _resolve_read_table_name(self, *, prefer_cleaned: bool) -> str:
        if prefer_cleaned:
            self.ensure_cleaned_prices()
            if self.has_cleaned_prices():
                return self.cleaned_table_name
        return self.table_name


class CachedSteamDTMarketAPI:
    market_name = "SteamDTCache"

    def __init__(
        self,
        snapshot_store: SteamDTPriceSnapshotStore,
        *,
        steamdt_client: SteamDTMarketAPI | None = None,
        max_age_seconds: float | None = None,
        write_back_on_fetch: bool = True,
        allow_live_fetch: bool = True,
        prefer_safe_price: bool = True,
        require_valid_prices: bool = True,
        refresh_cleaned_after_write: bool = True,
        normal_tradeup_only: bool = True,
    ) -> None:
        self.snapshot_store = snapshot_store
        self.allow_live_fetch = allow_live_fetch
        self.steamdt_client = steamdt_client or (SteamDTMarketAPI() if allow_live_fetch else None)
        self.thread_affine = bool(
            allow_live_fetch and getattr(self.steamdt_client, "thread_affine", False)
        )
        self.max_age_seconds = max_age_seconds
        self.write_back_on_fetch = write_back_on_fetch
        self.prefer_safe_price = prefer_safe_price
        self.require_valid_prices = require_valid_prices
        self.refresh_cleaned_after_write = refresh_cleaned_after_write
        self.normal_tradeup_only = normal_tradeup_only

    def __enter__(self) -> "CachedSteamDTMarketAPI":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        close = getattr(self.steamdt_client, "close", None)
        if callable(close):
            close()

    def warm_item_cache(
        self,
        item_name: str,
        *,
        exteriors: Iterable[Exterior | str] | None = None,
    ) -> tuple[SteamDTPriceSnapshot, ...]:
        if not self.allow_live_fetch or self.steamdt_client is None:
            return ()
        listings = self.steamdt_client.get_item_listings(item_name, exteriors=exteriors)
        snapshots: list[SteamDTPriceSnapshot] = []
        for listing in listings:
            try:
                snapshots.append(
                    build_steamdt_price_snapshot(
                        listing,
                        preferred_platforms=self.steamdt_client.preferred_platforms,
                        trend_sample_size=self.steamdt_client.trend_sample_size,
                        query=item_name.strip(),
                        source="steamdt_private_batch",
                    )
                )
            except MarketParseError:
                continue
        if self.write_back_on_fetch and snapshots:
            self.snapshot_store.insert_snapshots(snapshots)
            if self.prefer_safe_price and self.refresh_cleaned_after_write:
                self.snapshot_store.refresh_cleaned_prices()
        return tuple(snapshots)

    def warm_item_family_cache(
        self,
        item_name: str,
        *,
        exteriors: Iterable[Exterior | str] | None = None,
        include_normal: bool = True,
        include_stattrak: bool = True,
    ) -> tuple[SteamDTPriceSnapshot, ...]:
        if not self.allow_live_fetch or self.steamdt_client is None:
            return ()
        base_item_name, _ = split_item_variant_name(item_name)
        variants: list[ItemVariant] = []
        if include_normal:
            variants.append(ItemVariant.NORMAL)
        if include_stattrak:
            variants.append(ItemVariant.STATTRAK)

        snapshots: list[SteamDTPriceSnapshot] = []
        for variant in variants:
            snapshots.extend(
                self.warm_item_cache(
                    build_item_variant_name(base_item_name, variant),
                    exteriors=exteriors,
                )
            )
        return tuple(snapshots)

    def get_item_prices(
        self,
        item_name: str,
        exteriors: Iterable[Exterior | str] | None = None,
    ) -> dict[str, PriceQuote]:
        requested_exteriors = _ordered_exterior_labels(exteriors)
        quotes: dict[str, PriceQuote] = {}
        if requested_exteriors:
            for exterior_label in requested_exteriors:
                snapshot = self.snapshot_store.get_latest_snapshot(
                    item_name=item_name,
                    exterior=exterior_label,
                    max_age_seconds=self.max_age_seconds,
                    prefer_cleaned=self.prefer_safe_price,
                    require_valid=self.require_valid_prices,
                    require_tradeup_compatible_normal=self.normal_tradeup_only,
                )
                if snapshot is not None:
                    quotes[exterior_label] = snapshot.quote
        if requested_exteriors is not None and len(quotes) == len(requested_exteriors):
            return quotes

        stale_quotes: dict[str, PriceQuote] = {}
        if self.max_age_seconds is not None and requested_exteriors:
            for exterior_label in requested_exteriors:
                if exterior_label in quotes:
                    continue
                snapshot = self.snapshot_store.get_latest_snapshot(
                    item_name=item_name,
                    exterior=exterior_label,
                    max_age_seconds=None,
                    prefer_cleaned=self.prefer_safe_price,
                    require_valid=self.require_valid_prices,
                    require_tradeup_compatible_normal=self.normal_tradeup_only,
                )
                if snapshot is not None:
                    stale_quotes[exterior_label] = snapshot.quote

        if not self.allow_live_fetch or self.steamdt_client is None:
            return {**stale_quotes, **quotes}

        try:
            warmed_snapshots = self.warm_item_cache(item_name, exteriors=exteriors)
        except Exception:
            if quotes or stale_quotes:
                return {**stale_quotes, **quotes}
            raise

        for snapshot in warmed_snapshots:
            if snapshot.exterior is not None:
                quotes[snapshot.exterior] = snapshot.quote

        if requested_exteriors is not None:
            for exterior_label in requested_exteriors:
                if exterior_label not in quotes and exterior_label in stale_quotes:
                    quotes[exterior_label] = stale_quotes[exterior_label]
        return quotes

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        exterior_label = normalize_exterior_label(exterior)
        quotes = self.get_item_prices(item_name, exteriors=(exterior_label,))
        if exterior_label in quotes:
            return quotes[exterior_label]

        cached_snapshot = self.snapshot_store.get_latest_snapshot(
            item_name=item_name,
            exterior=exterior_label,
            max_age_seconds=None,
            prefer_cleaned=self.prefer_safe_price,
            require_valid=self.require_valid_prices,
            require_tradeup_compatible_normal=self.normal_tradeup_only,
        )
        if cached_snapshot is not None:
            return cached_snapshot.quote
        raise MarketParseError(
            f"SteamDT did not return {item_name.strip()} ({exterior_label})"
        )

    def warm_query_cache(self, query_name: str) -> SteamDTMarketPage:
        if not self.allow_live_fetch or self.steamdt_client is None:
            raise MarketParseError("Live SteamDT fetching is disabled for this cache client")
        page = self.steamdt_client.fetch_market_page(query_name=query_name)
        if self.write_back_on_fetch:
            self.snapshot_store.insert_market_page(
                page,
                preferred_platforms=self.steamdt_client.preferred_platforms,
                trend_sample_size=self.steamdt_client.trend_sample_size,
                query=query_name,
                source="steamdt_private_search",
            )
            if self.prefer_safe_price and self.refresh_cleaned_after_write:
                self.snapshot_store.refresh_cleaned_prices()
        return page


def crawl_all_steamdt_market_to_sqlite(
    snapshot_store: SteamDTPriceSnapshotStore | str | Path,
    *,
    steamdt_client: SteamDTMarketAPI | None = None,
    query_name: str = "",
    max_pages: int | None = None,
    scroll_pause_ms: int = 2_500,
    idle_scroll_limit: int = 3,
) -> SteamDTCrawlSummary:
    store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    owns_client = steamdt_client is None
    client = steamdt_client or SteamDTMarketAPI()
    try:
        pages = client.crawl_market_pages(
            query_name=query_name,
            max_pages=max_pages,
            scroll_pause_ms=scroll_pause_ms,
            idle_scroll_limit=idle_scroll_limit,
        )
        snapshots_inserted = 0
        unique_items: set[str] = set()
        total_available: int | None = None
        last_next_id: str | None = None
        for page in pages:
            total_available = page.total if total_available is None else total_available
            last_next_id = page.next_id
            snapshots_inserted += store.insert_market_page(
                page,
                preferred_platforms=client.preferred_platforms,
                trend_sample_size=client.trend_sample_size,
                query=query_name,
                source="steamdt_private_crawl",
            )
            unique_items.update(item.market_hash_name for item in page.items)
        items_seen = sum(len(page.items) for page in pages)
        store.refresh_cleaned_prices()
        return SteamDTCrawlSummary(
            query_name=query_name,
            pages_crawled=len(pages),
            items_seen=items_seen,
            unique_items=len(unique_items),
            snapshots_inserted=snapshots_inserted,
            total_available=total_available,
            last_next_id=last_next_id,
        )
    finally:
        if owns_client:
            client.close()


def sniff_steamdt_market_exchange(query_name: str = "") -> SteamDTCapturedExchange:
    with PlaywrightSteamDTTransport() as transport:
        return transport.sniff_market_exchange(query_name=query_name)


__all__ = [
    "CachedSteamDTMarketAPI",
    "PlaywrightSteamDTTransport",
    "SteamDTCrawlSummary",
    "SteamDTCapturedExchange",
    "SteamDTMarketAPI",
    "SteamDTMarketItem",
    "SteamDTMarketPage",
    "SteamDTPlatformPrice",
    "SteamDTPriceSnapshot",
    "SteamDTPriceSnapshotStore",
    "build_steamdt_price_snapshot",
    "crawl_all_steamdt_market_to_sqlite",
    "sniff_steamdt_market_exchange",
    "split_steamdt_market_hash_name",
]
