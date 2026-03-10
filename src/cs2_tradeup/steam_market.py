from __future__ import annotations

import csv
import json
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests

from .exceptions import MarketParseError, MarketRequestError
from .market import (
    DEFAULT_REQUEST_HEADERS,
    RandomizedRateLimiter,
    build_item_variant_name,
    normalize_exterior_label,
    parse_cookie_string,
    split_item_variant_name,
)
from .models import Exterior, ItemVariant, PriceQuote


@dataclass(frozen=True, slots=True)
class SteamMarketSearchEntry:
    market_hash_name: str
    sell_listings: int
    sell_price: float | None = None
    sell_price_text: str | None = None
    sale_price_text: str | None = None
    appid: int = 730
    raw: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "market_hash_name": self.market_hash_name,
            "sell_listings": self.sell_listings,
            "sell_price": self.sell_price,
            "sell_price_text": self.sell_price_text,
            "sale_price_text": self.sale_price_text,
            "appid": self.appid,
        }
        if self.raw is not None:
            payload["raw"] = self.raw
        return payload


@dataclass(frozen=True, slots=True)
class SteamPriceSnapshot:
    market_hash_name: str
    item_name: str
    exterior: str | None
    lowest_price: float
    recent_average_price: float | None = None
    sell_listings: int | None = None
    sell_price: float | None = None
    sell_price_text: str | None = None
    sale_price_text: str | None = None
    appid: int = 730
    currency: int | None = None
    country: str | None = None
    source: str = "steam"
    query: str | None = None
    fetched_at_epoch: float = 0.0
    fetched_at: str = ""
    raw_json: str | None = None

    @property
    def quote(self) -> PriceQuote:
        return PriceQuote(
            lowest_price=self.lowest_price,
            recent_average_price=self.recent_average_price,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_hash_name": self.market_hash_name,
            "item_name": self.item_name,
            "exterior": self.exterior,
            "lowest_price": self.lowest_price,
            "recent_average_price": self.recent_average_price,
            "sell_listings": self.sell_listings,
            "sell_price": self.sell_price,
            "sell_price_text": self.sell_price_text,
            "sale_price_text": self.sale_price_text,
            "appid": self.appid,
            "currency": self.currency,
            "country": self.country,
            "source": self.source,
            "query": self.query,
            "fetched_at_epoch": self.fetched_at_epoch,
            "fetched_at": self.fetched_at,
            "raw_json": self.raw_json,
        }


def _parse_price(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).replace(",", "").strip()
    numeric = []
    decimal_seen = False
    sign_seen = False
    for character in text:
        if character.isdigit():
            numeric.append(character)
            continue
        if character == "." and not decimal_seen:
            numeric.append(character)
            decimal_seen = True
            continue
        if character == "-" and not sign_seen and not numeric:
            numeric.append(character)
            sign_seen = True
            continue
        if numeric:
            break
    if not numeric or numeric == ["-"]:
        return None
    return float("".join(numeric))


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


def split_market_hash_name(market_hash_name: str) -> tuple[str, str | None]:
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


class SteamMarketAPI:
    market_name = "Steam"
    default_base_url = "https://steamcommunity.com"
    priceoverview_endpoint = "/market/priceoverview/"
    search_render_endpoint = "/market/search/render/"

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
        appid: int = 730,
        currency: int = 1,
        country: str = "US",
        browser_fallback: Any | None = None,
    ) -> None:
        self.base_url = (base_url or self.default_base_url).rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.appid = appid
        self.currency = currency
        self.country = country
        self.browser_fallback = browser_fallback
        self.rate_limiter = rate_limiter or RandomizedRateLimiter()
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_REQUEST_HEADERS)
        self.session.headers.setdefault("Referer", f"{self.base_url}/market/search?appid={appid}")
        if headers:
            self.session.headers.update(dict(headers))
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

    def _get_item_price_via_http(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        market_hash_name = self.build_market_hash_name(item_name, exterior)
        payload = self._request_json(
            "GET",
            self.priceoverview_endpoint,
            params={
                "appid": self.appid,
                "currency": self.currency,
                "country": self.country,
                "market_hash_name": market_hash_name,
            },
        )
        if not payload.get("success", False):
            raise MarketParseError(f"Steam priceoverview did not return success for {market_hash_name}")

        lowest_price = _parse_price(payload.get("lowest_price"))
        median_price = _parse_price(payload.get("median_price"))
        if lowest_price is None and median_price is None:
            raise MarketParseError(f"Steam priceoverview missing price data for {market_hash_name}")
        resolved_lowest_price = lowest_price if lowest_price is not None else median_price
        return PriceQuote(
            lowest_price=resolved_lowest_price,
            recent_average_price=median_price,
        )

    def build_market_hash_name(self, item_name: str, exterior: Exterior | str) -> str:
        exterior_label = normalize_exterior_label(exterior)
        if not exterior_label:
            return item_name
        suffix = f"({exterior_label})"
        normalized_item_name = item_name.strip()
        if normalized_item_name.endswith(suffix):
            return normalized_item_name
        return f"{normalized_item_name} {suffix}"

    def get_item_entries(
        self,
        item_name: str,
        exteriors: Iterable[Exterior | str] | None = None,
        *,
        count: int = 100,
    ) -> tuple[SteamMarketSearchEntry, ...]:
        requested_exteriors = _ordered_exterior_labels(exteriors)
        entries = self.crawl_search_results(
            query=item_name.strip(),
            count=count,
            max_pages=1,
        )
        matched_entries = []
        for entry in entries:
            matched_item_name, matched_exterior = split_market_hash_name(entry.market_hash_name)
            if matched_item_name != item_name.strip():
                continue
            if requested_exteriors is not None and matched_exterior not in requested_exteriors:
                continue
            matched_entries.append(entry)
        exterior_rank = {exterior.value: index for index, exterior in enumerate(Exterior.ordered())}
        return tuple(
            sorted(
                matched_entries,
                key=lambda entry: (
                    exterior_rank.get(
                        split_market_hash_name(entry.market_hash_name)[1] or "",
                        len(exterior_rank),
                    ),
                    entry.market_hash_name,
                ),
            )
        )

    def get_item_prices(
        self,
        item_name: str,
        exteriors: Iterable[Exterior | str] | None = None,
    ) -> dict[str, PriceQuote]:
        quotes: dict[str, PriceQuote] = {}
        for entry in self.get_item_entries(item_name, exteriors=exteriors):
            _, exterior_label = split_market_hash_name(entry.market_hash_name)
            if exterior_label is None:
                continue
            lowest_price = entry.sell_price
            if lowest_price is None and entry.sell_price_text is not None:
                lowest_price = _parse_price(entry.sell_price_text)
            if lowest_price is None:
                continue
            quotes[exterior_label] = PriceQuote(
                lowest_price=lowest_price,
                recent_average_price=_parse_price(entry.sale_price_text),
            )
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

    def crawl_search_page(
        self,
        *,
        query: str = "",
        start: int = 0,
        count: int = 100,
        sort_column: str = "price",
        sort_dir: str = "asc",
        search_descriptions: bool = False,
    ) -> tuple[list[SteamMarketSearchEntry], int]:
        payload = self._request_json(
            "GET",
            self.search_render_endpoint,
            params={
                "query": query,
                "start": start,
                "count": count,
                "search_descriptions": 1 if search_descriptions else 0,
                "sort_column": sort_column,
                "sort_dir": sort_dir,
                "appid": self.appid,
                "norender": 1,
            },
        )
        entries = self._parse_search_entries(payload)
        total_count = payload.get("total_count")
        if total_count is None:
            total_count = start + len(entries)
        return entries, int(total_count)

    def crawl_search_results(
        self,
        *,
        query: str = "",
        count: int = 100,
        max_pages: int | None = None,
        max_items: int | None = None,
        sort_column: str = "price",
        sort_dir: str = "asc",
        search_descriptions: bool = False,
    ) -> tuple[SteamMarketSearchEntry, ...]:
        all_entries: list[SteamMarketSearchEntry] = []
        start = 0
        page_index = 0
        total_count: int | None = None

        while True:
            if max_pages is not None and page_index >= max_pages:
                break
            page_entries, page_total_count = self.crawl_search_page(
                query=query,
                start=start,
                count=count,
                sort_column=sort_column,
                sort_dir=sort_dir,
                search_descriptions=search_descriptions,
            )
            total_count = page_total_count
            if not page_entries:
                break
            all_entries.extend(page_entries)
            if max_items is not None and len(all_entries) >= max_items:
                all_entries = all_entries[:max_items]
                break

            start += len(page_entries)
            page_index += 1
            if total_count is not None and start >= total_count:
                break
            if len(page_entries) < count:
                break

        return tuple(all_entries)

    def export_search_entries_json(
        self,
        entries: Iterable[SteamMarketSearchEntry],
        path: str | Path,
        *,
        indent: int = 2,
    ) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"items": [entry.to_dict() for entry in entries]}
        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=indent),
            encoding="utf-8",
        )
        return output_path

    def export_search_entries_csv(
        self,
        entries: Iterable[SteamMarketSearchEntry],
        path: str | Path,
    ) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
            fieldnames = [
                "market_hash_name",
                "sell_listings",
                "sell_price",
                "sell_price_text",
                "sale_price_text",
                "appid",
            ]
            writer = csv.DictWriter(
                handle,
                fieldnames=fieldnames,
            )
            writer.writeheader()
            for entry in entries:
                row = entry.to_dict()
                writer.writerow({field: row.get(field) for field in fieldnames})
        return output_path

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
            raise MarketRequestError(f"Steam request failed: {error}") from error
        except ValueError as error:
            raise MarketParseError("Steam returned non-JSON data") from error

    def _parse_search_entries(self, payload: Mapping[str, Any]) -> list[SteamMarketSearchEntry]:
        raw_results = payload.get("results")
        if not isinstance(raw_results, list):
            raise MarketParseError("Steam search/render payload does not contain a results list")

        entries: list[SteamMarketSearchEntry] = []
        for record in raw_results:
            if not isinstance(record, Mapping):
                continue
            market_hash_name = record.get("hash_name") or record.get("market_hash_name")
            if not market_hash_name:
                continue
            sell_listings = _parse_int(record.get("sell_listings"))
            if sell_listings is None:
                sell_listings = 0
            entries.append(
                SteamMarketSearchEntry(
                    market_hash_name=str(market_hash_name),
                    sell_listings=sell_listings,
                    sell_price=_parse_price(record.get("sell_price_text") or record.get("sell_price")),
                    sell_price_text=(
                        str(record["sell_price_text"])
                        if record.get("sell_price_text") is not None
                        else None
                    ),
                    sale_price_text=(
                        str(record["sale_price_text"])
                        if record.get("sale_price_text") is not None
                        else None
                    ),
                    appid=int(record.get("appid", self.appid)),
                    raw=dict(record),
                )
            )
        return entries


class SteamPriceSnapshotStore:
    default_table_name = "steam_price_snapshots"

    def __init__(
        self,
        path: str | Path,
        *,
        table_name: str = default_table_name,
    ) -> None:
        self.path = Path(path)
        self.table_name = table_name
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def insert_search_entries(
        self,
        entries: Iterable[SteamMarketSearchEntry],
        *,
        query: str | None = None,
        currency: int | None = None,
        country: str | None = None,
        fetched_at_epoch: float | None = None,
        fetched_at: str | None = None,
        source: str = "search_render",
    ) -> int:
        timestamp, iso_value = self._resolve_timestamp(fetched_at_epoch, fetched_at)
        rows = []
        for entry in entries:
            item_name, exterior = split_market_hash_name(entry.market_hash_name)
            lowest_price = entry.sell_price
            if lowest_price is None and entry.sell_price_text is not None:
                lowest_price = _parse_price(entry.sell_price_text)
            if lowest_price is None:
                continue
            rows.append(
                (
                    entry.market_hash_name,
                    item_name,
                    exterior,
                    lowest_price,
                    None,
                    entry.sell_listings,
                    entry.sell_price,
                    entry.sell_price_text,
                    entry.sale_price_text,
                    entry.appid,
                    currency,
                    country,
                    source,
                    query,
                    timestamp,
                    iso_value,
                    json.dumps(entry.raw, ensure_ascii=False) if entry.raw is not None else None,
                )
            )
        self._insert_rows(rows)
        return len(rows)

    def insert_price_quote(
        self,
        item_name: str,
        exterior: Exterior | str,
        quote: PriceQuote,
        *,
        appid: int = 730,
        currency: int | None = None,
        country: str | None = None,
        fetched_at_epoch: float | None = None,
        fetched_at: str | None = None,
        source: str = "priceoverview",
        raw_payload: Mapping[str, Any] | None = None,
        sell_listings: int | None = None,
        sell_price: float | None = None,
        sell_price_text: str | None = None,
        sale_price_text: str | None = None,
    ) -> SteamPriceSnapshot:
        timestamp, iso_value = self._resolve_timestamp(fetched_at_epoch, fetched_at)
        exterior_label = normalize_exterior_label(exterior)
        market_hash_name = (
            item_name
            if not exterior_label
            else f"{item_name.strip()} ({exterior_label})"
        )
        snapshot = SteamPriceSnapshot(
            market_hash_name=market_hash_name,
            item_name=item_name.strip(),
            exterior=exterior_label or None,
            lowest_price=quote.lowest_price,
            recent_average_price=quote.recent_average_price,
            sell_listings=sell_listings,
            sell_price=sell_price,
            sell_price_text=sell_price_text,
            sale_price_text=sale_price_text,
            appid=appid,
            currency=currency,
            country=country,
            source=source,
            fetched_at_epoch=timestamp,
            fetched_at=iso_value,
            raw_json=json.dumps(dict(raw_payload), ensure_ascii=False) if raw_payload is not None else None,
        )
        self._insert_rows(
            [
                (
                    snapshot.market_hash_name,
                    snapshot.item_name,
                    snapshot.exterior,
                    snapshot.lowest_price,
                    snapshot.recent_average_price,
                    snapshot.sell_listings,
                    snapshot.sell_price,
                    snapshot.sell_price_text,
                    snapshot.sale_price_text,
                    snapshot.appid,
                    snapshot.currency,
                    snapshot.country,
                    snapshot.source,
                    snapshot.query,
                    snapshot.fetched_at_epoch,
                    snapshot.fetched_at,
                    snapshot.raw_json,
                )
            ]
        )
        return snapshot

    def get_latest_snapshot(
        self,
        item_name: str,
        exterior: Exterior | str,
        *,
        max_age_seconds: float | None = None,
    ) -> SteamPriceSnapshot | None:
        exterior_label = normalize_exterior_label(exterior)
        where_clause = "item_name = ? AND exterior = ?"
        params: list[Any] = [item_name.strip(), exterior_label]
        if max_age_seconds is not None:
            where_clause += " AND fetched_at_epoch >= ?"
            params.append(time.time() - max_age_seconds)

        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                f'''
                SELECT *
                FROM "{self.table_name}"
                WHERE {where_clause}
                ORDER BY fetched_at_epoch DESC, id DESC
                LIMIT 1
                ''',
                tuple(params),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_snapshot(row)

    def count_rows(self) -> int:
        with closing(sqlite3.connect(self.path)) as connection:
            row = connection.execute(
                f'SELECT COUNT(*) FROM "{self.table_name}"'
            ).fetchone()
        return int(row[0])

    def _ensure_schema(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_hash_name TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    exterior TEXT,
                    lowest_price REAL NOT NULL,
                    recent_average_price REAL,
                    sell_listings INTEGER,
                    sell_price REAL,
                    sell_price_text TEXT,
                    sale_price_text TEXT,
                    appid INTEGER NOT NULL,
                    currency INTEGER,
                    country TEXT,
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
                CREATE INDEX IF NOT EXISTS "idx_{self.table_name}_item_exterior_ts"
                ON "{self.table_name}" (item_name, exterior, fetched_at_epoch DESC)
                '''
            )
            connection.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "idx_{self.table_name}_market_hash_ts"
                ON "{self.table_name}" (market_hash_name, fetched_at_epoch DESC)
                '''
            )
            connection.commit()

    def _insert_rows(self, rows: Iterable[tuple[Any, ...]]) -> None:
        rows = list(rows)
        if not rows:
            return
        with closing(sqlite3.connect(self.path)) as connection:
            connection.executemany(
                f'''
                INSERT INTO "{self.table_name}" (
                    market_hash_name,
                    item_name,
                    exterior,
                    lowest_price,
                    recent_average_price,
                    sell_listings,
                    sell_price,
                    sell_price_text,
                    sale_price_text,
                    appid,
                    currency,
                    country,
                    source,
                    query,
                    fetched_at_epoch,
                    fetched_at,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                rows,
            )
            connection.commit()

    def _resolve_timestamp(
        self,
        fetched_at_epoch: float | None,
        fetched_at: str | None,
    ) -> tuple[float, str]:
        if fetched_at_epoch is None and fetched_at is None:
            return _utc_now_parts()
        if fetched_at_epoch is None and fetched_at is not None:
            parsed = datetime.fromisoformat(fetched_at)
            return parsed.timestamp(), fetched_at
        if fetched_at_epoch is not None and fetched_at is None:
            iso_value = datetime.fromtimestamp(fetched_at_epoch, tz=timezone.utc).isoformat()
            return fetched_at_epoch, iso_value
        assert fetched_at_epoch is not None and fetched_at is not None
        return fetched_at_epoch, fetched_at

    def _row_to_snapshot(self, row: sqlite3.Row) -> SteamPriceSnapshot:
        return SteamPriceSnapshot(
            market_hash_name=row["market_hash_name"],
            item_name=row["item_name"],
            exterior=row["exterior"],
            lowest_price=float(row["lowest_price"]),
            recent_average_price=(
                float(row["recent_average_price"])
                if row["recent_average_price"] is not None
                else None
            ),
            sell_listings=row["sell_listings"],
            sell_price=float(row["sell_price"]) if row["sell_price"] is not None else None,
            sell_price_text=row["sell_price_text"],
            sale_price_text=row["sale_price_text"],
            appid=int(row["appid"]),
            currency=row["currency"],
            country=row["country"],
            source=row["source"],
            query=row["query"],
            fetched_at_epoch=float(row["fetched_at_epoch"]),
            fetched_at=row["fetched_at"],
            raw_json=row["raw_json"],
        )


class CachedSteamMarketAPI:
    market_name = "SteamCache"

    def __init__(
        self,
        snapshot_store: SteamPriceSnapshotStore,
        *,
        steam_client: SteamMarketAPI | None = None,
        max_age_seconds: float | None = None,
        write_back_on_fetch: bool = True,
    ) -> None:
        self.snapshot_store = snapshot_store
        self.steam_client = steam_client
        self.max_age_seconds = max_age_seconds
        self.write_back_on_fetch = write_back_on_fetch

    def warm_item_cache(
        self,
        item_name: str,
        *,
        exteriors: Iterable[Exterior | str] | None = None,
    ) -> tuple[SteamMarketSearchEntry, ...]:
        if self.steam_client is None:
            raise MarketParseError("A live SteamMarketAPI client is required to warm item cache")
        entries = self.steam_client.get_item_entries(item_name, exteriors=exteriors)
        if self.write_back_on_fetch and entries:
            self.snapshot_store.insert_search_entries(
                entries,
                query=item_name.strip(),
                currency=self.steam_client.currency,
                country=self.steam_client.country,
                source="search_render_batch",
            )
        return entries

    def warm_item_family_cache(
        self,
        item_name: str,
        *,
        exteriors: Iterable[Exterior | str] | None = None,
        include_normal: bool = True,
        include_stattrak: bool = True,
    ) -> tuple[SteamMarketSearchEntry, ...]:
        if self.steam_client is None:
            raise MarketParseError("A live SteamMarketAPI client is required to warm item cache")
        base_item_name, _ = split_item_variant_name(item_name)
        variants: list[ItemVariant] = []
        if include_normal:
            variants.append(ItemVariant.NORMAL)
        if include_stattrak:
            variants.append(ItemVariant.STATTRAK)
        entries: list[SteamMarketSearchEntry] = []
        for variant in variants:
            entries.extend(
                self.warm_item_cache(
                    build_item_variant_name(base_item_name, variant),
                    exteriors=exteriors,
                )
            )
        return tuple(entries)

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
                )
                if snapshot is not None:
                    quotes[exterior_label] = snapshot.quote
        if requested_exteriors is not None and len(quotes) == len(requested_exteriors):
            return quotes

        if self.steam_client is None:
            if quotes:
                return quotes
            raise MarketParseError(
                f"No cached Steam quote found for {item_name}"
            )

        stale_quotes: dict[str, PriceQuote] = {}
        if self.max_age_seconds is not None and requested_exteriors:
            for exterior_label in requested_exteriors:
                if exterior_label in quotes:
                    continue
                snapshot = self.snapshot_store.get_latest_snapshot(
                    item_name=item_name,
                    exterior=exterior_label,
                    max_age_seconds=None,
                )
                if snapshot is not None:
                    stale_quotes[exterior_label] = snapshot.quote

        try:
            entries = self.warm_item_cache(item_name, exteriors=exteriors)
        except Exception:
            if quotes or stale_quotes:
                return {**stale_quotes, **quotes}
            raise

        for entry in entries:
            _, exterior_label = split_market_hash_name(entry.market_hash_name)
            if exterior_label is None:
                continue
            lowest_price = entry.sell_price
            if lowest_price is None and entry.sell_price_text is not None:
                lowest_price = _parse_price(entry.sell_price_text)
            if lowest_price is None:
                continue
            quotes[exterior_label] = PriceQuote(
                lowest_price=lowest_price,
                recent_average_price=_parse_price(entry.sale_price_text),
            )

        if requested_exteriors is not None:
            for exterior_label in requested_exteriors:
                if exterior_label not in quotes and exterior_label in stale_quotes:
                    quotes[exterior_label] = stale_quotes[exterior_label]
        return quotes

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        cached_snapshot = self.snapshot_store.get_latest_snapshot(
            item_name=item_name,
            exterior=exterior,
            max_age_seconds=self.max_age_seconds,
        )
        if cached_snapshot is not None:
            return cached_snapshot.quote

        if self.steam_client is None:
            raise MarketParseError(
                f"No cached Steam quote found for {item_name} ({normalize_exterior_label(exterior)})"
            )

        quote = self.steam_client.get_item_price(item_name, exterior)
        if self.write_back_on_fetch:
            self.snapshot_store.insert_price_quote(
                item_name=item_name,
                exterior=exterior,
                quote=quote,
                appid=self.steam_client.appid,
                currency=self.steam_client.currency,
                country=self.steam_client.country,
                source="priceoverview",
            )
        return quote

    def crawl_and_cache_search_results(self, **kwargs) -> tuple[SteamMarketSearchEntry, ...]:
        if self.steam_client is None:
            raise MarketParseError("A live SteamMarketAPI client is required to crawl search results")
        entries = self.steam_client.crawl_search_results(**kwargs)
        self.snapshot_store.insert_search_entries(
            entries,
            query=kwargs.get("query", ""),
            currency=self.steam_client.currency,
            country=self.steam_client.country,
        )
        return entries
