from __future__ import annotations

import json
import re
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import requests

from .exceptions import MarketParseError, MarketRequestError
from .market import (
    DEFAULT_REQUEST_HEADERS,
    RandomizedRateLimiter,
    normalize_exterior_label,
    parse_cookie_string,
)
from .models import Exterior, PriceQuote


def build_market_name(item_name: str, exterior: Exterior | str | None = None) -> str:
    normalized_name = item_name.strip()
    if exterior is None:
        return normalized_name
    exterior_label = normalize_exterior_label(exterior)
    if not exterior_label:
        return normalized_name
    suffix = f" ({exterior_label})"
    if normalized_name.endswith(suffix):
        return normalized_name
    return f"{normalized_name}{suffix}"


def split_market_name(market_name: str) -> tuple[str, str | None]:
    normalized_name = market_name.strip()
    exterior_labels = sorted((exterior.value for exterior in Exterior), key=len, reverse=True)
    for exterior_label in exterior_labels:
        suffix = f" ({exterior_label})"
        if normalized_name.endswith(suffix):
            return normalized_name[: -len(suffix)], exterior_label
    return normalized_name, None


def _parse_price(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def _utc_now_parts() -> tuple[float, str]:
    timestamp = time.time()
    iso_value = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    return timestamp, iso_value


def _epoch_to_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _parse_igxe_datetime(value: str) -> tuple[float, str]:
    normalized = (
        value.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .replace("/", "-")
        .strip()
    )
    if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", normalized):
        normalized = f"{normalized} 00:00:00"
    parsed = datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return parsed.timestamp(), parsed.isoformat()


def _parse_c5_detail_fields(html: str) -> tuple[str | None, tuple[str, ...]]:
    wear_match = re.search(r"磨损[:：]\s*([^<\s]{1,16})", html)
    paintwear = wear_match.group(1).strip() if wear_match else None
    sticker_match = re.search(r"印花[:：]\s*(.*?)</center>", html, flags=re.DOTALL)
    stickers: tuple[str, ...] = ()
    if sticker_match:
        raw_value = re.sub(r"<.*?>", "", sticker_match.group(1)).strip()
        if raw_value:
            stickers = tuple(
                fragment.strip()
                for fragment in re.split(r"[,\n]+", raw_value)
                if fragment.strip()
            )
    return paintwear, stickers


@dataclass(frozen=True, slots=True)
class TrackedGoods:
    item_name: str
    buff_goods_id: int | None = None
    c5_goods_id: int | None = None
    igxe_goods_id: int | None = None


@dataclass(frozen=True, slots=True)
class TransactionRecord:
    platform: str
    goods_id: int | None
    goods_name: str
    price: float
    transact_time_epoch: float
    transact_time: str
    paintwear: str | None = None
    stickers: tuple[str, ...] = ()
    sticker_is_influence: int = 0
    external_record_id: str | None = None
    source_url: str | None = None
    raw_json: str | None = None

    def to_db_row(self) -> tuple[Any, ...]:
        return (
            self.platform,
            self.goods_id,
            self.goods_name,
            self.price,
            self.paintwear,
            json.dumps(self.stickers, ensure_ascii=False),
            self.sticker_is_influence,
            self.transact_time_epoch,
            self.transact_time,
            self.external_record_id,
            self.source_url,
            self.raw_json,
        )


@dataclass(frozen=True, slots=True)
class MarketPriceSnapshot:
    platform: str
    item_name: str
    exterior: str | None
    lowest_price: float
    recent_average_price: float | None
    sample_count: int
    fetched_at_epoch: float
    fetched_at: str
    source: str = "history_sync"

    @property
    def quote(self) -> PriceQuote:
        return PriceQuote(
            lowest_price=self.lowest_price,
            recent_average_price=self.recent_average_price,
        )

    def to_db_row(self) -> tuple[Any, ...]:
        return (
            self.platform,
            self.item_name,
            self.exterior,
            self.lowest_price,
            self.recent_average_price,
            self.sample_count,
            self.fetched_at_epoch,
            self.fetched_at,
            self.source,
        )


def build_price_snapshot(
    platform: str,
    records: Sequence["TransactionRecord"],
    *,
    source: str = "history_sync",
    fetched_at_epoch: float | None = None,
    fetched_at: str | None = None,
) -> "MarketPriceSnapshot":
    if not records:
        raise ValueError("records cannot be empty")
    resolved_epoch, resolved_at = (
        _utc_now_parts()
        if fetched_at_epoch is None and fetched_at is None
        else (
            fetched_at_epoch
            if fetched_at_epoch is not None
            else datetime.fromisoformat(str(fetched_at)).timestamp(),
            fetched_at
            if fetched_at is not None
            else _epoch_to_iso(float(fetched_at_epoch)),
        )
    )
    base_item_name, exterior = split_market_name(records[0].goods_name)
    prices = [record.price for record in records]
    return MarketPriceSnapshot(
        platform=platform,
        item_name=base_item_name,
        exterior=exterior,
        lowest_price=min(prices),
        recent_average_price=sum(prices) / len(prices),
        sample_count=len(prices),
        fetched_at_epoch=float(resolved_epoch),
        fetched_at=str(resolved_at),
        source=source,
    )


class TransactionHistoryStore:
    tracked_goods_table = "tracked_goods"
    transaction_records_table = "transaction_records"
    price_snapshots_table = "market_price_snapshots"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def upsert_tracked_goods(self, goods: Iterable[TrackedGoods]) -> int:
        rows = [
            (
                entry.item_name,
                entry.buff_goods_id,
                entry.c5_goods_id,
                entry.igxe_goods_id,
            )
            for entry in goods
        ]
        if not rows:
            return 0
        with closing(sqlite3.connect(self.path)) as connection:
            connection.executemany(
                f'''
                INSERT INTO "{self.tracked_goods_table}" (
                    item_name, buff_goods_id, c5_goods_id, igxe_goods_id
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(item_name) DO UPDATE SET
                    buff_goods_id=excluded.buff_goods_id,
                    c5_goods_id=excluded.c5_goods_id,
                    igxe_goods_id=excluded.igxe_goods_id
                ''',
                rows,
            )
            connection.commit()
        return len(rows)

    def list_tracked_goods(self) -> tuple[TrackedGoods, ...]:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                f'''
                SELECT item_name, buff_goods_id, c5_goods_id, igxe_goods_id
                FROM "{self.tracked_goods_table}"
                ORDER BY item_name
                '''
            ).fetchall()
        return tuple(
            TrackedGoods(
                item_name=row["item_name"],
                buff_goods_id=row["buff_goods_id"],
                c5_goods_id=row["c5_goods_id"],
                igxe_goods_id=row["igxe_goods_id"],
            )
            for row in rows
        )

    def get_tracked_goods(
        self,
        item_name: str,
        exterior: Exterior | str | None = None,
    ) -> TrackedGoods | None:
        market_name = build_market_name(item_name, exterior)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                f'''
                SELECT item_name, buff_goods_id, c5_goods_id, igxe_goods_id
                FROM "{self.tracked_goods_table}"
                WHERE item_name = ?
                LIMIT 1
                ''',
                (market_name,),
            ).fetchone()
        if row is None:
            return None
        return TrackedGoods(
            item_name=row["item_name"],
            buff_goods_id=row["buff_goods_id"],
            c5_goods_id=row["c5_goods_id"],
            igxe_goods_id=row["igxe_goods_id"],
        )

    def insert_records(self, records: Iterable[TransactionRecord]) -> int:
        rows = [record.to_db_row() for record in records]
        if not rows:
            return 0
        with closing(sqlite3.connect(self.path)) as connection:
            cursor = connection.executemany(
                f'''
                INSERT OR IGNORE INTO "{self.transaction_records_table}" (
                    platform,
                    goods_id,
                    goods_name,
                    price,
                    paintwear,
                    stickers,
                    sticker_is_influence,
                    transact_time_epoch,
                    transact_time,
                    external_record_id,
                    source_url,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                rows,
            )
            connection.commit()
            return cursor.rowcount if cursor.rowcount != -1 else 0

    def count_records(self) -> int:
        with closing(sqlite3.connect(self.path)) as connection:
            row = connection.execute(
                f'SELECT COUNT(*) FROM "{self.transaction_records_table}"'
            ).fetchone()
        return int(row[0])

    def insert_price_snapshots(
        self,
        snapshots: Iterable[MarketPriceSnapshot],
    ) -> int:
        rows = [snapshot.to_db_row() for snapshot in snapshots]
        if not rows:
            return 0
        with closing(sqlite3.connect(self.path)) as connection:
            cursor = connection.executemany(
                f'''
                INSERT INTO "{self.price_snapshots_table}" (
                    platform,
                    item_name,
                    exterior,
                    lowest_price,
                    recent_average_price,
                    sample_count,
                    fetched_at_epoch,
                    fetched_at,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                rows,
            )
            connection.commit()
            return cursor.rowcount if cursor.rowcount != -1 else 0

    def insert_price_snapshot(self, snapshot: MarketPriceSnapshot) -> MarketPriceSnapshot:
        self.insert_price_snapshots([snapshot])
        return snapshot

    def count_price_snapshots(self) -> int:
        with closing(sqlite3.connect(self.path)) as connection:
            row = connection.execute(
                f'SELECT COUNT(*) FROM "{self.price_snapshots_table}"'
            ).fetchone()
        return int(row[0])

    def get_latest_price_snapshot(
        self,
        item_name: str,
        exterior: Exterior | str | None = None,
        *,
        platforms: Sequence[str] | None = None,
        max_age_seconds: float | None = None,
    ) -> MarketPriceSnapshot | None:
        normalized_item_name = item_name.strip()
        normalized_exterior = (
            None if exterior is None else normalize_exterior_label(exterior)
        )
        params: list[Any] = [normalized_item_name]
        exterior_sql = ""
        if normalized_exterior is None:
            exterior_sql = " AND exterior IS NULL"
        else:
            exterior_sql = " AND exterior = ?"
            params.append(normalized_exterior)

        platform_sql = ""
        if platforms:
            placeholders = ", ".join("?" for _ in platforms)
            platform_sql = f" AND platform IN ({placeholders})"
            params.extend(platforms)

        age_sql = ""
        if max_age_seconds is not None:
            age_sql = " AND fetched_at_epoch >= ?"
            params.append(time.time() - max_age_seconds)

        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                f'''
                SELECT *
                FROM "{self.price_snapshots_table}"
                WHERE item_name = ?
                  {exterior_sql}
                  {platform_sql}
                  {age_sql}
                ORDER BY fetched_at_epoch DESC, id DESC
                LIMIT 1
                ''',
                tuple(params),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_snapshot(row)

    def get_recent_price_quote(
        self,
        item_name: str,
        exterior: Exterior | str | None = None,
        *,
        platforms: Sequence[str] | None = None,
        lookback_days: float = 7.0,
    ) -> PriceQuote | None:
        market_name = build_market_name(item_name, exterior)
        lookback_epoch = time.time() - lookback_days * 86400
        params: list[Any] = [market_name, lookback_epoch]
        platform_sql = ""
        if platforms:
            placeholders = ", ".join("?" for _ in platforms)
            platform_sql = f" AND platform IN ({placeholders})"
            params.extend(platforms)

        with closing(sqlite3.connect(self.path)) as connection:
            row = connection.execute(
                f'''
                SELECT MIN(price) AS lowest_price, AVG(price) AS average_price
                FROM "{self.transaction_records_table}"
                WHERE goods_name = ?
                  AND transact_time_epoch >= ?
                  {platform_sql}
                ''',
                tuple(params),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return PriceQuote(lowest_price=float(row[0]), recent_average_price=float(row[1]))

    def get_recent_records(
        self,
        item_name: str,
        exterior: Exterior | str | None = None,
        *,
        platforms: Sequence[str] | None = None,
        limit: int = 20,
    ) -> tuple[TransactionRecord, ...]:
        market_name = build_market_name(item_name, exterior)
        params: list[Any] = [market_name]
        platform_sql = ""
        if platforms:
            placeholders = ", ".join("?" for _ in platforms)
            platform_sql = f" AND platform IN ({placeholders})"
            params.extend(platforms)
        params.append(limit)

        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                f'''
                SELECT *
                FROM "{self.transaction_records_table}"
                WHERE goods_name = ?
                  {platform_sql}
                ORDER BY transact_time_epoch DESC, id DESC
                LIMIT ?
                ''',
                tuple(params),
            ).fetchall()
        return tuple(self._row_to_record(row) for row in rows)

    def _ensure_schema(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.tracked_goods_table}" (
                    item_name TEXT PRIMARY KEY,
                    buff_goods_id INTEGER,
                    c5_goods_id INTEGER,
                    igxe_goods_id INTEGER
                )
                '''
            )
            connection.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.transaction_records_table}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    goods_id INTEGER,
                    goods_name TEXT NOT NULL,
                    price REAL NOT NULL,
                    paintwear TEXT,
                    stickers TEXT,
                    sticker_is_influence INTEGER NOT NULL DEFAULT 0,
                    transact_time_epoch REAL NOT NULL,
                    transact_time TEXT NOT NULL,
                    external_record_id TEXT,
                    source_url TEXT,
                    raw_json TEXT
                )
                '''
            )
            connection.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.price_snapshots_table}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    exterior TEXT,
                    lowest_price REAL NOT NULL,
                    recent_average_price REAL,
                    sample_count INTEGER NOT NULL,
                    fetched_at_epoch REAL NOT NULL,
                    fetched_at TEXT NOT NULL,
                    source TEXT NOT NULL
                )
                '''
            )
            connection.execute(
                f'''
                CREATE UNIQUE INDEX IF NOT EXISTS "idx_{self.transaction_records_table}_unique"
                ON "{self.transaction_records_table}" (
                    platform,
                    goods_id,
                    price,
                    transact_time_epoch,
                    COALESCE(paintwear, ''),
                    COALESCE(external_record_id, '')
                )
                '''
            )
            connection.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "idx_{self.transaction_records_table}_name_time"
                ON "{self.transaction_records_table}" (goods_name, transact_time_epoch DESC)
                '''
            )
            connection.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "idx_{self.price_snapshots_table}_lookup"
                ON "{self.price_snapshots_table}" (
                    item_name,
                    exterior,
                    platform,
                    fetched_at_epoch DESC
                )
                '''
            )
            connection.commit()

    def _row_to_record(self, row: sqlite3.Row) -> TransactionRecord:
        stickers: tuple[str, ...] = ()
        if row["stickers"]:
            stickers = tuple(json.loads(row["stickers"]))
        return TransactionRecord(
            platform=row["platform"],
            goods_id=row["goods_id"],
            goods_name=row["goods_name"],
            price=float(row["price"]),
            transact_time_epoch=float(row["transact_time_epoch"]),
            transact_time=row["transact_time"],
            paintwear=row["paintwear"],
            stickers=stickers,
            sticker_is_influence=int(row["sticker_is_influence"]),
            external_record_id=row["external_record_id"],
            source_url=row["source_url"],
            raw_json=row["raw_json"],
        )

    def _row_to_snapshot(self, row: sqlite3.Row) -> MarketPriceSnapshot:
        return MarketPriceSnapshot(
            platform=row["platform"],
            item_name=row["item_name"],
            exterior=row["exterior"],
            lowest_price=float(row["lowest_price"]),
            recent_average_price=(
                float(row["recent_average_price"])
                if row["recent_average_price"] is not None
                else None
            ),
            sample_count=int(row["sample_count"]),
            fetched_at_epoch=float(row["fetched_at_epoch"]),
            fetched_at=row["fetched_at"],
            source=row["source"],
        )


class TransactionHistoryPriceAPI:
    def __init__(
        self,
        store: TransactionHistoryStore,
        *,
        platforms: Sequence[str] | None = None,
        lookback_days: float = 7.0,
        market_name: str = "HistoryAverage",
    ) -> None:
        self.store = store
        self.platforms = tuple(platforms or ())
        self.lookback_days = lookback_days
        self.market_name = market_name

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        quote = self.store.get_recent_price_quote(
            item_name=item_name,
            exterior=exterior,
            platforms=self.platforms or None,
            lookback_days=self.lookback_days,
        )
        if quote is None:
            raise MarketParseError(
                f"No historical records for {build_market_name(item_name, exterior)}"
            )
        return quote


class TransactionHistorySnapshotPriceAPI:
    def __init__(
        self,
        store: TransactionHistoryStore,
        *,
        platforms: Sequence[str] | None = None,
        max_age_seconds: float | None = None,
        market_name: str = "HistorySnapshot",
    ) -> None:
        self.store = store
        self.platforms = tuple(platforms or ())
        self.max_age_seconds = max_age_seconds
        self.market_name = market_name

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        snapshot = self.store.get_latest_price_snapshot(
            item_name=item_name,
            exterior=exterior,
            platforms=self.platforms or None,
            max_age_seconds=self.max_age_seconds,
        )
        if snapshot is None:
            raise MarketParseError(
                f"No cached snapshot for {build_market_name(item_name, exterior)}"
            )
        return snapshot.quote


class IGXECachedPriceAPI:
    def __init__(
        self,
        store: TransactionHistoryStore,
        *,
        crawler: "IGXETransactionHistoryCrawler | None" = None,
        max_age_seconds: float | None = None,
        write_back_on_fetch: bool = True,
        allow_stale_snapshot: bool = True,
        market_name: str = "IGXE",
    ) -> None:
        self.store = store
        self.crawler = crawler or IGXETransactionHistoryCrawler()
        self.max_age_seconds = max_age_seconds
        self.write_back_on_fetch = write_back_on_fetch
        self.allow_stale_snapshot = allow_stale_snapshot
        self.market_name = market_name

    def get_item_price(self, item_name: str, exterior: Exterior | str) -> PriceQuote:
        cached_snapshot = self.store.get_latest_price_snapshot(
            item_name=item_name,
            exterior=exterior,
            platforms=["IGXE"],
            max_age_seconds=self.max_age_seconds,
        )
        if cached_snapshot is not None:
            return cached_snapshot.quote

        stale_snapshot = None
        if self.allow_stale_snapshot:
            stale_snapshot = self.store.get_latest_price_snapshot(
                item_name=item_name,
                exterior=exterior,
                platforms=["IGXE"],
                max_age_seconds=None,
            )

        tracked_goods = self.store.get_tracked_goods(item_name, exterior)
        if tracked_goods is None or not tracked_goods.igxe_goods_id:
            if stale_snapshot is not None:
                return stale_snapshot.quote
            raise MarketParseError(
                f"Tracked IGXE goods_id not found for {build_market_name(item_name, exterior)}"
            )

        records = self.crawler.fetch_transaction_history(
            goods_id=int(tracked_goods.igxe_goods_id),
            goods_name=tracked_goods.item_name,
        )
        if not records:
            if stale_snapshot is not None:
                return stale_snapshot.quote
            raise MarketParseError(
                f"IGXE returned no historical sales for {tracked_goods.item_name}"
            )

        self.store.insert_records(records)
        snapshot = build_price_snapshot("IGXE", records, source="igxe_history")
        if self.write_back_on_fetch:
            self.store.insert_price_snapshot(snapshot)
        return snapshot.quote


class BaseTransactionHistoryCrawler:
    market_name = "History"
    default_base_url = ""

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
    ) -> None:
        self.base_url = (base_url or self.default_base_url).rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = rate_limiter or RandomizedRateLimiter()
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_REQUEST_HEADERS)
        if headers:
            self.session.headers.update(dict(headers))
        if cookies:
            self.session.cookies.update(dict(cookies))
        if cookie_string:
            self.session.cookies.update(parse_cookie_string(cookie_string))
        if proxies:
            self.session.proxies.update(dict(proxies))

    def _request_json(
        self,
        method: str,
        endpoint_or_url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        self.rate_limiter.acquire()
        url = (
            endpoint_or_url
            if endpoint_or_url.startswith("http")
            else f"{self.base_url}{endpoint_or_url}"
        )
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

    def _request_text(
        self,
        method: str,
        endpoint_or_url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        self.rate_limiter.acquire()
        url = (
            endpoint_or_url
            if endpoint_or_url.startswith("http")
            else f"{self.base_url}{endpoint_or_url}"
        )
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=dict(params or {}),
                headers=dict(headers or {}),
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as error:
            raise MarketRequestError(f"{self.market_name} request failed: {error}") from error


class BuffTransactionHistoryCrawler(BaseTransactionHistoryCrawler):
    market_name = "BUFFHistory"
    default_base_url = "https://buff.163.com"

    def fetch_transaction_history(
        self,
        goods_id: int,
        goods_name: str,
    ) -> tuple[TransactionRecord, ...]:
        payload = self._request_json(
            "GET",
            "/api/market/goods/bill_order",
            params={
                "game": "csgo",
                "goods_id": goods_id,
                "_": int(time.time() * 1000),
            },
        )
        items = payload.get("data", {}).get("items", [])
        if not isinstance(items, list):
            raise MarketParseError("BUFF history payload is missing data.items")

        records: list[TransactionRecord] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            price = _parse_price(item.get("price"))
            transact_time_value = item.get("transact_time")
            if price is None or transact_time_value is None:
                continue
            transact_time_epoch = float(transact_time_value)
            asset_info = item.get("asset_info") or {}
            stickers = tuple(
                f"{sticker.get('name')}|{sticker.get('wear')}"
                for sticker in (asset_info.get("info") or {}).get("stickers", [])
                if isinstance(sticker, Mapping) and sticker.get("name")
            )
            records.append(
                TransactionRecord(
                    platform="BUFF",
                    goods_id=int(item.get("goods_id", goods_id)),
                    goods_name=goods_name,
                    price=price,
                    paintwear=asset_info.get("paintwear"),
                    stickers=stickers,
                    transact_time_epoch=transact_time_epoch,
                    transact_time=_epoch_to_iso(transact_time_epoch),
                    source_url=(
                        f"{self.base_url}/api/market/goods/bill_order?game=csgo&goods_id={goods_id}"
                    ),
                    raw_json=json.dumps(dict(item), ensure_ascii=False),
                )
            )
        return tuple(records)


class C5TransactionHistoryCrawler(BaseTransactionHistoryCrawler):
    market_name = "C5History"
    default_base_url = "https://www.c5game.com"

    def fetch_transaction_history(
        self,
        goods_id: int,
        goods_name: str,
        *,
        include_detail: bool = True,
    ) -> tuple[TransactionRecord, ...]:
        payload = self._request_json(
            "GET",
            "/gw/steamtrade/sga/store/v2/recent-deal",
            params={
                "itemId": goods_id,
                "reqId": f"{int(time.time() * 1000)}5864132031",
            },
        )
        if payload is None:
            return ()
        if payload.get("errorMsg") == "请登录":
            raise MarketRequestError("C5 cookie expired or login required")
        items = payload.get("data", [])
        if not isinstance(items, list):
            raise MarketParseError("C5 history payload is missing data")

        records: list[TransactionRecord] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            price = _parse_price(item.get("price"))
            update_time = item.get("updateTime")
            if price is None or update_time is None:
                continue
            transact_time_epoch = float(update_time)
            paintwear = None
            stickers: tuple[str, ...] = ()
            product_id = item.get("productId")
            detail_url = None
            if include_detail and product_id:
                detail_url = f"{self.base_url}/steam/item/detail.html?id={product_id}"
                detail_html = self._request_text("GET", detail_url)
                paintwear, stickers = _parse_c5_detail_fields(detail_html)

            records.append(
                TransactionRecord(
                    platform="C5",
                    goods_id=int(item.get("itemId", goods_id)),
                    goods_name=goods_name,
                    price=price,
                    paintwear=paintwear,
                    stickers=stickers,
                    transact_time_epoch=transact_time_epoch,
                    transact_time=_epoch_to_iso(transact_time_epoch),
                    source_url=detail_url,
                    raw_json=json.dumps(dict(item), ensure_ascii=False),
                )
            )
        return tuple(records)


class IGXETransactionHistoryCrawler(BaseTransactionHistoryCrawler):
    market_name = "IGXEHistory"
    default_base_url = "https://www.igxe.cn"

    def fetch_transaction_history(
        self,
        goods_id: int,
        goods_name: str,
    ) -> tuple[TransactionRecord, ...]:
        payload = self._request_json(
            "GET",
            f"/product/get_product_sales_history/730/{goods_id}",
        )
        items = payload.get("data", [])
        if not isinstance(items, list):
            raise MarketParseError("IGXE history payload is missing data")

        records: list[TransactionRecord] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            price = _parse_price(item.get("unit_price"))
            last_updated = item.get("last_updated")
            if price is None or not last_updated:
                continue
            transact_time_epoch, transact_time = _parse_igxe_datetime(str(last_updated))
            stickers = tuple(
                f"{sticker.get('sticker_title')}|{sticker.get('wear')}"
                for sticker in item.get("sticker", [])
                if isinstance(sticker, Mapping) and sticker.get("sticker_title")
            )
            external_id = item.get("id")
            records.append(
                TransactionRecord(
                    platform="IGXE",
                    goods_id=int(item.get("product_id", goods_id)),
                    goods_name=goods_name,
                    price=price,
                    paintwear=item.get("exterior_wear"),
                    stickers=stickers,
                    transact_time_epoch=transact_time_epoch,
                    transact_time=transact_time,
                    external_record_id=str(external_id) if external_id is not None else None,
                    source_url=f"{self.base_url}/product/get_product_sales_history/730/{goods_id}",
                    raw_json=json.dumps(dict(item), ensure_ascii=False),
                )
            )
        return tuple(records)


class TransactionHistorySyncService:
    def __init__(
        self,
        store: TransactionHistoryStore,
        *,
        buff_crawler: BuffTransactionHistoryCrawler | None = None,
        c5_crawler: C5TransactionHistoryCrawler | None = None,
        igxe_crawler: IGXETransactionHistoryCrawler | None = None,
    ) -> None:
        self.store = store
        self.buff_crawler = buff_crawler
        self.c5_crawler = c5_crawler
        self.igxe_crawler = igxe_crawler

    def sync_tracked_goods(
        self,
        tracked_goods: Iterable[TrackedGoods] | None = None,
        *,
        platforms: Sequence[str] = ("BUFF", "C5", "IGXE"),
        include_c5_detail: bool = True,
        write_price_snapshots: bool = True,
    ) -> dict[str, int]:
        goods_list = tuple(tracked_goods or self.store.list_tracked_goods())
        counters = {
            "tracked_goods": len(goods_list),
            "records_inserted": 0,
            "snapshots_inserted": 0,
        }
        normalized_platforms = {platform.upper() for platform in platforms}

        for goods in goods_list:
            if "BUFF" in normalized_platforms and goods.buff_goods_id and self.buff_crawler:
                records = self.buff_crawler.fetch_transaction_history(
                    goods_id=goods.buff_goods_id,
                    goods_name=goods.item_name,
                )
                counters["records_inserted"] += self.store.insert_records(records)
                if write_price_snapshots:
                    counters["snapshots_inserted"] += self._write_price_snapshot("BUFF", records)

            if "C5" in normalized_platforms and goods.c5_goods_id and self.c5_crawler:
                records = self.c5_crawler.fetch_transaction_history(
                    goods_id=goods.c5_goods_id,
                    goods_name=goods.item_name,
                    include_detail=include_c5_detail,
                )
                counters["records_inserted"] += self.store.insert_records(records)
                if write_price_snapshots:
                    counters["snapshots_inserted"] += self._write_price_snapshot("C5", records)

            if "IGXE" in normalized_platforms and goods.igxe_goods_id and self.igxe_crawler:
                records = self.igxe_crawler.fetch_transaction_history(
                    goods_id=goods.igxe_goods_id,
                    goods_name=goods.item_name,
                )
                counters["records_inserted"] += self.store.insert_records(records)
                if write_price_snapshots:
                    counters["snapshots_inserted"] += self._write_price_snapshot("IGXE", records)

        return counters

    def _write_price_snapshot(
        self,
        platform: str,
        records: Sequence[TransactionRecord],
    ) -> int:
        if not records:
            return 0
        snapshot = self._build_snapshot(platform, records)
        self.store.insert_price_snapshot(snapshot)
        return 1

    def _build_snapshot(
        self,
        platform: str,
        records: Sequence[TransactionRecord],
    ) -> MarketPriceSnapshot:
        return build_price_snapshot(platform, records, source="history_sync")
