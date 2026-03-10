from __future__ import annotations

import multiprocessing
import random
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

from .catalog import ItemCatalog
from .market import RandomizedRateLimiter
from .steamdt_market import (
    CachedSteamDTMarketAPI,
    PlaywrightSteamDTTransport,
    SteamDTMarketAPI,
    SteamDTPriceSnapshotStore,
)

STEAMDT_QUOTA_ERROR_MARKERS: tuple[str, ...] = (
    "今日访问次数超限",
    "访问次数超限",
    "请明日再试",
)

DEFAULT_STEAMDT_WORKER_USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.6834.160 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
)
DEFAULT_STEAMDT_WORKER_VIEWPORTS: tuple[tuple[int, int], ...] = (
    (1600, 1200),
    (1536, 960),
    (1728, 1117),
)


@dataclass(frozen=True, slots=True)
class CatalogPriceCrawlSummary:
    total_items: int
    processed_items: int
    skipped_recent_items: int
    failed_items: tuple[str, ...]
    snapshots_before: int
    snapshots_after: int
    snapshots_inserted: int


@dataclass(frozen=True, slots=True)
class SteamDTCrawlWorkerProfile:
    worker_id: int
    user_agent: str | None = None
    locale: str | None = None
    timezone_id: str | None = None
    proxy_server: str | None = None
    proxy_username: str | None = None
    proxy_password: str | None = None
    extra_http_headers: Mapping[str, str] | None = None
    viewport_width: int = 1600
    viewport_height: int = 1200
    rate_limit_min_seconds: float = 1.0
    rate_limit_max_seconds: float = 2.0
    headless: bool = True
    timeout_ms: int = 120_000
    warmup_wait_ms: int = 8_000


@dataclass(frozen=True, slots=True)
class CatalogPriceCrawlWorkerSummary:
    worker_id: int
    assigned_items: int
    processed_items: int
    skipped_recent_items: int
    failed_items: tuple[str, ...]
    snapshots_inserted: int
    log_path: str | None = None


@dataclass(frozen=True, slots=True)
class MultiWorkerCatalogPriceCrawlSummary:
    total_items: int
    worker_count: int
    processed_items: int
    skipped_recent_items: int
    failed_items: tuple[str, ...]
    snapshots_before: int
    snapshots_after: int
    snapshots_inserted: int
    worker_summaries: tuple[CatalogPriceCrawlWorkerSummary, ...]


@dataclass(frozen=True, slots=True)
class CatalogPriceCrawlWorkerTask:
    worker_id: int
    catalog_path: str
    snapshot_store_path: str
    item_names: tuple[str, ...]
    include_normal: bool
    include_stattrak: bool
    sleep_min_seconds: float
    sleep_max_seconds: float
    batch_size: int
    batch_cooldown_min_seconds: float
    batch_cooldown_max_seconds: float
    retry_attempts: int
    failure_backoff_base_seconds: float
    skip_recent_seconds: float | None
    worker_profile: SteamDTCrawlWorkerProfile
    log_path: str | None = None


def build_steamdt_crawl_worker_profiles(
    worker_count: int,
    *,
    user_agents: Sequence[str] | None = None,
    proxy_servers: Sequence[str] | None = None,
    locales: Sequence[str] | None = None,
    timezone_ids: Sequence[str] | None = None,
    proxy_credentials: Sequence[tuple[str | None, str | None]] | None = None,
    headless: bool = True,
    rate_limit_min_seconds: float = 1.0,
    rate_limit_max_seconds: float = 2.0,
    rate_limit_step_seconds: float = 0.35,
) -> tuple[SteamDTCrawlWorkerProfile, ...]:
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")

    resolved_user_agents = tuple(user_agents or DEFAULT_STEAMDT_WORKER_USER_AGENTS)
    resolved_proxy_servers = tuple(proxy_servers or ())
    resolved_locales = tuple(locales or ("zh-CN", "en-US", "zh-TW"))
    resolved_timezones = tuple(timezone_ids or ())
    resolved_proxy_credentials = tuple(proxy_credentials or ())

    profiles: list[SteamDTCrawlWorkerProfile] = []
    for index in range(worker_count):
        width, height = DEFAULT_STEAMDT_WORKER_VIEWPORTS[index % len(DEFAULT_STEAMDT_WORKER_VIEWPORTS)]
        locale = resolved_locales[index % len(resolved_locales)] if resolved_locales else None
        headers = {"Accept-Language": f"{locale},en;q=0.8"} if locale else {}
        proxy_server = (
            resolved_proxy_servers[index % len(resolved_proxy_servers)]
            if resolved_proxy_servers
            else None
        )
        proxy_username: str | None = None
        proxy_password: str | None = None
        if resolved_proxy_credentials:
            proxy_username, proxy_password = resolved_proxy_credentials[
                index % len(resolved_proxy_credentials)
            ]
        profiles.append(
            SteamDTCrawlWorkerProfile(
                worker_id=index + 1,
                user_agent=resolved_user_agents[index % len(resolved_user_agents)],
                locale=locale,
                timezone_id=(
                    resolved_timezones[index % len(resolved_timezones)]
                    if resolved_timezones
                    else None
                ),
                proxy_server=proxy_server,
                proxy_username=proxy_username,
                proxy_password=proxy_password,
                extra_http_headers=headers,
                viewport_width=width,
                viewport_height=height,
                rate_limit_min_seconds=rate_limit_min_seconds + rate_limit_step_seconds * index,
                rate_limit_max_seconds=rate_limit_max_seconds + rate_limit_step_seconds * index,
                headless=headless,
            )
        )
    return tuple(profiles)


def partition_catalog_item_names(
    item_names: Sequence[str],
    worker_count: int,
) -> tuple[tuple[str, ...], ...]:
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    shards: list[list[str]] = [[] for _ in range(worker_count)]
    for index, item_name in enumerate(item_names):
        shards[index % worker_count].append(item_name)
    return tuple(tuple(shard) for shard in shards if shard)


def crawl_catalog_item_prices_to_sqlite(
    *,
    catalog: ItemCatalog | str | Path = Path("data") / "items.sqlite",
    snapshot_store: SteamDTPriceSnapshotStore | str | Path = Path("data") / "steamdt_prices.sqlite",
    steamdt_client: SteamDTMarketAPI | None = None,
    item_names: Iterable[str] | None = None,
    max_items: int | None = None,
    include_normal: bool = True,
    include_stattrak: bool = True,
    sleep_min_seconds: float = 0.8,
    sleep_max_seconds: float = 1.8,
    batch_size: int = 20,
    batch_cooldown_min_seconds: float = 6.0,
    batch_cooldown_max_seconds: float = 12.0,
    retry_attempts: int = 2,
    failure_backoff_base_seconds: float = 8.0,
    skip_recent_seconds: float | None = 6 * 3600,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> CatalogPriceCrawlSummary:
    resolved_catalog = catalog if isinstance(catalog, ItemCatalog) else ItemCatalog.from_path(catalog)
    resolved_store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    resolved_item_names = tuple(
        dict.fromkeys(
            name.strip()
            for name in (
                item_names
                if item_names is not None
                else (item.name for item in resolved_catalog.all_items())
            )
            if str(name).strip()
        )
    )
    if max_items is not None:
        resolved_item_names = resolved_item_names[:max_items]

    owned_client = steamdt_client is None
    live_client = steamdt_client or SteamDTMarketAPI()
    cached_client = CachedSteamDTMarketAPI(
        resolved_store,
        steamdt_client=live_client,
        max_age_seconds=None,
        write_back_on_fetch=True,
        allow_live_fetch=True,
        refresh_cleaned_after_write=False,
    )
    snapshots_before = resolved_store.count_snapshots()
    failed_items: list[str] = []
    processed_items = 0
    skipped_recent_items = 0
    fetched_since_cooldown = 0
    snapshots_inserted = 0

    try:
        total_items = len(resolved_item_names)
        for index, item_name in enumerate(resolved_item_names, start=1):
            if _should_skip_recent_item_family(
                resolved_catalog,
                resolved_store,
                item_name,
                include_normal=include_normal,
                include_stattrak=include_stattrak,
                skip_recent_seconds=skip_recent_seconds,
            ):
                skipped_recent_items += 1
                if progress_callback is not None:
                    progress_callback(index, total_items, f"跳过新鲜缓存：{item_name}")
                continue

            if progress_callback is not None:
                progress_callback(index, total_items, f"抓取价格：{item_name}")

            success = False
            last_error: Exception | None = None
            for attempt in range(retry_attempts + 1):
                try:
                    snapshots = cached_client.warm_item_family_cache(
                        item_name,
                        include_normal=include_normal,
                        include_stattrak=include_stattrak,
                    )
                    processed_items += 1
                    fetched_since_cooldown += 1
                    snapshots_inserted += len(snapshots)
                    success = True
                    break
                except Exception as error:
                    last_error = error
                    if _is_steamdt_quota_error(error):
                        if progress_callback is not None:
                            progress_callback(index, total_items, f"SteamDT 访问超限，停止本轮抓取：{error}")
                        raise
                    if attempt >= retry_attempts:
                        break
                    backoff_seconds = random.uniform(
                        max(0.0, failure_backoff_base_seconds * (attempt + 1)),
                        max(
                            failure_backoff_base_seconds * (attempt + 1),
                            failure_backoff_base_seconds * (attempt + 1) * 1.8,
                        ),
                    )
                    if progress_callback is not None:
                        progress_callback(
                            index,
                            total_items,
                            f"重试第 {attempt + 1} 次：{item_name} ({type(error).__name__}: {error})",
                        )
                    time.sleep(backoff_seconds)

            if not success:
                failed_items.append(item_name)
                if progress_callback is not None and last_error is not None:
                    progress_callback(
                        index,
                        total_items,
                        f"抓取失败：{item_name} ({type(last_error).__name__}: {last_error})",
                    )

            if index < total_items and sleep_max_seconds > 0 and success:
                time.sleep(random.uniform(max(0.0, sleep_min_seconds), max(sleep_min_seconds, sleep_max_seconds)))
            if (
                batch_size > 0
                and fetched_since_cooldown >= batch_size
                and index < total_items
                and batch_cooldown_max_seconds > 0
            ):
                cooldown_seconds = random.uniform(
                    max(0.0, batch_cooldown_min_seconds),
                    max(batch_cooldown_min_seconds, batch_cooldown_max_seconds),
                )
                if progress_callback is not None:
                    progress_callback(index, total_items, f"批次冷却 {cooldown_seconds:.1f}s")
                time.sleep(cooldown_seconds)
                fetched_since_cooldown = 0
    finally:
        cached_client.close()
        if not owned_client and steamdt_client is not None:
            pass

    snapshots_after = resolved_store.count_snapshots()
    resolved_store.refresh_cleaned_prices()
    return CatalogPriceCrawlSummary(
        total_items=len(resolved_item_names),
        processed_items=processed_items,
        skipped_recent_items=skipped_recent_items,
        failed_items=tuple(failed_items),
        snapshots_before=snapshots_before,
        snapshots_after=snapshots_after,
        snapshots_inserted=snapshots_inserted,
    )


def crawl_catalog_item_prices_multiworker_to_sqlite(
    *,
    catalog: ItemCatalog | str | Path = Path("data") / "items.sqlite",
    snapshot_store: SteamDTPriceSnapshotStore | str | Path = Path("data") / "steamdt_prices.sqlite",
    item_names: Iterable[str] | None = None,
    max_items: int | None = None,
    include_normal: bool = True,
    include_stattrak: bool = True,
    sleep_min_seconds: float = 0.8,
    sleep_max_seconds: float = 1.8,
    batch_size: int = 20,
    batch_cooldown_min_seconds: float = 6.0,
    batch_cooldown_max_seconds: float = 12.0,
    retry_attempts: int = 2,
    failure_backoff_base_seconds: float = 8.0,
    skip_recent_seconds: float | None = 6 * 3600,
    worker_count: int = 2,
    worker_profiles: Sequence[SteamDTCrawlWorkerProfile] | None = None,
    worker_log_dir: str | Path | None = None,
    supervisor_restart_limit: int = 2,
    supervisor_backoff_base_seconds: float = 15.0,
    progress_callback: Callable[[int, int, str], None] | None = None,
    _worker_runner: Callable[[CatalogPriceCrawlWorkerTask], CatalogPriceCrawlWorkerSummary] | None = None,
    _supervised_batch_runner: (
        Callable[[Sequence[CatalogPriceCrawlWorkerTask]], tuple[CatalogPriceCrawlWorkerSummary, ...]] | None
    ) = None,
) -> MultiWorkerCatalogPriceCrawlSummary:
    resolved_catalog = catalog if isinstance(catalog, ItemCatalog) else ItemCatalog.from_path(catalog)
    resolved_store = (
        snapshot_store
        if isinstance(snapshot_store, SteamDTPriceSnapshotStore)
        else SteamDTPriceSnapshotStore(snapshot_store)
    )
    resolved_item_names = tuple(
        dict.fromkeys(
            name.strip()
            for name in (
                item_names
                if item_names is not None
                else (item.name for item in resolved_catalog.all_items())
            )
            if str(name).strip()
        )
    )
    if max_items is not None:
        resolved_item_names = resolved_item_names[:max_items]
    if worker_count <= 1:
        single_summary = crawl_catalog_item_prices_to_sqlite(
            catalog=resolved_catalog,
            snapshot_store=resolved_store,
            item_names=resolved_item_names,
            include_normal=include_normal,
            include_stattrak=include_stattrak,
            sleep_min_seconds=sleep_min_seconds,
            sleep_max_seconds=sleep_max_seconds,
            batch_size=batch_size,
            batch_cooldown_min_seconds=batch_cooldown_min_seconds,
            batch_cooldown_max_seconds=batch_cooldown_max_seconds,
            retry_attempts=retry_attempts,
            failure_backoff_base_seconds=failure_backoff_base_seconds,
            skip_recent_seconds=skip_recent_seconds,
            progress_callback=progress_callback,
        )
        worker_summary = CatalogPriceCrawlWorkerSummary(
            worker_id=1,
            assigned_items=single_summary.total_items,
            processed_items=single_summary.processed_items,
            skipped_recent_items=single_summary.skipped_recent_items,
            failed_items=single_summary.failed_items,
            snapshots_inserted=single_summary.snapshots_inserted,
        )
        return MultiWorkerCatalogPriceCrawlSummary(
            total_items=single_summary.total_items,
            worker_count=1,
            processed_items=single_summary.processed_items,
            skipped_recent_items=single_summary.skipped_recent_items,
            failed_items=single_summary.failed_items,
            snapshots_before=single_summary.snapshots_before,
            snapshots_after=single_summary.snapshots_after,
            snapshots_inserted=single_summary.snapshots_inserted,
            worker_summaries=(worker_summary,),
        )

    profiles = tuple(worker_profiles or build_steamdt_crawl_worker_profiles(worker_count))
    if len(profiles) < worker_count:
        raise ValueError("worker_profiles must provide at least worker_count profiles")

    shards = partition_catalog_item_names(resolved_item_names, worker_count)
    log_dir_path = Path(worker_log_dir) if worker_log_dir else None
    if log_dir_path is not None:
        log_dir_path.mkdir(parents=True, exist_ok=True)
    materialized_catalog_path: Path | None = None
    if isinstance(catalog, ItemCatalog):
        materialized_catalog_path = resolved_store.path.parent / ".multiworker_catalog.sqlite"
        resolved_catalog.to_sqlite(materialized_catalog_path)
    catalog_path_for_workers = Path(catalog) if not isinstance(catalog, ItemCatalog) else materialized_catalog_path
    assert catalog_path_for_workers is not None

    tasks = tuple(
        CatalogPriceCrawlWorkerTask(
            worker_id=index + 1,
            catalog_path=str(catalog_path_for_workers),
            snapshot_store_path=str(resolved_store.path),
            item_names=shard,
            include_normal=include_normal,
            include_stattrak=include_stattrak,
            sleep_min_seconds=sleep_min_seconds,
            sleep_max_seconds=sleep_max_seconds,
            batch_size=batch_size,
            batch_cooldown_min_seconds=batch_cooldown_min_seconds,
            batch_cooldown_max_seconds=batch_cooldown_max_seconds,
            retry_attempts=retry_attempts,
            failure_backoff_base_seconds=failure_backoff_base_seconds,
            skip_recent_seconds=skip_recent_seconds,
            worker_profile=profiles[index],
            log_path=(
                str(log_dir_path / f"steamdt_worker_{index + 1:02d}.log")
                if log_dir_path is not None
                else None
            ),
        )
        for index, shard in enumerate(shards)
    )

    snapshots_before = resolved_store.count_snapshots()
    summaries: list[CatalogPriceCrawlWorkerSummary] = []
    total_workers = len(tasks)
    if total_workers == 0:
        return MultiWorkerCatalogPriceCrawlSummary(
            total_items=0,
            worker_count=0,
            processed_items=0,
            skipped_recent_items=0,
            failed_items=(),
            snapshots_before=snapshots_before,
            snapshots_after=snapshots_before,
            snapshots_inserted=0,
            worker_summaries=(),
        )
    if progress_callback is not None:
        progress_callback(0, total_workers, f"已启动 {total_workers} 个独立浏览器抓取进程")

    try:
        if _worker_runner is not None:
            for completed_index, task in enumerate(tasks, start=1):
                summary = _worker_runner(task)
                summaries.append(summary)
                if progress_callback is not None:
                    progress_callback(
                        completed_index,
                        total_workers,
                        f"Worker {summary.worker_id} 完成：处理 {summary.processed_items}/{summary.assigned_items}",
                    )
        else:
            summaries.extend(
                _run_supervised_multiworker_batches(
                    tasks,
                    restart_limit=supervisor_restart_limit,
                    backoff_base_seconds=supervisor_backoff_base_seconds,
                    progress_callback=progress_callback,
                    batch_runner=_supervised_batch_runner,
                )
            )
    finally:
        if materialized_catalog_path is not None and materialized_catalog_path.exists():
            materialized_catalog_path.unlink()

    snapshots_after = resolved_store.count_snapshots()
    resolved_store.refresh_cleaned_prices()
    ordered_summaries = tuple(sorted(summaries, key=lambda row: row.worker_id))
    return MultiWorkerCatalogPriceCrawlSummary(
        total_items=len(resolved_item_names),
        worker_count=total_workers,
        processed_items=sum(summary.processed_items for summary in ordered_summaries),
        skipped_recent_items=sum(summary.skipped_recent_items for summary in ordered_summaries),
        failed_items=tuple(
            item_name
            for summary in ordered_summaries
            for item_name in summary.failed_items
        ),
        snapshots_before=snapshots_before,
        snapshots_after=snapshots_after,
        snapshots_inserted=sum(summary.snapshots_inserted for summary in ordered_summaries),
        worker_summaries=ordered_summaries,
    )


def _crawl_catalog_item_prices_worker_task(
    task: CatalogPriceCrawlWorkerTask,
) -> CatalogPriceCrawlWorkerSummary:
    if task.log_path:
        log_path = Path(task.log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("", encoding="utf-8")
    else:
        log_path = None

    def emit(message: str) -> None:
        if log_path is None:
            return
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(message + "\n")

    emit(f"START worker={task.worker_id} items={len(task.item_names)}")
    try:
        live_client = _build_worker_client(task.worker_profile)

        def progress(index: int, total: int, message: str) -> None:
            emit(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                f"[worker {task.worker_id}] [{index}/{total}] {message}"
            )

        try:
            summary = crawl_catalog_item_prices_to_sqlite(
                catalog=task.catalog_path,
                snapshot_store=task.snapshot_store_path,
                steamdt_client=live_client,
                item_names=task.item_names,
                include_normal=task.include_normal,
                include_stattrak=task.include_stattrak,
                sleep_min_seconds=task.sleep_min_seconds,
                sleep_max_seconds=task.sleep_max_seconds,
                batch_size=task.batch_size,
                batch_cooldown_min_seconds=task.batch_cooldown_min_seconds,
                batch_cooldown_max_seconds=task.batch_cooldown_max_seconds,
                retry_attempts=task.retry_attempts,
                failure_backoff_base_seconds=task.failure_backoff_base_seconds,
                skip_recent_seconds=task.skip_recent_seconds,
                progress_callback=progress,
            )
        finally:
            live_client.close()

        emit(
            f"SUMMARY worker={task.worker_id} processed={summary.processed_items} "
            f"skipped={summary.skipped_recent_items} failed={len(summary.failed_items)} "
            f"snapshots_inserted={summary.snapshots_inserted}"
        )
        return CatalogPriceCrawlWorkerSummary(
            worker_id=task.worker_id,
            assigned_items=len(task.item_names),
            processed_items=summary.processed_items,
            skipped_recent_items=summary.skipped_recent_items,
            failed_items=summary.failed_items,
            snapshots_inserted=summary.snapshots_inserted,
            log_path=task.log_path,
        )
    except Exception as error:
        emit(f"ERROR worker={task.worker_id} {error!r}")
        emit(traceback.format_exc())
        raise


def _run_supervised_multiworker_batches(
    tasks: Sequence[CatalogPriceCrawlWorkerTask],
    *,
    restart_limit: int,
    backoff_base_seconds: float,
    progress_callback: Callable[[int, int, str], None] | None = None,
    batch_runner: Callable[[Sequence[CatalogPriceCrawlWorkerTask]], tuple[CatalogPriceCrawlWorkerSummary, ...]] | None = None,
) -> tuple[CatalogPriceCrawlWorkerSummary, ...]:
    total_workers = len(tasks)
    completed_by_worker_id: dict[int, CatalogPriceCrawlWorkerSummary] = {}
    pending_tasks = tuple(tasks)
    round_index = 0

    while pending_tasks:
        round_index += 1
        completed_in_round: dict[int, CatalogPriceCrawlWorkerSummary] = {}
        round_failed = False
        round_error_text: str | None = None

        try:
            if batch_runner is not None:
                round_summaries = batch_runner(pending_tasks)
                for summary in round_summaries:
                    completed_in_round[summary.worker_id] = summary
                    completed_by_worker_id[summary.worker_id] = summary
                    if progress_callback is not None:
                        progress_callback(
                            len(completed_by_worker_id),
                            total_workers,
                            f"Worker {summary.worker_id} 完成：处理 {summary.processed_items}/{summary.assigned_items}",
                        )
            else:
                with ProcessPoolExecutor(
                    max_workers=len(pending_tasks),
                    mp_context=multiprocessing.get_context("spawn"),
                ) as executor:
                    future_map = {
                        executor.submit(_crawl_catalog_item_prices_worker_task, task): task
                        for task in pending_tasks
                    }
                    for future in as_completed(future_map):
                        task = future_map[future]
                        summary = future.result()
                        completed_in_round[task.worker_id] = summary
                        completed_by_worker_id[task.worker_id] = summary
                        if progress_callback is not None:
                            progress_callback(
                                len(completed_by_worker_id),
                                total_workers,
                                f"Worker {summary.worker_id} 完成：处理 {summary.processed_items}/{summary.assigned_items}",
                            )
        except BrokenProcessPool as error:
            round_failed = True
            round_error_text = repr(error)
        except Exception as error:
            if _is_steamdt_quota_error(error):
                if progress_callback is not None:
                    progress_callback(
                        len(completed_by_worker_id),
                        total_workers,
                        f"SteamDT 已触发日访问上限，停止本轮续跑：{error}",
                    )
                break
            round_failed = True
            round_error_text = repr(error)

        pending_tasks = tuple(
            task
            for task in pending_tasks
            if task.worker_id not in completed_in_round
        )
        if not round_failed:
            continue

        if round_index > restart_limit:
            if progress_callback is not None:
                progress_callback(
                    len(completed_by_worker_id),
                    total_workers,
                    f"Supervisor 超过重启上限，剩余 {len(pending_tasks)} 个分片未完成",
                )
            break

        backoff_seconds = max(0.0, backoff_base_seconds * round_index)
        if progress_callback is not None:
            progress_callback(
                len(completed_by_worker_id),
                total_workers,
                f"Supervisor 捕获异常，准备第 {round_index} 次续跑：{round_error_text}; {backoff_seconds:.1f}s 后重试",
            )
        if backoff_seconds > 0:
            time.sleep(backoff_seconds)

    for task in tasks:
        if task.worker_id in completed_by_worker_id:
            continue
        completed_by_worker_id[task.worker_id] = CatalogPriceCrawlWorkerSummary(
            worker_id=task.worker_id,
            assigned_items=len(task.item_names),
            processed_items=0,
            skipped_recent_items=0,
            failed_items=task.item_names,
            snapshots_inserted=0,
            log_path=task.log_path,
        )
    return tuple(sorted(completed_by_worker_id.values(), key=lambda row: row.worker_id))


def _is_steamdt_quota_error(error: BaseException) -> bool:
    text = str(error).strip()
    if not text:
        return False
    return any(marker in text for marker in STEAMDT_QUOTA_ERROR_MARKERS)


def _build_worker_client(profile: SteamDTCrawlWorkerProfile) -> SteamDTMarketAPI:
    transport = PlaywrightSteamDTTransport(
        headless=profile.headless,
        timeout_ms=profile.timeout_ms,
        warmup_wait_ms=profile.warmup_wait_ms,
        rate_limiter=RandomizedRateLimiter(
            profile.rate_limit_min_seconds,
            profile.rate_limit_max_seconds,
        ),
        viewport={"width": profile.viewport_width, "height": profile.viewport_height},
        user_agent=profile.user_agent,
        locale=profile.locale,
        timezone_id=profile.timezone_id,
        extra_http_headers=profile.extra_http_headers,
        proxy_server=profile.proxy_server,
        proxy_username=profile.proxy_username,
        proxy_password=profile.proxy_password,
    )
    return SteamDTMarketAPI(transport=transport)


def _should_skip_recent_item_family(
    catalog: ItemCatalog,
    store: SteamDTPriceSnapshotStore,
    item_name: str,
    *,
    include_normal: bool,
    include_stattrak: bool,
    skip_recent_seconds: float | None,
) -> bool:
    if skip_recent_seconds is None or skip_recent_seconds <= 0:
        return False
    try:
        item_definition = catalog.get_item(item_name)
    except KeyError:
        return False
    expected_variants = 0
    if include_normal:
        expected_variants += 1
    if include_stattrak and item_definition.supports_stattrak:
        expected_variants += 1
    if expected_variants <= 0:
        return False
    expected_snapshot_count = expected_variants * max(len(item_definition.available_exteriors), 1)
    fresh_snapshots = store.get_latest_snapshots_for_item_family(
        item_name,
        max_age_seconds=skip_recent_seconds,
    )
    return len(fresh_snapshots) >= expected_snapshot_count


__all__ = [
    "CatalogPriceCrawlSummary",
    "CatalogPriceCrawlWorkerSummary",
    "CatalogPriceCrawlWorkerTask",
    "DEFAULT_STEAMDT_WORKER_USER_AGENTS",
    "MultiWorkerCatalogPriceCrawlSummary",
    "SteamDTCrawlWorkerProfile",
    "build_steamdt_crawl_worker_profiles",
    "crawl_catalog_item_prices_multiworker_to_sqlite",
    "crawl_catalog_item_prices_to_sqlite",
    "partition_catalog_item_names",
]
