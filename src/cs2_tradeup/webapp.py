from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
import traceback
from collections import deque
from contextlib import closing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from uuid import uuid4

from flask import Flask, jsonify, redirect, render_template, request, url_for

from .catalog import ItemCatalog
from .catalog_sync import (
    build_steamdt_item_platform_detail_rows,
    translate_collection_zh_cn,
    translate_exterior_zh_cn,
    translate_item_name_zh_cn,
    translate_rarity_zh_cn,
    translate_variant_zh_cn,
)
from .localization import get_default_localization_index
from .models import Exterior
from .market import split_item_variant_name
from .price_crawl import (
    build_steamdt_crawl_worker_profiles,
    crawl_catalog_item_prices_multiworker_to_sqlite,
    crawl_catalog_item_prices_to_sqlite,
)
from .scan_storage import TradeUpScanResultStore
from .steamdt_market import SteamDTPriceSnapshotStore
from .steamdt_scan import build_steamdt_tradeup_scanner, scan_steamdt_tradeup_candidates
from .scanner import summarize_float_validation


DEFAULT_CATALOG_PATH = Path("data") / "items.sqlite"
DEFAULT_PRICE_SNAPSHOT_PATH = Path("data") / "steamdt_prices.sqlite"
DEFAULT_SCAN_RESULT_PATH = Path("data") / "scan_results.sqlite"
DEFAULT_CRAWL_LOG_CANDIDATES = (
    Path("output") / "full_price_resume_supervised.log",
    Path("output") / "full_price_crawl_2w.log",
    Path("output") / "full_price_crawl.log",
)
DEFAULT_CRAWL_WORKER_LOG_DIR_CANDIDATES = (
    Path("output") / "full_price_resume_workers",
    Path("output") / "full_price_crawl_workers",
)


@dataclass(slots=True)
class BackgroundJob:
    id: str
    job_type: str
    status: str = "queued"
    title: str = ""
    created_at: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    progress_current: int = 0
    progress_total: int = 0
    progress_message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] | None = None
    error_message: str | None = None
    traceback_text: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class BackgroundJobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, BackgroundJob] = {}
        self._lock = threading.Lock()

    def submit(
        self,
        *,
        job_type: str,
        title: str,
        target: Callable[[Callable[[int, int, str], None]], Mapping[str, Any] | None],
        metadata: Mapping[str, Any] | None = None,
    ) -> BackgroundJob:
        import datetime as _dt

        job = BackgroundJob(
            id=uuid4().hex,
            job_type=job_type,
            status="queued",
            title=title,
            created_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
            metadata=dict(metadata or {}),
        )
        with self._lock:
            self._jobs[job.id] = job

        def progress(current: int, total: int, message: str) -> None:
            with self._lock:
                record = self._jobs[job.id]
                record.progress_current = current
                record.progress_total = total
                record.progress_message = message

        def runner() -> None:
            import datetime as _dt

            with self._lock:
                record = self._jobs[job.id]
                record.status = "running"
                record.started_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
            try:
                result = dict(target(progress) or {})
            except Exception as error:
                with self._lock:
                    record = self._jobs[job.id]
                    record.status = "failed"
                    record.finished_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
                    record.error_message = str(error)
                    record.traceback_text = traceback.format_exc()
                return

            with self._lock:
                record = self._jobs[job.id]
                record.status = "completed"
                record.finished_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
                record.result = result

        thread = threading.Thread(target=runner, name=f"job-{job.job_type}-{job.id[:8]}", daemon=True)
        thread.start()
        return job

    def get(self, job_id: str) -> BackgroundJob:
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(job_id)
            return BackgroundJob(**self._jobs[job_id].to_dict())

    def list(self, *, limit: int = 20) -> tuple[BackgroundJob, ...]:
        with self._lock:
            values = list(self._jobs.values())
        values.sort(key=lambda job: (job.created_at, job.id), reverse=True)
        return tuple(BackgroundJob(**job.to_dict()) for job in values[:limit])


def create_app(
    *,
    catalog_path: str | Path = DEFAULT_CATALOG_PATH,
    price_snapshot_path: str | Path = DEFAULT_PRICE_SNAPSHOT_PATH,
    scan_result_path: str | Path = DEFAULT_SCAN_RESULT_PATH,
    crawl_log_path: str | Path | None = None,
    crawl_worker_log_dir: str | Path | None = None,
) -> Flask:
    template_folder = Path(__file__).with_name("templates")
    app = Flask(__name__, template_folder=str(template_folder))
    app.config.update(
        CATALOG_PATH=str(Path(catalog_path)),
        PRICE_SNAPSHOT_PATH=str(Path(price_snapshot_path)),
        SCAN_RESULT_PATH=str(Path(scan_result_path)),
        CRAWL_LOG_PATH=str(Path(crawl_log_path)) if crawl_log_path else "",
        CRAWL_WORKER_LOG_DIR=str(Path(crawl_worker_log_dir)) if crawl_worker_log_dir else "",
    )
    get_default_localization_index()
    app.job_manager = BackgroundJobManager()  # type: ignore[attr-defined]

    @app.after_request
    def apply_no_cache_headers(response):
        if request.path.startswith("/api/") or request.path in {
            "/",
            "/prices",
            "/ev",
            "/optimizer",
            "/crawl-progress",
        }:
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    def load_catalog() -> ItemCatalog:
        return ItemCatalog.from_path(app.config["CATALOG_PATH"])

    def load_price_store() -> SteamDTPriceSnapshotStore:
        return SteamDTPriceSnapshotStore(app.config["PRICE_SNAPSHOT_PATH"])

    def load_scan_store() -> TradeUpScanResultStore:
        return TradeUpScanResultStore(app.config["SCAN_RESULT_PATH"])

    @app.get("/")
    def index():
        return redirect(url_for("prices_page"))

    @app.get("/prices")
    def prices_page():
        return render_template("prices.html")

    @app.get("/ev")
    def ev_page():
        return render_template("ev.html")

    @app.get("/optimizer")
    def optimizer_page():
        return render_template("optimizer.html")

    @app.get("/crawl-progress")
    def crawl_progress_page():
        return render_template("crawl_progress.html")

    @app.get("/api/health")
    def api_health():
        catalog = load_catalog()
        price_store = load_price_store()
        scan_store = load_scan_store()
        return jsonify(
            {
                "ok": True,
                "catalog_path": app.config["CATALOG_PATH"],
                "price_snapshot_path": app.config["PRICE_SNAPSHOT_PATH"],
                "scan_result_path": app.config["SCAN_RESULT_PATH"],
                "catalog_items": len(catalog.all_items()),
                "price_snapshots": price_store.count_snapshots(),
                "scan_runs": len(scan_store.list_runs(limit=100)),
            }
        )

    @app.get("/api/crawl/progress")
    def api_crawl_progress():
        search = request.args.get("search", "").strip().lower()
        status = request.args.get("status", "incomplete").strip().lower()
        sort_by = request.args.get("sort_by", "missing_slots").strip().lower()
        sort_dir = request.args.get("sort_dir", "desc").strip().lower()
        limit = max(1, min(int(request.args.get("limit", "50")), 500))
        offset = max(0, int(request.args.get("offset", "0")))

        catalog = load_catalog()
        store = load_price_store()
        progress_payload = _build_crawl_progress_payload(
            app,
            catalog=catalog,
            store=store,
            search=search,
            status=status,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=limit,
            offset=offset,
        )
        return jsonify(progress_payload)

    @app.get("/api/catalog/items")
    def api_catalog_items():
        search = request.args.get("search", "").strip().lower()
        limit = max(1, min(int(request.args.get("limit", "30")), 200))
        catalog = load_catalog()
        rows = []
        for item in catalog.all_items():
            item_name_zh = translate_item_name_zh_cn(item.name)
            collection_zh = translate_collection_zh_cn(item.collection)
            rarity_name_zh = translate_rarity_zh_cn(item.rarity.name)
            haystack = " ".join(
                [
                    item.name.lower(),
                    item_name_zh.lower(),
                    item.collection.lower(),
                    collection_zh.lower(),
                    item.rarity.name.lower(),
                    rarity_name_zh.lower(),
                ]
            )
            if search and search not in haystack:
                continue
            rows.append(
                {
                    "name": item.name,
                    "name_zh": item_name_zh,
                    "collection": item.collection,
                    "collection_zh": collection_zh,
                    "rarity": int(item.rarity),
                    "rarity_name": item.rarity.name,
                    "rarity_name_zh": rarity_name_zh,
                    "available_exteriors": [exterior.value for exterior in item.available_exteriors],
                }
            )
            if len(rows) >= limit:
                break
        return jsonify({"rows": rows, "total": len(rows)})

    @app.get("/api/meta/options")
    def api_meta_options():
        catalog = load_catalog()
        store = load_price_store()
        price_rows = build_steamdt_item_platform_detail_rows(snapshot_store=store, catalog=catalog)
        collections = sorted({item.collection for item in catalog.all_items()})
        rarity_order = {item.rarity.name: int(item.rarity) for item in catalog.all_items()}
        rarities = sorted({item.rarity.name for item in catalog.all_items()}, key=lambda value: rarity_order.get(value, 999))
        exteriors = [exterior.value for exterior in Exterior.ordered()]
        variants = ["Normal", "StatTrak"]
        platforms = sorted(
            {
                entry.platform_name
                for row in price_rows
                for entry in row.platform_prices
                if entry.platform_name
            }
        )
        return jsonify(
            {
                "collections": [
                    {"value": name, "label": translate_collection_zh_cn(name)}
                    for name in collections
                ],
                "rarities": [
                    {"value": name, "label": translate_rarity_zh_cn(name)}
                    for name in rarities
                ],
                "exteriors": [
                    {"value": name, "label": translate_exterior_zh_cn(name)}
                    for name in exteriors
                ],
                "variants": [
                    {"value": name, "label": translate_variant_zh_cn(name)}
                    for name in variants
                ],
                "platforms": [
                    {"value": name, "label": name}
                    for name in platforms
                ],
            }
        )

    @app.get("/api/prices")
    def api_prices():
        search = request.args.get("search", "").strip().lower()
        variant = request.args.get("variant", "").strip()
        exterior = request.args.get("exterior", "").strip()
        collection = request.args.get("collection", "").strip()
        rarity_name = request.args.get("rarity_name", "").strip()
        platform_name = request.args.get("platform", "").strip()
        min_price = request.args.get("min_price", "").strip()
        max_price = request.args.get("max_price", "").strip()
        has_price_only = _parse_bool(request.args.get("has_price_only"), default=False)
        sort_by = request.args.get("sort_by", "item").strip().lower()
        sort_dir = request.args.get("sort_dir", "asc").strip().lower()
        limit = max(1, min(int(request.args.get("limit", "200")), 1000))
        offset = max(0, int(request.args.get("offset", "0")))

        catalog = load_catalog()
        store = load_price_store()
        rows = build_steamdt_item_platform_detail_rows(
            snapshot_store=store,
            catalog=catalog,
            prefer_cleaned=True,
            valid_only=False,
        )
        filtered = [_serialize_price_row(row) for row in rows]
        filtered = [
            row
            for row in filtered
            if _match_price_row(
                row,
                search=search,
                variant=variant,
                exterior=exterior,
                collection=collection,
                rarity_name=rarity_name,
                platform_name=platform_name,
                min_price=float(min_price) if min_price else None,
                max_price=float(max_price) if max_price else None,
                has_price_only=has_price_only,
            )
        ]
        reverse = sort_dir == "desc"
        filtered.sort(
            key=lambda row: _price_sort_key(row, sort_by=sort_by, platform_name=platform_name),
            reverse=reverse,
        )

        total = len(filtered)
        paged = filtered[offset : offset + limit]
        return jsonify({"rows": paged, "total": total, "offset": offset, "limit": limit})

    @app.get("/api/ev/runs")
    def api_ev_runs():
        limit = max(1, min(int(request.args.get("limit", "20")), 100))
        run_type = request.args.get("run_type") or None
        status = request.args.get("status") or None
        store = load_scan_store()
        rows = [_serialize_scan_run(run) for run in store.list_runs(limit=limit, run_type=run_type, status=status)]
        return jsonify({"rows": rows, "total": len(rows)})

    @app.get("/api/ev/results")
    def api_ev_results():
        limit = max(1, min(int(request.args.get("limit", "100")), 1000))
        min_roi = request.args.get("min_roi")
        max_roi = request.args.get("max_roi")
        min_expected_profit = request.args.get("min_expected_profit")
        max_total_cost = request.args.get("max_total_cost")
        search = request.args.get("search") or None
        run_id = request.args.get("run_id")
        target_collection = request.args.get("target_collection") or None
        target_exterior = request.args.get("target_exterior") or None
        target_rarity_name = request.args.get("target_rarity_name") or None
        sort_by = request.args.get("sort_by", "roi")
        sort_dir = request.args.get("sort_dir", "desc")
        store = load_scan_store()
        explicit_run_id = int(run_id) if run_id not in (None, "") else None
        rows = store.list_results(
            limit=limit,
            min_roi=float(min_roi) if min_roi not in (None, "") else None,
            max_roi=float(max_roi) if max_roi not in (None, "") else None,
            min_expected_profit=(
                float(min_expected_profit)
                if min_expected_profit not in (None, "")
                else None
            ),
            max_total_cost=(
                float(max_total_cost)
                if max_total_cost not in (None, "")
                else None
            ),
            search=search,
            run_id=explicit_run_id,
            latest_run_only=(explicit_run_id is None),
            target_collection=target_collection,
            target_exterior=target_exterior,
            target_rarity_name=target_rarity_name,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        payload = [_serialize_scan_result(row) for row in rows]
        return jsonify({"rows": payload, "total": len(payload)})

    @app.get("/api/optimizer")
    def api_optimizer():
        item_name = (request.args.get("item_name") or "").strip()
        exterior = (request.args.get("exterior") or "").strip()
        if not item_name or not exterior:
            return jsonify({"error": "必须提供 item_name 和 exterior"}), 400

        roi_threshold = float(request.args.get("roi_threshold", "0"))
        formula_limit = max(1, min(int(request.args.get("formula_limit", "20")), 100))
        cache_only = _parse_bool(request.args.get("cache_only"), default=False)
        persist = _parse_bool(request.args.get("persist"), default=False)
        conservative_float_mode = _parse_bool(
            request.args.get("conservative_float_mode"),
            default=True,
        )
        formula_options = {
            "min_target_count": int(request.args.get("min_target_count", "1")),
            "max_target_count": int(request.args.get("max_target_count", "10")),
            "max_auxiliary_collections": int(request.args.get("max_auxiliary_collections", "2")),
            "max_formulas": formula_limit,
        }
        scanner, cached_client, _, _ = build_steamdt_tradeup_scanner(
            catalog=app.config["CATALOG_PATH"],
            snapshot_store=app.config["PRICE_SNAPSHOT_PATH"],
            cache_only=cache_only,
            scanner_max_workers=4,
            price_max_workers=4,
            prefer_safe_price=True,
            require_valid_prices=True,
            normal_tradeup_only=True,
            conservative_float_mode=conservative_float_mode,
        )
        try:
            results = scanner.find_optimal_materials(
                item_name,
                exterior,
                roi_threshold=roi_threshold,
                result_limit=formula_limit,
                conservative_float_mode=conservative_float_mode,
                **formula_options,
            )
        finally:
            cached_client.close()
        summary = type(
            "OptimizerSummary",
            (),
            {
                "targets_scanned": 1,
                "results_found": len(results),
                "cache_only": cache_only,
                "live_fetch_enabled": not cache_only,
                "results": results,
            },
        )()
        run_id = None
        if persist:
            store = load_scan_store()
            run_id = store.create_run(
                run_type="optimizer",
                parameters={
                    "item_name": item_name,
                    "exterior": exterior,
                    "roi_threshold": roi_threshold,
                    "formula_limit": formula_limit,
                    "cache_only": cache_only,
                    "conservative_float_mode": conservative_float_mode,
                    "formula_options": formula_options,
                },
            )
            store.append_results(run_id, summary.results)
            store.complete_run(
                run_id,
                status="completed",
                summary={
                    "targets_scanned": summary.targets_scanned,
                    "results_found": summary.results_found,
                    "cache_only": summary.cache_only,
                    "live_fetch_enabled": summary.live_fetch_enabled,
                },
            )
        return jsonify(
            {
                "run_id": run_id,
                "summary": {
                    "targets_scanned": summary.targets_scanned,
                    "results_found": summary.results_found,
                    "cache_only": summary.cache_only,
                    "live_fetch_enabled": summary.live_fetch_enabled,
                    "conservative_float_mode": conservative_float_mode,
                },
                "rows": [_serialize_live_scan_result(result) for result in summary.results],
            }
        )

    @app.post("/api/tasks/crawl-prices")
    def api_task_crawl_prices():
        payload = request.get_json(silent=True) or {}
        item_names = _normalize_item_names(payload.get("item_names"))
        max_items = payload.get("max_items")
        worker_count = int(payload.get("worker_count", 1))
        sleep_min = float(payload.get("sleep_min_seconds", 0.8))
        sleep_max = float(payload.get("sleep_max_seconds", 1.8))
        batch_size = int(payload.get("batch_size", 20))
        batch_cooldown_min = float(payload.get("batch_cooldown_min_seconds", 6.0))
        batch_cooldown_max = float(payload.get("batch_cooldown_max_seconds", 12.0))
        retry_attempts = int(payload.get("retry_attempts", 2))
        failure_backoff = float(payload.get("failure_backoff_base_seconds", 8.0))
        skip_recent_seconds = payload.get("skip_recent_seconds", 6 * 3600)
        supervisor_restart_limit = int(payload.get("supervisor_restart_limit", 2))
        supervisor_backoff_base = float(payload.get("supervisor_backoff_base_seconds", 15.0))
        user_agents = _normalize_string_list(payload.get("user_agents"))
        proxy_servers = _normalize_string_list(payload.get("proxy_servers"))
        locales = _normalize_string_list(payload.get("locales"))
        timezone_ids = _normalize_string_list(payload.get("timezone_ids"))
        worker_log_dir = payload.get("worker_log_dir")

        def target(progress: Callable[[int, int, str], None]) -> Mapping[str, Any]:
            common_kwargs = {
                "catalog": app.config["CATALOG_PATH"],
                "snapshot_store": app.config["PRICE_SNAPSHOT_PATH"],
                "item_names": item_names,
                "max_items": int(max_items) if max_items not in (None, "") else None,
                "sleep_min_seconds": sleep_min,
                "sleep_max_seconds": sleep_max,
                "batch_size": batch_size,
                "batch_cooldown_min_seconds": batch_cooldown_min,
                "batch_cooldown_max_seconds": batch_cooldown_max,
                "retry_attempts": retry_attempts,
                "failure_backoff_base_seconds": failure_backoff,
                "skip_recent_seconds": (
                    float(skip_recent_seconds)
                    if skip_recent_seconds not in (None, "")
                    else None
                ),
                "progress_callback": progress,
            }
            if worker_count <= 1:
                summary = crawl_catalog_item_prices_to_sqlite(**common_kwargs)
                return {
                    "mode": "single_worker",
                    "worker_count": 1,
                    "total_items": summary.total_items,
                    "processed_items": summary.processed_items,
                    "skipped_recent_items": summary.skipped_recent_items,
                    "failed_items": list(summary.failed_items),
                    "snapshots_before": summary.snapshots_before,
                    "snapshots_after": summary.snapshots_after,
                    "snapshots_inserted": summary.snapshots_inserted,
                }

            worker_profiles = build_steamdt_crawl_worker_profiles(
                worker_count,
                user_agents=user_agents or None,
                proxy_servers=proxy_servers or None,
                locales=locales or None,
                timezone_ids=timezone_ids or None,
            )
            summary = crawl_catalog_item_prices_multiworker_to_sqlite(
                **common_kwargs,
                worker_count=worker_count,
                worker_profiles=worker_profiles,
                worker_log_dir=worker_log_dir,
                supervisor_restart_limit=supervisor_restart_limit,
                supervisor_backoff_base_seconds=supervisor_backoff_base,
            )
            return {
                "mode": "multi_worker",
                "worker_count": summary.worker_count,
                "total_items": summary.total_items,
                "processed_items": summary.processed_items,
                "skipped_recent_items": summary.skipped_recent_items,
                "failed_items": list(summary.failed_items),
                "snapshots_before": summary.snapshots_before,
                "snapshots_after": summary.snapshots_after,
                "snapshots_inserted": summary.snapshots_inserted,
                "workers": [
                    {
                        "worker_id": row.worker_id,
                        "assigned_items": row.assigned_items,
                        "processed_items": row.processed_items,
                        "skipped_recent_items": row.skipped_recent_items,
                        "failed_items": list(row.failed_items),
                        "snapshots_inserted": row.snapshots_inserted,
                        "log_path": row.log_path,
                    }
                    for row in summary.worker_summaries
                ],
            }

        job = app.job_manager.submit(  # type: ignore[attr-defined]
            job_type="crawl_prices",
            title="抓取枪械价格并写入 SQLite",
            target=target,
            metadata={
                "item_names": item_names,
                "max_items": max_items,
                "sleep_min_seconds": sleep_min,
                "sleep_max_seconds": sleep_max,
                "batch_size": batch_size,
                "batch_cooldown_min_seconds": batch_cooldown_min,
                "batch_cooldown_max_seconds": batch_cooldown_max,
                "retry_attempts": retry_attempts,
                "failure_backoff_base_seconds": failure_backoff,
                "supervisor_restart_limit": supervisor_restart_limit,
                "supervisor_backoff_base_seconds": supervisor_backoff_base,
                "skip_recent_seconds": skip_recent_seconds,
                "worker_count": worker_count,
                "user_agents": user_agents,
                "proxy_servers": proxy_servers,
                "locales": locales,
                "timezone_ids": timezone_ids,
                "worker_log_dir": worker_log_dir,
            },
        )
        return jsonify({"job": job.to_dict()})

    @app.post("/api/tasks/scan-ev")
    def api_task_scan_ev():
        payload = request.get_json(silent=True) or {}
        item_names = _normalize_item_names(payload.get("item_names"))
        cache_only = bool(payload.get("cache_only", False))
        min_roi = float(payload.get("min_roi", 1.05))
        formula_limit = int(payload.get("formula_limit_per_target", 25))
        max_targets = payload.get("max_targets")
        conservative_float_mode = bool(payload.get("conservative_float_mode", True))
        formula_options = {
            "min_target_count": int(payload.get("min_target_count", 2)),
            "max_target_count": int(payload.get("max_target_count", 4)),
            "max_auxiliary_collections": int(payload.get("max_auxiliary_collections", 2)),
            "max_formulas": formula_limit,
        }
        run_store = load_scan_store()
        run_id = run_store.create_run(
            run_type="ev_scan",
            parameters={
                "item_names": item_names,
                "cache_only": cache_only,
                "min_roi": min_roi,
                "formula_limit_per_target": formula_limit,
                "max_targets": max_targets,
                "conservative_float_mode": conservative_float_mode,
                "formula_options": formula_options,
            },
        )

        def target(progress: Callable[[int, int, str], None]) -> Mapping[str, Any]:
            try:
                progress(0, 1, "刷新价格风控表")
                load_price_store().refresh_cleaned_prices()
                if item_names:
                    for index, item_name in enumerate(item_names, start=1):
                        progress(index, len(item_names), f"准备扫描 {item_name}")
                summary = scan_steamdt_tradeup_candidates(
                    catalog=app.config["CATALOG_PATH"],
                    snapshot_store=app.config["PRICE_SNAPSHOT_PATH"],
                    item_names=item_names or None,
                    cache_only=cache_only,
                    roi_threshold=min_roi,
                    formula_limit_per_target=formula_limit,
                    formula_options=formula_options,
                    max_targets=int(max_targets) if max_targets not in (None, "") else None,
                    output_csv_path=None,
                    scanner_max_workers=4,
                    price_max_workers=4,
                    progress_callback=progress,
                    conservative_float_mode=conservative_float_mode,
                )
                run_store.delete_results_for_run(run_id)
                run_store.append_results(run_id, summary.results)
                summary_payload = {
                    "targets_scanned": summary.targets_scanned,
                    "results_found": summary.results_found,
                    "cache_only": summary.cache_only,
                    "live_fetch_enabled": summary.live_fetch_enabled,
                    "conservative_float_mode": conservative_float_mode,
                    "target_names": list(summary.target_names),
                }
                run_store.complete_run(run_id, status="completed", summary=summary_payload)
                return {"run_id": run_id, **summary_payload}
            except Exception as error:
                run_store.complete_run(
                    run_id,
                    status="failed",
                    summary={},
                    error_message=str(error),
                )
                raise

        job = app.job_manager.submit(  # type: ignore[attr-defined]
            job_type="scan_ev",
            title="扫描炼金 EV 并写入 SQLite",
            target=target,
            metadata={"run_id": run_id},
        )
        return jsonify({"job": job.to_dict(), "run_id": run_id})

    @app.get("/api/tasks")
    def api_tasks():
        limit = max(1, min(int(request.args.get("limit", "20")), 100))
        rows = [job.to_dict() for job in app.job_manager.list(limit=limit)]  # type: ignore[attr-defined]
        return jsonify({"rows": rows, "total": len(rows)})

    @app.get("/api/tasks/<job_id>")
    def api_task(job_id: str):
        try:
            job = app.job_manager.get(job_id)  # type: ignore[attr-defined]
        except KeyError:
            return jsonify({"error": "任务不存在"}), 404
        return jsonify({"job": job.to_dict()})

    return app


def _build_crawl_progress_payload(
    app: Flask,
    *,
    catalog: ItemCatalog,
    store: SteamDTPriceSnapshotStore,
    search: str,
    status: str,
    sort_by: str,
    sort_dir: str,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    family_snapshot_stats = _load_family_snapshot_stats(store)
    gap_rows: list[dict[str, Any]] = []
    total_expected_slots = 0
    total_cached_slots = 0
    complete_families = 0

    for item in catalog.all_items():
        expected_slots = max(1, len(item.available_exteriors)) * (1 + int(item.supports_stattrak))
        family_stat = family_snapshot_stats.get(item.name, {})
        cached_slots = int(family_stat.get("cached_slots", 0))
        cached_slots_capped = min(cached_slots, expected_slots)
        missing_slots = max(0, expected_slots - cached_slots_capped)
        total_expected_slots += expected_slots
        total_cached_slots += cached_slots_capped
        if missing_slots == 0:
            complete_families += 1

        item_name_zh = translate_item_name_zh_cn(item.name)
        collection_zh = translate_collection_zh_cn(item.collection)
        rarity_name_zh = translate_rarity_zh_cn(item.rarity.name)
        haystack = " ".join(
            [
                item.name.lower(),
                item_name_zh.lower(),
                item.collection.lower(),
                collection_zh.lower(),
                item.rarity.name.lower(),
                rarity_name_zh.lower(),
            ]
        )
        if search and search not in haystack:
            continue
        if status == "complete" and missing_slots > 0:
            continue
        if status == "incomplete" and missing_slots == 0:
            continue

        gap_rows.append(
            {
                "item_name": item.name,
                "item_name_zh": item_name_zh,
                "collection": item.collection,
                "collection_zh": collection_zh,
                "rarity_name": item.rarity.name,
                "rarity_name_zh": rarity_name_zh,
                "supports_stattrak": item.supports_stattrak,
                "expected_slots": expected_slots,
                "cached_slots": cached_slots_capped,
                "missing_slots": missing_slots,
                "completion_ratio": round((cached_slots_capped / expected_slots) if expected_slots else 0.0, 4),
                "available_exteriors": [exterior.value for exterior in item.available_exteriors],
                "available_exteriors_zh": [
                    translate_exterior_zh_cn(exterior.value)
                    for exterior in item.available_exteriors
                ],
                "latest_fetched_at": family_stat.get("latest_fetched_at"),
                "latest_fetched_at_epoch": family_stat.get("latest_fetched_at_epoch"),
            }
        )

    gap_rows = _sort_crawl_gap_rows(gap_rows, sort_by=sort_by, sort_dir=sort_dir)
    paged_rows = gap_rows[offset : offset + limit]

    sqlite_stats = _load_snapshot_table_stats(store)
    total_families = len(catalog.all_items())
    incomplete_families = max(0, total_families - complete_families)
    main_log_path = _resolve_existing_path(
        app.config.get("CRAWL_LOG_PATH"),
        DEFAULT_CRAWL_LOG_CANDIDATES,
        expect_dir=False,
    )
    worker_log_dir = _resolve_existing_path(
        app.config.get("CRAWL_WORKER_LOG_DIR"),
        DEFAULT_CRAWL_WORKER_LOG_DIR_CANDIDATES,
        expect_dir=True,
    )
    run_status = _read_crawl_run_status(main_log_path)
    worker_statuses = _read_worker_statuses(worker_log_dir)

    return {
        "summary": {
            "total_families": total_families,
            "complete_families": complete_families,
            "incomplete_families": incomplete_families,
            "family_completion_ratio": round((complete_families / total_families) if total_families else 0.0, 4),
            "expected_slots": total_expected_slots,
            "cached_slots": total_cached_slots,
            "missing_slots": max(0, total_expected_slots - total_cached_slots),
            "slot_completion_ratio": round((total_cached_slots / total_expected_slots) if total_expected_slots else 0.0, 4),
            **sqlite_stats,
        },
        "run": run_status,
        "workers": worker_statuses,
        "rows": paged_rows,
        "total": len(gap_rows),
        "limit": limit,
        "offset": offset,
        "filters": {
            "search": search,
            "status": status,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
        },
    }


def _load_family_snapshot_stats(store: SteamDTPriceSnapshotStore) -> dict[str, dict[str, Any]]:
    latest_by_variant_exterior: set[tuple[str, str | None]] = set()
    family_stats: dict[str, dict[str, Any]] = {}
    with closing(sqlite3.connect(store.path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            f'''
            SELECT id, item_name, exterior, fetched_at_epoch, fetched_at
            FROM "{store.table_name}"
            ORDER BY item_name ASC, exterior ASC, fetched_at_epoch DESC, id DESC
            '''
        ).fetchall()
    for row in rows:
        variant_key = (str(row["item_name"]), row["exterior"])
        if variant_key in latest_by_variant_exterior:
            continue
        latest_by_variant_exterior.add(variant_key)
        family_name = split_item_variant_name(str(row["item_name"]))[0]
        entry = family_stats.setdefault(
            family_name,
            {
                "cached_slots": 0,
                "latest_fetched_at_epoch": None,
                "latest_fetched_at": None,
            },
        )
        entry["cached_slots"] += 1
        fetched_epoch = float(row["fetched_at_epoch"]) if row["fetched_at_epoch"] is not None else None
        if fetched_epoch is None:
            continue
        current_epoch = entry["latest_fetched_at_epoch"]
        if current_epoch is None or fetched_epoch > current_epoch:
            entry["latest_fetched_at_epoch"] = fetched_epoch
            entry["latest_fetched_at"] = row["fetched_at"]
    return family_stats


def _load_snapshot_table_stats(store: SteamDTPriceSnapshotStore) -> dict[str, Any]:
    now_epoch = dt.datetime.now(dt.timezone.utc).timestamp()
    with closing(sqlite3.connect(store.path)) as connection:
        total_snapshots = int(
            connection.execute(f'SELECT COUNT(*) FROM "{store.table_name}"').fetchone()[0]
        )
        distinct_items = int(
            connection.execute(
                f'SELECT COUNT(DISTINCT item_name) FROM "{store.table_name}"'
            ).fetchone()[0]
        )
        distinct_market_hash_names = int(
            connection.execute(
                f'SELECT COUNT(DISTINCT market_hash_name) FROM "{store.table_name}"'
            ).fetchone()[0]
        )
        latest_row = connection.execute(
            f'''
            SELECT fetched_at_epoch, fetched_at
            FROM "{store.table_name}"
            ORDER BY fetched_at_epoch DESC, id DESC
            LIMIT 1
            '''
        ).fetchone()
        recent_5m = int(
            connection.execute(
                f'SELECT COUNT(*) FROM "{store.table_name}" WHERE fetched_at_epoch >= ?',
                (now_epoch - 5 * 60,),
            ).fetchone()[0]
        )
        recent_15m = int(
            connection.execute(
                f'SELECT COUNT(*) FROM "{store.table_name}" WHERE fetched_at_epoch >= ?',
                (now_epoch - 15 * 60,),
            ).fetchone()[0]
        )
        recent_60m = int(
            connection.execute(
                f'SELECT COUNT(*) FROM "{store.table_name}" WHERE fetched_at_epoch >= ?',
                (now_epoch - 60 * 60,),
            ).fetchone()[0]
        )
    latest_fetched_at = latest_row[1] if latest_row else None
    latest_fetched_at_epoch = float(latest_row[0]) if latest_row and latest_row[0] is not None else None
    return {
        "total_snapshots": total_snapshots,
        "distinct_items": distinct_items,
        "distinct_market_hash_names": distinct_market_hash_names,
        "latest_fetched_at": latest_fetched_at,
        "latest_fetched_at_epoch": latest_fetched_at_epoch,
        "recent_snapshots_5m": recent_5m,
        "recent_snapshots_15m": recent_15m,
        "recent_snapshots_60m": recent_60m,
    }


def _sort_crawl_gap_rows(
    rows: list[dict[str, Any]],
    *,
    sort_by: str,
    sort_dir: str,
) -> list[dict[str, Any]]:
    reverse = sort_dir == "desc"
    key_map = {
        "item": lambda row: (row["item_name_zh"], row["item_name"]),
        "collection": lambda row: (row["collection_zh"], row["collection"], row["item_name"]),
        "rarity": lambda row: (row["rarity_name"], row["item_name"]),
        "expected_slots": lambda row: (row["expected_slots"], row["item_name"]),
        "cached_slots": lambda row: (row["cached_slots"], row["item_name"]),
        "missing_slots": lambda row: (row["missing_slots"], row["item_name"]),
        "completion_ratio": lambda row: (row["completion_ratio"], row["item_name"]),
        "latest": lambda row: (row["latest_fetched_at_epoch"] or 0.0, row["item_name"]),
    }
    sort_key = key_map.get(sort_by, key_map["missing_slots"])
    return sorted(rows, key=sort_key, reverse=reverse)


def _resolve_existing_path(
    configured_path: str | None,
    candidates: Iterable[Path],
    *,
    expect_dir: bool,
) -> Path | None:
    candidate_paths: list[Path] = []
    if configured_path:
        candidate_paths.append(Path(configured_path))
    candidate_paths.extend(candidates)
    for candidate in candidate_paths:
        if expect_dir and candidate.is_dir():
            return candidate
        if not expect_dir and candidate.is_file():
            return candidate
    return None


def _tail_text_lines(path: Path | None, *, limit: int = 40) -> list[str]:
    if path is None or not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return list(deque((line.rstrip("\n") for line in handle), maxlen=limit))


def _read_crawl_run_status(log_path: Path | None) -> dict[str, Any]:
    lines = _tail_text_lines(log_path, limit=80)
    status = "idle"
    started_at = None
    finished_at = None
    error_message = None
    restarts = 0
    for line in lines:
        if line.startswith("START "):
            started_at = line.removeprefix("START ").strip()
        if "Supervisor 捕获异常" in line:
            restarts += 1
        if line.startswith("SUMMARY "):
            status = "completed"
            finished_at = line.removeprefix("SUMMARY ").strip()
        if line.startswith("ERROR "):
            status = "failed"
            error_message = line.removeprefix("ERROR ").strip()
        if line.startswith("[") and status == "idle":
            status = "running"
    if lines and status == "idle":
        status = "running"
    return {
        "status": status,
        "log_path": str(log_path) if log_path is not None else None,
        "started_at": started_at,
        "finished_at": finished_at,
        "error_message": error_message,
        "restart_count": restarts,
        "tail": lines[-20:],
        "last_update_at": dt.datetime.fromtimestamp(log_path.stat().st_mtime, tz=dt.timezone.utc).isoformat()
        if log_path is not None and log_path.exists()
        else None,
    }


def _read_worker_statuses(worker_log_dir: Path | None) -> list[dict[str, Any]]:
    if worker_log_dir is None or not worker_log_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for log_path in sorted(worker_log_dir.glob("steamdt_worker_*.log")):
        lines = _tail_text_lines(log_path, limit=60)
        worker_status: dict[str, Any] = {
            "worker_id": _parse_worker_id(log_path),
            "log_path": str(log_path),
            "status": "idle",
            "progress_current": 0,
            "progress_total": 0,
            "progress_ratio": 0.0,
            "message": "",
            "last_update_at": dt.datetime.fromtimestamp(log_path.stat().st_mtime, tz=dt.timezone.utc).isoformat(),
            "tail": lines[-10:],
        }
        for line in reversed(lines):
            if line.startswith("SUMMARY "):
                worker_status["status"] = "completed"
                worker_status["message"] = line.removeprefix("SUMMARY ").strip()
                break
            if line.startswith("ERROR "):
                worker_status["status"] = "failed"
                worker_status["message"] = line.removeprefix("ERROR ").strip()
                break
            parsed = _parse_worker_progress_line(line)
            if parsed is not None:
                worker_status.update(parsed)
                worker_status["status"] = "running"
                break
            if line.startswith("START worker="):
                worker_status["status"] = "starting"
                worker_status["message"] = line
                break
        rows.append(worker_status)
    return rows


def _parse_worker_progress_line(line: str) -> dict[str, Any] | None:
    if not line.startswith("[") or "[worker " not in line:
        return None
    parts = line.split("] ")
    if len(parts) < 3:
        return None
    timestamp = parts[0].lstrip("[")
    worker_part = parts[1]
    progress_part = parts[2]
    message = "] ".join(parts[3:]) if len(parts) > 3 else ""
    if not worker_part.startswith("[worker ") or not progress_part.startswith("["):
        return None
    try:
        worker_id = int(worker_part.removeprefix("[worker ").rstrip("]"))
        current_raw, total_raw = progress_part.lstrip("[").rstrip("]").split("/", 1)
        current = int(current_raw)
        total = int(total_raw)
    except Exception:
        return None
    return {
        "worker_id": worker_id,
        "progress_current": current,
        "progress_total": total,
        "progress_ratio": round((current / total) if total else 0.0, 4),
        "message": message,
        "line_timestamp": timestamp,
    }


def _parse_worker_id(path: Path) -> int:
    digits = "".join(character for character in path.stem if character.isdigit())
    return int(digits) if digits else 0


def _normalize_item_names(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = [part.strip() for part in value.splitlines() if part.strip()]
        if len(parts) == 1 and "," in parts[0]:
            parts = [part.strip() for part in parts[0].split(",") if part.strip()]
        return tuple(dict.fromkeys(parts))
    if isinstance(value, Iterable):
        return tuple(dict.fromkeys(str(item).strip() for item in value if str(item).strip()))
    return ()


def _normalize_string_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = [part.strip() for part in value.replace("\r", "\n").splitlines() if part.strip()]
        if len(parts) == 1 and "," in parts[0]:
            parts = [part.strip() for part in parts[0].split(",") if part.strip()]
        return tuple(parts)
    if isinstance(value, Iterable):
        return tuple(str(part).strip() for part in value if str(part).strip())
    return ()


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _match_price_row(
    row: Mapping[str, Any],
    *,
    search: str,
    variant: str,
    exterior: str,
    collection: str,
    rarity_name: str,
    platform_name: str,
    min_price: float | None,
    max_price: float | None,
    has_price_only: bool,
) -> bool:
    effective_price = row.get("safe_price")
    if effective_price in (None, 0):
        effective_price = row.get("lowest_price")
    haystack = " ".join(
        [
            str(row.get("base_item_name", "")),
            str(row.get("base_item_name_zh", "")),
            str(row.get("item_name", "")),
            str(row.get("item_name_zh", "")),
            str(row.get("collection", "")),
            str(row.get("collection_zh", "")),
            str(row.get("rarity_name", "")),
            str(row.get("rarity_name_zh", "")),
            str(row.get("variant", "")),
            str(row.get("variant_zh", "")),
            str(row.get("exterior", "")),
            str(row.get("exterior_zh", "")),
        ]
    ).lower()
    if search and search not in haystack:
        return False
    if variant and row.get("variant") != variant:
        return False
    if exterior and row.get("exterior") != exterior:
        return False
    if collection and row.get("collection") != collection:
        return False
    if rarity_name and row.get("rarity_name") != rarity_name:
        return False
    if has_price_only and (effective_price is None or float(effective_price) <= 0):
        return False
    if min_price is not None and (effective_price is None or float(effective_price) < min_price):
        return False
    if max_price is not None and (effective_price is None or float(effective_price) > max_price):
        return False
    if platform_name:
        platform_entry = next(
            (
                entry
                for entry in row.get("platform_prices", ())
                if entry.get("platform_name") == platform_name and entry.get("price") not in (None, 0)
            ),
            None,
        )
        if platform_entry is None:
            return False
    return True


def _price_sort_key(
    row: Mapping[str, Any],
    *,
    sort_by: str,
    platform_name: str,
) -> tuple[Any, ...]:
    effective_price = row.get("safe_price")
    if effective_price in (None, 0):
        effective_price = row.get("lowest_price")
    if sort_by == "lowest_price":
        return (
            row.get("lowest_price") if row.get("lowest_price") is not None else 10**12,
            row.get("base_item_name_zh"),
            row.get("variant"),
            row.get("exterior") or "",
        )
    if sort_by == "safe_price":
        return (
            effective_price if effective_price is not None else 10**12,
            row.get("base_item_name_zh"),
            row.get("variant"),
            row.get("exterior") or "",
        )
    if sort_by == "recent_average_price":
        return (
            row.get("recent_average_price") if row.get("recent_average_price") is not None else 10**12,
            row.get("base_item_name_zh"),
        )
    if sort_by == "sell_num":
        return (
            row.get("sell_num") if row.get("sell_num") is not None else -1,
            row.get("base_item_name_zh"),
        )
    if sort_by == "fetched_at":
        return (
            row.get("fetched_at") or "",
            row.get("base_item_name_zh"),
        )
    if sort_by == "platform_price" and platform_name:
        platform_entry = next(
            (
                entry
                for entry in row.get("platform_prices", ())
                if entry.get("platform_name") == platform_name
            ),
            None,
        )
        return (
            platform_entry.get("price") if platform_entry and platform_entry.get("price") is not None else 10**12,
            row.get("base_item_name_zh"),
        )
    return (
        row.get("base_item_name_zh"),
        row.get("variant"),
        row.get("exterior") or "",
    )


def _serialize_price_row(row: Any) -> dict[str, Any]:
    return {
        "base_item_name": row.base_item_name,
        "base_item_name_zh": translate_item_name_zh_cn(row.base_item_name),
        "variant": row.variant,
        "variant_zh": translate_variant_zh_cn(row.variant),
        "item_name": row.item_name,
        "item_name_zh": translate_item_name_zh_cn(row.item_name),
        "market_hash_name": row.market_hash_name,
        "exterior": row.exterior,
        "exterior_zh": translate_exterior_zh_cn(row.exterior),
        "collection": row.collection,
        "collection_zh": translate_collection_zh_cn(row.collection),
        "rarity": row.rarity,
        "rarity_name": row.rarity_name,
        "rarity_name_zh": translate_rarity_zh_cn(row.rarity_name),
        "min_float": row.min_float,
        "max_float": row.max_float,
        "lowest_price": row.lowest_price,
        "safe_price": row.safe_price,
        "effective_price": row.safe_price if row.safe_price not in (None, 0) else row.lowest_price,
        "is_valid": row.is_valid,
        "risk_level": row.risk_level,
        "anomaly_flags": list(row.anomaly_flags),
        "anomaly_notes": row.anomaly_notes,
        "recent_average_price": row.recent_average_price,
        "highest_buy_price": row.highest_buy_price,
        "sell_num": row.sell_num,
        "fetched_at": row.fetched_at,
        "source": row.source,
        "platform_prices": [
            {
                "platform": entry.platform,
                "platform_name": entry.platform_name,
                "price": entry.price,
                "link": entry.link,
            }
            for entry in row.platform_prices
        ],
    }


def _serialize_scan_run(run: Any) -> dict[str, Any]:
    return {
        "id": run.id,
        "run_type": run.run_type,
        "status": run.status,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "parameters": run.parameters,
        "summary": run.summary,
        "error_message": run.error_message,
    }


def _serialize_scan_result(row: Any) -> dict[str, Any]:
    materials = []
    for entry in row.materials:
        materials.append(
            {
                **entry,
                "item_name_zh": translate_item_name_zh_cn(entry["item_name"]),
                "collection_zh": translate_collection_zh_cn(entry["collection"]),
                "requested_exterior_zh": translate_exterior_zh_cn(entry["requested_exterior"]),
                "float_source_label": entry.get("float_source_label") or entry.get("float_source", ""),
            }
        )
    outcomes = []
    for entry in row.outcomes:
        outcomes.append(
            {
                **entry,
                "item_name_zh": translate_item_name_zh_cn(entry["item_name"]),
                "collection_zh": translate_collection_zh_cn(entry["collection"]),
                "exterior_zh": translate_exterior_zh_cn(entry["exterior"]),
            }
        )
    float_validation = summarize_float_validation(materials)
    return {
        "id": row.id,
        "run_id": row.run_id,
        "target_item": row.target_item,
        "target_item_zh": translate_item_name_zh_cn(row.target_item),
        "target_exterior": row.target_exterior,
        "target_exterior_zh": translate_exterior_zh_cn(row.target_exterior),
        "target_collection": row.target_collection,
        "target_collection_zh": translate_collection_zh_cn(row.target_collection),
        "target_rarity": row.target_rarity,
        "target_rarity_name": row.target_rarity_name,
        "target_rarity_name_zh": translate_rarity_zh_cn(row.target_rarity_name),
        "roi": row.roi,
        "roi_percent": row.roi_percent,
        "expected_profit": row.expected_profit,
        "expected_revenue": row.expected_revenue,
        "total_cost": row.total_cost,
        "target_probability": row.target_probability,
        "planned_average_metric": row.planned_average_metric,
        "formula_signature": row.formula_signature,
        "fee_rate": row.fee_rate,
        "collection_counts": row.collection_counts,
        "materials": materials,
        "outcomes": outcomes,
        **float_validation,
        "created_at": row.created_at,
    }


def _serialize_live_scan_result(result: Any) -> dict[str, Any]:
    return _serialize_scan_result(
        type(
            "LiveScanRecord",
            (),
            {
                "id": 0,
                "run_id": 0,
                "target_item": result.target_item.name,
                "target_exterior": result.target_exterior.value,
                "target_collection": result.target_item.collection,
                "target_rarity": int(result.target_item.rarity),
                "target_rarity_name": result.target_item.rarity.name,
                "roi": result.roi,
                "roi_percent": result.roi_percent,
                "expected_profit": result.expected_profit,
                "expected_revenue": result.expected_revenue,
                "total_cost": result.total_cost,
                "target_probability": result.target_probability,
                "planned_average_metric": result.planned_average_metric,
                "formula_signature": result.formula_signature,
                "fee_rate": result.fee_rate,
                "collection_counts": result.formula.collection_counts,
                "materials": [
                    {
                        "item_name": pricing.item.name,
                        "collection": pricing.item.collection,
                        "count": pricing.count,
                        "requested_exterior": pricing.requested_exterior.value,
                        "min_float": pricing.min_float,
                        "max_float": pricing.max_float,
                        "estimated_float": pricing.estimated_float,
                        "adjusted_float": pricing.adjusted_float,
                        "market_name": pricing.market_name,
                        "unit_price": pricing.unit_price,
                        "total_price": pricing.total_price,
                        "float_source": pricing.float_source,
                        "float_source_label": pricing.float_source_label,
                        "float_verified": pricing.float_verified,
                        "requires_float_verification": pricing.requires_float_verification,
                    }
                    for pricing in result.material_pricings
                ],
                "outcomes": [
                    {
                        "item_name": pricing.item.name,
                        "collection": pricing.item.collection,
                        "probability": pricing.probability,
                        "output_float": pricing.output_float,
                        "exterior": pricing.exterior.value,
                        "market_name": pricing.market_name,
                        "market_price": pricing.market_price,
                        "net_sale_price": pricing.net_sale_price,
                        "expected_revenue_contribution": pricing.expected_revenue_contribution,
                    }
                    for pricing in result.outcome_pricings
                ],
                "created_at": "",
            },
        )()
    )


def run_dev_server(
    *,
    host: str = "127.0.0.1",
    port: int = 5000,
    catalog_path: str | Path = DEFAULT_CATALOG_PATH,
    price_snapshot_path: str | Path = DEFAULT_PRICE_SNAPSHOT_PATH,
    scan_result_path: str | Path = DEFAULT_SCAN_RESULT_PATH,
    crawl_log_path: str | Path | None = None,
    crawl_worker_log_dir: str | Path | None = None,
    debug: bool = True,
) -> Flask:
    app = create_app(
        catalog_path=catalog_path,
        price_snapshot_path=price_snapshot_path,
        scan_result_path=scan_result_path,
        crawl_log_path=crawl_log_path,
        crawl_worker_log_dir=crawl_worker_log_dir,
    )
    app.run(host=host, port=port, debug=debug)
    return app


__all__ = [
    "DEFAULT_CATALOG_PATH",
    "DEFAULT_PRICE_SNAPSHOT_PATH",
    "DEFAULT_SCAN_RESULT_PATH",
    "BackgroundJob",
    "BackgroundJobManager",
    "create_app",
    "run_dev_server",
]
