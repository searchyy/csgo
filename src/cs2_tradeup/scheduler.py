"""Persistent crawl scheduler.

Runs a background thread that automatically triggers price-crawl + EV-scan
cycles at a configurable interval.  State (last run times, config) is stored
in a SQLite file so it survives app restarts.

Typical usage::

    scheduler = CrawlScheduler("data/scheduler.sqlite")
    scheduler.configure(
        enabled=True,
        interval_hours=12.0,
        crawl_config={"worker_count": 1, "skip_recent_seconds": 21600},
        scan_config={"min_roi": 1.05, "conservative_float_mode": True},
    )
    scheduler.start(
        job_manager=app.job_manager,
        catalog_path="data/items.sqlite",
        snapshot_path="data/steamdt_prices.sqlite",
        scan_result_path="data/scan_results.sqlite",
    )
"""

from __future__ import annotations

import json
import sqlite3
import threading
import traceback
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from .webapp import BackgroundJob, BackgroundJobManager

# How often the scheduler loop wakes up to check whether a cycle is due.
_CHECK_INTERVAL_SECONDS = 60

# Hard timeouts per phase so a hung job doesn't block the scheduler forever.
_CRAWL_TIMEOUT_HOURS = 8
_SCAN_TIMEOUT_HOURS = 3

_DEFAULT_INTERVAL_HOURS = 12.0

_DDL = """
CREATE TABLE IF NOT EXISTS scheduler_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


class CrawlScheduler:
    """Persistent background scheduler for price-crawl + EV-scan cycles.

    The scheduler persists its state in a SQLite key-value table so that
    the "time since last run" is preserved across process restarts.

    Thread safety: all state mutations are protected by ``_lock``.  The
    public API (``configure``, ``get_state``, ``trigger``, ``start``,
    ``stop``) is safe to call from any thread.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._trigger_event = threading.Event()

        # Runtime references injected via start()
        self._job_manager: BackgroundJobManager | None = None
        self._catalog_path: str = ""
        self._snapshot_path: str = ""
        self._scan_result_path: str = ""

        self._init_db()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def configure(
        self,
        *,
        enabled: bool | None = None,
        interval_hours: float | None = None,
        crawl_config: dict[str, Any] | None = None,
        scan_config: dict[str, Any] | None = None,
    ) -> None:
        """Persist schedule configuration.  Only supplied keys are updated."""
        with self._lock:
            if enabled is not None:
                self._set("enabled", "true" if enabled else "false")
            if interval_hours is not None:
                if interval_hours <= 0:
                    raise ValueError("interval_hours must be positive")
                self._set("interval_hours", str(float(interval_hours)))
            if crawl_config is not None:
                self._set("crawl_config_json", json.dumps(crawl_config, ensure_ascii=False))
            if scan_config is not None:
                self._set("scan_config_json", json.dumps(scan_config, ensure_ascii=False))

    def get_state(self) -> dict[str, Any]:
        """Return a snapshot of the current scheduler state (thread-safe)."""
        with self._lock:
            enabled = self._get("enabled", "false") == "true"
            interval_hours = float(self._get("interval_hours", str(_DEFAULT_INTERVAL_HOURS)))
            last_started_at = self._get("last_cycle_started_at")
            last_completed_at = self._get("last_cycle_completed_at")
            last_status = self._get("last_cycle_status", "none")
            active_crawl_job_id = self._get("active_crawl_job_id")
            active_scan_job_id = self._get("active_scan_job_id")
            crawl_config = json.loads(self._get("crawl_config_json", "{}"))
            scan_config = json.loads(self._get("scan_config_json", "{}"))

        completed_dt = _parse_iso(last_completed_at)
        next_run_at: str | None = None
        seconds_until_next: float | None = None
        if enabled and completed_dt is not None:
            next_run_dt = completed_dt + timedelta(hours=interval_hours)
            next_run_at = next_run_dt.isoformat()
            seconds_until_next = max(0.0, (next_run_dt - datetime.now(timezone.utc)).total_seconds())

        return {
            "enabled": enabled,
            "interval_hours": interval_hours,
            "last_cycle_started_at": last_started_at,
            "last_cycle_completed_at": last_completed_at,
            "last_cycle_status": last_status,
            "next_run_at": next_run_at,
            "seconds_until_next_run": seconds_until_next,
            "active_crawl_job_id": active_crawl_job_id,
            "active_scan_job_id": active_scan_job_id,
            "crawl_config": crawl_config,
            "scan_config": scan_config,
        }

    def trigger(self) -> None:
        """Manually request an immediate cycle (even if not yet due)."""
        self._trigger_event.set()

    def start(
        self,
        *,
        job_manager: BackgroundJobManager,
        catalog_path: str,
        snapshot_path: str,
        scan_result_path: str,
    ) -> None:
        """Start the background scheduler thread (idempotent)."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._job_manager = job_manager
        self._catalog_path = catalog_path
        self._snapshot_path = snapshot_path
        self._scan_result_path = scan_result_path
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="crawl-scheduler",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the scheduler thread to stop (returns immediately)."""
        self._stop_event.set()
        self._trigger_event.set()  # unblock any pending wait

    # ------------------------------------------------------------------
    # Scheduler loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if self._is_due() or self._trigger_event.is_set():
                    self._trigger_event.clear()
                    self._run_cycle()
            except Exception:
                # Log but keep the scheduler alive.
                _log(f"[scheduler] Unhandled exception in cycle:\n{traceback.format_exc()}")
            # Wait for the check interval, but wake up immediately on trigger or stop.
            self._trigger_event.wait(timeout=_CHECK_INTERVAL_SECONDS)
            self._trigger_event.clear()

    def _is_due(self) -> bool:
        with self._lock:
            enabled = self._get("enabled", "false") == "true"
            if not enabled:
                return False
            interval_hours = float(self._get("interval_hours", str(_DEFAULT_INTERVAL_HOURS)))
            last_completed_at = self._get("last_cycle_completed_at")

        if last_completed_at is None:
            return True  # Never completed a cycle; run immediately.
        completed_dt = _parse_iso(last_completed_at)
        if completed_dt is None:
            return True
        elapsed = (datetime.now(timezone.utc) - completed_dt).total_seconds()
        return elapsed >= interval_hours * 3600

    def _run_cycle(self) -> None:
        _log("[scheduler] Starting crawl cycle")
        with self._lock:
            self._set("last_cycle_started_at", _now_iso())
            self._set("last_cycle_status", "running")
            self._set("active_crawl_job_id", "")
            self._set("active_scan_job_id", "")

        try:
            # ── Phase 1: price crawl ──────────────────────────────────────
            crawl_job = self._submit_crawl_job()
            with self._lock:
                self._set("active_crawl_job_id", crawl_job.id)
            _log(f"[scheduler] Crawl job submitted: {crawl_job.id}")
            self._wait_for_job(crawl_job.id, timeout_hours=_CRAWL_TIMEOUT_HOURS, label="crawl")
            crawl_final = self._job_manager.get(crawl_job.id)  # type: ignore[union-attr]
            _log(f"[scheduler] Crawl done: status={crawl_final.status}")

            if self._stop_event.is_set():
                return

            # Refresh anomaly-detection table before scanning.
            try:
                from .steamdt_market import SteamDTPriceSnapshotStore

                SteamDTPriceSnapshotStore(self._snapshot_path).refresh_cleaned_prices()
            except Exception:
                _log(f"[scheduler] refresh_cleaned_prices failed:\n{traceback.format_exc()}")

            # ── Phase 2: EV scan ──────────────────────────────────────────
            scan_job = self._submit_scan_job()
            with self._lock:
                self._set("active_scan_job_id", scan_job.id)
            _log(f"[scheduler] Scan job submitted: {scan_job.id}")
            self._wait_for_job(scan_job.id, timeout_hours=_SCAN_TIMEOUT_HOURS, label="scan")
            scan_final = self._job_manager.get(scan_job.id)  # type: ignore[union-attr]
            _log(f"[scheduler] Scan done: status={scan_final.status}")

            final_status = (
                "completed"
                if crawl_final.status == "completed" and scan_final.status == "completed"
                else "completed_with_errors"
            )
        except Exception as exc:
            final_status = f"failed: {exc}"
            _log(f"[scheduler] Cycle failed:\n{traceback.format_exc()}")
        finally:
            with self._lock:
                self._set("last_cycle_completed_at", _now_iso())
                self._set("last_cycle_status", final_status)
                self._set("active_crawl_job_id", "")
                self._set("active_scan_job_id", "")
            _log(f"[scheduler] Cycle finished: {final_status}")

    def _wait_for_job(self, job_id: str, *, timeout_hours: float, label: str) -> None:
        deadline = datetime.now(timezone.utc) + timedelta(hours=timeout_hours)
        while not self._stop_event.is_set():
            try:
                job = self._job_manager.get(job_id)  # type: ignore[union-attr]
            except KeyError:
                return
            if job.status in ("completed", "failed"):
                return
            if datetime.now(timezone.utc) > deadline:
                raise TimeoutError(
                    f"{label} job {job_id} timed out after {timeout_hours}h"
                )
            # Poll every 10 s but also respect the stop event.
            self._stop_event.wait(10)

    # ------------------------------------------------------------------
    # Job builders
    # ------------------------------------------------------------------

    def _submit_crawl_job(self) -> BackgroundJob:
        from .price_crawl import crawl_catalog_item_prices_to_sqlite

        with self._lock:
            crawl_config: dict[str, Any] = json.loads(self._get("crawl_config_json", "{}"))

        catalog_path = self._catalog_path
        snapshot_path = self._snapshot_path

        def target(progress: Callable[[int, int, str], None]) -> dict[str, Any]:
            summary = crawl_catalog_item_prices_to_sqlite(
                catalog=catalog_path,
                snapshot_store=snapshot_path,
                skip_recent_seconds=float(
                    crawl_config.get("skip_recent_seconds", 6 * 3600)
                ),
                sleep_min_seconds=float(crawl_config.get("sleep_min_seconds", 0.8)),
                sleep_max_seconds=float(crawl_config.get("sleep_max_seconds", 1.8)),
                batch_size=int(crawl_config.get("batch_size", 20)),
                batch_cooldown_min_seconds=float(
                    crawl_config.get("batch_cooldown_min_seconds", 6.0)
                ),
                batch_cooldown_max_seconds=float(
                    crawl_config.get("batch_cooldown_max_seconds", 12.0)
                ),
                retry_attempts=int(crawl_config.get("retry_attempts", 2)),
                failure_backoff_base_seconds=float(
                    crawl_config.get("failure_backoff_base_seconds", 8.0)
                ),
                progress_callback=progress,
            )
            return {
                "total_items": summary.total_items,
                "processed_items": summary.processed_items,
                "skipped_recent_items": summary.skipped_recent_items,
                "failed_items": list(summary.failed_items),
                "snapshots_inserted": summary.snapshots_inserted,
            }

        return self._job_manager.submit(  # type: ignore[union-attr]
            job_type="crawl_prices",
            title="[调度] 自动抓取枪械价格",
            target=target,
        )

    def _submit_scan_job(self) -> BackgroundJob:
        from .scan_storage import TradeUpScanResultStore
        from .steamdt_scan import scan_steamdt_tradeup_candidates

        with self._lock:
            scan_config: dict[str, Any] = json.loads(self._get("scan_config_json", "{}"))

        catalog_path = self._catalog_path
        snapshot_path = self._snapshot_path
        scan_result_path = self._scan_result_path

        run_store = TradeUpScanResultStore(scan_result_path)
        run_id = run_store.create_run(
            run_type="ev_scan_scheduled",
            parameters=scan_config,
        )

        def target(progress: Callable[[int, int, str], None]) -> dict[str, Any]:
            try:
                formula_limit = int(scan_config.get("formula_limit_per_target", 25))
                summary = scan_steamdt_tradeup_candidates(
                    catalog=catalog_path,
                    snapshot_store=snapshot_path,
                    cache_only=True,  # Scheduled scans always use cached prices.
                    roi_threshold=float(scan_config.get("min_roi", 1.05)),
                    formula_limit_per_target=formula_limit,
                    formula_options={
                        "min_target_count": int(scan_config.get("min_target_count", 2)),
                        "max_target_count": int(scan_config.get("max_target_count", 4)),
                        "max_auxiliary_collections": int(
                            scan_config.get("max_auxiliary_collections", 2)
                        ),
                        "max_formulas": formula_limit,
                    },
                    output_csv_path=None,
                    scanner_max_workers=4,
                    price_max_workers=4,
                    progress_callback=progress,
                    conservative_float_mode=bool(
                        scan_config.get("conservative_float_mode", True)
                    ),
                )
                run_store.delete_results_for_run(run_id)
                run_store.append_results(run_id, summary.results)
                payload: dict[str, Any] = {
                    "targets_scanned": summary.targets_scanned,
                    "results_found": summary.results_found,
                    "run_id": run_id,
                }
                run_store.complete_run(run_id, status="completed", summary=payload)
                return payload
            except Exception as exc:
                run_store.complete_run(
                    run_id,
                    status="failed",
                    summary={},
                    error_message=str(exc),
                )
                raise

        return self._job_manager.submit(  # type: ignore[union-attr]
            job_type="scan_ev",
            title="[调度] 自动扫描炼金 EV",
            target=target,
            metadata={"run_id": run_id},
        )

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        with closing(sqlite3.connect(str(self._db_path))) as conn:
            conn.execute(_DDL)
            conn.commit()

    def _get(self, key: str, default: str | None = None) -> str | None:
        """Read a key from the DB.  Must be called with ``_lock`` held."""
        with closing(sqlite3.connect(str(self._db_path))) as conn:
            row = conn.execute(
                "SELECT value FROM scheduler_state WHERE key = ?", (key,)
            ).fetchone()
        return row[0] if row else default

    def _set(self, key: str, value: str) -> None:
        """Write a key to the DB.  Must be called with ``_lock`` held."""
        with closing(sqlite3.connect(str(self._db_path))) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO scheduler_state (key, value) VALUES (?, ?)",
                (key, value),
            )
            conn.commit()


def _log(message: str) -> None:
    import sys

    print(message, file=sys.stderr, flush=True)
