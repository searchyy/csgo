from __future__ import annotations

import sqlite3
import sys
import time
import traceback
from pathlib import Path
from typing import Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from cs2_tradeup.price_crawl import (  # noqa: E402
    build_steamdt_crawl_worker_profiles,
    crawl_catalog_item_prices_multiworker_to_sqlite,
)
from cs2_tradeup.scan_storage import TradeUpScanResultStore  # noqa: E402
from cs2_tradeup.steamdt_market import SteamDTPriceSnapshotStore  # noqa: E402
from cs2_tradeup.steamdt_scan import scan_steamdt_tradeup_candidates  # noqa: E402


CATALOG_PATH = REPO_ROOT / "data" / "items.sqlite"
PRICE_DB_PATH = REPO_ROOT / "data" / "steamdt_prices.sqlite"
SCAN_DB_PATH = REPO_ROOT / "data" / "scan_results.sqlite"
OUTPUT_DIR = REPO_ROOT / "output"
CRAWL_LOG_PATH = OUTPUT_DIR / "full_price_resume_supervised.log"
WORKER_LOG_DIR = OUTPUT_DIR / "full_price_resume_workers"
EV_LOG_PATH = OUTPUT_DIR / "full_price_resume_ev.log"
EV_CSV_PATH = OUTPUT_DIR / "ev_ranking_full_latest.csv"

WORKER_COUNT = 2
SKIP_RECENT_SECONDS = 7 * 24 * 3600
WAIT_HEARTBEAT_SECONDS = 60


def _ensure_clean_log(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def _emit_line(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\n")


def _reset_worker_logs(worker_log_dir: Path) -> None:
    worker_log_dir.mkdir(parents=True, exist_ok=True)
    for log_path in worker_log_dir.glob("steamdt_worker_*.log"):
        try:
            log_path.unlink()
        except OSError:
            log_path.write_text("", encoding="utf-8")


def _timestamp() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _crawl_progress(index: int, total: int, message: str) -> None:
    _emit_line(CRAWL_LOG_PATH, f"[{_timestamp()}] [{index}/{total}] {message}")


def _ev_progress(index: int, total: int, message: str) -> None:
    _emit_line(EV_LOG_PATH, f"[{_timestamp()}] [{index}/{total}] {message}")


def _contains_quota_text(message: str) -> bool:
    text = str(message or "")
    return "访问上限" in text or "访问次数超限" in text or "今日访问次数超限" in text


def _next_retry_epoch() -> float:
    now = time.time()
    local_now = time.localtime(now)
    tomorrow_midnight = time.mktime(
        (
            local_now.tm_year,
            local_now.tm_mon,
            local_now.tm_mday + 1,
            0,
            10,
            0,
            0,
            0,
            -1,
        )
    )
    return max(now + 15 * 60, tomorrow_midnight)


def _wait_until_retry(target_epoch: float) -> None:
    while True:
        remaining = target_epoch - time.time()
        if remaining <= 0:
            return
        minutes = int(max(1, round(remaining / 60)))
        _emit_line(
            CRAWL_LOG_PATH,
            f"[{_timestamp()}] [0/{WORKER_COUNT}] SteamDT 配额等待中，约 {minutes} 分钟后自动重试",
        )
        time.sleep(min(WAIT_HEARTBEAT_SECONDS, max(5.0, remaining)))


def _run_full_ev_rescan() -> Mapping[str, object]:
    _ensure_clean_log(EV_LOG_PATH)
    _emit_line(EV_LOG_PATH, f"START {_timestamp()}")
    start = time.time()

    price_store = SteamDTPriceSnapshotStore(PRICE_DB_PATH)
    clean_summary = price_store.refresh_cleaned_prices()
    _emit_line(
        EV_LOG_PATH,
        (
            "CLEANED "
            f"rows={clean_summary.total_rows} "
            f"valid={clean_summary.valid_rows} "
            f"invalid={clean_summary.invalid_rows} "
            f"variant_excluded={clean_summary.variant_excluded_rows}"
        ),
    )

    summary = scan_steamdt_tradeup_candidates(
        catalog=CATALOG_PATH,
        snapshot_store=PRICE_DB_PATH,
        cache_only=True,
        roi_threshold=0.0,
        formula_limit_per_target=25,
        cached_exteriors_only=True,
        output_csv_path=EV_CSV_PATH,
        prefer_safe_price=True,
        require_valid_prices=True,
        normal_tradeup_only=True,
        conservative_float_mode=True,
        progress_callback=_ev_progress,
    )

    store = TradeUpScanResultStore(SCAN_DB_PATH)
    with sqlite3.connect(SCAN_DB_PATH) as connection:
        connection.execute("DELETE FROM scan_results")
        connection.execute("DELETE FROM scan_runs")
        connection.commit()

    run_id = store.create_run(
        run_type="ev_scan_full_cache",
        parameters={
            "cache_only": True,
            "roi_threshold": 0.0,
            "formula_limit_per_target": 25,
            "cached_exteriors_only": True,
            "prefer_safe_price": True,
            "require_valid_prices": True,
            "normal_tradeup_only": True,
            "conservative_float_mode": True,
            "source_price_db": str(PRICE_DB_PATH),
            "output_csv_path": str(EV_CSV_PATH),
            "trigger": "post_resume_crawl",
        },
    )
    inserted = store.append_results(run_id, summary.results)
    result_summary = {
        "targets_scanned": summary.targets_scanned,
        "results_found": summary.results_found,
        "output_csv_path": summary.output_csv_path,
        "cleaned_total_rows": clean_summary.total_rows,
        "cleaned_valid_rows": clean_summary.valid_rows,
        "cleaned_invalid_rows": clean_summary.invalid_rows,
        "variant_excluded_rows": clean_summary.variant_excluded_rows,
        "elapsed_seconds": round(time.time() - start, 2),
        "inserted_results": inserted,
    }
    store.complete_run(run_id, status="completed", summary=result_summary)
    _emit_line(EV_LOG_PATH, f"SUMMARY run_id={run_id} {result_summary}")
    _emit_line(EV_LOG_PATH, f"END {_timestamp()}")
    return {"run_id": run_id, **result_summary}


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _ensure_clean_log(CRAWL_LOG_PATH)
    _emit_line(CRAWL_LOG_PATH, f"START {_timestamp()}")

    start = time.time()
    try:
        attempt = 0
        while True:
            attempt += 1
            _reset_worker_logs(WORKER_LOG_DIR)
            _emit_line(CRAWL_LOG_PATH, f"ATTEMPT {attempt} {_timestamp()}")
            worker_profiles = build_steamdt_crawl_worker_profiles(
                WORKER_COUNT,
                headless=True,
                rate_limit_min_seconds=1.2,
                rate_limit_max_seconds=2.4,
                rate_limit_step_seconds=0.35,
            )
            attempt_state = {"quota_hit": False}

            def crawl_progress(index: int, total: int, message: str) -> None:
                if _contains_quota_text(message):
                    attempt_state["quota_hit"] = True
                _crawl_progress(index, total, message)

            summary = crawl_catalog_item_prices_multiworker_to_sqlite(
                catalog=CATALOG_PATH,
                snapshot_store=PRICE_DB_PATH,
                include_normal=True,
                include_stattrak=True,
                sleep_min_seconds=1.2,
                sleep_max_seconds=2.4,
                batch_size=15,
                batch_cooldown_min_seconds=10.0,
                batch_cooldown_max_seconds=18.0,
                retry_attempts=2,
                failure_backoff_base_seconds=12.0,
                skip_recent_seconds=SKIP_RECENT_SECONDS,
                worker_count=WORKER_COUNT,
                worker_profiles=worker_profiles,
                worker_log_dir=WORKER_LOG_DIR,
                supervisor_restart_limit=3,
                supervisor_backoff_base_seconds=20.0,
                progress_callback=crawl_progress,
            )

            if attempt_state["quota_hit"]:
                retry_epoch = _next_retry_epoch()
                _emit_line(
                    CRAWL_LOG_PATH,
                    (
                        f"[{_timestamp()}] [0/{WORKER_COUNT}] "
                        f"SteamDT 日配额已用尽，计划在 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(retry_epoch))} 自动续跑"
                    ),
                )
                _wait_until_retry(retry_epoch)
                continue

            _emit_line(CRAWL_LOG_PATH, f"SUMMARY {_timestamp()}")
            _emit_line(CRAWL_LOG_PATH, f"DETAIL {summary}")
            _emit_line(CRAWL_LOG_PATH, f"ELAPSED_SECONDS {round(time.time() - start, 2)}")
            _emit_line(CRAWL_LOG_PATH, f"END {_timestamp()}")

            ev_summary = _run_full_ev_rescan()
            _emit_line(CRAWL_LOG_PATH, f"POST_EV {ev_summary}")
            break
    except Exception as exc:
        _emit_line(CRAWL_LOG_PATH, f"ERROR {exc!r}")
        _emit_line(CRAWL_LOG_PATH, traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
