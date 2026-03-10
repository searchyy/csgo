from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from pathlib import Path

from cs2_tradeup import (
    build_steamdt_crawl_worker_profiles,
    crawl_catalog_item_prices_multiworker_to_sqlite,
    crawl_catalog_item_prices_to_sqlite,
)


@dataclass(frozen=True, slots=True)
class ProxySpec:
    server: str
    username: str | None = None
    password: str | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crawl SteamDT prices into SQLite cache.")
    parser.add_argument("--catalog", default="data/items.sqlite")
    parser.add_argument("--snapshot-store", default="data/steamdt_prices.sqlite")
    parser.add_argument("--max-items", type=int, default=None)
    parser.add_argument("--sleep-min", type=float, default=1.5)
    parser.add_argument("--sleep-max", type=float, default=2.5)
    parser.add_argument("--batch-size", type=int, default=15)
    parser.add_argument("--batch-cooldown-min", type=float, default=10.0)
    parser.add_argument("--batch-cooldown-max", type=float, default=18.0)
    parser.add_argument("--retry-attempts", type=int, default=2)
    parser.add_argument("--failure-backoff-base", type=float, default=12.0)
    parser.add_argument("--skip-recent-seconds", type=float, default=6 * 3600)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--user-agent", action="append", default=[])
    parser.add_argument("--proxy", action="append", default=[])
    parser.add_argument("--locale", action="append", default=[])
    parser.add_argument("--timezone-id", action="append", default=[])
    parser.add_argument("--worker-log-dir", default=None)
    parser.add_argument("--supervisor-restart-limit", type=int, default=2)
    parser.add_argument("--supervisor-backoff-base", type=float, default=15.0)
    parser.add_argument("--log-file", default=None)
    return parser


def _parse_proxy_args(values: list[str]) -> tuple[ProxySpec, ...]:
    proxies: list[ProxySpec] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        parts = [part.strip() for part in text.split("|")]
        if len(parts) == 1:
            proxies.append(ProxySpec(server=parts[0]))
        elif len(parts) == 3:
            proxies.append(ProxySpec(server=parts[0], username=parts[1] or None, password=parts[2] or None))
        else:
            raise ValueError("Proxy format must be `server` or `server|username|password`")
    return tuple(proxies)


def main() -> None:
    args = build_parser().parse_args()
    start = time.time()
    log_file = Path(args.log_file) if args.log_file else None

    def emit(message: str) -> None:
        if log_file is None:
            print(message, flush=True)
        if log_file is not None:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            with log_file.open("a", encoding="utf-8") as handle:
                handle.write(message + "\n")

    emit(f"START {time.strftime('%Y-%m-%d %H:%M:%S')}")

    def progress(index: int, total: int, message: str) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        emit(f"[{now}] [{index}/{total}] {message}")

    try:
        common_kwargs = {
            "catalog": Path(args.catalog),
            "snapshot_store": Path(args.snapshot_store),
            "max_items": args.max_items,
            "include_normal": True,
            "include_stattrak": True,
            "sleep_min_seconds": args.sleep_min,
            "sleep_max_seconds": args.sleep_max,
            "batch_size": args.batch_size,
            "batch_cooldown_min_seconds": args.batch_cooldown_min,
            "batch_cooldown_max_seconds": args.batch_cooldown_max,
            "retry_attempts": args.retry_attempts,
            "failure_backoff_base_seconds": args.failure_backoff_base,
            "skip_recent_seconds": args.skip_recent_seconds,
            "progress_callback": progress,
        }
        if args.workers <= 1:
            summary = crawl_catalog_item_prices_to_sqlite(**common_kwargs)
        else:
            proxy_specs = _parse_proxy_args(args.proxy)
            worker_profiles = build_steamdt_crawl_worker_profiles(
                args.workers,
                user_agents=args.user_agent or None,
                proxy_servers=tuple(proxy.server for proxy in proxy_specs) or None,
                proxy_credentials=tuple((proxy.username, proxy.password) for proxy in proxy_specs) or None,
                locales=args.locale or None,
                timezone_ids=args.timezone_id or None,
            )
            summary = crawl_catalog_item_prices_multiworker_to_sqlite(
                **common_kwargs,
                worker_count=args.workers,
                worker_profiles=worker_profiles,
                worker_log_dir=args.worker_log_dir,
                supervisor_restart_limit=args.supervisor_restart_limit,
                supervisor_backoff_base_seconds=args.supervisor_backoff_base,
            )
        emit(f"SUMMARY {summary}")
        emit(f"ELAPSED_SECONDS {round(time.time() - start, 2)}")
        emit(f"END {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as exc:
        import traceback

        emit(f"ERROR {exc!r}")
        emit(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
