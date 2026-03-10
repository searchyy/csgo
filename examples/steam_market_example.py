from pathlib import Path

from cs2_tradeup import (
    CachedSteamMarketAPI,
    RandomizedRateLimiter,
    SteamMarketAPI,
    SteamPriceSnapshotStore,
)


def main() -> None:
    output_dir = Path("output")
    snapshot_store = SteamPriceSnapshotStore(output_dir / "steam_prices.sqlite")

    live_client = SteamMarketAPI(
        cookie_string="steamLoginSecure=YOUR_COOKIE",
        rate_limiter=RandomizedRateLimiter(2.0, 4.0),
        country="US",
        currency=1,
    )
    cached_client = CachedSteamMarketAPI(
        snapshot_store,
        steam_client=live_client,
        max_age_seconds=6 * 3600,
        write_back_on_fetch=True,
    )

    quote = cached_client.get_item_price("AK-47 | Slate", "Field-Tested")
    print("Steam quote:", quote)

    entries = cached_client.crawl_and_cache_search_results(query="", count=100, max_pages=1)
    print(f"Crawled and cached {len(entries)} market rows")
    print(f"SQLite rows: {snapshot_store.count_rows()}")

    live_client.export_search_entries_json(entries, output_dir / "steam_snapshot.json")
    live_client.export_search_entries_csv(entries, output_dir / "steam_snapshot.csv")


if __name__ == "__main__":
    main()
