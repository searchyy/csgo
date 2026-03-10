from pathlib import Path

from cs2_tradeup import SteamDTMarketAPI, SteamDTPriceSnapshotStore, crawl_all_steamdt_market_to_sqlite


def main() -> None:
    store = SteamDTPriceSnapshotStore(Path("data") / "steamdt_prices.sqlite")
    client = SteamDTMarketAPI()

    summary = crawl_all_steamdt_market_to_sqlite(
        store,
        steamdt_client=client,
        max_pages=None,
        scroll_pause_ms=2500,
        idle_scroll_limit=3,
    )
    print(summary)
    print("Total cached rows:", store.count_snapshots())

    client.close()


if __name__ == "__main__":
    main()
