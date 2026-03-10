from pathlib import Path

from cs2_tradeup import (
    CachedSteamDTMarketAPI,
    SteamDTMarketAPI,
    SteamDTPriceSnapshotStore,
    sniff_steamdt_market_exchange,
)


def main() -> None:
    exchange = sniff_steamdt_market_exchange("AK-47 | Redline")
    print("Captured endpoint:", exchange.url)
    print("Request method:", exchange.method)
    print("Response status:", exchange.status)

    store = SteamDTPriceSnapshotStore(Path("data") / "steamdt_prices.sqlite")
    live_client = SteamDTMarketAPI()
    cached_client = CachedSteamDTMarketAPI(
        store,
        steamdt_client=live_client,
        max_age_seconds=6 * 3600,
        write_back_on_fetch=True,
    )

    page = cached_client.warm_query_cache("AK-47 | Redline")
    print("Search total:", page.total)
    print("Cached snapshot rows:", store.count_snapshots())
    for item in page.items[:5]:
        print(item.market_hash_name, item.choose_selling_price(live_client.preferred_platforms))

    quote = cached_client.get_item_price("AK-47 | Redline", "Field-Tested")
    print("Cached quote:", quote)

    cached_client.close()


if __name__ == "__main__":
    main()
