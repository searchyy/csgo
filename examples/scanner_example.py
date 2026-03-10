from pathlib import Path

from cs2_tradeup import (
    BuffMarketAPI,
    CachedSteamMarketAPI,
    Exterior,
    MultiMarketPriceManager,
    RandomizedRateLimiter,
    SteamMarketAPI,
    SteamPriceSnapshotStore,
    TradeUpScanner,
    TransactionHistoryPriceAPI,
    TransactionHistoryStore,
    UUMarketAPI,
    WatchlistTarget,
    export_scan_results_csv,
    format_scan_results,
)


def build_scanner() -> TradeUpScanner:
    # 先运行 `examples/static_catalog_sync_example.py`，
    # 再从 `data/items.json` 读取完整静态饰品数据库。
    from cs2_tradeup import ItemCatalog

    catalog = ItemCatalog.from_path("data/items.json")

    buff = BuffMarketAPI(
        cookie_string="session=YOUR_SESSION; csrf_token=YOUR_TOKEN",
        headers={"Referer": "https://buff.163.com/market/csgo"},
        rate_limiter=RandomizedRateLimiter(2.0, 5.0),
    )
    uu = UUMarketAPI(
        cookies={"uu_session": "YOUR_SESSION"},
        headers={"Referer": "https://www.youpin898.com/"},
        rate_limiter=RandomizedRateLimiter(2.5, 5.5),
    )

    steam_live = SteamMarketAPI(
        cookie_string="steamLoginSecure=YOUR_STEAM_COOKIE",
        rate_limiter=RandomizedRateLimiter(2.0, 4.0),
        country="US",
        currency=1,
    )
    steam_snapshot_store = SteamPriceSnapshotStore("data/steam_prices.sqlite")
    steam_cached = CachedSteamMarketAPI(
        steam_snapshot_store,
        steam_client=steam_live,
        max_age_seconds=6 * 3600,
        write_back_on_fetch=True,
    )

    history_store = TransactionHistoryStore("data/history.sqlite")
    history_average = TransactionHistoryPriceAPI(
        history_store,
        platforms=["BUFF", "C5", "IGXE"],
        lookback_days=7,
        market_name="HistoryAverage",
    )

    price_manager = MultiMarketPriceManager(
        {
            "BUFF": buff,
            "UU": uu,
            "SteamCache": steam_cached,
            "HistoryAverage": history_average,
        },
        max_workers=12,
    )
    return TradeUpScanner(catalog=catalog, price_manager=price_manager, fee_rate=0.025)


def main() -> None:
    scanner = build_scanner()
    targets = [
        WatchlistTarget(
            item_name="AK-47 | Case Hardened",
            exterior=Exterior.FACTORY_NEW,
            formula_options={
                "min_target_count": 2,
                "max_target_count": 4,
                "max_auxiliary_collections": 2,
                "max_formulas": 30,
            },
        )
    ]
    results = scanner.scan_targets(targets, roi_threshold=1.05, formula_limit_per_target=30)
    print(format_scan_results(results))
    export_scan_results_csv(results, Path("output") / "scan_results.csv")


if __name__ == "__main__":
    main()
