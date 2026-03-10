from pathlib import Path

from cs2_tradeup import (
    CachedSteamDTMarketAPI,
    Exterior,
    MultiMarketPriceManager,
    SteamDTMarketAPI,
    SteamDTPriceSnapshotStore,
    TradeUpScanner,
    WatchlistTarget,
    export_scan_results_csv,
    format_scan_results,
)


def build_scanner() -> tuple[TradeUpScanner, CachedSteamDTMarketAPI]:
    from cs2_tradeup import ItemCatalog

    # 先运行 `examples/static_catalog_sync_example.py`，
    # 再从 `data/items.json` 读取完整静态饰品数据库。
    catalog = ItemCatalog.from_path("data/items.json")
    snapshot_store = SteamDTPriceSnapshotStore(Path("data") / "steamdt_prices.sqlite")
    live_client = SteamDTMarketAPI()
    cached_client = CachedSteamDTMarketAPI(
        snapshot_store,
        steamdt_client=live_client,
        max_age_seconds=6 * 3600,
        write_back_on_fetch=True,
    )
    price_manager = MultiMarketPriceManager(
        {
            "SteamDTCache": cached_client,
        },
        max_workers=4,
    )
    scanner = TradeUpScanner(catalog=catalog, price_manager=price_manager, fee_rate=0.025)
    return scanner, cached_client


def main() -> None:
    scanner, cached_client = build_scanner()
    cached_client.warm_query_cache("AK-47")
    cached_client.warm_query_cache("M4A4")

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
    export_scan_results_csv(results, Path("output") / "steamdt_scan_results.csv")
    cached_client.close()


if __name__ == "__main__":
    main()
