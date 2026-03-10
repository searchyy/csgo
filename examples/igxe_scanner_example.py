from pathlib import Path

from cs2_tradeup import (
    Exterior,
    IGXECachedPriceAPI,
    MultiMarketPriceManager,
    TradeUpScanner,
    TransactionHistoryStore,
    WatchlistTarget,
    export_scan_results_csv,
    format_scan_results,
)


def build_scanner() -> TradeUpScanner:
    from cs2_tradeup import ItemCatalog

    catalog = ItemCatalog.from_path("data/items.json")
    history_store = TransactionHistoryStore(Path("data") / "history.sqlite")
    igxe_market_api = IGXECachedPriceAPI(
        history_store,
        max_age_seconds=6 * 3600,
        market_name="IGXE",
    )

    price_manager = MultiMarketPriceManager(
        {
            "IGXE": igxe_market_api,
        },
        max_workers=4,
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

    results = scanner.scan_targets(
        targets,
        roi_threshold=1.05,
        formula_limit_per_target=30,
    )
    print(format_scan_results(results))
    export_scan_results_csv(results, Path("output") / "igxe_scan_results.csv")


if __name__ == "__main__":
    main()
