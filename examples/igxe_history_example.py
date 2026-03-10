from pathlib import Path

from cs2_tradeup import (
    IGXECachedPriceAPI,
    IGXETransactionHistoryCrawler,
    RandomizedRateLimiter,
    TransactionHistoryStore,
    TransactionHistorySyncService,
    TrackedGoods,
)


def main() -> None:
    store = TransactionHistoryStore(Path("data") / "history.sqlite")

    # `igxe_goods_id` 需要你自己提前准备好。
    # 一个饰品对应一个 IGXE 商品 ID。
    store.upsert_tracked_goods(
        [
            TrackedGoods(
                item_name="AK-47 | Slate (Field-Tested)",
                igxe_goods_id=571769,
            )
        ]
    )

    sync_service = TransactionHistorySyncService(
        store,
        igxe_crawler=IGXETransactionHistoryCrawler(
            rate_limiter=RandomizedRateLimiter(2.0, 5.0),
        ),
    )

    summary = sync_service.sync_tracked_goods(
        platforms=("IGXE",),
        write_price_snapshots=True,
    )
    print("IGXE sync summary:", summary)
    print("IGXE snapshot rows:", store.count_price_snapshots())

    market_api = IGXECachedPriceAPI(
        store,
        max_age_seconds=30 * 86400,
        market_name="IGXE",
    )
    quote = market_api.get_item_price("AK-47 | Slate", "Field-Tested")
    print("IGXE cached quote:", quote)

    recent_records = store.get_recent_records(
        "AK-47 | Slate",
        "Field-Tested",
        platforms=["IGXE"],
        limit=5,
    )
    for record in recent_records:
        print(record)


if __name__ == "__main__":
    main()
