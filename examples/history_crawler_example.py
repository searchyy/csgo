from pathlib import Path

from cs2_tradeup import (
    BuffTransactionHistoryCrawler,
    C5TransactionHistoryCrawler,
    IGXETransactionHistoryCrawler,
    RandomizedRateLimiter,
    TrackedGoods,
    TransactionHistoryPriceAPI,
    TransactionHistoryStore,
    TransactionHistorySyncService,
)


def main() -> None:
    store = TransactionHistoryStore(Path("data") / "history.sqlite")
    store.upsert_tracked_goods(
        [
            TrackedGoods(
                item_name="AK-47 | Slate (Field-Tested)",
                buff_goods_id=42192,
                c5_goods_id=553370868,
                igxe_goods_id=571769,
            )
        ]
    )

    sync_service = TransactionHistorySyncService(
        store,
        buff_crawler=BuffTransactionHistoryCrawler(
            cookie_string="_ntes_nnid=YOUR_BUFF_COOKIE",
            headers={"Referer": "https://buff.163.com/market/csgo"},
            rate_limiter=RandomizedRateLimiter(2.0, 5.0),
        ),
        c5_crawler=C5TransactionHistoryCrawler(
            cookie_string="YOUR_C5_COOKIE",
            headers={"Referer": "https://www.c5game.com/"},
            rate_limiter=RandomizedRateLimiter(4.0, 8.0),
        ),
        igxe_crawler=IGXETransactionHistoryCrawler(
            rate_limiter=RandomizedRateLimiter(2.0, 5.0),
        ),
    )

    summary = sync_service.sync_tracked_goods(include_c5_detail=True)
    print("Sync summary:", summary)

    history_api = TransactionHistoryPriceAPI(
        store,
        platforms=["BUFF", "C5", "IGXE"],
        lookback_days=7,
        market_name="HistoryAverage",
    )
    print(history_api.get_item_price("AK-47 | Slate", "Field-Tested"))


if __name__ == "__main__":
    main()
