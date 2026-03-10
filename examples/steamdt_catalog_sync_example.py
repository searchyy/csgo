from pathlib import Path

from cs2_tradeup import sync_bymykel_static_catalog, sync_steamdt_items_to_catalog


def main() -> None:
    static_summary = sync_bymykel_static_catalog(
        output_json_path=Path("data") / "items.json",
        output_sqlite_path=Path("data") / "items.sqlite",
    )
    print("Static catalog sync summary:", static_summary)

    summary = sync_steamdt_items_to_catalog(
        snapshot_store=Path("data") / "steamdt_prices.sqlite",
        base_catalog=Path("data") / "items.sqlite",
        output_json_path=Path("data") / "items.json",
        output_sqlite_path=Path("data") / "items.sqlite",
        item_limit=20,
        discovery_max_pages=10,
    )
    print("SteamDT catalog sync summary:", summary)


if __name__ == "__main__":
    main()
