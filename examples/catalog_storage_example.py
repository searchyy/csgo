from pathlib import Path

from cs2_tradeup import ItemCatalog


def main() -> None:
    source_path = Path("data") / "items.json"
    sqlite_path = Path("data") / "items.sqlite"

    catalog = ItemCatalog.from_path(source_path)
    catalog.to_sqlite(sqlite_path)

    reloaded = ItemCatalog.from_path(sqlite_path)
    print(f"Loaded {len(reloaded.all_items())} items from {sqlite_path}")


if __name__ == "__main__":
    main()
