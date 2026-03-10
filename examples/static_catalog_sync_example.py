from pathlib import Path

from cs2_tradeup import sync_bymykel_static_catalog


def main() -> None:
    summary = sync_bymykel_static_catalog(
        output_json_path=Path("data") / "items.json",
        output_sqlite_path=Path("data") / "items.sqlite",
    )
    print("Static catalog sync summary:", summary)


if __name__ == "__main__":
    main()
