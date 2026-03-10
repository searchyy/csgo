from pathlib import Path

from cs2_tradeup import run_dev_server


def main() -> None:
    run_dev_server(
        host="127.0.0.1",
        port=5000,
        catalog_path=Path("data") / "items.sqlite",
        price_snapshot_path=Path("data") / "steamdt_prices.sqlite",
        scan_result_path=Path("data") / "scan_results.sqlite",
        debug=True,
    )


if __name__ == "__main__":
    main()
