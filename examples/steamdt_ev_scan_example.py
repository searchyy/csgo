from pathlib import Path

from cs2_tradeup import format_scan_results, scan_steamdt_tradeup_candidates


def main() -> None:
    summary = scan_steamdt_tradeup_candidates(
        catalog=Path("data") / "items.sqlite",
        snapshot_store=Path("data") / "steamdt_prices.sqlite",
        item_names=(
            "AK-47 | Redline",
            "AK-47 | Case Hardened",
            "USP-S | Monster Mashup",
            "M4A1-S | Hyper Beast",
        ),
        cached_exteriors_only=False,
        default_exteriors=("Factory New", "Minimal Wear", "Field-Tested"),
        cache_only=False,
        max_cache_age_seconds=24 * 3600,
        roi_threshold=1.05,
        formula_limit_per_target=20,
        formula_options={
            "min_target_count": 2,
            "max_target_count": 4,
            "max_auxiliary_collections": 2,
            "max_formulas": 20,
        },
        max_targets=12,
        output_csv_path=Path("output") / "steamdt_real_ev_candidates.csv",
    )

    print(
        f"Scanned {summary.targets_scanned} targets, "
        f"found {summary.results_found} candidates."
    )
    print(format_scan_results(summary.results[:20]))
    if summary.output_csv_path:
        print("CSV:", summary.output_csv_path)


if __name__ == "__main__":
    main()
