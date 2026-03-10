from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cs2_tradeup import PriceAnomalyDetector, PriceAnomalyDetectorConfig


class PriceAnomalyDetectorTests(unittest.TestCase):
    def build_connection(self) -> sqlite3.Connection:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        connection = sqlite3.connect(Path(temp_dir.name) / "prices.sqlite")
        self.addCleanup(connection.close)
        connection.execute(
            """
            CREATE TABLE market_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_name TEXT NOT NULL,
                exterior TEXT NOT NULL,
                sell_price REAL,
                buy_price REAL,
                volume_24h INTEGER,
                is_souvenir INTEGER,
                is_tradeup_compatible_normal INTEGER,
                variant_filter_reason TEXT
            )
            """
        )
        connection.commit()
        return connection

    def test_clean_prices_applies_spread_circuit_breaker_and_liquidity_guard(self) -> None:
        connection = self.build_connection()
        connection.executemany(
            """
            INSERT INTO market_prices (item_name, exterior, sell_price, buy_price, volume_24h)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("Cold Illiquid Item", "Well-Worn", 16000.0, 5000.0, 1),
                ("Reference Item", "Field-Tested", 5200.0, 5000.0, 18),
            ],
        )
        connection.commit()

        detector = PriceAnomalyDetector(
            PriceAnomalyDetectorConfig(
                source_table="market_prices",
                target_table="market_prices_cleaned",
            )
        )
        summary = detector.clean_prices(connection)

        row = connection.execute(
            """
            SELECT safe_price, is_valid, risk_level, anomaly_flags, anomaly_notes
            FROM market_prices_cleaned
            WHERE item_name = ? AND exterior = ?
            """,
            ("Cold Illiquid Item", "Well-Worn"),
        ).fetchone()

        self.assertEqual(summary.total_rows, 2)
        self.assertEqual(summary.invalid_rows, 1)
        self.assertEqual(summary.spread_flagged_rows, 1)
        self.assertEqual(summary.low_liquidity_flagged_rows, 1)
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row[0], 5500.0)
        self.assertEqual(row[1], 0)
        self.assertEqual(row[2], "high")
        self.assertIn("spread", row[3])
        self.assertIn("low_liquidity", row[3])

    def test_clean_prices_corrects_exterior_inversion_from_ft_reference(self) -> None:
        connection = self.build_connection()
        connection.executemany(
            """
            INSERT INTO market_prices (item_name, exterior, sell_price, buy_price, volume_24h)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("USP-S | Example", "Field-Tested", 100.0, 95.0, 40),
                ("USP-S | Example", "Minimal Wear", 120.0, 110.0, 30),
                ("USP-S | Example", "Well-Worn", 170.0, 160.0, 22),
            ],
        )
        connection.commit()

        detector = PriceAnomalyDetector(
            PriceAnomalyDetectorConfig(
                source_table="market_prices",
                target_table="market_prices_cleaned",
            )
        )
        summary = detector.clean_prices(connection)

        row = connection.execute(
            """
            SELECT safe_price, is_valid, risk_level, anomaly_flags
            FROM market_prices_cleaned
            WHERE item_name = ? AND exterior = ?
            """,
            ("USP-S | Example", "Well-Worn"),
        ).fetchone()

        self.assertEqual(summary.total_rows, 3)
        self.assertEqual(summary.exterior_flagged_rows, 1)
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row[0], 100.0)
        self.assertEqual(row[1], 1)
        self.assertEqual(row[2], "high")
        self.assertIn("exterior_inversion", row[3])

    def test_clean_prices_marks_special_items_invalid_for_tradeup(self) -> None:
        connection = self.build_connection()
        connection.executemany(
            """
            INSERT INTO market_prices (item_name, exterior, sell_price, buy_price, volume_24h)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("Souvenir P90 | Facility Negative", "Minimal Wear", 1.53, None, 999),
                ("StatTrak™ AK-47 | Redline", "Minimal Wear", 500.0, 480.0, 40),
                ("★ Karambit | Doppler", "Factory New", 9999.0, 9800.0, 50),
                ("AK-47 | Redline", "Minimal Wear", 520.0, 500.0, 50),
            ],
        )
        connection.commit()

        detector = PriceAnomalyDetector(
            PriceAnomalyDetectorConfig(
                source_table="market_prices",
                target_table="market_prices_cleaned",
            )
        )
        summary = detector.clean_prices(connection)

        rows = connection.execute(
            """
            SELECT item_name, variant_name, is_tradeup_compatible_normal, is_valid,
                   variant_filter_reason, anomaly_flags
            FROM market_prices_cleaned
            ORDER BY id ASC
            """
        ).fetchall()

        self.assertEqual(summary.variant_excluded_rows, 3)
        self.assertEqual(rows[0][1], "Normal")
        self.assertEqual(rows[0][2], 0)
        self.assertEqual(rows[0][3], 0)
        self.assertEqual(rows[0][4], "souvenir")
        self.assertIn("souvenir_excluded", rows[0][5])
        self.assertEqual(rows[1][1], "StatTrak")
        self.assertEqual(rows[1][2], 0)
        self.assertEqual(rows[1][3], 0)
        self.assertEqual(rows[1][4], "stattrak")
        self.assertIn("stattrak_excluded_for_normal", rows[1][5])
        self.assertEqual(rows[2][2], 0)
        self.assertEqual(rows[2][3], 0)
        self.assertEqual(rows[2][4], "star")
        self.assertIn("star_item_excluded", rows[2][5])
        self.assertEqual(rows[3][2], 1)
        self.assertEqual(rows[3][3], 1)

    def test_clean_prices_uses_source_souvenir_flag_when_name_is_normalized(self) -> None:
        connection = self.build_connection()
        connection.executemany(
            """
            INSERT INTO market_prices (
                item_name, exterior, sell_price, buy_price, volume_24h,
                is_souvenir, is_tradeup_compatible_normal, variant_filter_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("P90 | Facility Negative", "Minimal Wear", 1.53, None, 999, 1, 0, "souvenir"),
                ("P90 | Facility Negative", "Minimal Wear", 10.8, 10.2, 40, 0, 1, None),
            ],
        )
        connection.commit()

        detector = PriceAnomalyDetector(
            PriceAnomalyDetectorConfig(
                source_table="market_prices",
                target_table="market_prices_cleaned",
            )
        )
        summary = detector.clean_prices(connection)

        rows = connection.execute(
            """
            SELECT sell_price, is_tradeup_compatible_normal, is_valid, variant_filter_reason, anomaly_flags
            FROM market_prices_cleaned
            WHERE item_name = ?
            ORDER BY id ASC
            """,
            ("P90 | Facility Negative",),
        ).fetchall()

        self.assertEqual(summary.variant_excluded_rows, 1)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 1.53)
        self.assertEqual(rows[0][1], 0)
        self.assertEqual(rows[0][2], 0)
        self.assertEqual(rows[0][3], "souvenir")
        self.assertIn("souvenir_excluded", rows[0][4])
        self.assertEqual(rows[1][1], 1)
        self.assertEqual(rows[1][2], 1)

    def test_clean_prices_invalidates_abnormally_low_prices(self) -> None:
        connection = self.build_connection()
        connection.executemany(
            """
            INSERT INTO market_prices (item_name, exterior, sell_price, buy_price, volume_24h)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("P250 | Sand Dune", "Factory New", 0.05, None, 100),
                ("P250 | Sand Dune", "Minimal Wear", 0.11, None, 100),
            ],
        )
        connection.commit()

        detector = PriceAnomalyDetector(
            PriceAnomalyDetectorConfig(
                source_table="market_prices",
                target_table="market_prices_cleaned",
            )
        )
        summary = detector.clean_prices(connection)

        rows = connection.execute(
            """
            SELECT exterior, safe_price, is_valid, anomaly_flags
            FROM market_prices_cleaned
            WHERE item_name = ?
            ORDER BY id ASC
            """,
            ("P250 | Sand Dune",),
        ).fetchall()

        self.assertEqual(summary.invalid_rows, 1)
        self.assertEqual(rows[0][0], "Factory New")
        self.assertAlmostEqual(rows[0][1], 0.05)
        self.assertEqual(rows[0][2], 0)
        self.assertIn("price_too_low", rows[0][3])
        self.assertEqual(rows[1][2], 1)


if __name__ == "__main__":
    unittest.main()
