#!/usr/bin/env python3
"""Offline unit test suite for Metric33.

Runs with standard library unittest (`python test_metric33.py`) or pytest (`pytest -q`).
"""
from __future__ import annotations

import datetime as dt
import re
import unittest

import metric33


def _make_kline(ts_ms: int, o: float, h: float, l: float, c: float, v: float = 100.0, tb: float = 55.0) -> list:
    qv = c * v
    return [ts_ms, str(o), str(h), str(l), str(c), str(v), ts_ms + 3_599_000, str(qv), 100, str(tb), "0", "0"]


class TestMetric33(unittest.TestCase):
    def setUp(self) -> None:
        self.now = dt.datetime(2026, 11, 23, 15, 0, tzinfo=dt.timezone.utc)
        self.now_ms = int(self.now.timestamp() * 1000)

    def test_empty_sources_render_33_rows_with_unavailable_and_no_none_tokens(self) -> None:
        snap = metric33.build_snapshot({"now_ms": self.now_ms}, self.now)
        rows = snap["rows"]
        self.assertEqual(len(rows), 33)
        for idx, line in enumerate(rows, start=1):
            self.assertRegex(line, rf"^\s*{idx}\s")
            self.assertNotRegex(line, r"\b(None|null|nan)\b")

    def test_full_synthetic_snapshot_populates_all_33_rows_without_unavailable(self) -> None:
        d1 = [
            _make_kline(
                self.now_ms - (405 - i) * 86_400_000,
                60_000 + (i % 11) * 300,
                61_200 + (i % 11) * 300,
                59_000 + (i % 11) * 300,
                60_200 + (i % 11) * 300,
                v=10_000.0,
                tb=5_200.0,
            )
            for i in range(400)
        ]
        # Make last bar forming so today-so-far volume is populated
        d1[-1][6] = self.now_ms + 3_600_000

        h4 = [
            _make_kline(
                self.now_ms - (505 - i) * 14_400_000,
                61_000 + (i % 9) * 250,
                62_000 + (i % 9) * 250,
                60_000 + (i % 9) * 250,
                61_500 + (i % 9) * 250,
                v=2_500.0,
                tb=1_300.0,
            )
            for i in range(500)
        ]
        h1 = [
            _make_kline(
                self.now_ms - (505 - i) * 3_600_000,
                62_000 + (i % 7) * 150,
                62_600 + (i % 7) * 150,
                61_400 + (i % 7) * 150,
                62_100 + (i % 7) * 150,
                v=600.0,
                tb=310.0,
            )
            for i in range(500)
        ]

        cards = {
            "price": {"price": 62500.0},
            "keyLevels": {
                "shorts": {"level": 63800.0, "distancePct": 2.08, "notional": 25_000_000.0},
                "longs": {"level": 61200.0, "distancePct": -2.08, "notional": 18_000_000.0},
            },
            "largestMagnetZone": {"side": "above", "level": 64200.0, "notional": 40_000_000.0},
            "liquidityAboveBelow": {"above": 120_000_000.0, "below": 80_000_000.0},
        }

        raw = {
            "now_ms": self.now_ms,
            "px_binance": 62_500.0,
            "px_coinbase": 62_485.0,
            "px_okx": 62_502.0,
            "klines_1d": d1,
            "klines_4h": h4,
            "klines_1h": h1,
            "fklines_1d": d1[:-1],
            "funding_binance": 0.00005,
            "oi_4h": [{"sumOpenInterest": str(90_000 + i * 100)} for i in range(12)],
            "oi_1h": [{"sumOpenInterest": "91100", "sumOpenInterestValue": "5693750000"}],
            "top_ratio": [{"longShortRatio": "1.85", "longAccount": "0.6491"}],
            "bybit_ticker": {"result": {"list": [{"fundingRate": "0.00004", "openInterestValue": "4200000000"}]}},
            "okx_funding": {"data": [{"fundingRate": "0.00003"}]},
            "okx_oi": {"data": [{"oiUsd": "2100000000"}]},
            "deribit_funding": {"result": 0.00002},
            "dvol_1h": {"result": {"data": [[self.now_ms - 3600_000, 48, 49, 47, 48.25]]}},
            "dvol_1d": {"result": {"data": [[self.now_ms - i * 86_400_000, 45, 50, 44, 45 + (i % 10)] for i in range(240)]}},
            "cmc_funding_overall": {"data": {"avgFundingRate": 0.0042, "avgFundingRate24hAgo": 0.0038}},
            "cmc_oi_overview": {"data": {"now": {"perpetuals": {"openInterest": 310_000_000_000.0, "chg": -1.25}}}},
            "cmc_liq_summary": {"data": {"total": 180_000_000.0, "longs": 110_000_000.0, "shorts": 70_000_000.0, "totalChange24h": 15.4}},
            "cmc_liq_map_24h": {"data": {"cards": cards}},
            "cmc_liq_map_7d": {"data": {"cards": cards}},
            "cmc_liq_map_30d": {"data": {"cards": cards}},
            "cmc_etf_metrics": {"data": {"current": {"value": 125_000_000.0}}},
            "cmc_etf_chart": {"data": {"points": [{"value": 25_000_000.0} for _ in range(25)]}},
        }

        snap = metric33.build_snapshot(raw, self.now)
        self.assertEqual(len(snap["rows"]), 33)
        for idx, line in enumerate(snap["rows"], start=1):
            self.assertNotIn("UNAVAILABLE", line, f"Row {idx} unexpectedly UNAVAILABLE: {line}")

    def test_cluster_bands_never_overlap_on_integer_edges(self) -> None:
        bands = metric33.cluster_bands([50_000.0, 50_650.0], 1_000.0)
        self.assertEqual(len(bands), 2)
        self.assertGreater(int(bands[1]["low"]), int(bands[0]["high"]))

    def test_row18_multiplies_raw_cmc_decimal_by_100(self) -> None:
        raw = {
            "now_ms": self.now_ms,
            "cmc_funding_overall": {"data": {"avgFundingRate": 0.0000364, "avgFundingRate24hAgo": -0.0000125}},
        }
        snap = metric33.build_snapshot(raw, self.now)
        row18 = snap["rows"][17]
        self.assertIn("+0.0036%", row18)
        self.assertIn("-0.0012%", row18)
        self.assertNotIn("+0.0000%", row18)

    def test_fapi_failure_never_disguises_okx_as_binance(self) -> None:
        d1 = [_make_kline(self.now_ms - (40 - i) * 86_400_000, 60_000, 61_000, 59_000, 60_000) for i in range(35)]
        raw = {
            "now_ms": self.now_ms,
            "klines_1d": d1,
            "funding_binance": None,
            "oi_1h": None,
            "fklines_1d": {"rows": d1, "source": "okx_fallback"},
            "top_ratio": {"rows": [{"longShortRatio": "1.75", "longAccount": "0.636"}], "source": "okx_fallback"},
            "oi_4h": {
                "rows": [{"sumOpenInterest": str(30_000 + i * 50)} for i in range(12)],
                "source": "okx_fallback",
            },
        }
        snap = metric33.build_snapshot(raw, self.now)
        rows = snap["rows"]
        self.assertEqual(rows[16], "17  funding 8h, Binance  UNAVAILABLE")
        self.assertEqual(rows[19], "20  OI Binance  UNAVAILABLE")
        self.assertIn("(OKX fallback)", rows[23])
        self.assertNotIn("BINANCE ONLY", rows[23])
        self.assertIn("(OKX fallback)", rows[26])
        self.assertIn("(OKX fallback)", rows[32])



if __name__ == "__main__":
    unittest.main()
