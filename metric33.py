#!/usr/bin/env python3
"""Metric33 — Standalone, zero-dependency live fetcher and math engine for the 33 BTC market metrics.

Usage:
    python metric33.py            # Print the 33-row BTC market snapshot
    python metric33.py --json     # Print structured JSON (header, timestamp, raw metrics, and 33 formatted rows)
    python metric33.py --check    # Run endpoint contract health check across all public APIs

Features:
  - 100% self-contained Python standard library (no .venv, no pip packages, no bash/jq, no cached packet files).
  - Fetches all public keyless endpoints concurrently via ThreadPoolExecutor (~2-4 seconds total).
  - Strict data honesty: never substitutes 0, null, or stale values for failed requests; missing data prints UNAVAILABLE.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import json
import math
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
TIMEOUT = 20
UNAVAILABLE = "UNAVAILABLE"

ALLOWED_HOSTS = (
    "api.binance.com",
    "data-api.binance.vision",
    "fapi.binance.com",
    "api.exchange.coinbase.com",
    "www.okx.com",
    "api.bybit.com",
    "www.deribit.com",
    "api.coinmarketcap.com",
)


# ============================================================================
# 1. Network Layer (Standard Library Only, Keyless Public Endpoints)
# ============================================================================

def get_json(url: str, timeout: int = TIMEOUT) -> Any:
    """Fetch a JSON endpoint from an allowed host. Raises on HTTP/network/decode error."""
    host = urllib.parse.urlparse(url).hostname or ""
    if host not in ALLOWED_HOSTS:
        raise ValueError(f"Host {host!r} not in ALLOWED_HOSTS")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if resp.status != 200:
            raise urllib.error.HTTPError(url, resp.status, "non-200", resp.headers, None)
        raw = resp.read()
    if not raw or not raw.strip():
        raise ValueError("empty response body")
    return json.loads(raw.decode("utf-8"))


def safe_call(fn: Callable[[], Any]) -> Any:
    """Execute a callable and return None on any exception so one dead endpoint never kills the block."""
    try:
        return fn()
    except Exception:
        return None


def _float(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def get_spot_json(path: str) -> Any:
    """Fetch Binance Spot JSON, preferring data-api.binance.vision so US GitHub Actions runners never hit HTTP 451."""
    last_exc: Exception | None = None
    for base in ("https://data-api.binance.vision", "https://api.binance.com"):
        try:
            return get_json(f"{base}{path}")
        except Exception as exc:
            last_exc = exc
    raise last_exc or RuntimeError(f"spot fetch failed for {path}")


def _fetch_fklines_with_source(now_ms: int) -> dict[str, Any] | None:
    """Fetch 1D futures klines from Binance fapi, or fall back to OKX BTC-USDT-SWAP with an explicit source tag."""
    try:
        rows = get_json("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1d&limit=60")
        return {"rows": rows, "source": "binance"}
    except Exception:
        tv = get_json(
            "https://www.okx.com/api/v5/rubik/stat/taker-volume-contract"
            "?instId=BTC-USDT-SWAP&period=1D&unit=0&limit=35"
        ).get("data") or []
        if not tv:
            return None
        out = []
        for r in reversed(tv):
            ts = int(r[0])
            sell_v, buy_v = float(r[1]), float(r[2])
            tot_v = sell_v + buy_v
            out.append([ts, "0", "0", "0", "0", str(tot_v), ts + 86_399_000, "0", 0, str(buy_v), "0", "0"])
        return {"rows": out, "source": "okx_fallback"}


def _fetch_oi_4h_with_source() -> dict[str, Any] | None:
    try:
        rows = get_json("https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=4h&limit=12")
        return {"rows": rows, "source": "binance"}
    except Exception:
        rows = get_json(
            "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history"
            "?instId=BTC-USDT-SWAP&period=4H&limit=12"
        ).get("data") or []
        if not rows:
            return None
        mapped = [{"sumOpenInterest": str(r[2]), "sumOpenInterestValue": str(r[3])} for r in reversed(rows)]
        return {"rows": mapped, "source": "okx_fallback"}


def _fetch_oi_1h_binance_only() -> list[dict[str, Any]]:
    """Row 20 is explicitly 'OI Binance': never substitute another exchange's book under Binance's name."""
    return get_json("https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=1")


def _fetch_top_ratio_with_source() -> dict[str, Any] | None:
    try:
        rows = get_json(
            "https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=1d&limit=1"
        )
        return {"rows": rows, "source": "binance"}
    except Exception:
        rows = get_json(
            "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader"
            "?instId=BTC-USDT-SWAP&period=1D"
        ).get("data") or []
        if not rows:
            return None
        r_top = float(rows[0][1])
        long_acc = r_top / (1.0 + r_top) if r_top > 0 else 0.5
        return {"rows": [{"longShortRatio": str(r_top), "longAccount": str(long_acc)}], "source": "okx_fallback"}


def _fetch_funding_binance_only() -> float | None:
    """Row 17 is explicitly 'funding 8h, Binance': return None if Binance fapi is unreachable."""
    return _float(get_json("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT")["lastFundingRate"])


def fetch_all_sources(now: dt.datetime | None = None) -> dict[str, Any]:
    """Fetch all required public endpoints in parallel."""
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    dvol_1h_start = now_ms - 86_400_000
    dvol_1d_start = now_ms - 240 * 86_400_000
    deribit_funding_start = now_ms - 8 * 3_600_000

    tasks: dict[str, Callable[[], Any]] = {
        # Spot prices (3 venues)
        "px_binance": lambda: _float(get_spot_json("/api/v3/ticker/price?symbol=BTCUSDT")["price"]),
        "px_coinbase": lambda: _float(get_json("https://api.exchange.coinbase.com/products/BTC-USD/ticker")["price"]),
        "px_okx": lambda: _float(
            get_json("https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT")["data"][0]["last"]
        ),
        # Spot & Futures klines
        "klines_1d": lambda: get_spot_json("/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=400"),
        "klines_4h": lambda: get_spot_json("/api/v3/klines?symbol=BTCUSDT&interval=4h&limit=500"),
        "klines_1h": lambda: get_spot_json("/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=500"),
        "fklines_1d": lambda: _fetch_fklines_with_source(now_ms),
        # Binance Futures funding, OI history, top trader long/short ratio
        "funding_binance": _fetch_funding_binance_only,
        "oi_4h": _fetch_oi_4h_with_source,
        "oi_1h": _fetch_oi_1h_binance_only,
        "top_ratio": _fetch_top_ratio_with_source,
        # Direct multi-venue funding & OI (Bybit, OKX, Deribit)
        "bybit_ticker": lambda: get_json("https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT"),
        "okx_funding": lambda: get_json("https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP"),
        "okx_oi": lambda: get_json(
            "https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP"
        ),
        "deribit_funding": lambda: get_json(
            "https://www.deribit.com/api/v2/public/get_funding_rate_value"
            f"?instrument_name=BTC-PERPETUAL&start_timestamp={deribit_funding_start}&end_timestamp={now_ms}"
        ),
        # Deribit DVOL (hourly for current close, daily for 240d percentile)
        "dvol_1h": lambda: get_json(
            "https://www.deribit.com/api/v2/public/get_volatility_index_data"
            f"?currency=BTC&start_timestamp={dvol_1h_start}&end_timestamp={now_ms}&resolution=3600"
        ),
        "dvol_1d": lambda: get_json(
            "https://www.deribit.com/api/v2/public/get_volatility_index_data"
            f"?currency=BTC&start_timestamp={dvol_1d_start}&end_timestamp={now_ms}&resolution=1D"
        ),
        # CoinMarketCap endpoints (market funding, OI overview, 24h liquidations, liquidation heatmap, ETF netflows)
        "cmc_funding_overall": lambda: get_json(
            "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/funding-rate/overall?marketCapRange.min=50000000"
        ),
        "cmc_oi_overview": lambda: get_json(
            "https://api.coinmarketcap.com/data-api/v4/open-interest/overview?convertId=2781"
        ),
        "cmc_liq_summary": lambda: get_json(
            "https://api.coinmarketcap.com/data-api/v3/liquidations/summary?symbol=BTC"
        ),
        "cmc_liq_map_24h": lambda: get_json(
            "https://api.coinmarketcap.com/data-api/v3/liquidations/map/history"
            "?exchangeId=8112&baseId=1&scope=token&convertId=2781&range=24h"
        ),
        "cmc_liq_map_7d": lambda: get_json(
            "https://api.coinmarketcap.com/data-api/v3/liquidations/map/history"
            "?exchangeId=8112&baseId=1&scope=token&convertId=2781&range=7d"
        ),
        "cmc_liq_map_30d": lambda: get_json(
            "https://api.coinmarketcap.com/data-api/v3/liquidations/map/history"
            "?exchangeId=8112&baseId=1&scope=token&convertId=2781&range=30d"
        ),
        "cmc_etf_metrics": lambda: get_json(
            "https://api.coinmarketcap.com/data-api/v3/etf/netflow/metrics?category=btc&convertId=2781"
        ),
        "cmc_etf_chart": lambda: get_json(
            "https://api.coinmarketcap.com/data-api/v3/etf/overview/netflow/chart?category=btc&range=30d&convertId=2781"
        ),
    }

    out: dict[str, Any] = {"now_ms": now_ms}
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {k: pool.submit(safe_call, fn) for k, fn in tasks.items()}
        for k, fut in futures.items():
            out[k] = fut.result()
    return out


# ============================================================================
# 2. Pure Math & Indicator Engine
# ============================================================================

def closed_klines(rows: list[list[Any]] | None, now_ms: int) -> list[dict[str, float | int]]:
    """Filter raw Binance klines to closed bars (closeTime < now_ms) and normalize fields."""
    if not rows:
        return []
    out = []
    for r in rows:
        if int(r[6]) >= now_ms:
            continue
        out.append({
            "t": int(r[0]),
            "o": float(r[1]),
            "h": float(r[2]),
            "l": float(r[3]),
            "c": float(r[4]),
            "v": float(r[5]),
            "ct": int(r[6]),
            "qv": float(r[7]),
            "tb": float(r[9]) if len(r) > 9 and r[9] not in (None, "") else 0.0,
        })
    return out


def true_ranges(bars: list[dict[str, Any]]) -> list[float]:
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = float(bars[i]["h"]), float(bars[i]["l"]), float(bars[i - 1]["c"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return trs


def wilder_smooth(vals: list[float], n: int) -> list[float]:
    if len(vals) < n:
        return []
    s = sum(vals[:n]) / n
    out = [s]
    for v in vals[n:]:
        s = (s * (n - 1) + v) / n
        out.append(s)
    return out


def atr_simple(bars: list[dict[str, Any]], n: int = 14) -> float:
    if len(bars) < n + 1:
        raise ValueError(f"atr_simple needs >={n + 1} bars")
    trs = true_ranges(bars[-(n + 1):])
    return sum(trs) / n


def atr_wilder(bars: list[dict[str, Any]], n: int = 14) -> float:
    if len(bars) < n + 1:
        raise ValueError(f"atr_wilder needs >={n + 1} bars")
    smoothed = wilder_smooth(true_ranges(bars), n)
    return smoothed[-1]


def atr_percentile_120d(bars: list[dict[str, Any]], n: int = 14, lookback: int = 120) -> float | None:
    if len(bars) < lookback + n:
        return None
    series = []
    for end in range(len(bars) - lookback, len(bars) + 1):
        try:
            series.append(atr_wilder(bars[:end], n))
        except ValueError:
            continue
    if not series:
        return None
    today = series[-1]
    return round(100.0 * sum(1 for x in series if x <= today) / len(series), 1)


def rsi_simple(bars: list[dict[str, Any]], n: int = 14) -> float:
    if len(bars) < n + 1:
        raise ValueError(f"rsi_simple needs >={n + 1} bars")
    gains = 0.0
    losses = 0.0
    for i in range(len(bars) - n, len(bars)):
        d = float(bars[i]["c"]) - float(bars[i - 1]["c"])
        if d > 0:
            gains += d
        else:
            losses += -d
    avg_g = gains / n
    avg_l = losses / n
    if avg_g == 0.0 and avg_l == 0.0:
        return 50.0
    if avg_l == 0.0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - 100.0 / (1.0 + rs)


def rsi_wilder(bars: list[dict[str, Any]], n: int = 14) -> float:
    if len(bars) < n + 1:
        raise ValueError(f"rsi_wilder needs >={n + 1} bars")
    gains, losses = [], []
    for i in range(1, len(bars)):
        d = float(bars[i]["c"]) - float(bars[i - 1]["c"])
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    g_s = wilder_smooth(gains, n)
    l_s = wilder_smooth(losses, n)
    avg_g, avg_l = g_s[-1], l_s[-1]
    if avg_g == 0.0 and avg_l == 0.0:
        return 50.0
    if avg_l == 0.0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_g / avg_l)


def adx_wilder(bars: list[dict[str, Any]], n: int = 14) -> float:
    if len(bars) < 2 * n:
        raise ValueError(f"adx_wilder needs >={2 * n} bars")
    trs, pdms, ndms = [], [], []
    for i in range(1, len(bars)):
        ph, pl, pc = float(bars[i - 1]["h"]), float(bars[i - 1]["l"]), float(bars[i - 1]["c"])
        h, l = float(bars[i]["h"]), float(bars[i]["l"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        up, dn = h - ph, pl - l
        pdms.append(up if (up > dn and up > 0) else 0.0)
        ndms.append(dn if (dn > up and dn > 0) else 0.0)
    atr_s = wilder_smooth(trs, n)
    pdm_s = wilder_smooth(pdms, n)
    ndm_s = wilder_smooth(ndms, n)
    m = min(len(atr_s), len(pdm_s), len(ndm_s))
    dxs = []
    for k in range(m):
        a = atr_s[k]
        pdi = 100.0 * pdm_s[k] / a if a else 0.0
        ndi = 100.0 * ndm_s[k] / a if a else 0.0
        s = pdi + ndi
        dxs.append(100.0 * abs(pdi - ndi) / s if s else 0.0)
    adx_s = wilder_smooth(dxs, n)
    if not adx_s:
        raise ValueError("insufficient DX values for ADX")
    return adx_s[-1]


def vwap(bars: list[dict[str, Any]]) -> float:
    if not bars:
        raise ValueError("vwap needs >=1 bar")
    num = sum(((float(b["h"]) + float(b["l"]) + float(b["c"])) / 3.0) * float(b["v"]) for b in bars)
    den = sum(float(b["v"]) for b in bars)
    if den <= 0:
        raise ValueError("zero volume in vwap")
    return num / den


def volume_profile_30d(bars_30d: list[dict[str, Any]], bins: int = 60) -> dict[str, Any]:
    """30-day volume profile on daily bars (60 bins, quote-volume weighted at close, 70% value area)."""
    if not bars_30d:
        raise ValueError("volume_profile_30d needs >=1 bar")
    lo = min(float(x["l"]) for x in bars_30d)
    hi = max(float(x["h"]) for x in bars_30d)
    if hi <= lo:
        raise ValueError("degenerate profile range")
    step = (hi - lo) / bins
    hist = [0.0] * bins
    for x in bars_30d:
        idx = min(bins - 1, max(0, int((float(x["c"]) - lo) / step)))
        hist[idx] += float(x.get("qv", float(x["v"]) * float(x["c"])))
    total = sum(hist)
    poc_i = max(range(bins), key=lambda i: hist[i])
    lo_i = hi_i = poc_i
    acc = hist[poc_i]
    while acc < total * 0.70 and (lo_i > 0 or hi_i < bins - 1):
        left = hist[lo_i - 1] if lo_i > 0 else -1.0
        right = hist[hi_i + 1] if hi_i < bins - 1 else -1.0
        if right >= left:
            hi_i += 1
            acc += hist[hi_i]
        else:
            lo_i -= 1
            acc += hist[lo_i]

    def price_at(i: int) -> float:
        return lo + step * (i + 0.5)

    peaks = [i for i in range(1, bins - 1) if hist[i] > hist[i - 1] and hist[i] > hist[i + 1]]
    peaks.sort(key=lambda i: -hist[i])
    bimodal = False
    pocs = [round(price_at(poc_i), 2)]
    if len(peaks) >= 2:
        a, b = peaks[0], peaks[1]
        valley = min(hist[min(a, b):max(a, b) + 1])
        if abs(a - b) >= bins * 0.12 and valley < 0.8 * min(hist[a], hist[b]) and hist[b] > 0.45 * hist[a]:
            bimodal = True
            pocs = [round(price_at(a), 2), round(price_at(b), 2)]
    return {
        "poc": None if bimodal else round(price_at(poc_i), 2),
        "pocs": pocs,
        "bimodal": bimodal,
        "val": round(price_at(lo_i), 2),
        "vah": round(price_at(hi_i), 2),
    }


def pivots(bars: list[dict[str, Any]], k: int = 2) -> tuple[list[float], list[float]]:
    highs, lows = [], []
    n = len(bars)
    for i in range(k, n - k):
        hi = float(bars[i]["h"])
        lo = float(bars[i]["l"])
        if hi == max(float(bars[j]["h"]) for j in range(i - k, i + k + 1)):
            highs.append(hi)
        if lo == min(float(bars[j]["l"]) for j in range(i - k, i + k + 1)):
            lows.append(lo)
    return highs, lows


def cluster_bands(levels: list[float], width: float) -> list[dict[str, Any]]:
    if width <= 0:
        raise ValueError("cluster_bands needs positive width")
    clusters: list[dict[str, Any]] = []
    for lv in sorted(float(x) for x in levels):
        if clusters and abs(lv - clusters[-1]["center"]) <= width / 2.0:
            g = clusters[-1]
            g["members"].append(lv)
            g["center"] = sum(g["members"]) / len(g["members"])
        else:
            clusters.append({"center": lv, "members": [lv]})
    for i, g in enumerate(clusters):
        g["touches"] = len(g["members"])
        lo_edge = g["center"] - width / 2.0
        hi_edge = g["center"] + width / 2.0
        if i > 0:
            lo_edge = max(lo_edge, (clusters[i - 1]["center"] + g["center"]) / 2.0)
        if i < len(clusters) - 1:
            hi_edge = min(hi_edge, (g["center"] + clusters[i + 1]["center"]) / 2.0)
        g["low"] = round(lo_edge, 2)
        g["high"] = round(hi_edge, 2)
        if i > 0 and g["low"] <= clusters[i - 1]["high"]:
            gap = 1.0 if (g["high"] - g["low"]) >= 100.0 else 0.01
            g["low"] = round(clusters[i - 1]["high"] + gap, 2)
    return clusters


def build_bands_and_sweeps(
    d1: list[dict[str, Any]],
    h4: list[dict[str, Any]],
    h1: list[dict[str, Any]],
    price: float,
    vp: dict[str, Any],
    vwap30_val: float,
) -> tuple[list[dict[str, Any]], str]:
    """Compute confluence-scored 4H/1H pivot bands and 200h 1H wick rejection sweeps."""
    atr1d = atr_wilder(d1[-299:], 14)
    width = 0.35 * atr1d
    h4_hi, h4_lo = pivots(h4, 2)
    h1_hi, h1_lo = pivots(h1, 2)
    bands = cluster_bands(h4_hi + h4_lo, width)
    h1_levels = h1_hi + h1_lo

    pocs = vp.get("pocs") or ([vp["poc"]] if vp.get("poc") else [])
    for b in bands:
        c = b["center"]
        t4 = b["touches"]
        t1 = sum(1 for lv in h1_levels if b["low"] <= lv <= b["high"])
        tags = []
        if any(abs(c - p) <= 0.35 * atr1d for p in pocs):
            tags.append("POC" if not vp.get("bimodal") else "POC-2")
        if abs(c - vp["val"]) <= 0.35 * atr1d:
            tags.append("VAL")
        if abs(c - vp["vah"]) <= 0.35 * atr1d:
            tags.append("VAH")
        if abs(c - vwap30_val) <= 0.35 * atr1d:
            tags.append("VWAP30")
        if abs(c % 1000) <= 150 or abs(c % 1000 - 1000) <= 150:
            tags.append("round-1k")
        b["confluence"] = t4 * 2 + t1 + len(tags) * 2

    active_bands = [b for b in bands if price * 0.85 <= b["center"] <= price * 1.15]
    events = []
    for b in active_bands:
        b_low, b_high, b_center = b["low"], b["high"], b["center"]
        band_label = f"{b_low:,.0f}-{b_high:,.0f}"
        for bar in h1[-200:]:
            h, l, c, ts = float(bar["h"]), float(bar["l"]), float(bar["c"]), int(bar["t"])
            role = "resistance" if b_center > c else "support"
            if role == "support" and l < b_low and c > b_low:
                depth = (b_low - l) / atr1d
                kind = "support sweep"
            elif role == "resistance" and h > b_high and c < b_high:
                depth = (h - b_high) / atr1d
                kind = "resistance sweep"
            else:
                continue
            if depth >= 0.05:
                events.append({"kind": kind, "band": band_label, "ts": ts, "depth_atr": round(depth, 2)})

    events.sort(key=lambda s: s["ts"])
    merged: list[dict[str, Any]] = []
    for s in events:
        if (
            merged
            and s["band"] == merged[-1]["band"]
            and s["kind"] == merged[-1]["kind"]
            and s["ts"] - merged[-1]["ts"] <= 6 * 3_600_000
        ):
            if s["depth_atr"] > merged[-1]["depth_atr"]:
                merged[-1] = s
        else:
            merged.append(s)
    merged.sort(key=lambda s: s["ts"], reverse=True)
    sweep_str = f"{merged[0]['kind']} {merged[0]['band']}" if merged else "none recorded"

    # Keep top 3 supports below, top 3 resistances above, plus any band containing price
    inside = [b for b in bands if b["low"] <= price <= b["high"]]
    supports = sorted([b for b in bands if b["high"] < price], key=lambda b: -b["center"])[:3]
    resistances = sorted([b for b in bands if b["low"] > price], key=lambda b: b["center"])[:3]
    return inside + supports + resistances, sweep_str


def ema(vals: list[float], n: int) -> list[float]:
    if len(vals) < n:
        raise ValueError(f"ema needs >={n} values")
    k = 2.0 / (n + 1.0)
    out = [sum(vals[:n]) / n]
    for v in vals[n:]:
        out.append(v * k + out[-1] * (1.0 - k))
    return out


def structure_and_bos_4h(h4: list[dict[str, Any]], k: int = 3) -> tuple[str, dict[str, Any]]:
    if len(h4) < 71:
        raise ValueError("structure_and_bos_4h needs >=71 4H bars")
    ph, pl = pivots(h4, k)
    if len(ph) < 2 or len(pl) < 2:
        piv = "CONTRACTING"
    else:
        hh, hl = ph[-1] > ph[-2], pl[-1] > pl[-2]
        lh, ll = ph[-1] < ph[-2], pl[-1] < pl[-2]
        if hh and hl:
            piv = "UPTREND (HH/HL)"
        elif lh and ll:
            piv = "DOWNTREND (LH/LL)"
        elif hh and ll:
            piv = "EXPANDING"
        else:
            piv = "CONTRACTING"

    closes = [float(b["c"]) for b in h4]
    e50 = ema(closes, 50)
    slope_down = e50[-1] < e50[-21]
    below = closes[-1] < e50[-1]
    if below and slope_down and not piv.startswith("DOWNTREND"):
        struct = f"DOWNTREND (EMA50 falling, pivots {piv.split(' ')[0].lower()})"
    elif (not below) and (not slope_down) and not piv.startswith("UPTREND"):
        struct = f"UPTREND (EMA50 rising, pivots {piv.split(' ')[0].lower()})"
    else:
        struct = piv

    bos_info = {
        "bos_down": closes[-1] < pl[-1],
        "bos_up": closes[-1] > ph[-1],
    }
    return struct, bos_info


def cvd_sum(bars: list[dict[str, Any]]) -> float:
    return sum(2.0 * float(b["tb"]) - float(b["v"]) for b in bars)


def realised_vol_series(d1: list[dict[str, Any]], window: int = 30) -> tuple[float, float]:
    """Return (current 30d annualised realised vol %, 1-year 365-observation percentile)."""
    if len(d1) < window + 1:
        raise ValueError(f"realised_vol_series needs >={window + 1} bars")
    closes = [float(b["c"]) for b in d1]
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    series = []
    for end in range(window, len(rets) + 1):
        w = rets[end - window:end]
        mean = sum(w) / window
        var = sum((x - mean) ** 2 for x in w) / (window - 1)
        series.append(math.sqrt(var * 365.0) * 100.0)
    series = series[-365:]
    cur = series[-1]
    pct = round(100.0 * sum(1 for x in series if x <= cur) / len(series), 1)
    return cur, pct


# ============================================================================
# 3. Formatting Helpers & 33-Row Renderer
# ============================================================================

def _int(x: Any) -> str:
    f = _float(x)
    return str(int(math.floor(f))) if f is not None else UNAVAILABLE


def _round_int(x: Any) -> str:
    f = _float(x)
    return str(int(round(f))) if f is not None else UNAVAILABLE


def _signed_int(x: Any) -> str:
    f = _float(x)
    if f is None:
        return UNAVAILABLE
    n = int(round(f))
    if n < 0:
        return str(n)
    if n > 0:
        return f"+{n}"
    return "-0" if f < 0 else "+0"


def _num(x: Any, places: int) -> str:
    f = _float(x)
    return f"{f:.{places}f}" if f is not None else UNAVAILABLE


def _signed_num(x: Any, places: int) -> str:
    f = _float(x)
    if f is None:
        return UNAVAILABLE
    s = f"{f:.{places}f}"
    return f"+{s}" if f >= 0 else s


def _bool(x: Any) -> str:
    if x is True:
        return "true"
    if x is False:
        return "false"
    return UNAVAILABLE


def build_snapshot(raw: dict[str, Any], now: dt.datetime | None = None) -> dict[str, Any]:
    """Transform raw endpoint responses into the 33 structured metrics and 33 formatted lines."""
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)
    now_ms = int(raw.get("now_ms") or int(now.timestamp() * 1000))

    d1 = closed_klines(raw.get("klines_1d"), now_ms)
    h4 = closed_klines(raw.get("klines_4h"), now_ms)
    h1 = closed_klines(raw.get("klines_1h"), now_ms)

    fk_obj = raw.get("fklines_1d")
    if isinstance(fk_obj, dict):
        f1d = closed_klines(fk_obj.get("rows"), now_ms)
        f1d_source = fk_obj.get("source", "binance")
    else:
        f1d = closed_klines(fk_obj, now_ms)
        f1d_source = "binance"

    px_bn = _float(raw.get("px_binance"))
    px_cb = _float(raw.get("px_coinbase"))
    px_okx = _float(raw.get("px_okx"))
    close_1d = float(d1[-1]["c"]) if d1 else None
    ref_price = px_bn or (float(h1[-1]["c"]) if h1 else close_1d)

    # SMA 200 / 30
    sma200 = sum(float(b["c"]) for b in d1[-200:]) / 200.0 if len(d1) >= 200 else None
    sma30 = sum(float(b["c"]) for b in d1[-30:]) / 30.0 if len(d1) >= 30 else None

    # ATRs & percentile (Wilder ATR across Row 5 and Row 6 for full internal consistency)
    atr1d = safe_call(lambda: atr_wilder(d1[-299:], 14))
    atr4h = safe_call(lambda: atr_wilder(h4, 14))
    atr1h = safe_call(lambda: atr_wilder(h1, 14))
    apct = safe_call(lambda: atr_percentile_120d(d1[-299:], 14, 120))

    # Volume profile 30d & VWAP 30d
    vp30 = safe_call(lambda: volume_profile_30d(d1[-30:], 60)) if len(d1) >= 30 else None
    pocs = vp30.get("pocs") if isinstance(vp30, dict) else None
    vwap30 = safe_call(lambda: vwap(d1[-30:])) if len(d1) >= 30 else None

    # Bands & sweeps
    bands: list[dict[str, Any]] = []
    sweeps_str: str | None = None
    if len(d1) >= 15 and len(h4) >= 5 and h1 and ref_price and vp30 and vwap30:
        res_bs = safe_call(lambda: build_bands_and_sweeps(d1, h4, h1, ref_price, vp30, vwap30))
        if res_bs:
            bands, sweeps_str = res_bs

    # 4H Structure & BOS
    struct_4h: str | None = None
    bos_4h: dict[str, Any] = {}
    if len(h4) >= 71:
        res_sb = safe_call(lambda: structure_and_bos_4h(h4, 3))
        if res_sb:
            struct_4h, bos_4h = res_sb

    # ADX 1D / 4H & RSI 1D / 4H
    adx1d = safe_call(lambda: adx_wilder(d1[-200:], 14))
    adx4h = safe_call(lambda: adx_wilder(h4[-200:], 14))
    rsi1d = safe_call(lambda: rsi_simple(d1, 14))
    rsi4h = safe_call(lambda: rsi_wilder(h4, 14))

    # Volume (quote volume in USDT)
    raw_1d = raw.get("klines_1d") or []
    vol_last_closed = float(d1[-1]["qv"]) if d1 else None
    vol_sma9 = sum(float(b["qv"]) for b in d1[-10:-1]) / 9.0 if len(d1) >= 10 else None
    vol_today = (
        _float(raw_1d[-1][7]) if raw_1d and int(raw_1d[-1][6]) >= now_ms else None
    )

    # Funding rates (Binance, CMC market average, direct 4-exchange venue breakdown)
    f_bn = _float(raw.get("funding_binance"))
    cmc_f_data = (raw.get("cmc_funding_overall") or {}).get("data") or {}
    f_avg = _float(cmc_f_data.get("avgFundingRate"))
    f_avg_24h = _float(cmc_f_data.get("avgFundingRate24hAgo"))

    venue_rates = []
    if f_bn is not None:
        venue_rates.append(("Binance", f_bn))
    bybit_row = (((raw.get("bybit_ticker") or {}).get("result") or {}).get("list") or [{}])[0]
    if _float(bybit_row.get("fundingRate")) is not None:
        venue_rates.append(("Bybit", float(bybit_row["fundingRate"])))
    okx_f_row = ((raw.get("okx_funding") or {}).get("data") or [{}])[0]
    if _float(okx_f_row.get("fundingRate")) is not None:
        venue_rates.append(("OKX", float(okx_f_row["fundingRate"])))
    deribit_f = _float((raw.get("deribit_funding") or {}).get("result"))
    if deribit_f is not None:
        venue_rates.append(("Deribit", deribit_f))

    # Open Interest (Binance 1h, multi-venue BTC direct, CMC all-perps overview, 4H OI delta)
    oi_1h_rows = raw.get("oi_1h") or []
    oi_bn_btc = _float(oi_1h_rows[-1].get("sumOpenInterest")) if oi_1h_rows else None
    oi_bn_usd = _float(oi_1h_rows[-1].get("sumOpenInterestValue")) if oi_1h_rows else None

    btc_venue_oi_usd = 0.0
    btc_venue_oi_count = 0
    if oi_bn_usd is not None:
        btc_venue_oi_usd += oi_bn_usd
        btc_venue_oi_count += 1
    if _float(bybit_row.get("openInterestValue")) is not None:
        btc_venue_oi_usd += float(bybit_row["openInterestValue"])
        btc_venue_oi_count += 1
    okx_oi_row = ((raw.get("okx_oi") or {}).get("data") or [{}])[0]
    okx_oi_usd = _float(okx_oi_row.get("oiUsd") or okx_oi_row.get("oiCcy"))
    if okx_oi_usd is not None:
        btc_venue_oi_usd += okx_oi_usd
        btc_venue_oi_count += 1

    cmc_perps = (((raw.get("cmc_oi_overview") or {}).get("data") or {}).get("now") or {}).get("perpetuals") or {}
    oi_all_perps_usd = _float(cmc_perps.get("openInterest"))
    oi_all_perps_chg = _float(cmc_perps.get("chg"))

    # Liquidations 24h & Heatmap key levels
    liq_data = (raw.get("cmc_liq_summary") or {}).get("data") or {}
    liq_tot = _float(liq_data.get("total"))
    liq_longs = _float(liq_data.get("longs"))
    liq_shorts = _float(liq_data.get("shorts"))
    liq_chg = _float(liq_data.get("totalChange24h"))

    cards_24h = ((raw.get("cmc_liq_map_24h") or {}).get("data") or {}).get("cards") or {}
    cards_7d = ((raw.get("cmc_liq_map_7d") or {}).get("data") or {}).get("cards") or {}
    cards_30d = ((raw.get("cmc_liq_map_30d") or {}).get("data") or {}).get("cards") or {}

    kl_above = _float(((cards_24h.get("keyLevels") or {}).get("shorts") or {}).get("level"))
    kl_below = _float(((cards_24h.get("keyLevels") or {}).get("longs") or {}).get("level"))
    ab_24h = cards_24h.get("liquidityAboveBelow") or {}
    ab_above = _float(ab_24h.get("above"))
    ab_below = _float(ab_24h.get("below"))
    ab_share = (
        ab_above / (ab_above + ab_below) * 100.0
        if ab_above is not None and ab_below is not None and (ab_above + ab_below) > 0
        else None
    )
    mag_1w = _float((cards_7d.get("largestMagnetZone") or {}).get("level"))
    mag_1m = _float((cards_30d.get("largestMagnetZone") or {}).get("level"))

    # CVD 30d (Spot & Futures)
    cvd_spot = safe_call(lambda: cvd_sum(d1[-30:])) if len(d1) >= 30 else None
    cvd_fut = safe_call(lambda: cvd_sum(f1d[-30:])) if len(f1d) >= 30 else None

    # ETF netflows (1D, 5D, 20D)
    etf_d1 = _float((((raw.get("cmc_etf_metrics") or {}).get("data") or {}).get("current") or {}).get("value"))
    etf_pts = ((raw.get("cmc_etf_chart") or {}).get("data") or {}).get("points") or []
    etf_vals = [float(p["value"]) for p in etf_pts if _float(p.get("value")) is not None]
    etf_d5 = sum(etf_vals[-5:]) if len(etf_vals) >= 5 else None
    etf_d20 = sum(etf_vals[-20:]) if len(etf_vals) >= 20 else None

    # Top trader ratio
    tr_obj = raw.get("top_ratio")
    if isinstance(tr_obj, dict):
        tr_rows = tr_obj.get("rows") or []
        tr_source = tr_obj.get("source", "binance")
    else:
        tr_rows = tr_obj or []
        tr_source = "binance"
    ls_ratio = _float(tr_rows[-1].get("longShortRatio")) if tr_rows else None
    ls_long_pct = (
        float(tr_rows[-1]["longAccount"]) * 100.0
        if tr_rows and _float(tr_rows[-1].get("longAccount")) is not None
        else None
    )

    # Realised Vol 30d (365-obs 1y percentile), Implied Vol DVOL (240d percentile), Variance Premium
    rvol_pair = safe_call(lambda: realised_vol_series(d1, 30)) if len(d1) >= 31 else None
    rvol30 = rvol_pair[0] if rvol_pair else None
    rvol_pct_1y = rvol_pair[1] if rvol_pair else None

    dvol_1h_rows = ((raw.get("dvol_1h") or {}).get("result") or {}).get("data") or []
    dvol_cur = _float(dvol_1h_rows[-1][4]) if dvol_1h_rows else None
    dvol_1d_rows = ((raw.get("dvol_1d") or {}).get("result") or {}).get("data") or []
    dvol_1d_vals = [float(r[4]) for r in dvol_1d_rows if len(r) >= 5 and _float(r[4]) is not None]
    dvol_pct_240d = (
        round(100.0 * sum(1 for x in dvol_1d_vals if x <= (dvol_cur or dvol_1d_vals[-1])) / len(dvol_1d_vals), 1)
        if dvol_1d_vals
        else None
    )
    vrp = (dvol_cur - rvol30) if (dvol_cur is not None and rvol30 is not None) else None

    # Last closed 4H bar, 1D taker buy ratio, 4H OI change
    last_4h = h4[-1] if h4 else None
    last_1d = d1[-1] if d1 else None
    taker_ratio = (float(last_1d["tb"]) / float(last_1d["v"])) if (last_1d and float(last_1d["v"]) > 0) else None
    taker_net = (2.0 * float(last_1d["tb"]) - float(last_1d["v"])) if last_1d else None

    oi_4h_obj = raw.get("oi_4h")
    if isinstance(oi_4h_obj, dict):
        oi_4h_rows = oi_4h_obj.get("rows") or []
        oi_4h_source = oi_4h_obj.get("source", "binance")
    else:
        oi_4h_rows = oi_4h_obj or []
        oi_4h_source = "binance"
    oi_4h_chg = oi_4h_pct = oi_24h_chg = None
    if len(oi_4h_rows) >= 2:
        last_oi = _float(oi_4h_rows[-1].get("sumOpenInterest"))
        prev_oi = _float(oi_4h_rows[-2].get("sumOpenInterest"))
        if last_oi is not None and prev_oi is not None and prev_oi != 0:
            oi_4h_chg = last_oi - prev_oi
            oi_4h_pct = (last_oi / prev_oi - 1.0) * 100.0
        if len(oi_4h_rows) >= 7:
            oi6 = _float(oi_4h_rows[-7].get("sumOpenInterest"))
            if last_oi is not None and oi6 is not None:
                oi_24h_chg = last_oi - oi6

    # ------------------------------------------------------------------
    # Format the 33 numbered rows
    # ------------------------------------------------------------------
    lines: list[str] = []

    # 1 price, 3 venues
    lines.append(
        f" 1  price, 3 venues  Binance {_num(px_bn, 1)} · Coinbase {_num(px_cb, 1)} · OKX {_num(px_okx, 1)}"
    )

    # 2 closed 1D close
    lines.append(f" 2  closed 1D close  {_num(close_1d, 2)}")

    # 3 & 4 bands containing price / next levels
    if not bands or ref_price is None:
        lines.append(f" 3  bands containing price  {UNAVAILABLE}")
        lines.append(f" 4  next levels  {UNAVAILABLE}")
    else:
        containing = [b for b in bands if b["low"] <= ref_price <= b["high"]]
        below_b = sorted([b for b in bands if b["high"] < ref_price], key=lambda b: b["high"], reverse=True)
        above_b = sorted([b for b in bands if b["low"] > ref_price], key=lambda b: b["low"])
        if containing:
            c_txt = "  &  ".join(f"{_int(b['low'])}-{_int(b['high'])} (conf {b['confluence']})" for b in containing)
        else:
            c_txt = "none (price between bands)"
        lines.append(f" 3  bands containing price  {c_txt}")
        b_txt = f"{_int(below_b[0]['low'])}-{_int(below_b[0]['high'])} (conf {below_b[0]['confluence']})" if below_b else "none"
        a_txt = f"{_int(above_b[0]['low'])}-{_int(above_b[0]['high'])} (conf {above_b[0]['confluence']})" if above_b else "none"
        lines.append(f" 4  next levels  below {b_txt} · above {a_txt}")

    # 5 ATR 1D/4H/1H
    lines.append(f" 5  ATR 1D/4H/1H  {_int(atr1d)} · {_int(atr4h)} · {_int(atr1h)}")

    # 6 ATR 1D percentile
    apct_s = f"{_num(apct, 1)}th of 120d" if apct is not None else UNAVAILABLE
    atr_pct_px = f"{atr1d / close_1d * 100.0:.2f}%" if (atr1d is not None and close_1d) else UNAVAILABLE
    lines.append(f" 6  ATR 1D percentile  {apct_s} · {atr_pct_px} of price")

    # 7 volume profile 30d
    pocs_s = " / ".join(_int(x) for x in pocs) if pocs else UNAVAILABLE
    lines.append(f" 7  volume profile 30d  POCs {pocs_s}")

    # 8 VWAP 30d
    lines.append(f" 8  VWAP 30d  {_int(vwap30)}")

    # 9 sweeps
    lines.append(f" 9  sweeps  {sweeps_str if sweeps_str else UNAVAILABLE}")

    # 10 & 11 SMA200 / SMA30
    sma200_pct = _signed_num((close_1d / sma200 - 1.0) * 100.0, 2) + "%" if (close_1d and sma200) else UNAVAILABLE
    sma30_pct = _signed_num((close_1d / sma30 - 1.0) * 100.0, 2) + "%" if (close_1d and sma30) else UNAVAILABLE
    lines.append(f"10  1D vs SMA200  {_int(sma200)} ({sma200_pct})")
    lines.append(f"11  price vs SMA30  {_int(sma30)} ({sma30_pct})  (proxy for short-term slope)")

    # 12 & 13 4H structure & BOS
    bos_down = bos_4h.get("bos_down")
    bos_up = bos_4h.get("bos_up")
    crack = ""
    if struct_4h and struct_4h.startswith("DOWNTREND") and bos_up is True:
        crack = " — first crack up"
    elif struct_4h and struct_4h.startswith("UPTREND") and bos_down is True:
        crack = " — first crack down"
    lines.append(f"12  4H structure  {struct_4h or UNAVAILABLE}{crack}")
    if bos_down is None or bos_up is None:
        lines.append(f"13  4H BOS  {UNAVAILABLE}")
    else:
        lines.append(f"13  4H BOS  down {_bool(bos_down)} · up {_bool(bos_up)}")

    # 14 ADX 1D / 4H
    lines.append(f"14  ADX 1D / 4H  {_num(adx1d, 1)} / {_num(adx4h, 1)}")

    # 15 RSI 14
    lines.append(f"15  RSI 14  1D {_num(rsi1d, 1)} · 4H {_num(rsi4h, 1)}")

    # 16 volume
    lc_s = f"${vol_last_closed / 1e9:.2f}B" if vol_last_closed is not None else f"${UNAVAILABLE}B"
    s9_s = f"${vol_sma9 / 1e9:.2f}B" if vol_sma9 is not None else f"${UNAVAILABLE}B"
    v_ratio = f"{vol_last_closed / vol_sma9 * 100.0:.0f}%" if (vol_last_closed and vol_sma9) else UNAVAILABLE
    td_s = f"${vol_today / 1e9:.2f}B" if vol_today is not None else f"${UNAVAILABLE}B"
    lines.append(f"16  volume  last closed 1D {lc_s} vs 9d SMA {s9_s} ({v_ratio}) · today-so-far {td_s} (partial)")

    # 17 funding 8h, Binance
    if f_bn is None:
        lines.append(f"17  funding 8h, Binance  {UNAVAILABLE}")
    else:
        lines.append(f"17  funding 8h, Binance  {f_bn * 100.0:+.4f}% ({f_bn * 100.0 * 3.0 * 365.0:+.2f}%/yr)")

    # 18 funding, market avg (CMC avgFundingRate is a raw decimal like 0.0000364; multiply by 100 for %)
    if f_avg is None or f_avg_24h is None:
        lines.append(f"18  funding, market avg  {UNAVAILABLE}")
    else:
        lines.append(f"18  funding, market avg  {f_avg * 100.0:+.4f}% (24h ago {f_avg_24h * 100.0:+.4f}%)")

    # 19 funding per venue
    if venue_rates:
        lo_v = min(venue_rates, key=lambda x: x[1])[0]
        hi_v = max(venue_rates, key=lambda x: x[1])[0]
        lines.append(f"19  funding per venue  {len(venue_rates)} venues · lowest {lo_v} · highest {hi_v}")
    else:
        lines.append(f"19  funding per venue  {UNAVAILABLE}")

    # 20 OI Binance
    if oi_bn_btc is not None and oi_bn_usd is not None:
        lines.append(f"20  OI Binance  {int(round(oi_bn_btc)):,} BTC (${oi_bn_usd / 1e9:.2f}B)")
    else:
        lines.append(f"20  OI Binance  {UNAVAILABLE}")

    # 21 OI BTC venues / all perps
    v_part = f"BTC {btc_venue_oi_count}-venue ${btc_venue_oi_usd / 1e9:.1f}B" if btc_venue_oi_count else None
    m_part = (
        f"all-crypto perps ${oi_all_perps_usd / 1e9:.1f}B ({_signed_num(oi_all_perps_chg, 2)}% 24h, CMC)"
        if oi_all_perps_usd is not None
        else None
    )
    if v_part and m_part:
        lines.append(f"21  OI BTC venues / all perps  {v_part} · {m_part}")
    elif v_part or m_part:
        lines.append(f"21  OI BTC venues / all perps  {v_part or m_part}")
    else:
        lines.append(f"21  OI BTC venues / all perps  {UNAVAILABLE}")

    # 22 liquidations 24h
    if None in (liq_tot, liq_longs, liq_shorts, liq_chg) or not liq_tot:
        lines.append(f"22  liquidations 24h  {UNAVAILABLE}")
    else:
        lines.append(
            f"22  liquidations 24h  ${liq_tot / 1e6:.1f}M "
            f"(longs ${liq_longs / 1e6:.1f}M / {liq_longs / liq_tot * 100.0:.0f}% · "
            f"shorts ${liq_shorts / 1e6:.1f}M · 24h {_signed_num(liq_chg, 1)}%)"
        )

    # 23 liq key levels
    sh_s = f"{_num(ab_share, 1)}%" if ab_share is not None else f"{UNAVAILABLE}%"
    lines.append(
        f"23  liq key levels  24H max pain above {_int(kl_above)} · below {_int(kl_below)} · "
        f"1W magnet {_int(mag_1w)} · 1M {_int(mag_1m)} · {sh_s} above"
    )

    # 24 CVD 30d
    if f1d_source == "okx_fallback":
        lines.append(f"24  CVD 30d  spot {_int(cvd_spot)} (BINANCE) · futures {_int(cvd_fut)} BTC (OKX fallback)")
    else:
        lines.append(f"24  CVD 30d  spot {_int(cvd_spot)} · futures {_int(cvd_fut)} BTC (BINANCE ONLY)")

    # 25 ETF netflow
    if None in (etf_d1, etf_d5, etf_d20):
        lines.append(f"25  ETF netflow  {UNAVAILABLE}")
    else:
        lines.append(
            f"25  ETF netflow  1D ${_signed_int(etf_d1 / 1e6)}M · "
            f"5D ${_signed_int(etf_d5 / 1e6)}M · 20D ${_signed_int(etf_d20 / 1e6)}M"
        )

    # 26 Coinbase premium
    if px_cb is not None and px_bn:
        diff = px_cb - px_bn
        bps = (px_cb / px_bn - 1.0) * 10000.0
        lines.append(f"26  Coinbase premium  ${_signed_num(diff, 2)} ({_signed_num(bps, 2)} bps)")
    else:
        lines.append(f"26  Coinbase premium  {UNAVAILABLE}")

    # 27 top traders
    if ls_ratio is not None and ls_long_pct is not None:
        tr_tag = " (OKX fallback)" if tr_source == "okx_fallback" else ""
        lines.append(f"27  top traders  L/S {_num(ls_ratio, 2)} ({_num(ls_long_pct, 1)}% long, accounts){tr_tag}")
    else:
        lines.append(f"27  top traders  {UNAVAILABLE}")

    # 28 realised vol 30d
    rv_s = f"{_num(rvol30, 2)}%" if rvol30 is not None else UNAVAILABLE
    rvp_s = f"{_num(rvol_pct_1y, 1)}th pct" if rvol_pct_1y is not None else UNAVAILABLE
    lines.append(f"28  realised vol 30d  {rv_s} · {rvp_s} of 1y")

    # 29 implied vol DVOL
    dv_s = f"{_num(dvol_cur, 2)}%" if dvol_cur is not None else f"{UNAVAILABLE}%"
    dvp_s = f"{_num(dvol_pct_240d, 1)}th pct" if dvol_pct_240d is not None else f"{UNAVAILABLE}th pct"
    lines.append(f"29  implied vol DVOL  {dv_s} ({dvp_s} of 240d)")

    # 30 variance premium
    lines.append(f"30  variance premium  {_signed_num(vrp, 2)} vol points")

    # 31 last closed 4H bar
    if not last_4h:
        lines.append(f"31  last closed 4H bar  {UNAVAILABLE}")
    else:
        ts_s = dt.datetime.fromtimestamp(int(last_4h["ct"]) / 1000.0, tz=dt.timezone.utc).strftime("%H:%MZ")
        lines.append(
            f"31  last closed 4H bar  {ts_s}  O {_round_int(last_4h['o'])} H {_round_int(last_4h['h'])} "
            f"L {_round_int(last_4h['l'])} C {_round_int(last_4h['c'])} · vol {_round_int(last_4h['v'])} BTC"
        )

    # 32 taker buy ratio
    if taker_ratio is None or taker_net is None:
        lines.append(f"32  taker buy ratio  {UNAVAILABLE}")
    else:
        lines.append(
            f"32  taker buy ratio  {taker_ratio * 100.0:.2f}% of last closed 1D "
            f"(net {_signed_int(taker_net)} BTC, BINANCE SPOT)"
        )

    # 33 4H OI change
    if oi_4h_chg is None or oi_4h_pct is None:
        lines.append(f"33  4H OI change  {UNAVAILABLE}")
    else:
        d24_s = _signed_int(oi_24h_chg) if oi_24h_chg is not None else UNAVAILABLE
        oi_tag = " (OKX fallback)" if oi_4h_source == "okx_fallback" else ""
        lines.append(
            f"33  4H OI change  {_signed_int(oi_4h_chg)} BTC ({_signed_num(oi_4h_pct, 2)}%) · 24h {d24_s} BTC{oi_tag}"
        )

    header = f"BTC spot · {now.strftime('%Y-%m-%d %H:%M:%SZ')} · Metric33 standalone live snapshot"
    return {
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "header": header,
        "rows": lines,
        "metrics": {
            "price": {"binance": px_bn, "coinbase": px_cb, "okx": px_okx},
            "close_1d": close_1d,
            "atr": {"1d": atr1d, "4h": atr4h, "1h": atr1h, "1d_percentile_120d": apct},
            "pocs_30d": pocs,
            "vwap_30d": vwap30,
            "sweeps": sweeps_str,
            "sma200": sma200,
            "sma30": sma30,
            "structure_4h": struct_4h,
            "bos_4h": bos_4h,
            "adx": {"1d": adx1d, "4h": adx4h},
            "rsi14": {"1d": rsi1d, "4h": rsi4h},
            "cvd_30d": {"spot_btc": cvd_spot, "futures_btc": cvd_fut},
            "realised_vol_30d": {"pct": rvol30, "percentile_1y": rvol_pct_1y},
            "implied_dvol": {"pct": dvol_cur, "percentile_240d": dvol_pct_240d},
            "variance_premium": vrp,
        },
    }


def check_endpoints() -> int:
    """Quick 5-second health check verifying all external endpoints respond with expected JSON."""
    t0 = time.time()
    now = dt.datetime.now(dt.timezone.utc)
    raw = fetch_all_sources(now)
    elapsed = time.time() - t0
    keys = [k for k in raw if k != "now_ms"]
    ok = [k for k in keys if raw[k] is not None]
    failed = [k for k in keys if raw[k] is None]
    for k in keys:
        state = "OK" if raw[k] is not None else "FAILED"
        print(f"{state:<7}  {k}")
    print(f"\nSUMMARY: {len(ok)}/{len(keys)} endpoints OK in {elapsed:.2f}s")
    if failed:
        print(f"FAILING: {', '.join(failed)}")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="metric33", description="Standalone 33-row BTC market metrics fetcher.")
    ap.add_argument("--json", action="store_true", help="Output structured JSON instead of plain text")
    ap.add_argument("--check", action="store_true", help="Run parallel endpoint health check")
    args = ap.parse_args(argv)

    if args.check:
        return check_endpoints()

    now = dt.datetime.now(dt.timezone.utc)
    raw = fetch_all_sources(now)
    snap = build_snapshot(raw, now)

    if args.json:
        print(json.dumps(snap, indent=2))
    else:
        print(snap["header"])
        print()
        for line in snap["rows"]:
            print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
