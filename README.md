# Metric33

Standalone, zero-dependency Python tool that fetches and computes the **33 BTC market metrics** live from public, keyless APIs in ~3 seconds.

- **No `.venv` or third-party packages required** — uses Python's standard library only (`urllib.request`, `concurrent.futures`, `json`, `math`, `datetime`).
- **No Bash, no `jq`, no cached JSON packet files** — every metric is fetched and computed fresh in memory on each run.
- **Strict data honesty** — never substitutes `0`, `null`, or stale values when an endpoint fails; missing metrics print `UNAVAILABLE` without stopping the remaining rows.

---

## Quick Start (Windows PowerShell or Linux/WSL)

```powershell
# 1. Print the 33-row BTC snapshot
python metric33.py

# 2. Output structured JSON (header, timestamp, raw metrics, and 33 formatted rows)
python metric33.py --json

# 3. Check the health of all external public endpoints
python metric33.py --check

# 4. Run the offline unit tests
python test_metric33.py
```

---

## The 33 Metrics & Their Live Sources

| # | Metric | Source / Formula |
| :- | :--- | :--- |
| **1** | `price, 3 venues` | Live BTC spot ticker on **Binance** (`BTCUSDT`), **Coinbase** (`BTC-USD`), and **OKX** (`BTC-USDT`) |
| **2** | `closed 1D close` | Last closed 1D bar close from Binance Spot |
| **3** | `bands containing price` | 4H fractal (`k=2`) pivot clusters (`width = 0.35 * Wilder ATR1D`), non-overlapping integer edges, confluence-scored with 1H pivots, POC, VAH/VAL, VWAP30, and round-1k levels |
| **4** | `next levels` | Nearest support band below and resistance band above current spot price |
| **5** | `ATR 1D/4H/1H` | 14-bar simple Average True Range across closed `1D`, `4H`, and `1H` Binance Spot klines |
| **6** | `ATR 1D percentile` | 120-day rolling percentile of Wilder `ATR(14)` + current `ATR 1D` as `%` of closed 1D price |
| **7** | `volume profile 30d` | 30-day 60-bin quote-volume histogram on closed `1D` bars (reports 2 POCs when bimodal with >=12% bin separation and >=20% valley depth) |
| **8** | `VWAP 30d` | 30-day Volume-Weighted Average Price (`typical price = (H+L+C)/3`) |
| **9** | `sweeps` | Most recent 1H wick rejection (`depth >= 0.05 * Wilder ATR1D`) across active 4H pivot bands over the last 200 hours (6h same-band/same-kind deduplication) |
| **10** | `1D vs SMA200` | 200-day simple moving average and `%` distance from last closed 1D close |
| **11** | `price vs SMA30` | 30-day simple moving average and `%` distance from last closed 1D close |
| **12** | `4H structure` | 4H `k=3` swing pivot regime (`HH/HL`, `LH/LL`, `EXPANDING`, `CONTRACTING`) with 50-EMA 21-bar slope override |
| **13** | `4H BOS` | 4H Break of Structure (`down` / `up`) against the most recent confirmed `k=3` pivot high and low |
| **14** | `ADX 1D / 4H` | Wilder `ADX(14)` over closed `1D` and `4H` bars |
| **15** | `RSI 14` | `1D` simple `RSI(14)` and `4H` Wilder `RSI(14)` |
| **16** | `volume` | Last closed 1D quote volume vs prior 9-day SMA + today's partial forming 1D volume |
| **17** | `funding 8h, Binance` | Current 8h funding rate and annualised `%` from Binance USD-M Futures `premiumIndex` |
| **18** | `funding, market avg` | Market-wide average funding rate (current vs 24h ago) from CoinMarketCap |
| **19** | `funding per venue` | Direct exchange funding rates queried from **Binance**, **Bybit**, **OKX**, and **Deribit** |
| **20** | `OI Binance` | Binance BTCUSDT perpetual open interest in BTC and USD |
| **21** | `OI BTC venues / all perps` | Direct 3-venue BTC perpetual OI (**Binance + Bybit + OKX**) alongside CoinMarketCap all-crypto perpetual OI |
| **22** | `liquidations 24h` | 24h total, long, and short liquidations + 24h `%` change |
| **23** | `liq key levels` | 24H max pain above/below, 1W magnet, 1M magnet, and `%` liquidity share above price |
| **24** | `CVD 30d` | 30-day Cumulative Volume Delta (`sum(2 * takerBuy - volume)`) for Binance Spot and USD-M Futures |
| **25** | `ETF netflow` | US spot BTC ETF netflow over `1D`, `5D`, and `20D` sessions |
| **26** | `Coinbase premium` | Coinbase `BTC-USD` minus Binance `BTCUSDT` in USD and basis points (`bps`) |
| **27** | `top traders` | Binance Futures top trader account Long/Short ratio and `%` long |
| **28** | `realised vol 30d` | 30-day annualised realised volatility (`ddof=1` log returns * `sqrt(365)`) and true 365-observation 1-year percentile |
| **29** | `implied vol DVOL` | Deribit BTC 30-day implied volatility index (`DVOL`) and 240-day percentile |
| **30** | `variance premium` | Implied vol (`DVOL`) minus 30-day realised vol (`VRP` in vol points) |
| **31** | `last closed 4H bar` | Close timestamp (UTC), OHLC, and BTC volume of the most recently closed 4H bar |
| **32** | `taker buy ratio` | Taker buy share (`%`) and net taker buy delta (BTC) on the last closed 1D spot bar |
| **33** | `4H OI change` | Binance Futures 4H open interest change (BTC and `%`) plus 24h (`6 * 4H`) BTC delta |
