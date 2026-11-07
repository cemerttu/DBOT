#!/usr/bin/env python3
"""
DERIV MULTI-MARKET DIGIT ANALYSER
ANALYSIS ONLY | PUBLIC MARKET DATA | NO RANDOM FALLBACK | NO TRADES

Monitors R_10, R_25, R_50, R_75 and R_100 on one WebSocket connection.
Keeps an independent rolling tick window for each market, calculates last-digit
frequencies, Over 1 / Under 8 historical hit rates, detects alerts, and logs
all received ticks to one CSV file.

Install:
    python -m pip install websocket-client certifi

Run:
    python deriv_multi_market_analyzer.py

Stop:
    Ctrl+C
"""

import csv
import json
import os
import ssl
import sys
import time
from collections import Counter, deque
from datetime import datetime, timezone

try:
    import websocket  # package: websocket-client
except ImportError:
    print("[ERROR] Missing package: websocket-client")
    print("Install with: python -m pip install websocket-client certifi")
    sys.exit(1)

try:
    import certifi
    CA_CERTS = certifi.where()
except ImportError:
    CA_CERTS = None

# ---------------------------------------------------------------------
# SETTINGS - edit these if needed
# ---------------------------------------------------------------------
MARKETS = {
    "R_10": 3,
    "R_25": 3,
    "R_50": 4,
    "R_75": 4,
    "R_100": 2,
}

WINDOW_SIZE = 500             # Recent ticks retained per market
HISTORY_COUNT = WINDOW_SIZE   # Initial history requested per market
HOT_MULT = 1.6                # 1.6 x 10% = 16%
COLD_MULT = 0.4               # 0.4 x 10% = 4%
SPIKE_DIGIT = 8               # Set to None to disable sequence alert
SPIKE_COUNT = 3
SPIKE_WINDOW = 5
ALERT_COOLDOWN_SEC = 30
DASHBOARD_REFRESH_SEC = 5
CSV_PATH = "deriv_multi_market_ticks.csv"
CONNECT_TIMEOUT = 20

# Official public endpoint first; legacy public endpoint as fallback.
ENDPOINTS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
]

BASELINE_DIGIT_PCT = 10.0
HOT_PCT = BASELINE_DIGIT_PCT * HOT_MULT
COLD_PCT = BASELINE_DIGIT_PCT * COLD_MULT

# The two conditions used by the user's Over/Under strategy.
# Over 1 means last digit > 1. Under 8 means last digit < 8.
OVER_BARRIER = 1
UNDER_BARRIER = 8


class MultiMarketAnalyzer:
    def __init__(self, market, decimals, window_size, csv_writer):
        self.market = market
        self.decimals = decimals
        self.window_size = window_size
        self.window = deque(maxlen=window_size)
        self.counts = Counter({digit: 0 for digit in range(10)})
        self.over1_hits = 0
        self.under8_hits = 0
        self.total_ticks = 0
        self.last_live_tick_id = None
        self.last_alert_time = {}
        self.csv_writer = csv_writer

    def _digit_from_quote(self, quote):
        # Round to the market's official displayed decimal places first.
        rounded = round(float(quote), self.decimals)
        formatted = f"{rounded:.{self.decimals}f}"
        return int(formatted[-1])

    def add_tick(self, quote, tick_id=None, timestamp=None, is_live=False):
        # Only de-duplicate live ticks when Deriv supplies an actual tick ID.
        # Historical timestamps are not unique enough to use as tick IDs.
        if is_live and tick_id is not None:
            if tick_id == self.last_live_tick_id:
                return False
            self.last_live_tick_id = tick_id

        digit = self._digit_from_quote(quote)

        # Remove the oldest observation from all rolling statistics.
        if len(self.window) == self.window_size:
            old_digit = self.window[0]
            self.counts[old_digit] -= 1
            if old_digit > OVER_BARRIER:
                self.over1_hits -= 1
            if old_digit < UNDER_BARRIER:
                self.under8_hits -= 1

        self.window.append(digit)
        self.counts[digit] += 1
        if digit > OVER_BARRIER:
            self.over1_hits += 1
        if digit < UNDER_BARRIER:
            self.under8_hits += 1
        self.total_ticks += 1

        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        window_n = len(self.window)
        digit_pct = self.pct_digit(digit)
        over_pct = self.over1_pct()
        under_pct = self.under8_pct()

        self.csv_writer.writerow([
            now_utc,
            self.market,
            tick_id if tick_id is not None else "",
            quote,
            digit,
            window_n,
            self.counts[digit],
            f"{digit_pct:.2f}",
            f"{over_pct:.2f}" if over_pct is not None else "",
            f"{under_pct:.2f}" if under_pct is not None else "",
        ])
        return True

    def pct_digit(self, digit):
        n = len(self.window)
        return (self.counts[digit] / n * 100.0) if n else 0.0

    def over1_pct(self):
        n = len(self.window)
        return (self.over1_hits / n * 100.0) if n else None

    def under8_pct(self):
        n = len(self.window)
        return (self.under8_hits / n * 100.0) if n else None

    def is_ready(self):
        return len(self.window) >= self.window_size

    def _can_alert(self, key):
        now = time.time()
        last = self.last_alert_time.get(key, 0)
        if now - last >= ALERT_COOLDOWN_SEC:
            self.last_alert_time[key] = now
            return True
        return False

    def check_alerts(self):
        if not self.is_ready():
            return

        for digit in range(10):
            pct = self.pct_digit(digit)
            if pct >= HOT_PCT and self._can_alert((digit, "hot")):
                print(f"[HOT] {self.market}: digit {digit} = {pct:.1f}%")
            elif pct <= COLD_PCT and self._can_alert((digit, "cold")):
                print(f"[COLD] {self.market}: digit {digit} = {pct:.1f}%")

        if SPIKE_DIGIT is not None and len(self.window) >= SPIKE_WINDOW:
            recent = list(self.window)[-SPIKE_WINDOW:]
            hits = recent.count(SPIKE_DIGIT)
            if hits >= SPIKE_COUNT and self._can_alert((SPIKE_DIGIT, "spike")):
                print(
                    f"[SPIKE] {self.market}: digit {SPIKE_DIGIT} appeared "
                    f"{hits} times in the last {SPIKE_WINDOW} ticks"
                )


class CSVLogger:
    def __init__(self, path):
        self.path = path
        new_file = not os.path.exists(path) or os.path.getsize(path) == 0
        self.file = open(path, "a", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        if new_file:
            self.writer.writerow([
                "timestamp_utc", "market", "tick_id", "quote", "last_digit",
                "window_ticks", "digit_count_in_window", "digit_pct",
                "over_1_pct", "under_8_pct"
            ])
            self.file.flush()

    def flush(self):
        self.file.flush()

    def close(self):
        try:
            self.file.flush()
            self.file.close()
        except Exception:
            pass


def make_ssl_options():
    # Keep TLS certificate verification enabled. Never disable certificate checks.
    options = {"cert_reqs": ssl.CERT_REQUIRED}
    if CA_CERTS:
        options["ca_certs"] = CA_CERTS
    return options


def connect_to_deriv():
    ssl_options = make_ssl_options()
    for endpoint in ENDPOINTS:
        print(f"[CONNECT] Trying {endpoint}")
        try:
            ws = websocket.create_connection(
                endpoint,
                timeout=CONNECT_TIMEOUT,
                sslopt=ssl_options,
                enable_multithread=True,
            )
            ws.settimeout(2)
            print("[CONNECTED] Deriv public market-data connection established.")
            return ws
        except Exception as exc:
            print(f"[CONNECTION FAILED] {type(exc).__name__}: {exc}")
    return None


def request_history(ws, analyzers):
    """Request a separate historical tick window for each market."""
    request_to_market = {}
    request_id = 100

    for market in analyzers:
        request_id += 1
        request_to_market[request_id] = market
        ws.send(json.dumps({
            "ticks_history": market,
            "count": HISTORY_COUNT,
            "end": "latest",
            "style": "ticks",
            "req_id": request_id,
        }))

    pending = set(request_to_market)
    deadline = time.monotonic() + 30

    while pending and time.monotonic() < deadline:
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            continue
        except Exception as exc:
            print(f"[HISTORY WARNING] {exc}")
            break

        if not raw:
            continue
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        echo = msg.get("echo_req", {})
        rid = echo.get("req_id")
        if "error" in msg:
            market = request_to_market.get(rid, "unknown market")
            print(f"[HISTORY ERROR] {market}: {msg['error'].get('message', msg['error'])}")
            if rid in pending:
                pending.remove(rid)
            continue

        if msg.get("msg_type") == "history":
            market = request_to_market.get(rid) or echo.get("ticks_history")
            if market not in analyzers:
                continue
            history = msg.get("history", {})
            prices = history.get("prices", [])
            times = history.get("times", [])
            analyzer = analyzers[market]
            for index, price in enumerate(prices):
                tick_time = times[index] if index < len(times) else None
                analyzer.add_tick(price, tick_id=None, timestamp=tick_time, is_live=False)
            print(f"[HISTORY] {market}: loaded {len(prices)} historical ticks")
            if rid in pending:
                pending.remove(rid)

    for rid in pending:
        print(f"[HISTORY WARNING] No history response received for {request_to_market[rid]}")


def subscribe_all_markets(ws, analyzers):
    """Create one live tick subscription per market on the same socket."""
    request_id = 200
    for market in analyzers:
        request_id += 1
        ws.send(json.dumps({
            "ticks": market,
            "subscribe": 1,
            "req_id": request_id,
        }))
        print(f"[SUBSCRIBE] Requested live ticks: {market}")


def print_dashboard(analyzers):
    print("\n" + "=" * 94)
    print("DERIV MULTI-MARKET LIVE ANALYSER | ROLLING WINDOW: {} TICKS".format(WINDOW_SIZE))
    print("=" * 94)
    print("{:<9} {:>8} {:>9} {:>10} {:>10} {:>10} {:>12}".format(
        "MARKET", "SAMPLES", "LAST DIG", "OVER 1", "UNDER 8", "AVG HIT", "STATUS"
    ))
    print("-" * 94)

    ready_markets = []
    for market, analyzer in analyzers.items():
        n = len(analyzer.window)
        last_digit = analyzer.window[-1] if n else "-"
        over_pct = analyzer.over1_pct()
        under_pct = analyzer.under8_pct()
        if analyzer.is_ready():
            avg_hit = (over_pct + under_pct) / 2.0
            status = "READY"
            ready_markets.append((avg_hit, market, over_pct, under_pct))
        else:
            avg_hit = None
            status = "WARMING"

        print("{:<9} {:>8} {:>9} {:>9} {:>9} {:>9} {:>12}".format(
            market,
            f"{n}/{WINDOW_SIZE}",
            str(last_digit),
            f"{over_pct:.1f}%" if over_pct is not None else "-",
            f"{under_pct:.1f}%" if under_pct is not None else "-",
            f"{avg_hit:.1f}%" if avg_hit is not None else "-",
            status,
        ))

    print("-" * 94)
    if ready_markets:
        ready_markets.sort(key=lambda item: item[0], reverse=True)
        print("HISTORICAL OBSERVED-RATE ORDER (not a prediction):")
        for rank, (avg_hit, market, over_pct, under_pct) in enumerate(ready_markets, 1):
            print(
                f"  {rank}. {market}: average {avg_hit:.2f}% "
                f"(Over 1 {over_pct:.2f}% | Under 8 {under_pct:.2f}%)"
            )
    else:
        print("Waiting for each market to fill its rolling window...")

    print("NOTE: Historical hit rates describe the sample only; they do not predict the next tick.")
    print("=" * 94, flush=True)


def run_analyzer():
    logger = CSVLogger(CSV_PATH)
    analyzers = {
        market: MultiMarketAnalyzer(market, decimals, WINDOW_SIZE, logger.writer)
        for market, decimals in MARKETS.items()
    }

    ws = None
    try:
        print("\n" + "=" * 70)
        print("DERIV MULTI-MARKET DIGIT ANALYSER")
        print("ANALYSIS ONLY | REAL DATA ONLY | NO RANDOM FALLBACK | NO TRADES")
        print("Markets:", ", ".join(MARKETS.keys()))
        print("Window per market:", WINDOW_SIZE)
        print(f"Digit alerts: HOT >= {HOT_PCT:.1f}% | COLD <= {COLD_PCT:.1f}%")
        print("Strategy observations: Over 1 (digit > 1), Under 8 (digit < 8)")
        print("CSV file:", os.path.abspath(CSV_PATH))
        print("TLS certificate verification: ON")
        print("=" * 70)

        ws = connect_to_deriv()
        if ws is None:
            print("\n[STOP] Could not connect to Deriv. No sample or random prices were used.")
            return

        request_history(ws, analyzers)
        subscribe_all_markets(ws, analyzers)
        ws.settimeout(2)

        last_dashboard = 0
        last_flush = time.time()
        print("\n[RUNNING] Receiving live ticks. Press Ctrl+C to stop.\n")

        while True:
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                raw = None
            except Exception as exc:
                print(f"[WEBSOCKET ERROR] {type(exc).__name__}: {exc}")
                break

            if raw:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    msg = {}

                if "error" in msg:
                    err = msg["error"]
                    print(f"[DERIV ERROR] {err.get('message', err)}")

                elif msg.get("msg_type") == "tick":
                    tick = msg.get("tick", {})
                    echo = msg.get("echo_req", {})
                    market = tick.get("symbol") or echo.get("ticks")

                    # Some responses may identify the requested symbol in echo_req.
                    if isinstance(market, list):
                        market = market[0] if len(market) == 1 else None

                    if market not in analyzers:
                        continue

                    quote = tick.get("quote")
                    if quote is None:
                        continue

                    analyzer = analyzers[market]
                    accepted = analyzer.add_tick(
                        quote,
                        tick_id=tick.get("id"),
                        timestamp=tick.get("epoch"),
                        is_live=True,
                    )
                    if accepted:
                        analyzer.check_alerts()

            now = time.time()
            if now - last_dashboard >= DASHBOARD_REFRESH_SEC:
                print_dashboard(analyzers)
                last_dashboard = now
            if now - last_flush >= 5:
                logger.flush()
                last_flush = now

    except KeyboardInterrupt:
        print("\n[STOPPING] User requested stop.")
    finally:
        if ws is not None:
            try:
                ws.send(json.dumps({"forget_all": "ticks"}))
            except Exception:
                pass
            try:
                ws.close()
            except Exception:
                pass

        logger.close()
        print("\n[FINISHED] Tick counts:")
        for market, analyzer in analyzers.items():
            print(f"  {market}: {analyzer.total_ticks} ticks")
        print(f"CSV saved to: {os.path.abspath(CSV_PATH)}")
        print("No trades were placed.")


if __name__ == "__main__":
    run_analyzer()
