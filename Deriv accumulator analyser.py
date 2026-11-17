#!/usr/bin/env python3
"""
DERIV ACCUMULATOR CONDITIONS ANALYSER  (analysis only, no trades)

Markets : Volatility 10, 25, 50, 75, 100 Index (R_10, R_25, R_50, R_75, R_100)
Growth  : 1%, 2%, 3%, 4%, 5% accumulator growth rates

Install:  python -m pip install websocket-client certifi
Run:      python deriv_accumulator_analyser.py
Stop:     Ctrl+C

What it does
  1. Connects to Deriv and loads the last 5000 ticks of each index, then streams live ticks.
  2. Measures volatility (std-dev of tick changes) and stability (recent vs long-term).
  3. Detects spikes (tick change bigger than 4 standard deviations) and abnormal volatility.
  4. For each growth rate, counts how often a tick would break the accumulator barrier.
  5. Backtests every growth rate: enter on every tick, hold HOLD_TICKS ticks, and record
     the win rate and the expected result per 1.00 staked.
  6. Names the index with the best conditions right now.
  7. Shows one signal per index: SUITABLE, UNSUITABLE or WAIT.

How the barrier is found
  It first asks Deriv for a real accumulator quote (proposal) and uses the barrier
  distance Deriv returns. If Deriv does not provide it, the script falls back to an
  ESTIMATE: the barrier that a fair game would use (survival chance 1/(1+growth)).
  The dashboard shows which source was used. Deriv's real barrier includes a margin
  for the house, so real results are normally slightly WORSE than the estimate.

What the signal means
  SUITABLE   = calm market (no spike, stable volatility) and the backtest is not worse
               than break-even by more than EV_TOLERANCE. It does NOT mean profit.
  UNSUITABLE = volatility is abnormal, or the backtest loses money.
  WAIT       = still collecting data, a spike just happened, or volatility is changing.

Honest warning: Volatility indices are generated with constant volatility, so spikes
and "conditions" are mostly noise and cannot predict the next tick. An accumulator
loses its whole stake the moment one tick breaks the barrier.
"""

import csv
import json
import ssl
import statistics
import sys
import time
from collections import deque
from datetime import datetime, timezone

try:
    import websocket  # websocket-client
except ImportError:
    sys.exit("Missing package. Run: python -m pip install websocket-client certifi")

try:
    import certifi
    CA = certifi.where()
except ImportError:
    CA = None

# ---------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------
MARKETS = {
    "R_10": "Volatility 10",
    "R_25": "Volatility 25",
    "R_50": "Volatility 50",
    "R_75": "Volatility 75",
    "R_100": "Volatility 100",
}
GROWTH_RATES = [0.01, 0.02, 0.03, 0.04, 0.05]

HISTORY = 5000            # ticks kept per market (Deriv allows up to 5000)
MIN_TICKS = 1500          # need at least this many ticks before giving a signal
SHORT = 50                # "recent" window for the stability check
SPIKE_SIGMA = 4.0         # tick change > 4 std-devs = spike
SPIKE_COOLDOWN = 20       # ticks to wait after a spike
HOLD_TICKS = 10           # backtest: enter on every tick, hold this many ticks
EV_TOLERANCE = 0.02       # SUITABLE if backtest result >= -0.02 per 1.00 staked
VOL_WAIT = 1.3            # recent/long volatility above this -> WAIT
VOL_BAD = 1.5             # recent/long volatility above this -> UNSUITABLE
SHOW_EVERY = 10           # seconds between dashboard prints
BARRIER_REFRESH = 120     # seconds between asking Deriv for barrier quotes
CSV_FILE = "deriv_ticks_accumulator.csv"

URLS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
]

# ---------------------------------------------------------------------
# State
# ---------------------------------------------------------------------
prices = {m: deque(maxlen=HISTORY) for m in MARKETS}
last_epoch = {m: 0 for m in MARKETS}
logged_epoch = {m: 0 for m in MARKETS}
cached_sigma = {m: None for m in MARKETS}
deriv_barrier = {}                 # (market, growth) -> barrier distance in price units
proposal_ids = {}                  # req_id -> (market, growth)
proposal_error_shown = False
NORMAL = statistics.NormalDist()


# ---------------------------------------------------------------------
# Maths
# ---------------------------------------------------------------------
def tick_changes(m):
    p = list(prices[m])
    return [p[i] - p[i - 1] for i in range(1, len(p))]


def barrier_for(m, g, sigma):
    """Returns (barrier distance, source)."""
    if (m, g) in deriv_barrier:
        return deriv_barrier[(m, g)], "Deriv"
    survive = 1.0 / (1.0 + g)                       # fair-game survival chance per tick
    z = NORMAL.inv_cdf((1.0 + survive) / 2.0)
    return z * sigma, "estimate"


def backtest(changes, barrier, g, hold):
    """Enter on every tick, hold `hold` ticks. Returns stats dict."""
    n = len(changes)
    breaks = [1 if abs(x) >= barrier else 0 for x in changes]
    total_breaks = sum(breaks)
    prefix = [0]
    for b in breaks:
        prefix.append(prefix[-1] + b)
    entries = n - hold + 1
    if entries <= 0:
        return None
    survived = sum(1 for i in range(entries) if prefix[i + hold] - prefix[i] == 0)
    win_rate = survived / entries
    ev = win_rate * (1.0 + g) ** hold - 1.0         # result per 1.00 staked
    return {
        "break_rate": total_breaks / n,
        "fair_break": g / (1.0 + g),
        "ticks_per_break": (n / total_breaks) if total_breaks else None,
        "win_rate": win_rate,
        "ev": ev,
        "n": n,
    }


def analyse(m):
    changes = tick_changes(m)
    n = len(changes)
    if n < 100:
        return None
    sigma = statistics.pstdev(changes)
    if sigma == 0:
        return None
    cached_sigma[m] = sigma
    short = statistics.pstdev(changes[-SHORT:])
    vol_ratio = short / sigma
    spike_idx = [i for i, x in enumerate(changes) if abs(x) > SPIKE_SIGMA * sigma]
    since_spike = (n - 1 - spike_idx[-1]) if spike_idx else None
    rows = {}
    for g in GROWTH_RATES:
        barrier, src = barrier_for(m, g, sigma)
        res = backtest(changes, barrier, g, HOLD_TICKS)
        if res:
            res["barrier"] = barrier
            res["source"] = src
            rows[g] = res
    if not rows:
        return None
    best_g = max(rows, key=lambda g: rows[g]["ev"])
    return {
        "n": n,
        "sigma_pct": sigma / prices[m][-1] * 100.0,
        "vol_ratio": vol_ratio,
        "spikes": len(spike_idx),
        "since_spike": since_spike,
        "rows": rows,
        "best_g": best_g,
        "best_ev": rows[best_g]["ev"],
    }


def signal(a):
    if a is None or a["n"] < MIN_TICKS:
        return "WAIT"
    if a["since_spike"] is not None and a["since_spike"] < SPIKE_COOLDOWN:
        return "WAIT"
    if a["vol_ratio"] > VOL_BAD or a["best_ev"] < -EV_TOLERANCE:
        return "UNSUITABLE"
    if a["vol_ratio"] > VOL_WAIT:
        return "WAIT"
    return "SUITABLE"


# ---------------------------------------------------------------------
# Deriv barrier quotes
# ---------------------------------------------------------------------
def request_barriers(ws):
    rid = 1000
    for m in MARKETS:
        for g in GROWTH_RATES:
            rid += 1
            proposal_ids[rid] = (m, g)
            ws.send(json.dumps({
                "proposal": 1, "amount": 10, "basis": "stake",
                "contract_type": "ACCU", "currency": "USD",
                "symbol": m, "growth_rate": g, "req_id": rid,
            }))


def parse_barrier(msg):
    """Barrier distance (price units) from a proposal reply, or None."""
    cd = msg.get("proposal", {}).get("contract_details", {})
    try:
        if cd.get("barrier_spot_distance") not in (None, ""):
            return abs(float(cd["barrier_spot_distance"]))
        if cd.get("high_barrier") not in (None, "") and cd.get("low_barrier") not in (None, ""):
            return abs(float(cd["high_barrier"]) - float(cd["low_barrier"])) / 2.0
    except (TypeError, ValueError):
        pass
    return None


# ---------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------
def fmt_since(v):
    return "-" if v is None else str(v)


def show(results):
    line = "=" * 92
    print("\n" + line)
    print(f"DERIV ACCUMULATOR CONDITIONS | backtest: enter every tick, hold {HOLD_TICKS} ticks "
          f"| data: last {HISTORY} ticks")
    print(line)
    print(f"{'INDEX':<16}{'TICKS':>7}{'VOL%/tick':>11}{'RECENT/LONG':>13}{'SPIKES':>8}"
          f"{'SINCE':>7}{'BEST RATE':>11}{'RESULT':>9}  SIGNAL")
    print("-" * 92)
    sigs = {}
    for m, name in MARKETS.items():
        a = results[m]
        s = signal(a)
        sigs[m] = s
        if a is None:
            print(f"{name:<16}{len(prices[m]):>7}  collecting data...    WAIT")
            continue
        print(f"{name:<16}{a['n']:>7}{a['sigma_pct']:>10.4f}%{a['vol_ratio']:>13.2f}"
              f"{a['spikes']:>8}{fmt_since(a['since_spike']):>7}"
              f"{a['best_g'] * 100:>10.0f}%{a['best_ev']:>+9.3f}  {s}")
    print("-" * 92)

    ranked = [m for m in MARKETS if results[m] is not None and len(prices[m]) > MIN_TICKS]
    if not ranked:
        print(f"Collecting data... need {MIN_TICKS} ticks per index before giving a signal.")
        print(line, flush=True)
        return
    suitable = [m for m in ranked if sigs[m] == "SUITABLE"]
    pool = suitable if suitable else ranked
    top = max(pool, key=lambda m: results[m]["best_ev"])
    a = results[top]

    print(f"DETAIL: {MARKETS[top]} ({'best conditions now' if suitable else 'closest to suitable, but none is SUITABLE'})")
    print(f"{'GROWTH':>7}{'BARRIER':>12}{'SOURCE':>10}{'BREAKS':>9}{'FAIR':>8}"
          f"{'TICKS/BREAK':>13}{'WIN RATE':>10}{'RESULT':>9}")
    for g, r in a["rows"].items():
        tpb = f"{r['ticks_per_break']:.0f}" if r["ticks_per_break"] else ">%d" % r["n"]
        print(f"{g * 100:>6.0f}%{r['barrier']:>12.5f}{r['source']:>10}{r['break_rate'] * 100:>8.2f}%"
              f"{r['fair_break'] * 100:>7.2f}%{tpb:>13}{r['win_rate'] * 100:>9.1f}%{r['ev']:>+9.3f}")
    print("-" * 92)
    if suitable:
        print(f"BEST INDEX: {MARKETS[top]} at {a['best_g'] * 100:.0f}% growth   ->   SIGNAL: {sigs[top]}")
    else:
        print("BEST INDEX: none right now   ->   SIGNAL: WAIT / UNSUITABLE for all indices")
    print("RESULT = average profit per 1.00 staked in the backtest (negative = loses money).")
    print("BREAKS vs FAIR: breaks above FAIR means the barrier is hit more often than break-even allows.")
    print("A good backtest does not predict the next tick. One barrier break loses the whole stake.")
    print(line, flush=True)


# ---------------------------------------------------------------------
# Data handling
# ---------------------------------------------------------------------
def log_tick(writer, m, price, epoch):
    if epoch > logged_epoch[m]:
        logged_epoch[m] = epoch
        when = datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        writer.writerow([when, m, price])


def on_history(msg, writer):
    m = msg["echo_req"]["ticks_history"]
    if m not in prices:
        return
    h = msg["history"]
    prices[m].clear()
    last_epoch[m] = 0
    for price, epoch in zip(h["prices"], h["times"]):
        prices[m].append(float(price))
        last_epoch[m] = int(epoch)
        log_tick(writer, m, price, int(epoch))
    print(f"[HISTORY] {m}: loaded {len(prices[m])} ticks")


def on_tick(msg, writer):
    t = msg["tick"]
    m = t["symbol"]
    epoch = int(t["epoch"])
    if m not in prices or epoch <= last_epoch[m]:
        return
    last_epoch[m] = epoch
    price = float(t["quote"])
    prev = prices[m][-1] if prices[m] else None
    prices[m].append(price)
    log_tick(writer, m, t["quote"], epoch)
    sigma = cached_sigma[m]
    if prev is not None and sigma and abs(price - prev) > SPIKE_SIGMA * sigma:
        print(f"[SPIKE] {MARKETS[m]}: tick moved {price - prev:+.5f} "
              f"({abs(price - prev) / sigma:.1f} std-devs)")


def on_proposal(msg):
    global proposal_error_shown
    key = proposal_ids.get(msg.get("echo_req", {}).get("req_id"))
    if key is None:
        return
    if "error" in msg:
        if not proposal_error_shown:
            err = msg["error"]
            print("[BARRIER] Deriv did not give a barrier quote "
                  f"({err.get('message', err) if isinstance(err, dict) else err}). "
                  "Using the fair-game ESTIMATE instead.")
            proposal_error_shown = True
        return
    dist = parse_barrier(msg)
    if dist:
        deriv_barrier[key] = dist


# ---------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------
def connect():
    opts = {"cert_reqs": ssl.CERT_REQUIRED}
    if CA:
        opts["ca_certs"] = CA
    for url in URLS:
        try:
            ws = websocket.create_connection(url, timeout=20, sslopt=opts)
            ws.settimeout(2)
            print("[CONNECTED]", url)
            return ws
        except Exception as exc:
            print("[CONNECTION FAILED]", exc)
    return None


def run(ws, writer, f):
    for m in MARKETS:
        ws.send(json.dumps({"ticks_history": m, "count": HISTORY, "end": "latest",
                            "style": "ticks", "subscribe": 1}))
    request_barriers(ws)
    now = time.time()
    last_show = last_ping = 0.0
    last_barrier = now
    last_ping = now
    while True:
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            raw = None
        if raw == "":
            raise ConnectionError("server closed the connection")

        if raw:
            msg = json.loads(raw)
            mtype = msg.get("msg_type")
            if mtype == "history":
                on_history(msg, writer)
            elif mtype == "tick":
                on_tick(msg, writer)
            elif mtype == "proposal" or msg.get("echo_req", {}).get("proposal"):
                on_proposal(msg)
            elif "error" in msg:
                print("[DERIV ERROR]", msg["error"].get("message"))

        now = time.time()
        if now - last_ping > 30:
            ws.send(json.dumps({"ping": 1}))
            last_ping = now
        if now - last_barrier > BARRIER_REFRESH:
            request_barriers(ws)
            last_barrier = now
        if now - last_show >= SHOW_EVERY:
            show({m: analyse(m) for m in MARKETS})
            f.flush()
            last_show = now


def main():
    print("DERIV ACCUMULATOR CONDITIONS ANALYSER | analysis only, no trades")
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if f.tell() == 0:
            writer.writerow(["time_utc", "market", "price"])
        try:
            while True:
                ws = connect()
                if ws is None:
                    print("No connection. Retrying in 10 seconds...")
                    time.sleep(10)
                    continue
                try:
                    run(ws, writer, f)
                except Exception as exc:
                    print("[DISCONNECTED]", exc, "- reconnecting in 5 seconds...")
                finally:
                    try:
                        ws.close()
                    except Exception:
                        pass
                time.sleep(5)
        except KeyboardInterrupt:
            print("\nStopped. Ticks saved to", CSV_FILE)


if __name__ == "__main__":
    main()