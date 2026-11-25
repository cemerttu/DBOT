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

When to enter and when to stop (new)
  ENTER NOW  appears when the backtest is not losing, volatility is normal, there was no
             recent spike, and the market has been calm for CALM_TICKS ticks in a row.
  CLOSE NOW  appears after TARGET_TICKS ticks (take profit), or when a tick comes close
             to the barrier (NEAR_MISS), or when volatility turns hot.
  The bot follows its own signals with a VIRTUAL position and keeps score of what would
  have happened, so you can see whether the rules help. No real trade is ever placed.

Honest warning: Volatility indices are generated with constant volatility, so spikes
and "conditions" are mostly noise and cannot predict the next tick. An accumulator
loses its whole stake the moment one tick breaks the barrier.
"""

import csv
import json
import math
import os
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

# ENTRY AND EXIT RULES
TARGET_TICKS = HOLD_TICKS   # take profit: close after this many ticks
CALM_TICKS = 5              # entry needs this many calm ticks in a row
CALM_FRAC = 0.5             # a tick is "calm" if it moved less than this x the barrier
NEAR_MISS = 0.8             # close if a tick moves this x the barrier or more
REQUIRE_SUITABLE = True     # False = ignore the backtest/volatility gate for entries
USE_COLOR = True
SOUND = True

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

# Deriv's newer endpoint calls the market field "underlying_symbol";
# the older endpoint calls it "symbol".
SYMBOL_FIELD = ["underlying_symbol"]
symbol_toggled = [False]
requote_now = [False]

latest = {m: None for m in MARKETS}          # last full analysis
calm_run = {m: 0 for m in MARKETS}           # calm ticks in a row
spike_age = {m: 999 for m in MARKETS}        # ticks since the last spike
rearm = {m: True for m in MARKETS}           # may a new entry start?
position = {m: None for m in MARKETS}        # virtual position, or None
vrecord = {m: {"n": 0, "profit": 0, "broken": 0, "net": 0.0} for m in MARKETS}


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
                SYMBOL_FIELD[0]: m, "growth_rate": g, "req_id": rid,
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
# ---------------------------------------------------------------------
# Entry and exit rules
# ---------------------------------------------------------------------
def c(text, code):
    """Colour text for the terminal (set USE_COLOR = False to turn off)."""
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text


def plan(m):
    """Growth rate and barrier the bot would use for this index right now."""
    a = latest[m]
    if a is None:
        return None
    g = a["best_g"]
    r = a["rows"][g]
    return {"g": g, "barrier": r["barrier"], "src": r["source"]}


def sync_state(results):
    for m in MARKETS:
        latest[m] = results[m]
        a = results[m]
        if a is not None and spike_age[m] == 999 and a["since_spike"] is not None:
            spike_age[m] = a["since_spike"]


def entry_ready(m):
    """(True, 'ready') or (False, reason) for starting a new position."""
    a = latest[m]
    if a is None or a["n"] < MIN_TICKS:
        return False, "collecting data"
    if REQUIRE_SUITABLE:
        if a["vol_ratio"] > VOL_BAD:
            return False, "volatility is abnormal"
        if a["best_ev"] < -EV_TOLERANCE:
            return False, (f"backtest loses money ({a['best_ev']:+.3f} per 1.00, "
                           f"limit -{EV_TOLERANCE:.2f})")
    if a["vol_ratio"] > VOL_WAIT:
        return False, "volatility is rising"
    if spike_age[m] < SPIKE_COOLDOWN:
        return False, f"spike {spike_age[m]} ticks ago"
    if calm_run[m] < CALM_TICKS:
        return False, f"waiting for calm ({calm_run[m]}/{CALM_TICKS} ticks)"
    if not rearm[m]:
        return False, "waiting for the next calm period"
    return True, "ready"


def after_tick(m, change):
    """Runs on every live tick: update calm/spike counters, manage the position."""
    d = abs(change)
    sigma = cached_sigma[m]

    if sigma and d > SPIKE_SIGMA * sigma:
        spike_age[m] = 0
    else:
        spike_age[m] += 1

    p = plan(m)
    if p is None:
        return

    if d < CALM_FRAC * p["barrier"]:
        calm_run[m] += 1
    else:
        calm_run[m] = 0
        rearm[m] = True

    pos = position[m]
    if pos is not None:
        manage_position(m, pos, d)
        return

    ok, _ = entry_ready(m)
    if ok:
        open_position(m, p)


def open_position(m, p):
    position[m] = {"g": p["g"], "barrier": p["barrier"], "src": p["src"], "held": 0}
    rearm[m] = False
    a = latest[m]
    since = "a long time" if spike_age[m] >= 999 else f"{spike_age[m]} ticks"
    if SOUND:
        print("\a", end="")
    print()
    print(c("=" * 72, "92"))
    print(c(f" ENTER NOW   {MARKETS[m]}   (analysis only, no trade placed)", "92;1"))
    print(c("=" * 72, "92"))
    print(f" Growth rate : {p['g'] * 100:.0f}%")
    print(f" Barrier     : {p['barrier']:.5f} either side of the price ({p['src']})")
    print(f" Why         : {CALM_TICKS} calm ticks in a row, no spike for {since}, "
          f"volatility {a['vol_ratio']:.2f}x normal")
    print(f" Plan        : hold up to {TARGET_TICKS} ticks "
          f"(stake would grow to {(1 + p['g']) ** TARGET_TICKS:.2f}x)")
    print(f" CLOSE when  : {TARGET_TICKS} ticks pass, or a tick reaches "
          f"{NEAR_MISS * 100:.0f}% of the barrier, or volatility turns hot")
    print(c(" Risk        : one tick beyond the barrier loses the WHOLE stake.", "93"))
    print(c("=" * 72, "92"), flush=True)


def manage_position(m, pos, d):
    pos["held"] += 1
    held = pos["held"]
    value = (1 + pos["g"]) ** held
    a = latest[m]

    if d >= pos["barrier"]:
        close_position(m, pos, "a tick went beyond the barrier, the stake is lost",
                       -1.0, broken=True)
    elif d >= NEAR_MISS * pos["barrier"]:
        close_position(m, pos, "NEAR MISS: a tick came close to the barrier", value - 1)
    elif a is not None and a["vol_ratio"] > VOL_WAIT:
        close_position(m, pos, "volatility turned hot", value - 1)
    elif held >= TARGET_TICKS:
        close_position(m, pos, "target reached, take profit", value - 1)


def close_position(m, pos, reason, result, broken=False):
    position[m] = None
    r = vrecord[m]
    r["n"] += 1
    r["net"] += result
    r["broken" if broken else "profit"] += 1
    colour = "91" if broken else "93"
    title = "BARRIER BROKEN" if broken else "CLOSE NOW"
    if SOUND:
        print("\a", end="")
    print()
    print(c("=" * 72, colour))
    print(c(f" {title}   {MARKETS[m]}   (analysis only, no trade placed)", colour + ";1"))
    print(c("=" * 72, colour))
    print(f" Reason      : {reason}")
    print(f" Held        : {pos['held']} ticks")
    print(f" Result      : {result * 100:+.1f}% of the stake (virtual)")
    if broken:
        print(" Note        : in a real trade this loss happens instantly; "
              "no signal can prevent it.")
    print(c("=" * 72, colour), flush=True)


def entry_evidence(m):
    """
    Did entering after CALM_TICKS calm ticks survive TARGET_TICKS ticks more often
    than entering on any tick? Returns (rate_rule, n_rule, rate_all, verdict).
    """
    p = plan(m)
    if p is None:
        return None
    changes = tick_changes(m)
    n = len(changes)
    hold = TARGET_TICKS
    barrier = p["barrier"]
    if n < hold + 100:
        return None

    prefix = [0]
    for x in changes:
        prefix.append(prefix[-1] + (1 if abs(x) >= barrier else 0))

    run = 0
    all_s = all_n = rule_s = rule_n = 0
    for i in range(n - hold + 1):
        survived = (prefix[i + hold] - prefix[i]) == 0
        all_n += 1
        all_s += survived
        if run >= CALM_TICKS:
            rule_n += 1
            rule_s += survived
        run = run + 1 if abs(changes[i]) < CALM_FRAC * barrier else 0

    if all_n == 0 or rule_n == 0:
        return None
    rate_all = all_s / all_n
    rate_rule = rule_s / rule_n
    independent = rule_n / hold          # overlapping entries are not independent
    if independent < 30:
        verdict = "not enough calm entries to judge"
    else:
        se = math.sqrt(max(rate_all * (1 - rate_all), 1e-9) / independent)
        verdict = ("rule supported by history" if rate_rule > rate_all + 3 * se
                   else "no measurable effect")
    return rate_rule, rule_n, rate_all, verdict


def show_plan():
    print("WHEN TO ENTER AND WHEN TO STOP")
    print(f"  ENTER : backtest not losing, volatility normal, no recent spike, and "
          f"{CALM_TICKS} calm ticks in a row")
    print(f"          (a calm tick moves less than {CALM_FRAC * 100:.0f}% of the barrier)")
    print(f"  STOP  : close after {TARGET_TICKS} ticks, OR a tick reaches "
          f"{NEAR_MISS * 100:.0f}% of the barrier, OR volatility turns hot")
    print()
    print(f"  {'INDEX':<16}NOW")
    for m, name in MARKETS.items():
        pos = position[m]
        if pos is not None:
            held = pos["held"]
            line = c(f"IN POSITION: held {held}/{TARGET_TICKS} ticks, stake value "
                     f"{(1 + pos['g']) ** held:.2f}x", "92;1")
        else:
            ok, why = entry_ready(m)
            line = c("ready: entry fires on the next calm tick", "92") if ok else f"wait: {why}"
        print(f"  {name:<16}{line}")

    print()
    print(f"  Does the entry rule work? (survived {TARGET_TICKS} ticks in the stored history)")
    for m, name in MARKETS.items():
        ev = entry_evidence(m)
        if ev is None:
            print(f"    {name:<16}not enough data yet")
        else:
            rate_rule, n_rule, rate_all, verdict = ev
            print(f"    {name:<16}after calm {rate_rule * 100:.1f}% (n={n_rule})   "
                  f"any tick {rate_all * 100:.1f}%   {verdict}")

    print()
    total = sum(v["n"] for v in vrecord.values())
    print("  IF YOU HAD FOLLOWED THE SIGNALS (virtual, since the bot started)")
    if total == 0:
        print("    no completed virtual trades yet")
    else:
        for m, name in MARKETS.items():
            v = vrecord[m]
            if v["n"]:
                print(f"    {name:<16}{v['n']} trades, {v['profit']} closed in profit, "
                      f"{v['broken']} broken, net {v['net']:+.2f} per 1.00 staked")
        net = sum(v["net"] for v in vrecord.values())
        broken = sum(v["broken"] for v in vrecord.values())
        print(f"    {'ALL':<16}{total} trades, {broken} broken, net {net:+.2f} "
              f"(average {net / total:+.3f} per trade)")
    print("  " + "=" * 90, flush=True)


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
    if prev is not None:
        after_tick(m, price - prev)


def on_proposal(msg):
    global proposal_error_shown
    key = proposal_ids.get(msg.get("echo_req", {}).get("req_id"))
    if key is None:
        return
    if "error" in msg:
        err0 = msg["error"]
        text0 = str(err0.get("message", err0)) if isinstance(err0, dict) else str(err0)
        if (not symbol_toggled[0] and "validation" in text0.lower() and "symbol" in text0):
            SYMBOL_FIELD[0] = ("symbol" if SYMBOL_FIELD[0] == "underlying_symbol"
                               else "underlying_symbol")
            symbol_toggled[0] = True
            requote_now[0] = True
            return
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
            SYMBOL_FIELD[0] = ("underlying_symbol" if "api.derivws.com" in url else "symbol")
            symbol_toggled[0] = False
            return ws
        except Exception as exc:
            print("[CONNECTION FAILED]", exc)
    return None


def run(ws, writer, f):
    for m in MARKETS:
        ws.send(json.dumps({"ticks_history": m, "count": HISTORY, "end": "latest",
                            "style": "ticks", "subscribe": 1}))
    request_barriers(ws)
    for mk in MARKETS:
        position[mk] = None
        calm_run[mk] = 0
        spike_age[mk] = 999
        rearm[mk] = True
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
        if requote_now[0]:
            request_barriers(ws)
            requote_now[0] = False
            last_barrier = now
        if now - last_barrier > BARRIER_REFRESH:
            request_barriers(ws)
            last_barrier = now
        if now - last_show >= SHOW_EVERY:
            results = {m: analyse(m) for m in MARKETS}
            sync_state(results)
            show(results)
            show_plan()
            f.flush()
            last_show = now


def main():
    os.system("")  # lets Windows terminals show colours
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