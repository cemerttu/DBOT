#!/usr/bin/env python3
"""
DERIV MULTI-MARKET ANALYZER (with tick DURATION)
READ-ONLY: No trades are placed.

Markets:
R_10, R_25, R_50, R_75, R_100

Contracts:
OVER 1 / UNDER 8

New: after every trigger (a miss) the bot tracks what happens at
1, 2, 3, 4 and 5 ticks AFTER the entry signal, and shows the
duration (in ticks) with the best completed record.

Install:
python -m pip install websocket-client certifi

Run:
python deriv_multi_market_analyzer.py
"""

import csv
import json
import math
import os
import ssl
import sys
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    import websocket
except ImportError:
    sys.exit(
        "Install required package:\n"
        "python -m pip install websocket-client certifi"
    )

try:
    import certifi
    CA_FILE = certifi.where()
except ImportError:
    CA_FILE = None


# ============================================================
# SETTINGS
# ============================================================

MARKETS = ["R_10", "R_25", "R_50", "R_75", "R_100"]
CONTRACTS = ["OVER 1", "UNDER 8"]

# Durations (in ticks) to test after each signal.
DURATIONS = [1, 2, 3, 4, 5]
MAX_DURATION = max(DURATIONS)

# Minimum completed signals before a duration can be called "best".
MIN_SAMPLES = 20

# A duration is only called an edge if its win rate beats the plain
# hit rate by this many standard errors (3 = very unlikely to be luck).
EDGE_SIGMA = 3

# How many recent tick gaps are averaged to get seconds per tick.
GAP_SAMPLES = 200

# Display options
USE_COLOR = True    # coloured terminal text
SOUND = True        # beep when an entry signal appears
VERBOSE = False     # True = also print the detailed statistics tables

# EMA trend: fast EMA above slow EMA = UP, below = DOWN.
EMA_FAST = 10
EMA_SLOW = 30
TRENDS = ["UP", "DOWN", "FLAT"]

# Trend filter: a signal only becomes an ENTRY alert when the EMA trend
# at signal time is allowed for that contract. Blocked signals are still
# tracked and recorded, so you can compare filtered vs unfiltered results.
TREND_FILTER = True
TREND_RULE = {
    "OVER 1": ["UP"],
    "UNDER 8": ["DOWN"],
}

WINDOW = 500
SHOW_EVERY = 10
PING_EVERY = 30

TICK_FILE = "deriv_ticks.csv"
# New file name because the columns changed (duration added).
SIGNAL_FILE = "deriv_signals_v3.csv"

ENDPOINTS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://ws.deriv.com/websockets/v3?app_id=1089",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
]

MAX_SEEN = 3000


# ============================================================
# STATE
# ============================================================

digits = {m: deque(maxlen=WINDOW) for m in MARKETS}
decimals = {m: 2 for m in MARKETS}
last_epoch = {m: 0 for m in MARKETS}
last_price = {m: None for m in MARKETS}
tick_gaps = {m: deque(maxlen=GAP_SAMPLES) for m in MARKETS}

ema_fast = {m: None for m in MARKETS}
ema_slow = {m: None for m in MARKETS}

# trend_record[(market, contract, trend)][duration] = [wins, completed]
trend_record = {
    (m, c, t): {d: [0, 0] for d in DURATIONS}
    for m in MARKETS
    for c in CONTRACTS
    for t in TRENDS
}

history_ready = {m: False for m in MARKETS}
live_buffer = {m: deque() for m in MARKETS}

# Active signals still collecting ticks (list per market/contract).
active = {
    (m, c): []
    for m in MARKETS
    for c in CONTRACTS
}

# record[(market, contract)][duration] = [wins, completed]
record = {
    (m, c): {d: [0, 0] for d in DURATIONS}
    for m in MARKETS
    for c in CONTRACTS
}

seen_pairs = {m: set() for m in MARKETS}
seen_order = {m: deque() for m in MARKETS}

alerted = set()


# ============================================================
# HELPERS
# ============================================================

def utc_string(epoch):
    return datetime.fromtimestamp(
        int(epoch), timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S")


def now_utc():
    return datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def hit(contract, digit):
    if contract == "OVER 1":
        return digit > 1
    if contract == "UNDER 8":
        return digit < 8
    return False


def last_digit(market, price):
    precision = decimals[market]
    quantum = Decimal("1").scaleb(-precision)

    value = Decimal(str(price)).quantize(
        quantum,
        rounding=ROUND_HALF_UP
    )

    text = f"{value:.{precision}f}"
    return int(text[-1])


def is_duplicate(market, epoch, price):
    key = (int(epoch), str(price))

    if key in seen_pairs[market]:
        return True

    seen_pairs[market].add(key)
    seen_order[market].append(key)

    while len(seen_order[market]) > MAX_SEEN:
        old = seen_order[market].popleft()
        seen_pairs[market].discard(old)

    return False


# ============================================================
# CSV RECORDS
# ============================================================

def load_records():
    try:
        with open(SIGNAL_FILE, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                market = row.get("market")
                contract = row.get("contract")
                win = row.get("win")

                try:
                    duration = int(row.get("duration", ""))
                except ValueError:
                    continue

                key = (market, contract)

                if key not in record:
                    continue

                if duration not in record[key]:
                    continue

                if win not in ("0", "1"):
                    continue

                record[key][duration][1] += 1

                if win == "1":
                    record[key][duration][0] += 1

                trend_key = (market, contract, row.get("trend"))

                if trend_key in trend_record:
                    trend_record[trend_key][duration][1] += 1

                    if win == "1":
                        trend_record[trend_key][duration][0] += 1

        print("Previous signal records loaded.")

    except FileNotFoundError:
        print("No previous signal file. Starting new record.")

    except Exception as e:
        print("Signal record loading error:", e)


def write_tick(writer, epoch, market, price, digit):
    writer.writerow([
        utc_string(epoch),
        market,
        price,
        digit
    ])


def write_signal(
    writer,
    market,
    contract,
    trigger_epoch,
    trigger_digit,
    duration,
    result_epoch,
    result_digit,
    won,
    trend
):
    writer.writerow([
        utc_string(trigger_epoch),
        market,
        contract,
        trigger_digit,
        duration,
        utc_string(result_epoch),
        result_digit,
        int(won),
        trend
    ])


# ============================================================
# HISTORY
# ============================================================

def process_history(market, prices, times):
    """
    Historical data initializes the rolling window.
    It does NOT count as live signal performance.
    """

    pairs = sorted(
        zip(prices, times),
        key=lambda x: int(x[1])
    )

    digits[market].clear()
    seen_pairs[market].clear()
    seen_order[market].clear()

    last_epoch[market] = 0
    last_price[market] = None
    tick_gaps[market].clear()
    ema_fast[market] = None
    ema_slow[market] = None

    for price, epoch in pairs:
        epoch = int(epoch)
        digit = last_digit(market, price)
        record_gap(market, epoch)
        update_ema(market, price)

        digits[market].append(digit)
        last_epoch[market] = epoch
        last_price[market] = price

        key = (epoch, str(price))
        seen_pairs[market].add(key)
        seen_order[market].append(key)

        while len(seen_order[market]) > MAX_SEEN:
            old = seen_order[market].popleft()
            seen_pairs[market].discard(old)

    history_ready[market] = True

    print(
        f"{market}: loaded {len(digits[market])} historical ticks"
    )


# ============================================================
# LIVE TICK PROCESSING
# ============================================================

def process_live(
    market,
    price,
    epoch,
    tick_writer,
    signal_writer
):
    epoch = int(epoch)

    # Do not accept ticks older than current market state.
    if epoch < last_epoch[market]:
        return False

    # Prevent exact duplicate ticks on reconnect.
    if is_duplicate(market, epoch, price):
        return False

    digit = last_digit(market, price)
    record_gap(market, epoch)
    update_ema(market, price)

    # --------------------------------------------------------
    # SCORE ACTIVE SIGNALS AT EACH DURATION
    # Tick 1 after the trigger = 1-tick contract result,
    # tick 2 = 2-tick contract result, and so on.
    # --------------------------------------------------------

    for contract in CONTRACTS:
        key = (market, contract)
        still_active = []

        for signal in active[key]:
            signal["count"] += 1
            duration = signal["count"]

            if duration in DURATIONS:
                won = hit(contract, digit)

                record[key][duration][1] += 1

                if won:
                    record[key][duration][0] += 1

                trend_key = (market, contract, signal["trend"])
                trend_record[trend_key][duration][1] += 1

                if won:
                    trend_record[trend_key][duration][0] += 1

                write_signal(
                    signal_writer,
                    market,
                    contract,
                    signal["epoch"],
                    signal["digit"],
                    duration,
                    epoch,
                    digit,
                    won,
                    signal["trend"]
                )

            if duration < MAX_DURATION:
                still_active.append(signal)

        active[key] = still_active

    # --------------------------------------------------------
    # STORE CURRENT TICK
    # --------------------------------------------------------

    digits[market].append(digit)
    last_epoch[market] = epoch
    last_price[market] = price

    write_tick(
        tick_writer,
        epoch,
        market,
        price,
        digit
    )

    # --------------------------------------------------------
    # CREATE NEW SIGNALS AFTER A MISS
    # --------------------------------------------------------

    for contract in CONTRACTS:
        key = (market, contract)

        if not hit(contract, digit):
            active[key].append({
                "epoch": epoch,
                "digit": digit,
                "count": 0,
                "trend": current_trend(market) or "FLAT"
            })

    return True


# ============================================================
# ANALYSIS
# ============================================================

def rate(market, contract):
    values = digits[market]

    if not values:
        return 0.0

    wins = sum(
        1 for d in values
        if hit(contract, d)
    )

    return wins / len(values) * 100


def ready():
    return all(
        len(digits[m]) >= WINDOW
        for m in MARKETS
    )


def best():
    choices = [
        (m, c)
        for m in MARKETS
        for c in CONTRACTS
    ]

    return max(
        choices,
        key=lambda x: rate(x[0], x[1])
    )


def fresh_signal(market, contract):
    """Signal created on the tick that was just processed."""
    for signal in reversed(active[(market, contract)]):
        if signal["count"] == 0:
            return signal
    return None


def update_ema(market, price):
    """Update fast and slow EMA of the price for this market."""
    p = float(price)

    for store, length in (
        (ema_fast, EMA_FAST),
        (ema_slow, EMA_SLOW)
    ):
        prev = store[market]

        if prev is None:
            store[market] = p
        else:
            alpha = 2 / (length + 1)
            store[market] = alpha * p + (1 - alpha) * prev


def current_trend(market):
    """UP / DOWN / FLAT from fast vs slow EMA (None while warming up)."""
    fast = ema_fast[market]
    slow = ema_slow[market]

    if fast is None or slow is None:
        return None

    if fast > slow:
        return "UP"

    if fast < slow:
        return "DOWN"

    return "FLAT"


def trend_text(market):
    return current_trend(market) or "..."


def beats_baseline_counts(market, contract, wins, total):
    """Same edge test as beats_baseline, for any wins/total counts."""
    if total < MIN_SAMPLES:
        return False

    baseline = rate(market, contract) / 100
    observed = wins / total
    std_err = math.sqrt(baseline * (1 - baseline) / total)

    return observed > baseline + EDGE_SIGMA * std_err


def trend_confirms(market, contract, trend=None):
    """True if the EMA trend allows an entry for this contract."""
    if not TREND_FILTER:
        return True

    trend = trend or current_trend(market)

    if trend is None:
        return False

    return trend in TREND_RULE.get(contract, TRENDS)


def confirmed_record(market, contract, duration):
    """Wins/completed counting only signals the trend filter allows."""
    allowed = TREND_RULE.get(contract, TRENDS) if TREND_FILTER else TRENDS

    wins = 0
    total = 0

    for t in allowed:
        w, n = trend_record[(market, contract, t)][duration]
        wins += w
        total += n

    return wins, total


def trend_table(market, contract):
    trend = current_trend(market)

    print()
    print(
        f"EMA({EMA_FAST}/{EMA_SLOW}) trend now: "
        f"{trend or 'warming up'}"
    )
    print("Results split by the trend at signal time "
          "(* = beats baseline):")

    for t in ("UP", "DOWN"):
        parts = []

        for d in DURATIONS:
            wins, total = trend_record[(market, contract, t)][d]

            if total == 0:
                parts.append(f"{d}t: -")
                continue

            star = (
                "*"
                if beats_baseline_counts(market, contract, wins, total)
                else ""
            )

            parts.append(
                f"{d}t: {wins}/{total} "
                f"({wins / total * 100:.0f}%){star}"
            )

        marker = "<-- now" if t == trend else ""
        print(f"  {t:<5}" + "  ".join(parts) + f"  {marker}")

    if TREND_FILTER:
        allowed = "/".join(TREND_RULE.get(contract, TRENDS))
        parts = []

        for d in DURATIONS:
            wins, total = confirmed_record(market, contract, d)

            if total == 0:
                parts.append(f"{d}t: -")
                continue

            star = (
                "*"
                if beats_baseline_counts(market, contract, wins, total)
                else ""
            )

            parts.append(
                f"{d}t: {wins}/{total} "
                f"({wins / total * 100:.0f}%){star}"
            )

        print()
        print(
            f"TREND FILTER ON: {contract} enters only when trend is "
            f"{allowed}"
        )
        print(f"  ENTRY-ONLY  " + "  ".join(parts))


def record_gap(market, epoch):
    """Store seconds between this tick and the previous one."""
    prev = last_epoch[market]

    if prev > 0 and epoch > prev:
        tick_gaps[market].append(epoch - prev)


def avg_gap(market):
    """Measured average seconds per tick (None until enough data)."""
    gaps = tick_gaps[market]

    if len(gaps) < 5:
        return None

    return sum(gaps) / len(gaps)


def gap_text(market):
    gap = avg_gap(market)

    if gap is None:
        return "..."

    return f"{gap:.1f}s"


def duration_label(market, duration):
    label = f"{duration} tick" + ("s" if duration > 1 else "")
    gap = avg_gap(market)

    if gap is not None:
        label += f" (~{duration * gap:.0f}s)"

    return label


def beats_baseline(market, contract, duration):
    """
    True only if this duration's win rate is higher than the plain
    hit rate by more than EDGE_SIGMA standard errors.
    """
    wins, total = record[(market, contract)][duration]

    if total < MIN_SAMPLES:
        return False

    baseline = rate(market, contract) / 100
    observed = wins / total
    std_err = math.sqrt(baseline * (1 - baseline) / total)

    return observed > baseline + EDGE_SIGMA * std_err


def best_duration(market, contract):
    """
    Duration (ticks) with the highest completed win rate,
    only counting durations with at least MIN_SAMPLES results.
    Returns None if there is not enough data yet.
    """
    candidates = []

    for d in DURATIONS:
        wins, total = record[(market, contract)][d]

        if total >= MIN_SAMPLES:
            candidates.append((wins / total, total, -d, d))

    if not candidates:
        return None

    return max(candidates)[3]


def record_text(market, contract, duration):
    wins, total = record[(market, contract)][duration]

    if total == 0:
        return "No completed signals"

    return (
        f"{wins}/{total} "
        f"({wins / total * 100:.2f}%)"
    )


def duration_table(market, contract):
    best_d = best_duration(market, contract)

    print(
        f"Plain hit rate (baseline): "
        f"{rate(market, contract):.2f}%   "
        f"Tick length: {gap_text(market)}"
    )

    print(
        f"{'DURATION':<18}"
        f"{'RECORD':<24}"
        f"{'NOTE'}"
    )

    for d in DURATIONS:
        note = ""

        if d == best_d:
            note = "<-- highest so far"

        if beats_baseline(market, contract, d):
            note = "<-- BEATS BASELINE"

        print(
            f"{duration_label(market, d):<18}"
            f"{record_text(market, contract, d):<24}"
            f"{note}"
        )

    trend_table(market, contract)


def duration_advice(market, contract):
    best_d = best_duration(market, contract)

    if best_d is None:
        return (
            "NOT ENOUGH DATA YET "
            f"(need {MIN_SAMPLES} completed signals per duration)"
        )

    label = duration_label(market, best_d)

    if not beats_baseline(market, contract, best_d):
        return (
            f"NO PROVEN EDGE - highest so far is {label}, "
            "but it is within normal random variation"
        )

    return f"{label} - beats the plain hit rate beyond random noise"


# ============================================================
# SIGNAL ALERT
# ============================================================

def c(text, code):
    """Colour text for the terminal (turn off with USE_COLOR = False)."""
    if not USE_COLOR:
        return text

    return f"\033[{code}m{text}\033[0m"


def plain_duration(market, contract):
    """(ticks, text, note): the suggested duration in plain words."""
    best_d = best_duration(market, contract)

    if best_d is None:
        ticks = 1
        note = "still collecting results, so 1 tick is only a default"
    elif beats_baseline(market, contract, best_d):
        ticks = best_d
        note = "has beaten the normal win rate on this market"
    else:
        ticks = best_d
        note = "leading so far, but not proven better than the others"

    gap = avg_gap(market)
    text = f"{ticks} tick" + ("s" if ticks > 1 else "")

    if gap is not None:
        text += f" (about {ticks * gap:.0f} seconds)"

    return ticks, text, note


def miss_text(contract):
    if contract == "OVER 1":
        return "0 or 1"

    return "8 or 9"


def past_result_text(market, contract, ticks):
    wins, total = confirmed_record(market, contract, ticks)

    if total == 0:
        return "no completed results yet"

    return f"{wins} of {total} won ({wins / total * 100:.0f}%)"


def alert(market, contract):
    signal = fresh_signal(market, contract)

    if signal is None:
        return

    alert_key = (
        market,
        contract,
        signal["epoch"],
        signal["digit"]
    )

    if alert_key in alerted:
        return

    alerted.add(alert_key)

    if not trend_confirms(market, contract, signal["trend"]):
        allowed = "/".join(TREND_RULE.get(contract, TRENDS))

        print(c(
            f"  skipped: {market} {contract} - trend is "
            f"{signal['trend']}, needs {allowed}",
            "2"
        ))
        return

    ticks, duration_text, note = plain_duration(market, contract)

    if SOUND:
        print("\a", end="")

    print()
    print(c("  " + "=" * 52, "92"))
    print(c("   ENTRY SIGNAL   (analysis only, no trade placed)", "92;1"))
    print(c("  " + "=" * 52, "92"))
    print(f"   Market      : {market}")
    print(f"   Trade       : {contract}")
    print(f"   Enter       : on the NEXT tick")
    print(f"   Hold for    : {duration_text}")
    print(f"   Trend       : {signal['trend']} (agrees with the rule)")
    print(
        "   Past result : "
        f"{past_result_text(market, contract, ticks)} at this duration"
    )
    print(f"   Duration    : {note}")
    print(c(
        "   Reminder    : winning ~80% still loses money long term.",
        "93"
    ))
    print(c("  " + "=" * 52, "92"))

    if VERBOSE:
        print()
        duration_table(market, contract)



# ============================================================
# DASHBOARD
# ============================================================

def show():
    print()
    print(c(
        f"  DERIV ANALYZER    {datetime.now():%H:%M:%S} (your time)",
        "96;1"
    ))
    print("  " + "-" * 56)
    print(
        f"  {'Market':<9}{'Last':<7}{'Trend':<8}"
        f"{'Over 1':<9}{'Under 8':<9}{'Tick'}"
    )

    for market in MARKETS:
        digit = digits[market][-1] if digits[market] else "-"
        over = f"{rate(market, 'OVER 1'):.0f}%"
        under = f"{rate(market, 'UNDER 8'):.0f}%"

        print(
            f"  {market:<9}{str(digit):<7}{trend_text(market):<8}"
            f"{over:<9}{under:<9}{gap_text(market)}"
        )

    print("  " + "-" * 56)
    print(c(
        f"  Over 1 / Under 8 = how often each would have won in the "
        f"last {WINDOW} ticks",
        "2"
    ))

    if not ready():
        print()
        print("  Collecting history, please wait...")

        for market in MARKETS:
            print(f"    {market:<7}{len(digits[market])}/{WINDOW}")

        return

    market, contract = best()

    print()
    print(
        f"  Watching : {market} {contract} "
        f"({rate(market, contract):.0f}% wins recently)"
    )

    signal = fresh_signal(market, contract)

    if signal and not trend_confirms(
        market, contract, signal["trend"]
    ):
        allowed = "/".join(TREND_RULE.get(contract, TRENDS))

        print(c(
            f"  Status   : skipped - trend is {signal['trend']}, "
            f"needs {allowed}",
            "93"
        ))
    elif signal:
        print(c(
            "  Status   : SIGNAL - enter on the NEXT tick",
            "92;1"
        ))
    else:
        print(
            "  Status   : waiting for a miss "
            f"(a digit of {miss_text(contract)})"
        )

    ticks, duration_text, note = plain_duration(market, contract)

    print(f"  Duration : {duration_text}")
    print(c(f"             {note}", "2"))
    print()
    print(c(
        "  No trade is placed. Digits are random, so a ~80% win rate "
        "does not give a profit.",
        "2"
    ))

    if VERBOSE:
        print()
        duration_table(market, contract)



# ============================================================
# CONNECTION
# ============================================================

def connect():
    ssl_options = {
        "cert_reqs": ssl.CERT_REQUIRED
    }

    if CA_FILE:
        ssl_options["ca_certs"] = CA_FILE

    for url in ENDPOINTS:
        print("\nTrying:", url)

        try:
            ws = websocket.create_connection(
                url,
                timeout=20,
                sslopt=ssl_options
            )

            ws.settimeout(2)

            print("CONNECTED SUCCESSFULLY:", url)
            return ws

        except Exception as e:
            print("Connection failed:", e)

    return None


# ============================================================
# SUBSCRIPTIONS
# ============================================================

def subscribe(ws):
    for market in MARKETS:
        request = {
            "ticks_history": market,
            "count": WINDOW,
            "end": "latest",
            "style": "ticks",
            "subscribe": 1
        }

        ws.send(json.dumps(request))

        print("Requested:", market)


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def run(ws, tick_writer, signal_writer, tick_file, signal_file):
    subscribe(ws)

    last_show = time.time()
    last_ping = time.time()

    while True:
        try:
            raw = ws.recv()

        except websocket.WebSocketTimeoutException:
            raw = None

        if raw == "":
            raise ConnectionError("Server closed connection.")

        if raw:
            msg = json.loads(raw)

            # ------------------------------------------------
            # ERROR
            # ------------------------------------------------

            if "error" in msg:
                print(
                    "Deriv error:",
                    msg["error"].get("message", msg["error"])
                )

            # ------------------------------------------------
            # HISTORY RESPONSE
            # ------------------------------------------------

            elif msg.get("msg_type") == "history":
                market = (
                    msg.get("echo_req", {})
                    .get("ticks_history")
                )

                if market not in MARKETS:
                    continue

                history = msg.get("history", {})

                prices = history.get("prices", [])
                times = history.get("times", [])

                decimals[market] = int(
                    msg.get("pip_size", decimals[market])
                )

                process_history(
                    market,
                    prices,
                    times
                )

                # Process buffered live ticks that arrived
                # while historical data was loading.

                buffered = list(live_buffer[market])
                live_buffer[market].clear()

                buffered.sort(
                    key=lambda t: int(t["epoch"])
                )

                for tick in buffered:
                    process_live(
                        market,
                        tick["quote"],
                        tick["epoch"],
                        tick_writer,
                        signal_writer
                    )

                tick_file.flush()
                signal_file.flush()

            # ------------------------------------------------
            # LIVE TICK
            # ------------------------------------------------

            elif msg.get("msg_type") == "tick":
                tick = msg.get("tick", {})

                market = tick.get("symbol")
                price = tick.get("quote")
                epoch = tick.get("epoch")

                if market not in MARKETS:
                    continue

                if price is None or epoch is None:
                    continue

                # Buffer ticks until history has arrived.

                if not history_ready[market]:
                    live_buffer[market].append({
                        "quote": price,
                        "epoch": epoch
                    })
                    continue

                accepted = process_live(
                    market,
                    price,
                    epoch,
                    tick_writer,
                    signal_writer
                )

                if accepted:
                    tick_file.flush()
                    signal_file.flush()

                    if ready():
                        best_market, best_contract = best()

                        if fresh_signal(
                            best_market, best_contract
                        ) is not None:

                            alert(
                                best_market,
                                best_contract
                            )

        # ----------------------------------------------------
        # PING
        # ----------------------------------------------------

        now = time.time()

        if now - last_ping >= PING_EVERY:
            ws.send(json.dumps({"ping": 1}))
            last_ping = now

        # ----------------------------------------------------
        # DASHBOARD
        # ----------------------------------------------------

        if now - last_show >= SHOW_EVERY:
            show()

            tick_file.flush()
            signal_file.flush()

            last_show = now


# ============================================================
# MAIN
# ============================================================

def main():
    os.system("")  # lets Windows terminals show colours
    load_records()

    with open(
        TICK_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as tick_file:

        tick_writer = csv.writer(tick_file)

        if tick_file.tell() == 0:
            tick_writer.writerow([
                "time_utc",
                "market",
                "price",
                "last_digit"
            ])

        with open(
            SIGNAL_FILE,
            "a",
            newline="",
            encoding="utf-8"
        ) as signal_file:

            signal_writer = csv.writer(signal_file)

            if signal_file.tell() == 0:
                signal_writer.writerow([
                    "trigger_time_utc",
                    "market",
                    "contract",
                    "trigger_digit",
                    "duration",
                    "result_time_utc",
                    "result_digit",
                    "win",
                    "trend"
                ])

            retry_delay = 5

            try:
                while True:
                    ws = connect()

                    if ws is None:
                        print(
                            f"All endpoints failed. "
                            f"Retrying in {retry_delay} seconds."
                        )

                        time.sleep(retry_delay)

                        retry_delay = min(
                            retry_delay * 2,
                            60
                        )

                        continue

                    retry_delay = 5

                    # Reset connection state.

                    for market in MARKETS:
                        history_ready[market] = False
                        live_buffer[market].clear()
                        for contract in CONTRACTS:
                            active[(market, contract)] = []

                    try:
                        run(
                            ws,
                            tick_writer,
                            signal_writer,
                            tick_file,
                            signal_file
                        )

                    except KeyboardInterrupt:
                        raise

                    except Exception as e:
                        print("\nConnection interrupted:", e)
                        print("Reconnecting...")

                    finally:
                        try:
                            ws.close()
                        except Exception:
                            pass

                    time.sleep(retry_delay)

            except KeyboardInterrupt:
                print("\n")
                print("=" * 65)
                print("ANALYZER STOPPED")
                print("Ticks saved:", TICK_FILE)
                print("Signals saved:", SIGNAL_FILE)
                print("No trades were placed.")
                print("=" * 65)


if __name__ == "__main__":
    main()