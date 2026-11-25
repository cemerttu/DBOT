#!/usr/bin/env python3
"""
DERIV TOUCH / NO TOUCH ANALYZER
READ-ONLY: No trades are placed.

What it does
------------
1. Reads live ticks for R_10 / R_25 / R_50 / R_75 / R_100.
2. Measures how much each market normally moves per tick.
3. For several barrier distances and tick durations, it estimates the
   chance that the price TOUCHES the barrier (and the chance it does not).
4. Asks Deriv for the real payout of each option and works out the
   expected value per $1 staked:

       value = chance_of_winning x payout_ratio - 1

   A negative value means the option loses money on average.

The bot cannot predict direction. These markets are random walks, so it
only checks whether a payout is fair.

Install:
python -m pip install websocket-client certifi

Run:
python deriv_touch_analyzer.py
"""

import json
import math
import os
import ssl
import sys
import time
from collections import deque
from datetime import datetime

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

# Contract lengths to test, in ticks (check Deriv for the lengths it
# offers for Touch / No Touch on each market).
TICK_DURATIONS = [5, 10]

# Barrier distance in "standard deviations of the move over the contract".
# 0.75 = close barrier (touch is likely), 2.0 = far barrier (unlikely).
Z_LEVELS = [0.75, 1.0, 1.5, 2.0]

WINDOW = 1500          # ticks of history kept per market
SIGMA_WINDOW = 500     # ticks used to measure normal movement

STAKE = 10
CURRENCY = "USD"

SHOW_EVERY = 15        # seconds between screen updates
PROPOSAL_EVERY = 60    # seconds between payout lookups
PING_EVERY = 30
TOP_N = 8              # rows shown in the options table

# Value alert: model value must be at least this (0.02 = +2%) and the
# last WINDOW ticks must also show a positive value.
VALUE_ALERT = 0.02

# Correction because ticks are checked one by one, not continuously.
DISCRETE_SHIFT = 0.5826

USE_COLOR = True
SOUND = True

ENDPOINTS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://ws.deriv.com/websockets/v3?app_id=1089",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
]


# ============================================================
# STATE
# ============================================================

prices = {m: deque(maxlen=WINDOW) for m in MARKETS}
pip = {m: 2 for m in MARKETS}
last_epoch = {m: 0 for m in MARKETS}
last_price = {m: None for m in MARKETS}
tick_gaps = {m: deque(maxlen=200) for m in MARKETS}

history_ready = {m: False for m in MARKETS}
live_buffer = {m: deque() for m in MARKETS}

# quotes[(market, ticks, z, contract_type)] = payout information
quotes = {}
req_map = {}
req_counter = [0]
quote_errors = []

alerted = set()


# ============================================================
# HELPERS
# ============================================================

def c(text, code):
    """Colour text for the terminal (turn off with USE_COLOR = False)."""
    if not USE_COLOR:
        return text

    return f"\033[{code}m{text}\033[0m"


def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def local_time():
    return datetime.now().strftime("%H:%M:%S")


def ready():
    return all(len(prices[m]) >= SIGMA_WINDOW for m in MARKETS)


# ============================================================
# MARKET MEASUREMENTS
# ============================================================

def tick_sigma(market):
    """Typical price move per tick (root mean square of changes)."""
    values = list(prices[market])[-SIGMA_WINDOW:]

    if len(values) < 30:
        return None

    moves = [b - a for a, b in zip(values, values[1:])]

    return math.sqrt(sum(x * x for x in moves) / len(moves))


def avg_gap(market):
    gaps = tick_gaps[market]

    if len(gaps) < 5:
        return None

    return sum(gaps) / len(gaps)


def barrier_points(market, ticks, z):
    """Barrier distance in price points, rounded to the market's pip."""
    sigma = tick_sigma(market)
    step = 10 ** (-pip[market])

    raw = z * sigma * math.sqrt(ticks)

    return max(round(raw, pip[market]), step)


def touch_probability(market, ticks, barrier):
    """
    Chance a driftless random walk touches a barrier within `ticks`
    ticks (reflection principle with a correction for discrete ticks).
    """
    sigma = tick_sigma(market)

    effective = barrier + DISCRETE_SHIFT * sigma
    z = effective / (sigma * math.sqrt(ticks))

    return min(1.0, max(0.0, 2 * (1 - norm_cdf(z))))


def excursions(market, ticks):
    """Largest move up and down within `ticks` ticks, for every start."""
    values = list(prices[market])
    result = []

    for i in range(len(values) - ticks):
        start = values[i]
        window = values[i + 1:i + ticks + 1]
        result.append((max(window) - start, start - min(window)))

    return result


def history_touch_rate(exc, barrier):
    """How often a barrier was touched in the stored history."""
    if not exc:
        return None

    hits = 0

    for up, down in exc:
        hits += (up >= barrier) + (down >= barrier)

    return hits / (2 * len(exc))


# ============================================================
# HISTORY AND LIVE TICKS
# ============================================================

def record_gap(market, epoch):
    prev = last_epoch[market]

    if prev > 0 and epoch > prev:
        tick_gaps[market].append(epoch - prev)


def process_history(market, price_list, time_list):
    pairs = sorted(zip(price_list, time_list), key=lambda x: int(x[1]))

    prices[market].clear()
    tick_gaps[market].clear()
    last_epoch[market] = 0
    last_price[market] = None

    for price, epoch in pairs:
        epoch = int(epoch)

        record_gap(market, epoch)
        prices[market].append(float(price))

        last_epoch[market] = epoch
        last_price[market] = price

    history_ready[market] = True

    print(f"{market}: loaded {len(prices[market])} historical ticks")


def process_live(market, price, epoch):
    epoch = int(epoch)

    if epoch < last_epoch[market]:
        return

    if epoch == last_epoch[market] and price == last_price[market]:
        return

    record_gap(market, epoch)
    prices[market].append(float(price))

    last_epoch[market] = epoch
    last_price[market] = price


# ============================================================
# OPTION TABLE
# ============================================================

def build_rows():
    rows = []

    for market in MARKETS:
        if tick_sigma(market) is None:
            continue

        for ticks in TICK_DURATIONS:
            exc = excursions(market, ticks)

            for z in Z_LEVELS:
                touch_quote = quotes.get((market, ticks, z, "ONETOUCH"))

                if touch_quote and touch_quote.get("barrier") is not None:
                    barrier = touch_quote["barrier"]
                else:
                    barrier = barrier_points(market, ticks, z)

                p_model = touch_probability(market, ticks, barrier)
                p_past = history_touch_rate(exc, barrier)

                for ctype in ("ONETOUCH", "NOTOUCH"):
                    if ctype == "ONETOUCH":
                        chance = p_model
                        past = p_past
                    else:
                        chance = 1 - p_model
                        past = None if p_past is None else 1 - p_past

                    quote = quotes.get((market, ticks, z, ctype))
                    ratio = None

                    if quote and quote.get("ratio"):
                        ratio = quote["ratio"]

                    value = None
                    value_past = None

                    if ratio is not None:
                        value = chance * ratio - 1

                        if past is not None:
                            value_past = past * ratio - 1

                    rows.append({
                        "market": market,
                        "ticks": ticks,
                        "z": z,
                        "ctype": ctype,
                        "barrier": barrier,
                        "chance": chance,
                        "past": past,
                        "ratio": ratio,
                        "breakeven": 1 / chance if chance > 0 else None,
                        "value": value,
                        "value_past": value_past
                    })

    return rows


def trade_name(ctype):
    return "TOUCH" if ctype == "ONETOUCH" else "NO TOUCH"


def time_text(market, ticks):
    gap = avg_gap(market)

    if gap is None:
        return f"{ticks}t"

    return f"{ticks}t (~{ticks * gap:.0f}s)"


def pct(x):
    return "-" if x is None else f"{x * 100:.0f}%"


# ============================================================
# ALERT
# ============================================================

def check_alerts(rows):
    for row in rows:
        if row["value"] is None or row["value_past"] is None:
            continue

        if row["value"] < VALUE_ALERT or row["value_past"] <= 0:
            continue

        key = (
            row["market"], row["ticks"], row["z"],
            row["ctype"], round(row["ratio"], 2)
        )

        if key in alerted:
            continue

        alerted.add(key)

        if SOUND:
            print("\a", end="")

        print()
        print(c("  " + "=" * 56, "92"))
        print(c("   POSSIBLE VALUE FOUND   (analysis only, no trade placed)",
                "92;1"))
        print(c("  " + "=" * 56, "92"))
        print(f"   Market   : {row['market']}")
        print(f"   Trade    : {trade_name(row['ctype'])}")
        print(f"   Barrier  : +{row['barrier']:.{pip[row['market']]}f} "
              f"from the entry price")
        print(f"   Time     : {time_text(row['market'], row['ticks'])}")
        print(f"   Chance   : {pct(row['chance'])} (model), "
              f"{pct(row['past'])} (last {WINDOW} ticks)")
        print(f"   Payout   : {row['ratio']:.2f}x stake")
        print(f"   Value    : {row['value'] * 100:+.1f}% per $1 "
              f"(model), {row['value_past'] * 100:+.1f}% (history)")
        print(c("   Note     : check the payout on Deriv before acting; "
                "it can change, and history is a small sample.", "93"))
        print(c("  " + "=" * 56, "92"))


# ============================================================
# DASHBOARD
# ============================================================

def show():
    print()
    print(c(f"  TOUCH / NO TOUCH ANALYZER    {local_time()} (your time)",
            "96;1"))
    print("  " + "-" * 70)
    print(f"  {'Market':<9}{'Tick':<8}{'Normal move per tick':<24}"
          f"{'Ticks stored'}")

    for market in MARKETS:
        sigma = tick_sigma(market)
        gap = avg_gap(market)

        sigma_text = "-" if sigma is None else f"{sigma:.{pip[market] + 1}f}"
        gap_text = "-" if gap is None else f"{gap:.1f}s"

        print(f"  {market:<9}{gap_text:<8}{sigma_text:<24}"
              f"{len(prices[market])}/{WINDOW}")

    print("  " + "-" * 70)

    if not ready():
        print("  Collecting history, please wait...")
        return

    rows = build_rows()

    have_quotes = any(r["value"] is not None for r in rows)

    if have_quotes:
        rows = [r for r in rows if r["value"] is not None]
        rows.sort(key=lambda r: r["value"], reverse=True)

        print(c("  BEST OPTIONS RIGHT NOW (highest value first)", "1"))
        print(f"  {'#':<3}{'Market':<8}{'Trade':<10}{'Barrier':<10}"
              f"{'Time':<14}{'Chance':<8}{'Past':<7}{'Payout':<8}{'Value'}")

        for i, r in enumerate(rows[:TOP_N], 1):
            color = "92" if r["value"] > 0 else "91"

            print(
                f"  {i:<3}{r['market']:<8}{trade_name(r['ctype']):<10}"
                f"+{r['barrier']:<9.{pip[r['market']]}f}"
                f"{time_text(r['market'], r['ticks']):<14}"
                f"{pct(r['chance']):<8}{pct(r['past']):<7}"
                f"{r['ratio']:.2f}x".ljust(8)
                + c(f"{r['value'] * 100:+.0f}%", color)
            )

        best = rows[0]["value"]

        print()
        print("  Chance = estimated chance this trade wins.   "
              "Past = how often it won in stored ticks.")
        print("  Payout = what Deriv pays per $1 staked.   "
              "Value = chance x payout - 1.")

        if best <= 0:
            print(c(
                "  Verdict : NO VALUE. Every option is expected to lose "
                "money (Deriv's payouts include a margin).",
                "93"
            ))
        else:
            print(c(
                "  Verdict : a positive value appears. Treat it with "
                "care: the estimate is rough and payouts change.",
                "93"
            ))

        check_alerts(rows)

    else:
        print(c("  Payouts from Deriv are not available yet.", "93"))

        if quote_errors:
            print(f"  Reason: {quote_errors[0]}")

        print("  Break-even payout = the lowest payout (x stake) at which "
              "the trade is worth it.")
        print(f"  {'Market':<8}{'Trade':<10}{'Barrier':<10}{'Time':<14}"
              f"{'Chance':<8}{'Break-even payout'}")

        for r in rows[:TOP_N * 2]:
            print(
                f"  {r['market']:<8}{trade_name(r['ctype']):<10}"
                f"+{r['barrier']:<9.{pip[r['market']]}f}"
                f"{time_text(r['market'], r['ticks']):<14}"
                f"{pct(r['chance']):<8}{r['breakeven']:.2f}x"
            )

    print()
    print(c("  No trade is placed. Prices are random, so direction "
            "cannot be predicted.", "2"))


# ============================================================
# CONNECTION
# ============================================================

def connect():
    ssl_options = {"cert_reqs": ssl.CERT_REQUIRED}

    if CA_FILE:
        ssl_options["ca_certs"] = CA_FILE

    for url in ENDPOINTS:
        print("\nTrying:", url)

        try:
            ws = websocket.create_connection(
                url, timeout=20, sslopt=ssl_options
            )

            ws.settimeout(2)

            print("CONNECTED SUCCESSFULLY:", url)
            return ws

        except Exception as e:
            print("Connection failed:", e)

    return None


def subscribe(ws):
    for market in MARKETS:
        ws.send(json.dumps({
            "ticks_history": market,
            "count": WINDOW,
            "end": "latest",
            "style": "ticks",
            "subscribe": 1
        }))

        print("Requested:", market)


def request_quotes(ws):
    """Ask Deriv for the payout of every option in the table."""
    req_map.clear()
    quote_errors.clear()

    for market in MARKETS:
        if tick_sigma(market) is None:
            continue

        for ticks in TICK_DURATIONS:
            for z in Z_LEVELS:
                barrier = barrier_points(market, ticks, z)

                for ctype in ("ONETOUCH", "NOTOUCH"):
                    req_counter[0] += 1
                    req_id = req_counter[0]

                    key = (market, ticks, z, ctype)
                    req_map[req_id] = key
                    quotes[key] = {"barrier": barrier, "ratio": None}

                    ws.send(json.dumps({
                        "proposal": 1,
                        "req_id": req_id,
                        "amount": STAKE,
                        "basis": "stake",
                        "contract_type": ctype,
                        "currency": CURRENCY,
                        "duration": ticks,
                        "duration_unit": "t",
                        "symbol": market,
                        "barrier": f"+{barrier:.{pip[market]}f}"
                    }))

                    time.sleep(0.05)


def handle_proposal(msg):
    req_id = msg.get("req_id") or msg.get("echo_req", {}).get("req_id")
    key = req_map.get(req_id)

    if key is None:
        return False

    if "error" in msg:
        message = msg["error"].get("message", str(msg["error"]))

        quotes[key]["ratio"] = None
        quotes[key]["error"] = message

        if message not in quote_errors:
            quote_errors.append(message)

        return True

    proposal = msg.get("proposal", {})
    payout = proposal.get("payout")
    cost = proposal.get("ask_price")

    if payout and cost:
        quotes[key]["ratio"] = float(payout) / float(cost)

    return True


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def run(ws):
    subscribe(ws)

    last_show = time.time()
    last_ping = time.time()
    last_quote = 0

    while True:
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            raw = None

        if raw == "":
            raise ConnectionError("Server closed connection.")

        if raw:
            msg = json.loads(raw)

            if msg.get("msg_type") == "proposal":
                handle_proposal(msg)

            elif "error" in msg:
                if not handle_proposal(msg):
                    print("Deriv error:",
                          msg["error"].get("message", msg["error"]))

            elif msg.get("msg_type") == "history":
                market = msg.get("echo_req", {}).get("ticks_history")

                if market in MARKETS:
                    history = msg.get("history", {})

                    pip[market] = int(msg.get("pip_size", pip[market]))

                    process_history(
                        market,
                        history.get("prices", []),
                        history.get("times", [])
                    )

                    buffered = sorted(
                        live_buffer[market],
                        key=lambda t: int(t["epoch"])
                    )
                    live_buffer[market].clear()

                    for tick in buffered:
                        process_live(market, tick["quote"], tick["epoch"])

            elif msg.get("msg_type") == "tick":
                tick = msg.get("tick", {})

                market = tick.get("symbol")
                price = tick.get("quote")
                epoch = tick.get("epoch")

                if market in MARKETS and price is not None \
                        and epoch is not None:

                    if history_ready[market]:
                        process_live(market, price, epoch)
                    else:
                        live_buffer[market].append(
                            {"quote": price, "epoch": epoch}
                        )

        now = time.time()

        if now - last_ping >= PING_EVERY:
            ws.send(json.dumps({"ping": 1}))
            last_ping = now

        if ready() and now - last_quote >= PROPOSAL_EVERY:
            request_quotes(ws)
            last_quote = time.time()

        if now - last_show >= SHOW_EVERY:
            show()
            last_show = now


# ============================================================
# MAIN
# ============================================================

def main():
    os.system("")  # lets Windows terminals show colours

    retry_delay = 5

    try:
        while True:
            ws = connect()

            if ws is None:
                print(f"All endpoints failed. Retrying in "
                      f"{retry_delay} seconds.")

                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
                continue

            retry_delay = 5

            for market in MARKETS:
                history_ready[market] = False
                live_buffer[market].clear()

            quotes.clear()
            req_map.clear()

            try:
                run(ws)

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
        print("ANALYZER STOPPED. No trades were placed.")


if __name__ == "__main__":
    main()