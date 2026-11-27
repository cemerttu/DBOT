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

# ENTRY RULE (when to enter)
# The bot compares how much the market moved over the last RECENT_TICKS
# ticks with its normal movement:
#   moving much LESS than normal  -> CALM -> rule says NO TOUCH
#   moving much MORE than normal  -> WILD -> rule says TOUCH
RECENT_TICKS = 30
CALM_RATIO = 0.75      # recent move / normal move below this = CALM
WILD_RATIO = 1.30      # recent move / normal move above this = WILD
ENTRY_Z = 1.0          # barrier distance used for the entry suggestion

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
last_regime = {m: None for m in MARKETS}

# Deriv's newer endpoint calls the market field 'underlying_symbol';
# the older endpoints call it 'symbol'.
SYMBOL_FIELD = ["underlying_symbol"]
field_fix = {"toggled": False, "requote": False}


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

def classify(ratio):
    if ratio < CALM_RATIO:
        return "CALM"

    if ratio > WILD_RATIO:
        return "WILD"

    return "NORMAL"


def current_regime(market):
    """(regime, ratio): is the market calmer or wilder than normal now?"""
    values = list(prices[market])[-(SIGMA_WINDOW + 1):]

    if len(values) < RECENT_TICKS + 30:
        return None, None

    moves = [b - a for a, b in zip(values, values[1:])]

    recent = math.sqrt(
        sum(x * x for x in moves[-RECENT_TICKS:]) / RECENT_TICKS
    )
    normal = math.sqrt(sum(x * x for x in moves) / len(moves))

    if normal == 0:
        return None, None

    ratio = recent / normal

    return classify(ratio), ratio


def regime_history(market, ticks, z):
    """
    Look back over stored ticks: how often was the barrier touched
    after a CALM, NORMAL or WILD market?
    Returns {regime: [touches, trials]}.
    """
    values = list(prices[market])
    count = len(values)
    exc = excursions(market, ticks)
    barrier = barrier_points(market, ticks, z)

    prefix = [0.0]

    for k in range(count - 1):
        prefix.append(prefix[-1] + (values[k + 1] - values[k]) ** 2)

    stats = {"CALM": [0, 0], "NORMAL": [0, 0], "WILD": [0, 0]}

    for i in range(max(RECENT_TICKS, 200), len(exc)):
        span = min(i, SIGMA_WINDOW)

        recent = (prefix[i] - prefix[i - RECENT_TICKS]) / RECENT_TICKS
        normal = (prefix[i] - prefix[i - span]) / span

        if normal <= 0:
            continue

        regime = classify(math.sqrt(recent / normal))
        up, down = exc[i]

        stats[regime][0] += (up >= barrier) + (down >= barrier)
        stats[regime][1] += 2

    return stats


def regime_evidence(stats, ticks):
    """
    Did the touch rate really differ after CALM / WILD markets?
    Returns (text, {"CALM": bool, "WILD": bool}) where True means the
    difference is bigger than random noise and points the way the rule
    assumes (calm -> fewer touches, wild -> more touches).
    """
    touches = sum(v[0] for v in stats.values())
    trials = sum(v[1] for v in stats.values())

    if trials == 0:
        return "not enough history", {"CALM": False, "WILD": False}

    overall = touches / trials
    supported = {"CALM": False, "WILD": False}

    for name in ("CALM", "WILD"):
        t, n = stats[name]

        # overlapping starts are not independent, so shrink the sample
        independent = n / ticks

        if independent < 30:
            continue

        rate_here = t / n
        std_err = math.sqrt(overall * (1 - overall) / independent)

        if name == "CALM":
            supported[name] = rate_here < overall - 3 * std_err
        else:
            supported[name] = rate_here > overall + 3 * std_err

    if supported["CALM"] or supported["WILD"]:
        return "rule supported by history", supported

    return "no measurable effect", supported


def entry_option(market, regime):
    """The trade the rule suggests for this market right now."""
    ctype = "NOTOUCH" if regime == "CALM" else "ONETOUCH"
    ticks = TICK_DURATIONS[0]

    quote = quotes.get((market, ticks, ENTRY_Z, ctype))
    touch_quote = quotes.get((market, ticks, ENTRY_Z, "ONETOUCH"))

    if touch_quote and touch_quote.get("barrier") is not None:
        barrier = touch_quote["barrier"]
    else:
        barrier = barrier_points(market, ticks, ENTRY_Z)

    p_touch = touch_probability(market, ticks, barrier)
    chance = p_touch if ctype == "ONETOUCH" else 1 - p_touch

    ratio = quote.get("ratio") if quote else None
    value = None if ratio is None else chance * ratio - 1

    return {
        "ctype": ctype,
        "ticks": ticks,
        "barrier": barrier,
        "chance": chance,
        "ratio": ratio,
        "breakeven": 1 / chance,
        "value": value
    }


def check_entry_alert(market):
    """Print an ENTRY box when a market newly turns CALM or WILD."""
    regime, ratio = current_regime(market)
    previous = last_regime[market]
    last_regime[market] = regime

    if regime not in ("CALM", "WILD") or regime == previous:
        return

    option = entry_option(market, regime)
    stats = regime_history(market, option["ticks"], ENTRY_Z)
    verdict, _ = regime_evidence(stats, option["ticks"])

    if SOUND:
        print("\a", end="")

    print()
    print(c("  " + "=" * 56, "92"))
    print(c("   ENTRY SIGNAL   (analysis only, no trade placed)", "92;1"))
    print(c("  " + "=" * 56, "92"))
    print(f"   Market   : {market}")
    print(f"   Trade    : {trade_name(option['ctype'])}")
    print(f"   Why      : market just turned {regime} "
          f"(moving {ratio:.2f}x its normal size)")
    print(f"   Barrier  : +{option['barrier']:.{pip[market]}f}   "
          f"Time: {time_text(market, option['ticks'])}")
    print(f"   Chance   : {pct(option['chance'])} to win (model)")

    if option["ratio"] is not None:
        print(f"   Payout   : {option['ratio']:.2f}x stake   "
              f"Value: {option['value'] * 100:+.0f}% per $1")
    else:
        print(f"   Payout   : needs at least {option['breakeven']:.2f}x "
              f"stake to be worth it")

    print(f"   Enter    : now; the signal ends when the market "
          f"is no longer {regime}")
    print(c(f"   Evidence : {verdict}", "93"))
    print(c("  " + "=" * 56, "92"))


def print_entry_section():
    print()
    print(c("  WHEN TO ENTER   (rule: calm market -> NO TOUCH, "
            "wild market -> TOUCH)", "1"))
    print(f"  {'Market':<9}{'Now':<9}{'Moving':<10}{'Signal'}")

    for market in MARKETS:
        regime, ratio = current_regime(market)

        if regime is None:
            print(f"  {market:<9}{'...':<9}")
            continue

        if regime == "NORMAL":
            print(f"  {market:<9}{regime:<9}{ratio:.2f}x".ljust(30)
                  + c("wait", "2"))
            continue

        option = entry_option(market, regime)
        line = (
            f"ENTER {trade_name(option['ctype'])} "
            f"+{option['barrier']:.{pip[market]}f} "
            f"{time_text(market, option['ticks'])}"
        )

        if option["ratio"] is not None:
            line += (f"  payout {option['ratio']:.2f}x  "
                     f"value {option['value'] * 100:+.0f}%")
        else:
            line += f"  needs {option['breakeven']:.2f}x or more"

        print(f"  {market:<9}{regime:<9}{ratio:.2f}x".ljust(30)
              + c(line, "92;1"))

    print()
    print("  Does the rule work? Touch rate in the last "
          f"{WINDOW} ticks, split by market state:")

    ticks = TICK_DURATIONS[0]

    for market in MARKETS:
        stats = regime_history(market, ticks, ENTRY_Z)
        verdict, _ = regime_evidence(stats, ticks)

        parts = []

        for name in ("CALM", "NORMAL", "WILD"):
            t, n = stats[name]
            parts.append(f"{name.lower()} {pct(t / n if n else None)}")

        print(f"    {market:<8}" + "   ".join(parts).ljust(40) + verdict)


def print_ranking(title, ctype, rows):
    """Rank the markets for one trade type, using each market's best option."""
    best_per_market = {}

    # rows are sorted best value first, so the first one seen is the best
    for r in rows:
        if r["ctype"] == ctype and r["market"] not in best_per_market:
            best_per_market[r["market"]] = r

    ranked = list(best_per_market.values())

    if not ranked:
        return

    print()
    print(c(f"  BEST MARKET FOR {title}   "
            f"(best option found in each market)", "1"))
    print(f"  {'#':<3}{'Market':<9}{'Barrier':<10}{'Time':<14}"
          f"{'Chance':<8}{'Past':<7}{'Payout':<9}{'Value'}")

    for i, r in enumerate(ranked, 1):
        color = "92" if r["value"] > 0 else "91"
        tag = "   <-- best" if i == 1 else ""

        print(
            f"  {i:<3}{r['market']:<9}"
            f"+{r['barrier']:<9.{pip[r['market']]}f}"
            f"{time_text(r['market'], r['ticks']):<14}"
            f"{pct(r['chance']):<8}{pct(r['past']):<7}"
            + f"{r['ratio']:.2f}x".ljust(9)
            + c(f"{r['value'] * 100:+.0f}%", color)
            + tag
        )

    gap = ranked[0]["value"] - ranked[-1]["value"]

    if len(ranked) > 1 and gap < 0.03:
        print(c(
            f"  The markets are almost the same for {title} "
            f"(only {gap * 100:.1f} points apart), so the ranking "
            f"means little.",
            "2"
        ))


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

        print_ranking("TOUCH", "ONETOUCH", rows)
        print_ranking("NO TOUCH", "NOTOUCH", rows)

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

        print()
        print("  'Needs' = the lowest payout (x stake) at which that trade "
              "is worth taking.")
        print("  Compare it with the payout shown on Deriv: if Deriv pays "
              "less, skip it.")

        first = TICK_DURATIONS[0]

        for market in MARKETS:
            mrows = [
                r for r in rows
                if r["market"] == market and r["ticks"] == first
            ]

            if not mrows:
                continue

            print()
            print(c(f"  {market}   contract length: "
                    f"{time_text(market, first)}", "1"))
            print(f"    {'Barrier':<11}{'TOUCH wins':<12}{'needs':<10}"
                  f"{'NO TOUCH wins':<15}{'needs'}")

            for z in Z_LEVELS:
                touch = next(r for r in mrows
                             if r["z"] == z and r["ctype"] == "ONETOUCH")
                notouch = next(r for r in mrows
                               if r["z"] == z and r["ctype"] == "NOTOUCH")

                barrier_text = f"+{touch['barrier']:.{pip[market]}f}"

                print(
                    "    "
                    + barrier_text.ljust(11)
                    + pct(touch["chance"]).ljust(12)
                    + f"{touch['breakeven']:.2f}x".ljust(10)
                    + pct(notouch["chance"]).ljust(15)
                    + f"{notouch['breakeven']:.2f}x"
                )

    print_entry_section()

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

            SYMBOL_FIELD[0] = (
                "underlying_symbol"
                if "api.derivws.com" in url
                else "symbol"
            )
            field_fix["toggled"] = False
            field_fix["requote"] = False

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
                        SYMBOL_FIELD[0]: market,
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

        if (
            "symbol" in message
            and "validation" in message.lower()
            and not field_fix["toggled"]
        ):
            SYMBOL_FIELD[0] = (
                "symbol"
                if SYMBOL_FIELD[0] == "underlying_symbol"
                else "underlying_symbol"
            )
            field_fix["toggled"] = True
            field_fix["requote"] = True

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

                        if ready():
                            check_entry_alert(market)
                    else:
                        live_buffer[market].append(
                            {"quote": price, "epoch": epoch}
                        )

        now = time.time()

        if now - last_ping >= PING_EVERY:
            ws.send(json.dumps({"ping": 1}))
            last_ping = now

        if field_fix["requote"]:
            field_fix["requote"] = False
            last_quote = 0

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
    retry_delay = 5

    try:
        os.system("")  # lets Windows terminals show colours

        while True:
            ws = connect()

            if ws is None:
                print()
                print(c("  Cannot reach Deriv. This is usually your "
                        "internet or DNS, not the bot.", "93"))
                print("  Check that you are online, turn off any VPN, "
                      "and try again.")
                print(f"  Retrying in {retry_delay} seconds "
                      f"(press Ctrl+C to stop).")

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