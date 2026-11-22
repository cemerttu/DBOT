"""
Deriv last-digit ENTRY SCANNER (no trading, no login, no API token).

Watches Volatility 10, 25, 50, 75 and 100 at the same time. For OVER 1 and
UNDER 8 it applies this rule to every market:

  1. The combined share of the LOSING digits is low
        Over 1  loses on 0 and 1  -> 0+1 share must be <= LOSE_LIMIT (18%)
        Under 8 loses on 8 and 9  -> 8+9 share must be <= LOSE_LIMIT (18%)
  2. The losing-digit share is not increasing (short window vs long window)
  3. The last CONFIRM_TICKS digits were winning digits

When all three are true it prints ENTER NOW with the market and barrier.

SETUP:  pip install websockets
RUN:    python deriv_entry_scanner.py

NOTE: Ticks on these indices are random and independent. This rule gives you a
consistent entry point. It does not change the true odds of the next tick.
"""

import asyncio
import json
import math
import time
from collections import deque

import websockets

# ---------------- SETTINGS ----------------
# Deriv's current public market-data endpoint (no app_id or token needed)
URL = "wss://api.derivws.com/trading/v1/options/ws/public"

SYMBOLS = {
    "R_10": "Volatility 10",
    "R_25": "Volatility 25",
    "R_50": "Volatility 50",
    "R_75": "Volatility 75",
    "R_100": "Volatility 100",
}

WINDOWS = (50, 200, 1000)        # short, medium, long
WEIGHTS = (0.2, 0.3, 0.5)        # weights used to rank markets
LOSE_LIMIT = 18.0                # losing-digit share must be at or below this %
TREND_TOLERANCE = 0.0            # short-window share must be <= long-window share + this
CONFIRM_TICKS = 2                # winning digits in a row needed before entry
MIN_TICKS = 50                   # ticks needed before a market is analysed
REFRESH_SECONDS = 1.0            # dashboard refresh rate
CLEAR_SCREEN = True              # set False if your terminal shows odd characters

# label -> (losing digits, barrier to choose on Deriv)
CONTRACTS = {
    "OVER 1": ({0, 1}, 1),
    "UNDER 8": ({8, 9}, 8),
}
# ------------------------------------------


def last_digit(price, pip_size):
    # pip_size may arrive as 3, 3.0 (decimal places) or 0.001 (step size)
    pip = float(pip_size)
    decimals = int(round(pip)) if pip >= 1 else int(round(-math.log10(pip)))
    return int(f"{price:.{decimals}f}"[-1])


def lose_share(data, losing, window):
    chunk = data[-window:]
    return sum(1 for d in chunk if d in losing) * 100 / len(chunk)


def digit_shares(data, window):
    chunk = data[-window:]
    return [chunk.count(d) * 100 / len(chunk) for d in range(10)]


def winning_streak(data, losing):
    run = 0
    for d in reversed(data):
        if d in losing:
            break
        run += 1
    return run


def analyse_market(data, losing):
    shares = [lose_share(data, losing, w) for w in WINDOWS]
    score = sum(s * wt for s, wt in zip(shares, WEIGHTS))   # lower is better
    long_share = shares[-1]
    short_share = shares[0]
    diff = short_share - long_share
    streak = winning_streak(data, losing)

    check_share = long_share <= LOSE_LIMIT
    check_trend = diff <= TREND_TOLERANCE
    check_last = streak >= CONFIRM_TICKS

    if diff < -1:
        trend = "decreasing"
    elif diff > 1:
        trend = "increasing"
    else:
        trend = "flat"

    return {
        "shares": shares,
        "score": score,
        "long": long_share,
        "short": short_share,
        "trend": trend,
        "streak": streak,
        "check_share": check_share,
        "check_trend": check_trend,
        "check_last": check_last,
        "ok": check_share and check_trend and check_last,
    }


def mark(ok):
    return "[x]" if ok else "[ ]"


def render(store):
    ready = {s: list(d) for s, d in store.items() if len(d) >= MIN_TICKS}
    lines = ["DERIV DIGIT ENTRY SCANNER", ""]

    if not ready:
        lines.append("Loading ticks...")
        return "\n".join(lines)

    for label, (losing, barrier) in CONTRACTS.items():
        losing_text = "+".join(str(d) for d in sorted(losing))
        rows = []
        for sym, data in ready.items():
            info = analyse_market(data, losing)
            info["sym"] = sym
            info["data"] = data
            rows.append(info)
        rows.sort(key=lambda r: r["score"])   # lowest losing share first

        lines.append(f"=== {label} (barrier {barrier}) | losing digits {losing_text} | fair share 20% ===")
        head = " | ".join(f"last {w:>4}" for w in WINDOWS)
        lines.append(f"  #  {'Market':<14} {head} | trend      | win-run | last 8 digits")
        for i, r in enumerate(rows, 1):
            share_text = " | ".join(f"{s:8.1f}%" for s in r["shares"])
            last8 = " ".join(str(d) for d in r["data"][-8:])
            lines.append(
                f"  {i}  {SYMBOLS[r['sym']]:<14} {share_text} | {r['trend']:<10} | "
                f"{r['streak']:>7} | {last8}"
            )
        lines.append(f"  (numbers above are the combined {losing_text} share: lower is better)")
        lines.append("")

        passing = [r for r in rows if r["ok"]]
        focus = passing[0] if passing else rows[0]
        name = SYMBOLS[focus["sym"]]

        lines.append(f"  Checks for {name}:")
        lines.append(
            f"   {mark(focus['check_share'])} {losing_text} share over last {WINDOWS[-1]} ticks is "
            f"{focus['long']:.1f}% (needs <= {LOSE_LIMIT:.0f}%)"
        )
        lines.append(
            f"   {mark(focus['check_trend'])} trend {focus['trend']}: last {WINDOWS[0]} = {focus['short']:.1f}% "
            f"vs last {WINDOWS[-1]} = {focus['long']:.1f}% (must not be increasing)"
        )
        lines.append(
            f"   {mark(focus['check_last'])} last {CONFIRM_TICKS} digits are winning digits "
            f"(current run: {focus['streak']})"
        )

        pcts = digit_shares(focus["data"], WINDOWS[-1])
        hi, lo = pcts.index(max(pcts)), pcts.index(min(pcts))
        digit_text = "  ".join(
            f"{d}:{p:.1f}%{'^' if d == hi else 'v' if d == lo else ''}" for d, p in enumerate(pcts)
        )
        lines.append(f"  Digit shares (last {WINDOWS[-1]}): {digit_text}   (^ most, v least)")

        if focus["ok"]:
            lines.append(f"  >>> ENTER NOW: {label} on {name}, barrier {barrier}, duration 1 tick")
        else:
            reasons = []
            if not focus["check_share"]:
                reasons.append(f"{losing_text} share too high ({focus['long']:.1f}%)")
            if not focus["check_trend"]:
                reasons.append("losing-digit share is increasing")
            if not focus["check_last"]:
                reasons.append(f"need {CONFIRM_TICKS - focus['streak']} more winning digit(s) in a row")
            lines.append("  WAIT: " + "; ".join(reasons))
        lines.append("")

    lines.append("Rule: losing-digit share <= limit, not increasing, then confirming winning digits.")
    lines.append("This is a consistent entry rule, not a prediction. Each tick is random.")
    return "\n".join(lines)


async def main():
    store = {s: deque(maxlen=max(WINDOWS)) for s in SYMBOLS}
    pip = {s: 2 for s in SYMBOLS}
    last_draw = 0.0

    async with websockets.connect(URL) as ws:
        for sym in SYMBOLS:
            await ws.send(json.dumps({
                "ticks_history": sym,
                "count": max(WINDOWS),
                "end": "latest",
                "style": "ticks",
                "subscribe": 1,
            }))

        print("Connecting to all markets... press Ctrl+C to stop.")

        while True:
            msg = json.loads(await ws.recv())

            if "error" in msg:
                print("API error:", msg["error"].get("message"))
                continue

            if "history" in msg:
                sym = msg["echo_req"]["ticks_history"]
                pip[sym] = msg.get("pip_size", pip[sym])
                for price in msg["history"]["prices"]:
                    store[sym].append(last_digit(float(price), pip[sym]))

            elif "tick" in msg:
                tick = msg["tick"]
                sym = (
                    tick.get("symbol")
                    or tick.get("underlying_symbol")
                    or msg.get("echo_req", {}).get("ticks_history")
                )
                if sym in store:
                    pip[sym] = tick.get("pip_size", pip[sym])
                    store[sym].append(last_digit(float(tick["quote"]), pip[sym]))
            else:
                continue

            now = time.monotonic()
            if now - last_draw >= REFRESH_SECONDS:
                last_draw = now
                if CLEAR_SCREEN:
                    print("\033[H\033[J", end="")
                print(render(store))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")