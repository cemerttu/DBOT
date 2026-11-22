#!/usr/bin/env python3
"""
DERIV MARKET PICKER + BOT LAUNCHER (analysis only, this script places no trades)

Install:  python -m pip install websocket-client certifi
Run:      python deriv_best.py
   or:    python deriv_best.py   <drag your bot file into the terminal here>   then Enter
Stop:     Ctrl+C

What it does
  1. Collects the last 500 ticks for R_10, R_25, R_50, R_75, R_100.
  2. Compares Over 1 and Under 8 on every market and picks the one with the
     highest recent hit rate. It only switches to a new market when that market
     beats the current one by SWITCH_MARGIN points, so the choice does not flip
     back and forth.
  3. Writes the choice to best_market.json, updated live. Your bot can read it.
  4. If you gave it your bot file, it starts your bot once data is ready and
     gives it the choice as environment variables:
         DERIV_MARKET    e.g. R_50
         DERIV_CONTRACT  e.g. OVER 1  or  UNDER 8
         DERIV_BEST_FILE full path of best_market.json (always up to date)
  5. Gives an ENTRY signal (enter on the tick right AFTER a miss) and scores
     every signal so you see the REAL win rate of the rule.

Honest warning: R_10..R_100 digits are random. The "best" market changes all the
time and the entry rule cannot beat chance. Watch the SIGNAL RECORD line: it
should stay near 80%. To profit you need a win rate above your break-even,
which is 100 / (100 + payout%). Check the payout shown on Deriv for the contract.
"""

import csv
import json
import os
import ssl
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    import websocket
except ImportError:
    sys.exit("Missing package. Run: python -m pip install websocket-client certifi")

try:
    import certifi
    CA = certifi.where()
except ImportError:
    CA = None

MARKETS = ["R_10", "R_25", "R_50", "R_75", "R_100"]
CONTRACTS = ["OVER 1", "UNDER 8"]
WINDOW = 500
SHOW_EVERY = 10                     # seconds between dashboard prints
SWITCH_MARGIN = 1.0                 # a new market must beat the current one by this many points
CSV_FILE = "deriv_ticks.csv"
BEST_FILE = os.path.abspath("best_market.json")
URLS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",
    "wss://ws.derivws.com/websockets/v3?app_id=1089",
]

digits = {m: deque(maxlen=WINDOW) for m in MARKETS}
decimals = {m: 2 for m in MARKETS}
last_epoch = {m: 0 for m in MARKETS}
pending = {(m, c): False for m in MARKETS for c in CONTRACTS}   # signal waiting for next tick
record = {(m, c): [0, 0] for m in MARKETS for c in CONTRACTS}   # [wins, signals]
last_alert = {}

chosen = None          # (market, contract) currently selected
bot_path = None        # path of the user's bot, if given
bot_proc = None        # running bot process


def hit(contract, d):
    return d > 1 if contract == "OVER 1" else d < 8


def last_digit(m, price):
    q = Decimal(1).scaleb(-decimals[m])
    return int(f"{Decimal(str(price)).quantize(q, rounding=ROUND_HALF_UP):.{decimals[m]}f}"[-1])


def add_tick(m, price, epoch, writer):
    if epoch <= last_epoch[m]:
        return False
    last_epoch[m] = epoch
    d = last_digit(m, price)

    for c in CONTRACTS:                      # score the previous signal first
        if pending[(m, c)]:
            record[(m, c)][1] += 1
            record[(m, c)][0] += hit(c, d)
            pending[(m, c)] = False
    for c in CONTRACTS:                      # a miss now = entry signal for the next tick
        if not hit(c, d):
            pending[(m, c)] = True

    digits[m].append(d)
    when = datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    writer.writerow([when, m, price, d])
    return True


def rate(m, c):
    w = digits[m]
    return sum(hit(c, d) for d in w) / len(w) * 100 if w else 0.0


def ready():
    return all(len(digits[m]) >= WINDOW for m in MARKETS)


def best():
    return max(((m, c) for m in MARKETS for c in CONTRACTS), key=lambda k: rate(*k))


def choose():
    """Pick the market, but only switch when the new one is clearly better."""
    global chosen
    top = best()
    if chosen is None:
        chosen = top
    elif top != chosen and rate(*top) - rate(*chosen) >= SWITCH_MARGIN:
        print(f"\n*** Switching {chosen[0]} {chosen[1]} -> {top[0]} {top[1]} ***")
        chosen = top
    return chosen


def record_text(m, c):
    wins, n = record[(m, c)]
    return f"{wins}/{n} = {wins / n * 100:.1f}%" if n else "no signals yet"


def write_choice():
    """Write the current choice to best_market.json (atomic, so a bot never reads half a file)."""
    if chosen is None or not ready():
        return
    m, c = chosen
    wins, n = record[(m, c)]
    data = {
        "market": m,
        "contract": c,
        "hit_rate_percent": round(rate(m, c), 2),
        "enter_now": bool(pending[(m, c)]),
        "last_digit": digits[m][-1],
        "signal_record": {"wins": wins, "signals": n},
        "all_rates": {mk: {ct: round(rate(mk, ct), 2) for ct in CONTRACTS} for mk in MARKETS},
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }
    tmp = BEST_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        os.replace(tmp, BEST_FILE)
    except OSError as e:
        print("Could not write", BEST_FILE, "-", e)


def clean_path(p):
    """Dragging a file into a terminal can add quotes or '& ' (PowerShell). Remove them."""
    p = p.strip()
    if p.startswith("&"):
        p = p[1:].strip()
    return p.strip("'\"")


def ask_for_bot():
    global bot_path
    arg = " ".join(sys.argv[1:]).strip()
    if not arg:
        try:
            arg = input("Drag your bot file here and press Enter (or just press Enter to skip): ")
        except EOFError:
            arg = ""
    arg = clean_path(arg)
    if not arg:
        print("No bot given. Only the market picker will run (see best_market.json).")
        return
    if not os.path.isfile(arg):
        sys.exit(f"Bot file not found: {arg}")
    bot_path = os.path.abspath(arg)
    print("Bot:", bot_path)


def launch_bot():
    """Start the user's bot once, with the chosen market in environment variables."""
    global bot_proc
    if bot_path is None or bot_proc is not None or chosen is None:
        return
    m, c = chosen
    env = dict(os.environ, DERIV_MARKET=m, DERIV_CONTRACT=c, DERIV_BEST_FILE=BEST_FILE)
    cmd = [sys.executable, bot_path] if bot_path.lower().endswith(".py") else [bot_path]
    print(f"\n>>> STARTING YOUR BOT on {m} {c}")
    try:
        bot_proc = subprocess.Popen(cmd, env=env, cwd=os.path.dirname(bot_path))
    except OSError as e:
        print("Could not start the bot:", e)
        bot_proc = False        # do not try again


def check_bot():
    global bot_proc
    if bot_proc and bot_proc.poll() is not None:
        print(f"\nYour bot stopped (exit code {bot_proc.returncode}). The market picker keeps running.")
        bot_proc = False


def stop_bot():
    if bot_proc:
        try:
            bot_proc.terminate()
        except Exception:
            pass


def show():
    print("\n" + "=" * 64)
    if not ready():
        got = min(len(digits[m]) for m in MARKETS)
        print(f"Collecting data... {got}/{WINDOW} ticks. Please wait.")
        return
    print(f"{'MARKET':<8}{'LAST':>6}{'OVER 1':>10}{'UNDER 8':>10}")
    print("-" * 64)
    for m in MARKETS:
        print(f"{m:<8}{digits[m][-1]:>6}{rate(m,'OVER 1'):>9.1f}%{rate(m,'UNDER 8'):>9.1f}%")
    print("-" * 64)
    top = best()
    m, c = choose()
    print(f"TOP NOW  : {top[0]}  {top[1]}   ({rate(*top):.1f}% of last {WINDOW} ticks)")
    print(f"SELECTED : {m}  {c}   ({rate(m, c):.1f}%)  <- sent to your bot")
    if pending[(m, c)]:
        print(f"ENTRY    : ENTER NOW  (last digit was {digits[m][-1]}, a miss)")
    else:
        print("ENTRY    : WAIT  (enter right after a miss)")
    print(f"SIGNAL RECORD for {m} {c}: {record_text(m, c)}   <- the real result of this rule")
    if bot_path:
        state = "running" if bot_proc else ("stopped" if bot_proc is False else "not started")
        print(f"BOT      : {state}")
    print("Random digits: expect about 80%. This is not a guarantee of profit.")


def alert(m, c):
    key = (m, c)
    if time.time() - last_alert.get(key, 0) > 5:
        last_alert[key] = time.time()
        print(f"\n>>> ENTER NOW: {m} {c}  (last digit {digits[m][-1]})  "
              f"record: {record_text(m, c)}")


def connect():
    opts = {"cert_reqs": ssl.CERT_REQUIRED}
    if CA:
        opts["ca_certs"] = CA
    for url in URLS:
        try:
            ws = websocket.create_connection(url, timeout=20, sslopt=opts)
            ws.settimeout(2)
            print("Connected:", url)
            return ws
        except Exception as e:
            print("Could not connect:", e)
    return None


def run(ws, writer, f):
    for m in MARKETS:
        ws.send(json.dumps({"ticks_history": m, "count": WINDOW, "end": "latest",
                            "style": "ticks", "subscribe": 1}))
    last_show = last_ping = time.time()
    while True:
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            raw = None
        if raw == "":
            raise ConnectionError("server closed the connection")

        if raw:
            msg = json.loads(raw)
            if "error" in msg:
                print("Deriv error:", msg["error"].get("message"))
            elif msg.get("msg_type") == "history":
                m = msg["echo_req"]["ticks_history"]
                if m in digits:
                    decimals[m] = int(msg.get("pip_size", decimals[m]))
                    digits[m].clear()
                    last_epoch[m] = 0
                    h = msg["history"]
                    for price, epoch in zip(h["prices"], h["times"]):
                        add_tick(m, price, int(epoch), writer)
            elif msg.get("msg_type") == "tick":
                t = msg["tick"]
                m = t["symbol"]
                if m in digits and add_tick(m, t["quote"], int(t["epoch"]), writer):
                    if ready():
                        cm, cc = choose()
                        if m == cm:
                            write_choice()          # keep best_market.json fresh for the bot
                            if pending[(cm, cc)]:
                                alert(cm, cc)
                        launch_bot()

        now = time.time()
        if now - last_ping > 30:
            ws.send(json.dumps({"ping": 1}))
            last_ping = now
        if now - last_show >= SHOW_EVERY:
            show()
            write_choice()
            check_bot()
            f.flush()
            last_show = now


def main():
    ask_for_bot()
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if f.tell() == 0:
            writer.writerow(["time_utc", "market", "price", "last_digit"])
        try:
            while True:
                ws = connect()
                if ws is None:
                    print("No connection. Retrying in 10 seconds...")
                    time.sleep(10)
                    continue
                try:
                    run(ws, writer, f)
                except Exception as e:
                    print("Disconnected:", e, "- reconnecting in 5 seconds...")
                finally:
                    try:
                        ws.close()
                    except Exception:
                        pass
                time.sleep(5)
        except KeyboardInterrupt:
            print("\nStopped. Data saved to", CSV_FILE)
        finally:
            stop_bot()


if __name__ == "__main__":
    main()