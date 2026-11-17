#!/usr/bin/env python3
"""
DERIV BEST-MARKET FINDER (analysis only, no trades)

Install:  python -m pip install websocket-client certifi
Run:      python deriv_best.py
Stop:     Ctrl+C

What it does
  1. Collects the last 500 ticks for R_10, R_25, R_50, R_75, R_100.
  2. Compares Over 1 and Under 8 on every market and names the one with the
     highest recent hit rate ("BEST NOW").
  3. Gives an ENTRY signal for that choice: enter on the tick right AFTER a miss
     (Over 1 misses on digit 0 or 1, Under 8 misses on digit 8 or 9).
  4. Scores every signal it gives (win/loss on the next tick) so you can see
     the REAL win rate of the entry rule, not a guess.

Honest warning: R_10..R_100 digits are random. The best market changes all the
time, and the entry rule cannot beat chance. Watch the "SIGNAL RECORD" line:
it should stay near 80%. To profit you need a win rate above your break-even,
which is 100 / (100 + payout%). Check the payout shown on Deriv for the contract.
"""

import csv
import json
import ssl
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
CSV_FILE = "deriv_ticks.csv"
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


def record_text(m, c):
    wins, n = record[(m, c)]
    return f"{wins}/{n} = {wins / n * 100:.1f}%" if n else "no signals yet"


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
    m, c = best()
    print(f"BEST NOW : {m}  {c}   ({rate(m, c):.1f}% of last {WINDOW} ticks)")
    if pending[(m, c)]:
        print(f"ENTRY    : ENTER NOW  (last digit was {digits[m][-1]}, a miss)")
    else:
        print("ENTRY    : WAIT  (enter right after a miss)")
    print(f"SIGNAL RECORD for {m} {c}: {record_text(m, c)}   <- the real result of this rule")
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
                        bm, bc = best()
                        if m == bm and pending[(bm, bc)]:
                            alert(bm, bc)

        now = time.time()
        if now - last_ping > 30:
            ws.send(json.dumps({"ping": 1}))
            last_ping = now
        if now - last_show >= SHOW_EVERY:
            show()
            f.flush()
            last_show = now


def main():
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


if __name__ == "__main__":
    main()