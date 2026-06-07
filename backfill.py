"""
backfill.py
===========
जब OpenAlgo + recorder दोनों एक साथ बंद थे, तो उस window का live tick data lost
हो जाता है (broker replay नहीं करता)। यह script OpenAlgo के REST `history()` API
से **1-minute OHLCV** लाकर `bars_1m` hypertable में भर देती है।

Logic:
1. हर symbol का `bars_1m` में last timestamp देखो
   (अगर खाली है → आज की market open से शुरू)
2. वहाँ से अब तक का 1-min data REST से fetch करो
3. ON CONFLICT DO NOTHING से idempotent insert (दोबारा चलाओ → कोई duplicate नहीं)

Usage:
    python backfill.py                        # सभी symbols, last gap → now
    python backfill.py --days 3               # पिछले 3 दिन का data
    python backfill.py --symbol RELIANCE      # सिर्फ़ एक symbol
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from openalgo import api as OpenAlgo

load_dotenv()

OA_API_KEY = os.getenv("OPENALGO_API_KEY", "")
OA_HOST    = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")

PG_DSN = dict(
    host     = os.getenv("PG_HOST", "127.0.0.1"),
    port     = int(os.getenv("PG_PORT", "5432")),
    dbname   = os.getenv("PG_DB", "marketdata"),
    user     = os.getenv("PG_USER", "postgres"),
    password = os.getenv("PG_PASSWORD", "postgres"),
)

SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", "symbols.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill")

INSERT_SQL = """
INSERT INTO bars_1m (time, exchange, symbol, open, high, low, close, volume)
VALUES %s
ON CONFLICT (exchange, symbol, time) DO NOTHING
"""


def load_symbols(path: str) -> list[dict]:
    out: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 2:
                out.append({"exchange": parts[0], "symbol": parts[1]})
    return out


def last_bar_time(cur, exchange: str, symbol: str) -> datetime | None:
    cur.execute(
        "SELECT max(time) FROM bars_1m WHERE exchange=%s AND symbol=%s",
        (exchange, symbol),
    )
    return cur.fetchone()[0]


def backfill_symbol(client, conn, exchange: str, symbol: str,
                    start: datetime, end: datetime) -> int:
    df = client.history(
        symbol     = symbol,
        exchange   = exchange,
        interval   = "1m",
        start_date = start.strftime("%Y-%m-%d"),
        end_date   = end.strftime("%Y-%m-%d"),
    )

    if isinstance(df, dict) and df.get("status") == "error":
        log.warning("%s:%s history error → %s", exchange, symbol, df.get("message"))
        return 0
    if df is None or df.empty:
        return 0

    # df.index is tz-aware (Asia/Kolkata)
    rows = []
    for ts, r in df.iterrows():
        # सिर्फ़ gap window की rows रखें
        if ts < start or ts > end:
            continue
        rows.append((
            ts.to_pydatetime().astimezone(timezone.utc),
            exchange, symbol,
            float(r.get("open",  0) or 0),
            float(r.get("high",  0) or 0),
            float(r.get("low",   0) or 0),
            float(r.get("close", 0) or 0),
            int  (r.get("volume", 0) or 0),
        ))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows, page_size=1000)
    conn.commit()
    return len(rows)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--days",   type=int, default=None,
                   help="कितने दिन पीछे से fill करें (default: last gap)")
    p.add_argument("--symbol", type=str, default=None,
                   help="सिर्फ़ एक symbol")
    args = p.parse_args()

    if not OA_API_KEY:
        log.error("OPENALGO_API_KEY .env में सेट करें"); return 2

    symbols = load_symbols(SYMBOLS_FILE)
    if args.symbol:
        symbols = [s for s in symbols if s["symbol"] == args.symbol]
        if not symbols:
            log.error("symbols.txt में %s नहीं मिला", args.symbol); return 2

    client = OpenAlgo(api_key=OA_API_KEY, host=OA_HOST)
    conn   = psycopg2.connect(**PG_DSN)
    end    = datetime.now(tz=timezone.utc)

    total = 0
    for inst in symbols:
        ex, sy = inst["exchange"], inst["symbol"]

        if args.days is not None:
            start = end - timedelta(days=args.days)
        else:
            with conn.cursor() as cur:
                last = last_bar_time(cur, ex, sy)
            start = (last or (end - timedelta(days=1))) + timedelta(seconds=1)

        if start >= end:
            log.info("%s:%s already up to date", ex, sy); continue

        try:
            n = backfill_symbol(client, conn, ex, sy, start, end)
            log.info("%s:%s ← %d bars (%s → %s)",
                     ex, sy, n,
                     start.strftime("%Y-%m-%d %H:%M"),
                     end.strftime("%Y-%m-%d %H:%M"))
            total += n
        except Exception as e:
            log.error("%s:%s failed: %s", ex, sy, e)

    conn.close()
    log.info("Done. Total bars inserted: %d", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
