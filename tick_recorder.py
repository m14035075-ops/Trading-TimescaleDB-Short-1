"""
tick_recorder.py
================
OpenAlgo WebSocket → TimescaleDB recorder.

• 50 (या जितने भी symbols.txt में हों) NSE शेयरों का live tick + L2 depth data
  TimescaleDB में store करता है।
• Auto-reconnect + exponential backoff।
• Batched insert (psycopg2 execute_values) से DB load कम।
• Ctrl+C पर graceful shutdown — pending buffer flush हो जाता है।

Usage:
    cp .env.example .env       # values भर दें
    psql -d marketdata -f schema.sql
    pip install -r requirements.txt
    python tick_recorder.py
"""

from __future__ import annotations

import os
import signal
import sys
import time
import threading
import logging
from datetime import datetime, timezone
from queue import Queue, Empty
from typing import Any

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from openalgo import api as OpenAlgo


# ---------------------------------------------------------------- config
load_dotenv()

OA_API_KEY    = os.getenv("OPENALGO_API_KEY", "")
OA_HOST       = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
OA_WS_PORT    = int(os.getenv("OPENALGO_WS_PORT", "8765"))

PG_DSN = dict(
    host     = os.getenv("PG_HOST", "127.0.0.1"),
    port     = int(os.getenv("PG_PORT", "5432")),
    dbname   = os.getenv("PG_DB", "marketdata"),
    user     = os.getenv("PG_USER", "postgres"),
    password = os.getenv("PG_PASSWORD", "postgres"),
)

RECORD_MODE        = os.getenv("RECORD_MODE", "both").lower()  # quote / depth / both
SYMBOLS_FILE       = os.getenv("SYMBOLS_FILE", "symbols.txt")
BATCH_SIZE         = int(os.getenv("BATCH_SIZE", "200"))
FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "1.0"))
RECONNECT_MIN      = int(os.getenv("RECONNECT_MIN", "2"))
RECONNECT_MAX      = int(os.getenv("RECONNECT_MAX", "60"))

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%H:%M:%S",
)
log = logging.getLogger("recorder")


# ---------------------------------------------------------------- helpers
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


def ts_to_dt(ms: int | None) -> datetime:
    """OpenAlgo timestamp (ms) → aware UTC datetime; fallback now()."""
    if ms and ms > 0:
        # कुछ brokers seconds भेजते हैं — auto-detect
        if ms < 10_000_000_000:        # 10-digit → seconds
            return datetime.fromtimestamp(ms, tz=timezone.utc)
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------- DB writer
class DBWriter(threading.Thread):
    """Background thread — queue से rows पढ़कर TimescaleDB में batch insert।"""

    TICK_SQL = """
        INSERT INTO ticks (time, exchange, symbol, ltp, volume, open, high, low,
                           close, last_trade_qty, avg_trade_price, change, change_percent)
        VALUES %s
    """
    DEPTH_SQL = """
        INSERT INTO depth (time, exchange, symbol, ltp, side, level, price, quantity, orders)
        VALUES %s
    """

    def __init__(self) -> None:
        super().__init__(daemon=True, name="DBWriter")
        self.q: Queue = Queue(maxsize=100_000)
        self.stop_evt = threading.Event()
        self.conn = self._connect()

    def _connect(self):
        while not self.stop_evt.is_set():
            try:
                c = psycopg2.connect(**PG_DSN)
                c.autocommit = True
                log.info("TimescaleDB connected → %s/%s", PG_DSN["host"], PG_DSN["dbname"])
                return c
            except Exception as e:
                log.error("DB connect fail: %s — retry 5s", e)
                time.sleep(5)

    def put(self, kind: str, row: tuple) -> None:
        try:
            self.q.put_nowait((kind, row))
        except Exception:
            log.warning("Queue full — dropping row")

    def run(self) -> None:
        ticks_buf: list[tuple] = []
        depth_buf: list[tuple] = []
        last_flush = time.time()

        while not self.stop_evt.is_set() or not self.q.empty():
            try:
                kind, row = self.q.get(timeout=0.2)
                (ticks_buf if kind == "tick" else depth_buf).append(row)
            except Empty:
                pass

            now = time.time()
            if (len(ticks_buf) >= BATCH_SIZE or len(depth_buf) >= BATCH_SIZE
                    or now - last_flush >= FLUSH_INTERVAL_SEC):
                self._flush(ticks_buf, depth_buf)
                last_flush = now

        # final flush
        self._flush(ticks_buf, depth_buf)
        try: self.conn.close()
        except Exception: pass
        log.info("DBWriter stopped.")

    def _flush(self, ticks_buf: list[tuple], depth_buf: list[tuple]) -> None:
        if not ticks_buf and not depth_buf:
            return
        try:
            with self.conn.cursor() as cur:
                if ticks_buf:
                    execute_values(cur, self.TICK_SQL, ticks_buf, page_size=500)
                if depth_buf:
                    execute_values(cur, self.DEPTH_SQL, depth_buf, page_size=500)
            log.debug("flushed ticks=%d depth=%d", len(ticks_buf), len(depth_buf))
            ticks_buf.clear()
            depth_buf.clear()
        except Exception as e:
            log.error("DB write error: %s — reconnecting", e)
            try: self.conn.close()
            except Exception: pass
            self.conn = self._connect()


# ---------------------------------------------------------------- callbacks
writer: DBWriter  # set in main()


def on_quote(msg: dict[str, Any]) -> None:
    d  = msg.get("data", {})
    ex = msg.get("exchange"); sy = msg.get("symbol")
    writer.put("tick", (
        ts_to_dt(d.get("timestamp")),
        ex, sy,
        d.get("ltp"), d.get("volume"),
        d.get("open"), d.get("high"), d.get("low"), d.get("close"),
        d.get("last_trade_quantity"), d.get("avg_trade_price"),
        d.get("change"), d.get("change_percent"),
    ))


def on_depth(msg: dict[str, Any]) -> None:
    d  = msg.get("data", {})
    ex = msg.get("exchange"); sy = msg.get("symbol")
    t  = ts_to_dt(d.get("timestamp"))
    ltp = d.get("ltp")
    book = d.get("depth", {}) or {}
    for side in ("buy", "sell"):
        for i, lvl in enumerate(book.get(side, []) or [], start=1):
            writer.put("depth", (
                t, ex, sy, ltp, side, i,
                lvl.get("price"), lvl.get("quantity"), lvl.get("orders"),
            ))


# ---------------------------------------------------------------- WS manager
class FeedManager:
    """OpenAlgo connect + subscribe + auto-reconnect with exponential backoff."""

    def __init__(self, instruments: list[dict]) -> None:
        self.instruments = instruments
        self.client: OpenAlgo | None = None
        self.stop_evt = threading.Event()

    def stop(self) -> None:
        self.stop_evt.set()
        if self.client:
            try: self.client.disconnect()
            except Exception: pass

    def _subscribe_all(self) -> None:
        if RECORD_MODE in ("quote", "both"):
            log.info("Subscribing QUOTE for %d symbols…", len(self.instruments))
            self.client.subscribe_quote(self.instruments, on_data_received=on_quote)
        if RECORD_MODE in ("depth", "both"):
            log.info("Subscribing DEPTH (L2) for %d symbols…", len(self.instruments))
            self.client.subscribe_depth(self.instruments, on_data_received=on_depth)

    def run(self) -> None:
        backoff = RECONNECT_MIN
        while not self.stop_evt.is_set():
            try:
                self.client = OpenAlgo(
                    api_key = OA_API_KEY,
                    host    = OA_HOST,
                    ws_port = OA_WS_PORT,
                    verbose = 1,
                )
                if not self.client.connect():
                    raise RuntimeError("connect/auth failed")

                self._subscribe_all()
                backoff = RECONNECT_MIN  # success — reset
                log.info("Streaming live data… (Ctrl+C to stop)")

                # WS thread तक monitor करते रहो
                while not self.stop_evt.is_set() and self.client.connected:
                    time.sleep(1)

                if not self.stop_evt.is_set():
                    log.warning("WebSocket disconnected — reconnecting…")

            except Exception as e:
                log.error("Feed error: %s", e)

            finally:
                try: self.client and self.client.disconnect()
                except Exception: pass
                self.client = None

            if self.stop_evt.is_set():
                break

            log.info("Reconnect in %ds…", backoff)
            for _ in range(backoff):
                if self.stop_evt.is_set(): break
                time.sleep(1)
            backoff = min(backoff * 2, RECONNECT_MAX)


# ---------------------------------------------------------------- main
def main() -> int:
    global writer

    if not OA_API_KEY:
        log.error("OPENALGO_API_KEY .env में सेट करें")
        return 2

    instruments = load_symbols(SYMBOLS_FILE)
    if not instruments:
        log.error("symbols.txt खाली है"); return 2
    log.info("Loaded %d symbols from %s", len(instruments), SYMBOLS_FILE)
    log.info("Mode = %s | Batch = %d | Flush = %.1fs",
             RECORD_MODE.upper(), BATCH_SIZE, FLUSH_INTERVAL_SEC)

    writer = DBWriter(); writer.start()
    feed   = FeedManager(instruments)

    def shutdown(signum, _frame):
        log.info("Signal %s — shutting down…", signum)
        feed.stop()
        writer.stop_evt.set()

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    feed.run()
    writer.join(timeout=10)
    log.info("Bye 👋")
    return 0


if __name__ == "__main__":
    sys.exit(main())
