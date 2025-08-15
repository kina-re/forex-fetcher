import os
import sys
import logging
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values

# -------- Config from environment (GitHub Secrets) --------
REQUIRED_VARS = ["API_KEY", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT"]
missing = [k for k in REQUIRED_VARS if not os.getenv(k)]
if missing:
    print(f"Missing required secrets: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

API_KEY = os.getenv("API_KEY")
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

SYMBOL = os.getenv("SYMBOL", "EUR/USD")
INTERVAL = os.getenv("INTERVAL", "1min")
API_URL = "https://api.twelvedata.com/time_series"

# -------- Logging --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("gha_run")

DDL = """
CREATE TABLE IF NOT EXISTS forex_bars (
  id SERIAL PRIMARY KEY,
  symbol VARCHAR(16) NOT NULL,
  "datetime" TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low  DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  pip_hl DOUBLE PRECISION NOT NULL,
  pip_oc DOUBLE PRECISION NOT NULL,
  confidence_score DOUBLE PRECISION NOT NULL,
  confidence_tag TEXT NOT NULL,
  UNIQUE (symbol, "datetime")
);
"""

def ensure_table():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    finally:
        conn.close()

def fetch_bars(outputsize=10):
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "apikey": API_KEY,
        "outputsize": outputsize,
        "timezone": "UTC",
    }
    r = requests.get(API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(data.get("message"))

    values = data.get("values") or []
    bars = []
    for row in reversed(values):  # oldest -> newest
        dt = datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        bars.append({
            "symbol": SYMBOL,
            "datetime": dt,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        })
    log.info("Fetched bars: %s", ", ".join(b["datetime"].strftime("%Y-%m-%d %H:%M:%S") for b in bars))
    return bars

def add_metrics(bar):
    open_, high, low, close = bar["open"], bar["high"], bar["low"], bar["close"]
    pip_hl = (high - low) * 10000.0
    pip_oc = (close - open_) * 10000.0
    confidence_score = abs(pip_oc) / pip_hl if pip_hl != 0 else 0.0
    confidence_tag = "high" if confidence_score > 0.7 else "low"
    out = dict(bar)
    out.update(
        pip_hl=pip_hl,
        pip_oc=pip_oc,
        confidence_score=confidence_score,
        confidence_tag=confidence_tag,
    )
    return out

def upsert_rows(rows):
    if not rows:
        log.info("No rows to upsert.")
        return (0, 0)

    sql = """
        INSERT INTO forex_bars
        (symbol, "datetime", open, high, low, close, pip_hl, pip_oc, confidence_score, confidence_tag)
        VALUES %s
        ON CONFLICT (symbol, "datetime") DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close = EXCLUDED.close,
            pip_hl = EXCLUDED.pip_hl,
            pip_oc = EXCLUDED.pip_oc,
            confidence_score = EXCLUDED.confidence_score,
            confidence_tag = EXCLUDED.confidence_tag;
    """
    vals = [(
        r["symbol"], r["datetime"], r["open"], r["high"], r["low"], r["close"],
        r["pip_hl"], r["pip_oc"], r["confidence_score"], r["confidence_tag"]
    ) for r in rows]

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, vals)
        conn.commit()
    finally:
        conn.close()
    return (len(rows), None)

def main():
    ensure_table()
    bars = fetch_bars(outputsize=10)
    rows = [add_metrics(b) for b in bars]
    inserted, _ = upsert_rows(rows)
    log.info("Upserted %d rows up to %s", inserted, rows[-1]["datetime"].strftime("%Y-%m-%d %H:%M:%S %Z") if rows else "N/A")

if __name__ == "__main__":
    main()
