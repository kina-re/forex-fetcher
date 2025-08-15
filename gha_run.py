import os
import sys
import logging
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values

# -------- Config from environment (GitHub Secrets) --------
# Only API_KEY is required when SKIP_DB=1 (testing mode).
REQUIRED_VARS = ["API_KEY"]
missing = [k for k in REQUIRED_VARS if not os.getenv(k)]
if missing:
    print(f"Missing required secrets: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

API_KEY = os.getenv("API_KEY")
SKIP_DB = os.getenv("SKIP_DB", "0") == "1"  # if '1' we won't touch the DB

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "connect_timeout": 10,
    "sslmode": "require",  # required by most cloud Postgres providers
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
            cur.execute("SET statement_timeout = '15s';")
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
    log.info("Requesting last %s bars for %s @ %s", outputsize, SYMBOL, INTERVAL)
    r = requests.get(API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(f"TwelveData error: {data.get('message')}")

    values = data.get("values") or []
    bars = []
    for row in reversed(values):  # process oldest -> newest
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
        return

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
            cur.execute("SET statement_timeout = '15s';")
            execute_values(cur, sql, vals)
        conn.commit()
    finally:
        conn.close()

def main():
    bars = fetch_bars(outputsize=10)
    if not bars:
        print("No bars fetched.")
        return

    rows = [add_metrics(b) for b in bars]

    if SKIP_DB:
        print("SKIP_DB=1 -> not inserting. Last timestamps:",
              ", ".join(b["datetime"].strftime("%Y-%m-%d %H:%M:%S") for b in bars[-5:]))
        return

    # Ensure DB secrets exist before connect
    required_db = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT"]
    missing_db = [k for k in required_db if not os.getenv(k)]
    if missing_db:
        print(f"Missing DB secrets: {', '.join(missing_db)}", file=sys.stderr)
        sys.exit(1)

    ensure_table()
    upsert_rows(rows)
    print(f"Upserted {len(rows)} rows up to {rows[-1]['datetime']:%Y-%m-%d %H:%M:%S %Z}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("ERROR:", str(e))
        traceback.print_exc()
        raise
