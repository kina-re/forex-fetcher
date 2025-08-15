import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import logging
import os

# --- LOAD ENV ---
load_dotenv()

API_KEY = os.getenv('API_KEY')
SYMBOL = 'EUR/USD'
INTERVAL = '1min'
API_URL = 'https://api.twelvedata.com/time_series'

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Optional: log to file
handler = logging.FileHandler("forex.log")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- FETCH DATA ---
def fetch_forex_data():
    params = {
        'symbol': SYMBOL,
        'interval': INTERVAL,
        'apikey': API_KEY,
        'outputsize': 1
    }
    
    try:
        logger.info(f"Requesting data for {SYMBOL} at interval {INTERVAL}")
        response = requests.get(API_URL, params=params)
        response.raise_for_status()  # Raise error for bad status codes

        data = response.json()
        logger.info("Data fetched successfully")
        return data['values'][0]

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error: {req_err}")
    except KeyError:
        logger.error("Unexpected response format: 'values' key missing")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

# # --- CALCULATE METRICS ---
# def calculate_metrics(data):
#     open_ = float(data['open'])
#     high = float(data['high'])
#     low = float(data['low'])
#     close = float(data['close'])

#     pip_hl = (high - low) * 10000
#     pip_oc = (close - open_) * 10000
#     confidence_score = abs(pip_oc) / pip_hl if pip_hl != 0 else 0
#     confidence_tag = 'high' if confidence_score > 0.7 else 'low'

#     return {
#         'symbol': SYMBOL,
#         'datetime': datetime.strptime(data['datetime'], '%Y-%m-%d %H:%M:%S'),
#         'open': open_,
#         'high': high,
#         'low': low,
#         'close': close,
#         'pip_hl': pip_hl,
#         'pip_oc': pip_oc,
#         'confidence_score': confidence_score,
#         'confidence_tag': confidence_tag
#     }

# --- INSERT INTO DB ---
def insert_into_db(metrics):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO forex_data (
            symbol, datetime, open, high, low, close,
            
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        metrics['symbol'], metrics['datetime'], metrics['open'], metrics['high'],
        metrics['low'], metrics['close']
    ))
    logger.info("Inserting data into database...")

    conn.commit()
    cur.close()
    conn.close()

# --- MAIN ---
if __name__ == '__main__':
    raw_data = fetch_forex_data()
    #metrics = calculate_metrics(raw_data)
    #insert_into_db(metrics)
   # print("✅ Data inserted:", metrics)
