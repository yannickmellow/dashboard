import pandas as pd
from datetime import datetime, timedelta
import os
import pickle
from yahooquery import Ticker
import requests
import csv
from collections import defaultdict
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pytz

# ==========================================
# 1. CONFIG & UTILS
# ==========================================

os.makedirs("cache", exist_ok=True)
os.makedirs("docs", exist_ok=True)

def fetch_tickers_and_sectors_from_csv(cache_file):
    mapping = {}
    industry_map = {}
    if os.path.exists(cache_file):
        with open(cache_file, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ticker = row.get('Ticker')
                sector = row.get('Sector')
                industry = row.get('Industry')
                if ticker:
                    mapping[ticker.strip()] = sector.strip() if sector else "Unknown"
                    industry_map[ticker.strip()] = industry.strip() if industry else "Unknown"
    return mapping, industry_map

def load_or_fetch_price_data(tickers, interval, period, cache_key):
    cache_key = cache_key.upper()
    cache_file = os.path.join("cache", f"price_cache_{cache_key}.pkl")
    
    weekday = datetime.utcnow().weekday()
    is_weekend = weekday >= 5

    if is_weekend and os.path.exists(cache_file):
        print(f"📦 [Weekend] Using cached data: {cache_file}")
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    print(f"🌐 Fetching fresh data for {cache_key}...")
    all_data = {}
    batch_size = 50

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            t = Ticker(batch)
            batch_data = t.history(interval=interval, period=period)
            if isinstance(batch_data, pd.DataFrame):
                for ticker in batch:
                    if (ticker,) in batch_data.index:
                        all_data[ticker] = batch_data.xs(ticker, level=0)
            time.sleep(0.1)
        except Exception:
            pass
        
    with open(cache_file, "wb") as f:
        pickle.dump(all_data, f)
    return all_data

# ==========================================
# 2. SIGNAL LOGIC
# ==========================================

def compute_dm_signals(df):
    close = df["close"].values
    length = len(close)
    if length < 20: return False, False, False, False

    TD = [0] * length; TS = [0] * length
    for i in range(4, length):
        TD[i] = TD[i - 1] + 1 if close[i] > close[i - 4] else 0
        TS[i] = TS[i - 1] + 1 if close[i] < close[i - 4] else 0

    def valuewhen_reset(arr, idx):
        for j in range(idx - 1, 0, -1):
            if arr[j] < arr[j - 1]: return arr[j]
        return 0

    TDUp = [TD[i] - valuewhen_reset(TD, i) for i in range(length)]
    TDDn = [TS[i] - valuewhen_reset(TS, i) for i in range(length)]

    return TDUp[-1] == 9, TDUp[-1] == 13, TDDn[-1] == 9, TDDn[-1] == 13

def compute_wyckoff_signals(df):
    if len(df) < 35: return False
    close = df['close']
    prev_30_max = close.iloc[-31:-1].max()
    current_close = close.iloc[-1]
    is_breakout = current_close > prev_30_max
    
    up_days = (close.diff() > 0).astype(int)
    last_5_count = up_
