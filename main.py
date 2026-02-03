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
    if weekday >= 5 and os.path.exists(cache_file):
        with open(cache_file, "rb") as f: return pickle.load(f)
    all_data = {}
    for i in range(0, len(tickers), 50):
        batch = tickers[i:i + 50]
        try:
            t = Ticker(batch)
            batch_data = t.history(interval=interval, period=period)
            if isinstance(batch_data, pd.DataFrame):
                for ticker in batch:
                    if (ticker,) in batch_data.index:
                        all_data[ticker] = batch_data.xs(ticker, level=0)
            time.sleep(0.1)
        except: pass
    with open(cache_file, "wb") as f: pickle.dump(all_data, f)
    return all_data

# ==========================================
# 2. SIGNAL LOGIC
# ==========================================

def compute_dm_signals(df):
    close = df["close"].values
    if len(close) < 20: return False, False, False, False
    TD = [0] * len(close); TS = [0] * len(close)
    for i in range(4, len(close)):
        TD[i] = TD[i - 1] + 1 if close[i] > close[i - 4] else 0
        TS[i] = TS[i - 1] + 1 if close[i] < close[i - 4] else 0
    def val_reset(arr, idx):
        for j in range(idx - 1, 0, -1):
            if arr[j] < arr[j - 1]: return arr[j]
        return 0
    TDUp = [TD[i] - val_reset(TD, i) for i in range(len(close))]
    TDDn = [TS[i] - val_reset(TS, i) for i in range(len(close))]
    return TDUp[-1] == 9, TDUp[-1] == 13, TDDn[-1] == 9, TDDn[-1] == 13

def compute_wyckoff_signals(df):
    if len(df) < 35: return False
    close = df['close']
    is_breakout = close.iloc[-1] > close.iloc[-31:-1].max()
    is_trending = (close.diff() > 0).astype(int).iloc[-5:].sum() > 4 
    return is_breakout and is_trending

# ==========================================
# 3. SCANNERS
# ==========================================

def scan_timeframe(ticker_map, industry_map, label, interval):
    results = {"Tops": [], "Bottoms": []}
    sector_counts = {"Tops": defaultdict(int), "Bottoms": defaultdict(int)}
    tickers = list(ticker_map.keys())
    period = '2y' if interval == '1wk' else '6mo'
    data = load_or_fetch_price_data(tickers, interval, period, label)
    candle_date = None
    for ticker, df in data.items():
        try:
            if df.empty: continue
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            if not candle_date:
                ld = pd.to_datetime(df['date'].iloc[-1]).tz_localize(None)
                candle_date = ld.strftime("%Y-%m-%d")
            p = float(df['close'].iloc[-1])
            dm9t, dm13t, dm9b, dm13b = compute_dm_signals(df)
            sec, ind = ticker_map.get(ticker, "Unknown"), industry_map.get(ticker, "Unknown")
            if dm9t or dm13t:
                results["Tops"].append((ticker, p, "DM13 Top" if dm13t else "DM9 Top", ind))
                sector_counts["Tops"][sec] += 1
            if dm9b or dm13b:
                results["Bottoms"].append((ticker, p, "DM13 Bot" if dm13b else "DM9 Bot", ind))
                sector_counts["Bottoms"][sec] += 1
        except: pass
    return results, sector_counts, candle_date if candle_date else "N/A"

def scan_wyckoff(ticker_map, industry_map):
    cache = os.path.join("cache", "price_cache_1D.pkl")
    if not os.path.exists(cache): return []
    with open(cache, "rb") as f: data = pickle.load(f)
    res = []
    for t, df in data.items():
        try:
            df = df.reset_index(); df.columns = [c.lower() for c in df.columns]
            if compute_wyckoff_signals(df):
                p = float(df['close'].iloc[-1])
                pct = ((p - df['close'].iloc[-2]) / df['close'].iloc[-2]) * 100
                res.append((t, p, ticker_map.get(t, "Unknown"), industry_map.get(t, "Unknown"), pct))
        except: pass
    return sorted(res, key=lambda x: (x[2], x[0]))

# ==========================================
# 4. FEAR & GREED / PLOTS
# ==========================================

def get_fear_and_greed():
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        h = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0 Safari/537.36"}
        d = requests.get(url, headers=h, timeout=10).json()
        fg = d.get("fear_and_greed", {})
        score, prev = round(fg.get("score", 0)), round(fg.get("previous_close", 0))
        with open("fear_and_greed_history.csv", "a", newline="") as f:
            csv.writer(f).writerow([datetime.utcnow().strftime("%Y-%m-%d"), score, prev])
        return score, prev, datetime.utcnow().strftime("%Y-%m-%d")
    except: return "N/A", "N/A", "N/A"

def plot_trends(d_sec, w_sec):
    secs = sorted(list(set(d_sec["Tops"].keys()) | set(w_sec["Tops"].keys())))
    if not secs: return
    d_c = [d_sec["Tops"].get(s,0) + d_sec["Bottoms"].get(s,0) for s in secs]
    w_c = [w_sec["Tops"].get(s,0) + w_sec["Bottoms"].get(s,0) for s in secs]
    plt.figure(figsize=(14, 8))
    plt.barh([i-0.17 for i in range(len(secs))], d_c, 0.35, label="Daily", color="lightcoral")
    plt.barh([i+0.17 for i in range(len(secs))], w_c, 0.35, label="Weekly", color="skyblue")
    plt.yticks(range(len(secs)), secs); plt.legend(); plt.tight_layout(); plt.savefig("docs/sector_trends.png"); plt.close()

# ==========================================
# 5. HTML GENERATION
# ==========================================

def get_shared_style(fg_color):
    css = """
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; font-size: 16px; }
        h1 { color: #333; display: flex; align-items: baseline; gap: 12px; }
        .date-subtitle { margin-top: 6px; font-size: 0.95em; color: #333; margin-bottom: 12px; }
        .fg-box { color: white; padding: 10px; margin-bottom: 20px; border-radius: 5px; display: inline-block; font-weight: bold; font-size: 1.1em; background-color: """ + fg_color + """; }
        .summary-table { border-collapse: collapse; margin: 20px 0; width: 100%; }
        .summary-table th, .summary-table td { border: 1px solid #ccc; padding: 6px 10px; text-align: center; }
        .summary-table th { background-color: #f0f0f0; }
        .row { display: flex; flex-direction: column; margin-bottom: 30px; }
        .column { flex: 1; margin: 10px 0; width: 100%; }
        
        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 1em; table-layout: auto; }
        th, td { border: 1px solid #ccc; padding: 8px 10px; text-align: left; }
        th { background-color: #f0f0f0; cursor: pointer; color: #007bff; text-decoration: underline; }
        
        .nav-bar { margin-bottom: 20px; }
        .nav-link { font-size: 1.1em; font-weight: bold; margin-right: 20px; text-decoration: none; color: #007bff; }
        .nav-link:hover { text-decoration: underline; color: #0056b3; }
        .active-link { color: #333; text-decoration: none; cursor: default; }

        @media (max-width: 37.5em) {
            body { margin: 5px; width: 100%; }
            table { width: 100% !important; }
            html { -webkit-text-size-adjust: none; text-size-adjust: none; }
            th, td, a { 
                font-size: 14px !important; 
                line-height: 1.4;
                padding: 6px 2px;
            }
            td:nth-child(4) { 
                white-space: normal;
                overflow-wrap: break-word; 
                word-wrap: break-word;
                min-width: 60px;
            }
        }

        @media (min-width: 64em) {
            .row { flex-direction: row; }
            .column { margin: 0 10px; }
            .summary-table { width: 60%; }
        }
    </style>
    <script>
    document.addEventListener("DOMContentLoaded", function() {
        document.querySelectorAll("table.sortable").forEach(table => {
            const headers = table.querySelectorAll("th");
            headers.forEach((header, i) => {
                header.addEventListener("click", () => {
                    const tbody = table.tBodies[0];
                    const rows = Array.from(tbody.querySelectorAll("tr"));
                    const asc = !header.classList.contains("asc");
                    headers.forEach(h => h.classList.remove("
