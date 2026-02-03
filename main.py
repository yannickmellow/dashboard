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
    # CSS Template with Placeholder REPLACEMENT_FG_COLOR
    css = """
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; font-size: 16px; }
        h1 { color: #333; display: flex; align-items: baseline; gap: 12px; }
        .date-subtitle { margin-top: 6px; font-size: 0.95em; color: #333; margin-bottom: 12px; }
        .fg-box { color: white; padding: 10px; margin-bottom: 20px; border-radius: 5px; display: inline-block; font-weight: bold; font-size: 1.1em; background-color: REPLACEMENT_FG_COLOR; }
        .summary-table { border-collapse: collapse; margin: 20px 0; width: 100%; }
        .summary-table th, .summary-table td { border: 1px solid #ccc; padding: 6px 10px; text-align: center; }
        .summary-table th { background-color: #f0f0f0; }
        .row { display: flex; flex-direction: column; margin-bottom: 30px; }
        .column { flex: 1; margin: 10px 0; width: 100%; }
        
        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 1em; table-layout: auto; }
        
        /* Default Desktop Padding */
        th, td { border: 1px solid #ccc; padding: 8px 10px; text-align: left; }
        th { background-color: #f0f0f0; cursor: pointer; color: #007bff; text-decoration: underline; }
        
        .nav-bar { margin-bottom: 20px; }
        .nav-link { font-size: 1.1em; font-weight: bold; margin-right: 20px; text-decoration: none; color: #007bff; }
        .nav-link:hover { text-decoration: underline; color: #0056b3; }
        .active-link { color: #333; text-decoration: none; cursor: default; }

        /* CRITICAL MOBILE FIXES */
        @media (max-width: 37.5em) {
            body { margin: 5px; width: 100%; }
            table { width: 100% !important; }
            
            /* STOP TEXT INFLATION */
            html { -webkit-text-size-adjust: none; text-size-adjust: none; }
            
            /* FORCE Uniform Font Size */
            th, td, a { 
                font-size: 14px !important; 
                line-height: 1.4;
                padding: 6px 2px;
            }
            
            /* Allow Industry column to wrap nicely */
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
                    headers.forEach(h => h.classList.remove("asc", "desc"));
                    header.classList.add(asc ? "asc" : "desc");
                    rows.sort((a, b) => {
                        const aT = a.cells[i].innerText.trim(), bT = b.cells[i].innerText.trim();
                        const aN = parseFloat(aT.replace(/[^0-9.-]/g, "")), bN = parseFloat(bT.replace(/[^0-9.-]/g, ""));
                        return !isNaN(aN) && !isNaN(bN) ? (asc ? aN - bN : bN - aN) : (asc ? aT.localeCompare(bT) : bT.localeCompare(aT));
                    });
                    rows.forEach(r => tbody.appendChild(r));
                });
            });
        });
    });
    </script>
    """
    # Safe Replacement
    return css.replace("REPLACEMENT_FG_COLOR", fg_color)

def gen_table(signals):
    if not signals: return "<p>No signals.</p>"
    h = "<table class='sortable'><thead><tr><th>Ticker</th><th>Price</th><th>Signal</th><th>Industry</th></tr></thead><tbody>"
    for t, p, s, ind in signals:
        bg = "#ffb3b3" if "Top" in s else "#d4edda"
        link = f"<a href='https://www.tradingview.com/chart/?symbol={t}' target='_blank' style='text-decoration:none; color:#007bff; font-weight:bold;'>{t}</a>"
        h += f"<tr><td>{link}</td><td>{p:.2f}</td><td style='background-color:{bg}; font-weight:{'bold' if '13' in s else 'normal'}'>{s}</td><td>{ind}</td></tr>"
    return h + "</tbody></table>"

def gen_sec_table(title, counts):
    if not counts: return ""
    h = f"<h3>{title}</h3><table><tr><th>Sector</th><th>Count</th></tr>"
    for s, c in sorted(counts.items(), key=lambda x: x[1], reverse=True): h += f"<tr><td>{s}</td><td>{c}</td></tr>"
    return h + "</table>"

def write_reports(daily, weekly, d_sec, w_sec, fg, wyckoff, date_str):
    f_val, f_prev, f_date = fg
    f_col = "#dc3545" if isinstance(f_val, int) and f_val >= 60 else "#ffc107" if isinstance(f_val, int) and f_val >= 45 else "#28a745"
    style = get_shared_style(f_col)
    
    # Meta tag for mobile viewport
    meta = '<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">'
    
    # Index HTML
    html_i = f"""<html><head>{meta}<title>Dashboard</title>{style}</head><body>
    <div class="nav-bar"><a href="index.html" class="nav-link active-link">DeMark</a><a href="wyckoff.html" class="nav-link">Wyckoff</a></div>
    <h1>📈 US DM Dashboard 📉</h1><div class="date-subtitle">{date_str}</div>
    <div class="fg-box">CNN Fear & Greed: {f_val} (Prev: {f_prev}) on {f_date}</div>
    <img src="fg_trend.png" style="max-width: 480px; display:block; margin:6px 0 16px 0;">
    <h2>Signal Summary</h2><table class="summary-table"><tr><th>Totals</th><th>Daily</th><th>Weekly</th></tr>
    <tr><td><strong>Bottoms</strong></td><td>{len(daily["Bottoms"])}</td><td>{len(weekly["Bottoms"])}</td></tr>
    <tr><td><strong>Tops</strong></td><td>{len(daily["Tops"])}</td><td>{len(weekly["Tops"])}</td></tr></table>
    <div class="row">
        <div class="column"><h3>Daily Bottoms</h3>{gen_table(daily["Bottoms"])}{gen_sec_table("Daily Bottoms by Sector", d_sec["Bottoms"])}</div>
        <div class="column"><h3>Daily Tops</h3>{gen_table(daily["Tops"])}{gen_sec_table("Daily Tops by Sector", d_sec["Tops"])}</div>
    </div>
    <div class="row">
        <div class="column"><h3>Weekly Bottoms</h3>{gen_table(weekly["Bottoms"])}{gen_sec_table("Weekly Bottoms by Sector", w_sec["Bottoms"])}</div>
        <div class="column"><h3>Weekly Tops</h3>{gen_table(weekly["Tops"])}{gen_sec_table("Weekly Tops by Sector", w_sec["Tops"])}</div>
    </div>
    <h3>Sector Trends</h3><img src="sector_trends.png" style="max-width:100%"></body></html>"""
    with open("docs/index.html", "w", encoding="utf-8") as f: f.write(html_i)

    # Wyckoff HTML
    w_rows = ""
    for t, p, sec, ind, pct in wyckoff:
        lk = f"<a href='https://www.tradingview.com/chart/?symbol={t}' target='_blank' style='text-decoration:none; color:#007bff; font-weight:bold;'>{t}</a>"
        w_rows += f"<tr><td>{lk}</td><td>{p:.2f}</td><td style='color:{'green' if pct>0 else 'red'}'>{pct:+.2f}%</td><td>{ind}</td><td style='background-color:#d4edda'>SOS</td></tr>"
    
    html_w = f"""<html><head>{meta}<title>Wyckoff</title>{style}</head><body>
    <div class="nav-bar"><a href="index.html" class="nav-link">DeMark</a><a href="wyckoff.html" class="nav-link active-link">Wyckoff</a></div>
    <h1>💪 Wyckoff SOS</h1><div class="date-subtitle">{date_str}</div>
    <table class="sortable"><thead><tr><th>Ticker</th><th>Price</th><th>%</th><th>Industry</th><th>Pattern</th></tr></thead><tbody>{w_rows if w_rows else "<tr><td colspan='5'>None</td></tr>"}</tbody></table></body></html>"""
    with open("docs/wyckoff.html", "w", encoding="utf-8") as f: f.write(html_w)

def main():
    maps, inds = {}, {}
    for f in ["sp_cache.csv", "russell_cache.csv", "nasdaq_cache.csv", "NDQ_cache.csv", "AMEX_cache.csv", "NYSE_cache.csv"]:
        m, i = fetch_tickers_and_sectors_from_csv(f); maps.update(m); inds.update(i)
    daily, d_s, d_date = scan_timeframe(maps, inds, "1D", "1d")
    weekly, w_s, _ = scan_timeframe(maps, inds, "1W", "1wk")
    wyckoff = scan_wyckoff(maps, inds)
    fg = get_fear_and_greed()
    plot_trends(d_s, w_s)
    try:
        ds = f"Signals triggered on {datetime.strptime(d_date, '%Y-%m-%d').strftime('%A, %b %d, %Y')} (as of NY close)"
    except: ds = f"Signals triggered on {d_date} (as of NY close)"
    write_reports(daily, weekly, d_s, w_s, fg, wyckoff, ds)

if __name__ == "__main__": main()
