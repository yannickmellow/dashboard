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
    mapping, industry_map = {}, {}
    if os.path.exists(cache_file):
        with open(cache_file, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ticker = row.get('Ticker')
                sector, industry = row.get('Sector'), row.get('Industry')
                if ticker:
                    mapping[ticker.strip()] = sector.strip() if sector else "Unknown"
                    industry_map[ticker.strip()] = industry.strip() if industry else "Unknown"
    return mapping, industry_map

def load_or_fetch_price_data(tickers, interval, period, cache_key):
    cache_key = cache_key.upper()
    cache_file = os.path.join("cache", f"price_cache_{cache_key}.pkl")
    weekday = datetime.utcnow().weekday()
    # If weekend, use cache
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
    TD, TS = [0] * len(close), [0] * len(close)
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
            
            # --- FIX FOR FALSE WEEKLY SIGNALS ---
            # If Weekly, we must ensure we aren't reading the current "in-progress" week
            # unless it is Friday after close (which we approximate here)
            if interval == '1wk':
                last_date = pd.to_datetime(df['date'].iloc[-1])
                # Simple check: if last date is within last 3 days, it might be incomplete.
                # Ideally, just drop the last row if we are scanning mid-week.
                # Assuming script runs daily:
                if len(df) > 1:
                    df = df.iloc[:-1] # Always look at the last *closed* week
            
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
        
    # Sort Descending (Z-A) by Default as requested
    results["Tops"].sort(key=lambda x: x[0], reverse=True)
    results["Bottoms"].sort(key=lambda x: x[0], reverse=True)
        
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
    # Sort Descending (Z-A) by default
    return sorted(res, key=lambda x: x[0], reverse=True)

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
        
        # Save History
        file_exists = os.path.exists("fear_and_greed_history.csv")
        with open("fear_and_greed_history.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists: writer.writerow(["Date", "Index", "Previous Close"])
            writer.writerow([datetime.utcnow().strftime("%Y-%m-%d"), score, prev])
            
        return score, prev, datetime.utcnow().strftime("%Y-%m-%d")
    except: return "N/A", "N/A", "N/A"

def plot_fear_greed_history():
    try:
        df = pd.read_csv("fear_and_greed_history.csv")
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date').tail(90) # Last 90 days
        
        plt.figure(figsize=(10, 5))
        plt.plot(df['Date'], df['Index'], color='#333', linewidth=2)
        
        # FORCE 0-100 SCALE
        plt.ylim(0, 100)
        
        plt.title("Fear & Greed Index (Last 90 Days)")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig("docs/fg_trend.png")
        plt.close()
    except: pass

# ==========================================
# 5. HTML GENERATION
# ==========================================

def get_shared_style(fg_color):
    css = """
    <style>
        :root {
            --bg-color: #ffffff;
            --text-color: #333333;
            --table-bg: #ffffff;
            --th-bg: #f0f0f0;
            --border-color: #cccccc;
            --link-color: #007bff;
            --fg-box-text: #ffffff;
        }
        
        [data-theme="dark"] {
            --bg-color: #1a1a1a;
            --text-color: #e0e0e0;
            --table-bg: #2d2d2d;
            --th-bg: #404040;
            --border-color: #555555;
            --link-color: #66b3ff;
        }

        body { font-family: Arial, sans-serif; margin: 20px; font-size: 16px; background-color: var(--bg-color); color: var(--text-color); transition: background 0.3s, color 0.3s; }
        h1 { display: flex; align-items: baseline; gap: 12px; }
        
        .date-subtitle { margin-top: 6px; font-size: 0.95em; opacity: 0.8; margin-bottom: 12px; }
        .fg-box { padding: 10px; margin-bottom: 20px; border-radius: 5px; display: inline-block; font-weight: bold; font-size: 1.1em; background-color: REPLACEMENT_FG_COLOR; color: var(--fg-box-text); }
        
        /* Tables */
        .summary-table { border-collapse: collapse; margin: 20px 0; width: 100%; }
        .summary-table th, .summary-table td { border: 1px solid var(--border-color); padding: 6px 10px; text-align: center; background-color: var(--table-bg); }
        .summary-table th { background-color: var(--th-bg); }
        
        /* Summary Table Header - NO LINK STYLE */
        .summary-table th { cursor: default; color: var(--text-color); text-decoration: none; }
        .summary-table th:hover { color: var(--text-color); }

        .row { display: flex; flex-direction: column; margin-bottom: 30px; }
        .column { flex: 1; margin: 10px 0; width: 100%; }
        
        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 1em; table-layout: auto; }
        th, td { border: 1px solid var(--border-color); padding: 8px 10px; text-align: left; background-color: var(--table-bg); }
        
        /* Sortable Headers Only */
        table.sortable th { cursor: pointer; color: var(--link-color); text-decoration: underline; background-color: var(--th-bg); }
        
        /* Dark Mode Toggle */
        .theme-toggle { position: absolute; top: 20px; right: 20px; cursor: pointer; font-size: 24px; user-select: none; }
        
        .nav-bar { margin-bottom: 20px; }
        .nav-link { font-size: 1.1em; font-weight: bold; margin-right: 20px; text-decoration: none; color: var(--link-color); }
        .nav-link:hover { text-decoration: underline; opacity: 0.8; }
        .active-link { color: var(--text-color); text-decoration: none; cursor: default; }
        
        .update-footer { margin-top: 50px; font-size: 0.85em; color: #888; text-align: center; border-top: 1px solid var(--border-color); padding-top: 10px; }

        /* Arrows Hidden By Default */
        .sortable th::after { content: ""; margin-left: 5px; }
        .sortable th.asc::after { content: " ▲"; font-size: 0.8em; }
        .sortable th.desc::after { content: " ▼"; font-size: 0.8em; }

        /* Mobile Fixes */
        @media (max-width: 37.5em) {
            body { margin: 10px; }
            table { width: 100% !important; }
            html { -webkit-text-size-adjust: none; text-size-adjust: none; }
            .fg-chart { display: none !important; }
            th, td, a { font-size: 14px !important; line-height: 1.4; padding: 8px 8px; }
            td:nth-child(4) { white-space: normal; overflow-wrap: break-word; word-wrap: break-word; min-width: 60px; }
        }

        @media (min-width: 64em) {
            .row { flex-direction: row; }
            .column { margin: 0 10px; }
            .summary-table { width: 60%; }
        }
    </style>
    <script>
    document.addEventListener("DOMContentLoaded", function() {
        // Dark Mode Logic
        const toggle = document.getElementById('theme-toggle');
        const currentTheme = localStorage.getItem('theme');
        if (currentTheme) {
            document.documentElement.setAttribute('data-theme', currentTheme);
            toggle.textContent = currentTheme === 'dark' ? '☀️' : '🌙';
        }
        toggle.addEventListener('click', () => {
            let theme = document.documentElement.getAttribute('data-theme');
            let newTheme = theme === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-theme', newTheme);
            localStorage.setItem('theme', newTheme);
            toggle.textContent = newTheme === 'dark' ? '☀️' : '🌙';
        });

        // Sorting Logic
        document.querySelectorAll("table.sortable").forEach(table => {
            const headers = table.querySelectorAll("th");
            headers.forEach((header, i) => {
                header.addEventListener("click", () => {
                    const tbody = table.tBodies[0];
                    const rows = Array.from(tbody.querySelectorAll("tr"));
                    const wasAsc = header.classList.contains("asc");
                    
                    // Reset other headers
                    headers.forEach(h => h.classList.remove("asc", "desc"));
                    
                    // Toggle state
                    if (wasAsc) {
                        header.classList.add("desc");
                    } else {
                        header.classList.add("asc");
                    }
                    const asc = !wasAsc;

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
    return css.replace("REPLACEMENT_FG_COLOR", fg_color)

def gen_table(signals):
    if not signals: return "<p>No signals.</p>"
    # Added "sortable" class to make sure JS targets it
    h = "<table class='sortable'><thead><tr><th>Ticker</th><th>Price</th><th>Signal</th><th>Industry</th></tr></thead><tbody>"
    for t, p, s, ind in signals:
        bg = "#ffb3b3" if "Top" in s else "#d4edda"
        # Strip words for clean display
        display_s = s.replace(" Top", "").replace(" Bot", "")
        # Dark mode overrides for specific cells can be tricky, 
        # so we use a span with slight transparency for background colors in dark mode? 
        # For now, keeping hardcoded colors as requested, but text color handles contrast.
        link = f"<a href='https://www.tradingview.com/chart/?symbol={t}' target='_blank' style='text-decoration:none; color:var(--link-color); font-weight:bold;'>{t}</a>"
        # We apply text-color black for these specific colored cells to ensure readability even in dark mode
        h += f"<tr><td>{link}</td><td>{p:.2f}</td><td style='background-color:{bg}; color:#000; font-weight:{'bold' if '13' in s else 'normal'}'>{display_s}</td><td>{ind}</td></tr>"
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
    meta = '<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">'
    updated_at = f'<div class="update-footer">Last updated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}</div>'
    
    # Common Toggle HTML
    toggle = '<div id="theme-toggle" class="theme-toggle">🌙</div>'
    
    # --- INDEX HTML ---
    # Transposed Summary Table: Rows=Time, Cols=Signal
    html_i = f"""<html><head>{meta}<title>Dashboard</title>{style}</head><body>
    {toggle}
    <div class="nav-bar"><a href="index.html" class="nav-link active-link">DeMark</a><a href="wyckoff.html" class="nav-link">Wyckoff</a></div>
    <h1>📈 US DM Dashboard 📉</h1><div class="date-subtitle">{date_str}</div>
    
    <div class="fg-box">CNN Fear & Greed: {f_val} (Prev: {f_prev}) on {f_date}</div>
    <img src="fg_trend.png" class="fg-chart" style="max-width: 480px; display:block; margin:6px 0 16px 0;">
    
    <h2>Signal Summary</h2>
    <table class="summary-table">
        <tr><th>Period</th><th>Bottoms</th><th>Tops</th></tr>
        <tr><td><strong>Daily</strong></td><td>{len(daily["Bottoms"])}</td><td>{len(daily["Tops"])}</td></tr>
        <tr><td><strong>Weekly</strong></td><td>{len(weekly["Bottoms"])}</td><td>{len(weekly["Tops"])}</td></tr>
    </table>
    
    <div class="row">
        <div class="column"><h3>Daily Bottoms</h3>{gen_table(daily["Bottoms"])}{gen_sec_table("Daily Bottoms by Sector", d_sec["Bottoms"])}</div>
        <div class="column"><h3>Daily Tops</h3>{gen_table(daily["Tops"])}{gen_sec_table("Daily Tops by Sector", d_sec["Tops"])}</div>
    </div>
    <div class="row">
        <div class="column"><h3>Weekly Bottoms</h3>{gen_table(weekly["Bottoms"])}{gen_sec_table("Weekly Bottoms by Sector", w_sec["Bottoms"])}</div>
        <div class="column"><h3>Weekly Tops</h3>{gen_table(weekly["Tops"])}{gen_sec_table("Weekly Tops by Sector", w_sec["Tops"])}</div>
    </div>
    {updated_at}</body></html>"""
    with open("docs/index.html", "w", encoding="utf-8") as f: f.write(html_i)

    # --- WYCKOFF HTML ---
    w_rows = ""
    for t, p, sec, ind, pct in wyckoff:
        lk = f"<a href='https://www.tradingview.com/chart/?symbol={t}' target='_blank' style='text-decoration:none; color:var(--link-color); font-weight:bold;'>{t}</a>"
        w_rows += f"<tr><td>{lk}</td><td>{p:.2f}</td><td style='color:{'green' if pct>0 else 'red'}'>{pct:+.2f}%</td><td>{ind}</td><td style='background-color:#d4edda; color:#000;'>SOS</td></tr>"
    
    html_w = f"""<html><head>{meta}<title>Wyckoff</title>{style}</head><body>
    {toggle}
    <div class="nav-bar"><a href="index.html" class="nav-link">DeMark</a><a href="wyckoff.html" class="nav-link active-link">Wyckoff</a></div>
    <h1>💪 Wyckoff SOS</h1><div class="date-subtitle">{date_str}</div>
    <table class="sortable"><thead><tr><th>Ticker</th><th>Price</th><th>%</th><th>Industry</th><th>Pattern</th></tr></thead><tbody>{w_rows if w_rows else "<tr><td colspan='5'>None</td></tr>"}</tbody></table>
    {updated_at}</body></html>"""
    with open("docs/wyckoff.html", "w", encoding="utf-8") as f: f.write(html_w)

def main():
    maps, inds = {}, {}
    for f in ["sp_cache.csv", "russell_cache.csv", "nasdaq_cache.csv", "NDQ_cache.csv", "AMEX_cache.csv", "NYSE_cache.csv"]:
        m, i = fetch_tickers_and_sectors_from_csv(f); maps.update(m); inds.update(i)
    
    # Run Scans
    daily, d_s, d_date = scan_timeframe(maps, inds, "1D", "1d")
    weekly, w_s, _ = scan_timeframe(maps, inds, "1W", "1wk")
    wyckoff = scan_wyckoff(maps, inds)
    fg = get_fear_and_greed()
    
    # Generate Graph
    plot_fear_greed_history()
    
    try:
        ds = f"Signals triggered on {datetime.strptime(d_date, '%Y-%m-%d').strftime('%A, %b %d, %Y')} (as of NY close)"
    except: ds = f"Signals triggered on {d_date} (as of NY close)"
    
    write_reports(daily, weekly, d_s, w_s, fg, wyckoff, ds)

if __name__ == "__main__": main()
