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
    # Ensure cache key consistency (always uppercase for filename)
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
    # 1. Breakout: Today > Max of previous 30 days (excluding today)
    prev_30_max = close.iloc[-31:-1].max()
    current_close = close.iloc[-1]
    is_breakout = current_close > prev_30_max
    
    # 2. Momentum: 5 consecutive up days (Count Up > 4)
    up_days = (close.diff() > 0).astype(int)
    last_5_count = up_days.iloc[-5:].sum()
    is_trending = last_5_count > 4 # Implies 5
    
    return is_breakout and is_trending

# ==========================================
# 3. SCANNERS
# ==========================================

def scan_timeframe(ticker_sector_map, ticker_industry_map, interval_label, interval):
    results = {"Tops": [], "Bottoms": []}
    sector_counts = {"Tops": defaultdict(int), "Bottoms": defaultdict(int)}
    tickers = list(ticker_sector_map.keys())
    
    period = '2y' if interval == '1wk' else '6mo'
    price_data = load_or_fetch_price_data(tickers, interval, period, interval_label)
    candle_date = None

    for ticker, df in price_data.items():
        try:
            if df.empty: continue
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            
            if not candle_date:
                last_date = pd.to_datetime(df['date'].iloc[-1])
                if getattr(last_date, "tzinfo", None) is not None: last_date = last_date.tz_localize(None)
                candle_date = last_date.strftime("%Y-%m-%d")

            last_close = float(df['close'].iloc[-1])
            DM9Top, DM13Top, DM9Bot, DM13Bot = compute_dm_signals(df)
            sector = ticker_sector_map.get(ticker, "Unknown")
            industry = ticker_industry_map.get(ticker, "Unknown")

            if DM9Top or DM13Top:
                signal = "DM13 Top" if DM13Top else "DM9 Top"
                results["Tops"].append((ticker, last_close, signal, industry))
                sector_counts["Tops"][sector] += 1
            if DM9Bot or DM13Bot:
                signal = "DM13 Bot" if DM13Bot else "DM9 Bot"
                results["Bottoms"].append((ticker, last_close, signal, industry))
                sector_counts["Bottoms"][sector] += 1
        except: pass

    results["Tops"].sort(key=lambda x: x[0])
    results["Bottoms"].sort(key=lambda x: x[0])
    return results, sector_counts, candle_date if candle_date else datetime.utcnow().strftime("%Y-%m-%d")

def scan_wyckoff_signals(ticker_sector_map, ticker_industry_map):
    # Ensure we look for the exact cache file generated by scan_timeframe
    cache_file = os.path.join("cache", "price_cache_1D.pkl")
    
    if not os.path.exists(cache_file): 
        print(f"⚠️ Warning: {cache_file} not found. Wyckoff scan skipped.")
        return []
    
    print(f"📖 Reading cache for Wyckoff scan: {cache_file}")
    with open(cache_file, "rb") as f:
        price_data = pickle.load(f)
        
    results = []
    for ticker, df in price_data.items():
        if ticker not in ticker_sector_map: continue
        try:
            if df.empty: continue
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            
            if compute_wyckoff_signals(df):
                last_close = float(df['close'].iloc[-1])
                sector = ticker_sector_map.get(ticker, "Unknown")
                industry = ticker_industry_map.get(ticker, "Unknown")
                pct_change = ((df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2]) * 100
                results.append((ticker, last_close, sector, industry, pct_change))
        except: pass
        
    return sorted(results, key=lambda x: (x[2], x[0]))

# ==========================================
# 4. FEAR & GREED / PLOTTING
# ==========================================

def get_fear_and_greed():
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        headers = {"User-Agent": "Mozilla/5.0"}
        data = requests.get(url, headers=headers, timeout=5).json()
        fg = data.get("fear_and_greed", {})
        
        date = datetime.utcnow().strftime("%Y-%m-%d")
        with open("fear_and_greed_history.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0: writer.writerow(["Date", "Index", "Previous Close"])
            writer.writerow([date, round(fg.get("score", 0)), round(fg.get("previous_close", 0))])
            
        return round(fg.get("score", 0)), round(fg.get("previous_close", 0)), date
    except:
        return "N/A", "N/A", "N/A"

def plot_trends(daily_sec, weekly_sec):
    sectors = sorted(list(set(daily_sec["Tops"].keys()) | set(weekly_sec["Tops"].keys())))
    if not sectors: return
    d_counts = [daily_sec["Tops"].get(s,0) + daily_sec["Bottoms"].get(s,0) for s in sectors]
    w_counts = [weekly_sec["Tops"].get(s,0) + weekly_sec["Bottoms"].get(s,0) for s in sectors]
    
    plt.figure(figsize=(14, 8))
    x = range(len(sectors)); width=0.35
    plt.barh([i-width/2 for i in x], d_counts, width, label="Daily", color="lightcoral")
    plt.barh([i+width/2 for i in x], w_counts, width, label="Weekly", color="skyblue")
    plt.yticks(x, sectors); plt.legend()
    plt.title("Sector Signal Trends")
    plt.tight_layout()
    plt.savefig("docs/sector_trends.png")
    plt.close()

# ==========================================
# 5. HTML GENERATION (SPLIT FUNCTIONS)
# ==========================================

def get_shared_style():
    return """
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #333; display: flex; align-items: baseline; gap: 12px; }
        .nav-link { font-size: 1.2em; font-weight: bold; margin-right: 15px; text-decoration: none; color: #007bff; }
        .nav-link:hover { text-decoration: underline; color: #0056b3; }
        .active-link { color: #333; text-decoration: none; cursor: default; border-bottom: 2px solid #333; }
        
        .date-subtitle { margin-top: 6px; font-size: 0.95em; color: #333; margin-bottom: 12px; }
        .fg-box { padding: 10px; margin-bottom: 20px; border-radius: 5px; display: inline-block; color: white; }
        
        .summary-table { border-collapse: collapse; margin: 20px 0; width: 100%; }
        .summary-table th, .summary-table td { border: 1px solid #ccc; padding: 6px 10px; text-align: center; }
        .summary-table th { background-color: #f0f0f0; }
        
        .row { display: flex; flex-direction: column; margin-bottom: 30px; }
        .column { flex: 1; margin: 10px 0; width: 100%; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 1.1em; display: block; white-space: normal; }
        table tbody { display: table; width: 100%; }
        th, td { border: 1px solid #ccc; padding: 6px 6px; text-align: left; }
        th { background-color: #f0f0f0; cursor: pointer; }
        
        @media (min-width: 64em) {
            .row { flex-direction: row; }
            .column { margin: 0 10px; }
            .summary-table { width: 60%; }
        }
        
        th.asc::after { content: " ▲"; font-size: 0.9em; color: #333; }
        th.desc::after { content: " ▼"; font-size: 0.9em; color: #333; }
    </style>
    <script>
    document.addEventListener("DOMContentLoaded", function() {
        document.querySelectorAll("table").forEach(table => {
            table.querySelectorAll("th").forEach((header, i) => {
                header.addEventListener("click", () => {
                    const tbody = table.querySelector("tbody") || table;
                    const rows = Array.from(tbody.querySelectorAll("tr")).filter(r => r.parentNode === tbody && !r.querySelector("th"));
                    const isAsc = header.classList.toggle("asc");
                    header.classList.remove("desc");
                    if (!isAsc) header.classList.add("desc");
                    
                    rows.sort((a, b) => {
                        const tA = a.cells[i].innerText;
                        const tB = b.cells[i].innerText;
                        const nA = parseFloat(tA.replace(/[^0-9.-]/g, ""));
                        const nB = parseFloat(tB.replace(/[^0-9.-]/g, ""));
                        
                        if (!isNaN(nA) && !isNaN(nB)) return isAsc ? nA - nB : nB - nA;
                        return isAsc ? tA.localeCompare(tB) : tB.localeCompare(tA);
                    });
                    rows.forEach(r => tbody.appendChild(r));
                });
            });
        });
    });
    </script>
    """

def signals_to_html_table(signals):
    if not signals: return "<p>No signals.</p>"
    html = "<table><thead><tr><th>Ticker</th><th>Price</th><th>Signal</th><th>Industry</th></tr></thead><tbody>"
    for t, price, sig, ind in signals:
        bg = "#ffb3b3" if "Top" in sig else "#d4edda"
        html += f"<tr><td>{t}</td><td>{price:.2f}</td><td style='background-color:{bg}'>{sig}</td><td>{ind}</td></tr>"
    return html + "</tbody></table>"

def write_index_html(daily, weekly, fg_data, report_date):
    print("✍️ Generating Index HTML...")
    fg_val, fg_prev, fg_date = fg_data
    fg_color = "#dc3545" if isinstance(fg_val, int) and fg_val >= 60 else "#ffc107" if isinstance(fg_val, int) and fg_val >= 30 else "#28a745"

    html = f"""<html><head><meta charset="UTF-8"><title>US DM Dashboard</title>{get_shared_style()}</head><body>
    <div>
        <a href="index.html" class="nav-link active-link">DeMark Dashboard</a>
        <a href="wyckoff.html" class="nav-link">Wyckoff Scans</a>
    </div>
    <h1>📉 US DM Dashboard</h1>
    <div class="date-subtitle">{report_date}</div>
    <div class="fg-box" style="background-color: {fg_color};"><strong>CNN Fear & Greed:</strong> {fg_val} (Prev: {fg_prev})</div>
    
    <h2>Signal Summary</h2>
    <table class="summary-table">
        <tr><th>Totals</th><th>Daily</th><th>Weekly</th></tr>
        <tr><td><strong>Bottoms</strong></td><td>{len(daily["Bottoms"])}</td><td>{len(weekly["Bottoms"])}</td></tr>
        <tr><td><strong>Tops</strong></td><td>{len(daily["Tops"])}</td><td>{len(weekly["Tops"])}</td></tr>
    </table>
    
    <div class="row">
        <div class="column"><h3>Daily Bottoms</h3>{signals_to_html_table(daily["Bottoms"])}</div>
        <div class="column"><h3>Daily Tops</h3>{signals_to_html_table(daily["Tops"])}</div>
    </div>
    <div class="row">
        <div class="column"><h3>Weekly Bottoms</h3>{signals_to_html_table(weekly["Bottoms"])}</div>
        <div class="column"><h3>Weekly Tops</h3>{signals_to_html_table(weekly["Tops"])}</div>
    </div>
    
    <h3>Sector Trends</h3><img src="sector_trends.png" style="max-width:100%">
    </body></html>"""
    
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ Successfully wrote docs/index.html")

def write_wyckoff_html(wyckoff_res, report_date):
    print(f"✍️ Generating Wyckoff HTML with {len(wyckoff_res)} candidates...")
    
    w_rows = ""
    if wyckoff_res:
        for t, price, sec, ind, pct in wyckoff_res:
            color = "green" if pct > 0 else "red"
            w_rows += f"""<tr>
                <td><a href="https://www.tradingview.com/chart/?symbol={t}" target="_blank">{t}</a></td>
                <td>{price:.2f}</td>
                <td style="color:{color}">{pct:+.2f}%</td>
                <td>{sec}</td>
                <td>{ind}</td>
                <td style="background-color:#d4edda">Breakout + Streak</td>
            </tr>"""
    else:
        w_rows = "<tr><td colspan='6' style='text-align:center; padding:20px;'>No Wyckoff candidates found matching criteria today.</td></tr>"

    html = f"""<html><head><meta charset="UTF-8"><title>Wyckoff Screener</title>{get_shared_style()}</head><body>
    <div>
        <a href="index.html" class="nav-link">DeMark Dashboard</a>
        <a href="wyckoff.html" class="nav-link active-link">Wyckoff Scans</a>
    </div>
    <h1>💪 Wyckoff "Sign of Strength"</h1>
    <div class="date-subtitle">{report_date}</div>
    
    <div style="background-color:#e2e6ea; padding:15px; border-radius:5px; margin-bottom:20px;">
        <strong>Criteria:</strong> Close > Max(Previous 30 Days) <strong>AND</strong> 5 Consecutive Up-Close Days.
    </div>
    
    <table>
        <thead>
            <tr><th>Ticker</th><th>Price</th><th>Change %</th><th>Sector</th><th>Industry</th><th>Pattern</th></tr>
        </thead>
        <tbody>{w_rows}</tbody>
    </table>
    </body></html>"""

    file_path = "docs/wyckoff.html"
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"✅ Successfully wrote {file_path}")
    except Exception as e:
        print(f"❌ Failed to write {file_path}: {e}")

# ==========================================
# 6. MAIN EXECUTION
# ==========================================

def main():
    print("⏳ Starting Scanner...")
    # 1. Load Maps
    maps = {}; industries = {}
    for f in ["sp_cache.csv", "russell_cache.csv", "nasdaq_cache.csv", "NDQ_cache.csv", "AMEX_cache.csv", "NYSE_cache.csv"]:
        m, i = fetch_tickers_and_sectors_from_csv(f)
        maps.update(m); industries.update(i)
    
    sec_map, sec_ind = fetch_tickers_and_sectors_from_csv("sectors_cache.csv")

    # 2. DeMark Scans
    daily_res, daily_sec, _ = scan_timeframe(maps, industries, "1D", "1d")
    weekly_res, weekly_sec, _ = scan_timeframe(maps, industries, "1W", "1wk")
    scan_timeframe(sec_map, sec_ind, "Sector", "1d")

    # 3. Wyckoff Scan
    wyckoff_res = scan_wyckoff_signals(maps, industries)
    print(f"💪 Wyckoff Candidates found: {len(wyckoff_res)}")

    # 4. Aux
    fg_data = get_fear_and_greed()
    plot_trends(daily_sec, weekly_sec)
    
    # 5. Write Reports
    report_date = datetime.utcnow().strftime("%A, %b %d, %Y - %H:%M UTC")
    
    # Writes are now separated and guarded
    write_index_html(daily_res, weekly_res, fg_data, report_date)
    write_wyckoff_html(wyckoff_res, report_date)
    
    print("\n✅ All operations complete.")

if __name__ == "__main__":
    main()
