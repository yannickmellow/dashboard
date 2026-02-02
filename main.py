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
from collections import defaultdict
import pytz

# ==========================================
# 1. CONFIG & UTILS
# ==========================================

# Ensure folders exist
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
        print(f"✅ Loaded {len(mapping)} tickers from {cache_file}")
    else:
        print(f"❌ Cache file {cache_file} not found!")
    return mapping, industry_map

def is_friday_after_close():
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    return now.weekday() == 4 and now.time() > datetime.strptime("16:30", "%H:%M").time()

def load_or_fetch_price_data(tickers, interval, period, cache_key):
    cache_file = os.path.join("cache", f"price_cache_{cache_key}.pkl")

    # Detect if today is Saturday or Sunday (UTC)
    weekday = datetime.utcnow().weekday()
    is_weekend = weekday >= 5

    if is_weekend and os.path.exists(cache_file):
        print(f"📦 [Weekend] Using cached data: {cache_file}")
        with open(cache_file, "rb") as f:
            return pickle.load(f)
            
    # If file exists and is recent (optional optimization), you could load it here too
    # But for now, we follow your logic of fetching fresh if not weekend.

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
            else:
                print(f"⚠️ Unexpected format in batch {batch}: {type(batch_data)}")
        except Exception as e:
             print(f"⚠️ Error fetching batch starting {batch[0]}: {e}")
        
        # Small sleep to be nice to API
        time.sleep(0.5)

    with open(cache_file, "wb") as f:
        pickle.dump(all_data, f)

    print(f"💾 Saved fresh data to cache: {cache_file}")
    return all_data

# ==========================================
# 2. LOGIC: DEMARK & WYCKOFF
# ==========================================

def compute_dm_signals(df):
    close = df["close"].values
    length = len(close)
    if length < 20:
        return False, False, False, False

    TD = [0] * length
    TDUp = [0] * length
    TS = [0] * length
    TDDn = [0] * length

    for i in range(4, length):
        TD[i] = TD[i - 1] + 1 if close[i] > close[i - 4] else 0
        TS[i] = TS[i - 1] + 1 if close[i] < close[i - 4] else 0

    def valuewhen_reset(arr, idx):
        for j in range(idx - 1, 0, -1):
            if arr[j] < arr[j - 1]:
                return arr[j]
        return 0

    for i in range(4, length):
        TDUp[i] = TD[i] - valuewhen_reset(TD, i)
        TDDn[i] = TS[i] - valuewhen_reset(TS, i)

    DM9Top = TDUp[-1] == 9
    DM13Top = TDUp[-1] == 13
    DM9Bot = TDDn[-1] == 9
    DM13Bot = TDDn[-1] == 13

    return DM9Top, DM13Top, DM9Bot, DM13Bot

def compute_wyckoff_signals(df):
    """
    Logic:
    1. Close > 1 day ago max(30, close)
    2. Count UP (5, close) > 4  (Implies 5 consecutive up days)
    """
    if len(df) < 35:
        return False
    
    close = df['close']
    
    # 1. Breakout Check: Current close > Max of previous 30 days (excluding today)
    # .iloc[-31:-1] grabs the window from 30 days ago up to yesterday
    prev_30_max = close.iloc[-31:-1].max()
    current_close = close.iloc[-1]
    is_breakout = current_close > prev_30_max
    
    # 2. Momentum Check: 5 consecutive up days
    # diff() > 0 checks if today > yesterday
    up_days = (close.diff() > 0).astype(int)
    # Sum the last 5 days flags. If sum > 4 (i.e., 5), it's a streak.
    last_5_count = up_days.iloc[-5:].sum()
    is_trending = last_5_count > 4
    
    return is_breakout and is_trending

# ==========================================
# 3. SCANNERS
# ==========================================

def scan_timeframe(ticker_sector_map, ticker_industry_map, interval_label, interval):
    results = {"Tops": [], "Bottoms": []}
    sector_counts = {"Tops": defaultdict(int), "Bottoms": defaultdict(int)}
    tickers = list(ticker_sector_map.keys())
    print(f"\n🔍 Scanning {len(tickers)} tickers on {interval_label} timeframe...")

    period = '2y' if interval == '1wk' else '6mo'
    price_data = load_or_fetch_price_data(tickers, interval, period, interval_label)

    candle_date = None
    
    for ticker, df in price_data.items():
        try:
            if df.empty: continue

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            last_close = float(df['close'].iloc[-1])

            # Date Handling
            if not candle_date:
                last_date = pd.to_datetime(df['date'].iloc[-1])
                if getattr(last_date, "tzinfo", None) is not None:
                     last_date = last_date.tz_localize(None)
                candle_date = last_date.strftime("%Y-%m-%d")

            # Logic
            DM9Top, DM13Top, DM9Bot, DM13Bot = compute_dm_signals(df)
            
            sector = ticker_sector_map.get(ticker, "Unknown")
            industry = ticker_sector_map.get(ticker, "Unknown") if interval_label == "Sector" else ticker_industry_map.get(ticker, "Unknown")

            if DM9Top or DM13Top:
                signal = "DM13 Top" if DM13Top else "DM9 Top"
                results["Tops"].append((ticker, last_close, signal, industry))
                sector_counts["Tops"][sector] += 1

            if DM9Bot or DM13Bot:
                signal = "DM13 Bot" if DM13Bot else "DM9 Bot"
                results["Bottoms"].append((ticker, last_close, signal, industry))
                sector_counts["Bottoms"][sector] += 1

        except Exception:
            pass

    results["Tops"] = sorted(results["Tops"], key=lambda x: x[0])
    results["Bottoms"] = sorted(results["Bottoms"], key=lambda x: x[0])

    if not candle_date:
        candle_date = datetime.utcnow().strftime("%Y-%m-%d")

    return results, sector_counts, candle_date

def scan_wyckoff_signals(ticker_sector_map, ticker_industry_map):
    """
    Scans specifically for the Wyckoff criteria using the 1D cache.
    """
    print(f"\n💪 Scanning for Wyckoff SOS (Breakout + Momentum)...")
    
    # Reuse the 1D cache explicitly
    cache_file = os.path.join("cache", "price_cache_1D.pkl")
    if not os.path.exists(cache_file):
        print("⚠️ No Daily cache found. Run DeMark scan first.")
        return [], "N/A"
        
    with open(cache_file, "rb") as f:
        price_data = pickle.load(f)
        
    results = []
    
    for ticker, df in price_data.items():
        if ticker not in ticker_sector_map: continue # Only scan known tickers
        
        try:
            if df.empty: continue
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            
            if compute_wyckoff_signals(df):
                last_close = float(df['close'].iloc[-1])
                sector = ticker_sector_map.get(ticker, "Unknown")
                industry = ticker_industry_map.get(ticker, "Unknown")
                
                # Calculate % move today for context
                pct_change = ((df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2]) * 100
                
                results.append((ticker, last_close, sector, industry, pct_change))
                
        except Exception:
            continue
            
    # Sort by Sector then Ticker
    results = sorted(results, key=lambda x: (x[2], x[0]))
    return results

# ==========================================
# 4. FEAR & GREED / PLOTTING
# ==========================================

def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Referer": "https://edition.cnn.com/",
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        fg_data = data.get("fear_and_greed", {})
        fg_value = round(fg_data.get("score", 0))
        fg_previous = round(fg_data.get("previous_close", 0))
        timestamp = fg_data.get("timestamp")
        
        if isinstance(timestamp, str):
            date_obj = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            date = date_obj.strftime("%Y-%m-%d")
        else:
            date = datetime.utcnow().strftime("%Y-%m-%d")

        with open("fear_and_greed_history.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0: writer.writerow(["Date", "Index", "Previous Close"])
            writer.writerow([date, fg_value, fg_previous])

        return fg_value, fg_previous, date
    except Exception as e:
        print(f"⚠️ Error fetching Fear & Greed Index: {e}")
        return "N/A", "N/A", "N/A"

def plot_fear_greed_trend(csv_path="fear_and_greed_history.csv", out_path="docs/fg_trend.png"):
    try:
        if not os.path.exists(csv_path): return None
        df = pd.read_csv(csv_path)
        if df.empty: return None
        
        df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
        df = df.dropna().sort_values("Date")
        cutoff = pd.Timestamp.now(tz=pytz.UTC) - pd.Timedelta(days=120)
        df = df[df["Date"] >= cutoff]
        
        if df.empty: return None
        
        df["Date"] = df["Date"].dt.tz_localize(None) # Make naive for matplotlib

        plt.figure(figsize=(9, 4.4))
        plt.plot(df["Date"], df["Index"], color="#333", linewidth=2)
        plt.title("CNN Fear & Greed (Last 120 Days)")
        plt.grid(True, alpha=0.3)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        plt.gcf().autofmt_xdate()
        plt.tight_layout()
        plt.savefig(out_path, bbox_inches="tight")
        plt.close()
        return out_path
    except Exception:
        return None

def plot_sector_trends(daily_sectors, weekly_sectors):
    all_sectors = set(daily_sectors["Tops"].keys()) | set(daily_sectors["Bottoms"].keys()) | \
                  set(weekly_sectors["Tops"].keys()) | set(weekly_sectors["Bottoms"].keys())
    sectors = sorted(all_sectors)
    daily_counts = [daily_sectors["Tops"].get(s, 0) + daily_sectors["Bottoms"].get(s, 0) for s in sectors]
    weekly_counts = [weekly_sectors["Tops"].get(s, 0) + weekly_sectors["Bottoms"].get(s, 0) for s in sectors]

    x = range(len(sectors))
    width = 0.35

    plt.figure(figsize=(14, 8))
    plt.barh([i - width/2 for i in x], daily_counts, height=width, label="Daily Signals", color="lightcoral")
    plt.barh([i + width/2 for i in x], weekly_counts, height=width, label="Weekly Signals", color="skyblue")
    plt.yticks(x, sectors)
    plt.xlabel("Number of Signals")
    plt.title("Sector Signal Trends")
    plt.legend()
    plt.tight_layout()
    plt.savefig("docs/sector_trends.png", bbox_inches="tight")
    plt.close()

# ==========================================
# 5. HTML GENERATION
# ==========================================

def get_common_css():
    return """
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f9f9f9; }
        h1, h2, h3 { color: #2c3e50; }
        .nav-bar { background-color: #34495e; padding: 15px; border-radius: 8px; margin-bottom: 25px; display: flex; gap: 20px; }
        .nav-bar a { color: white; text-decoration: none; font-weight: bold; font-size: 1.1em; padding: 5px 10px; border-radius: 4px; transition: background 0.3s;}
        .nav-bar a:hover, .nav-bar a.active { background-color: #1abc9c; }
        
        table { width: 100%; border-collapse: collapse; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 30px; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #ecf0f1; cursor: pointer; }
        tr:hover { background-color: #f1f1f1; }
        
        .tag { padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 0.9em; display: inline-block; }
        .tag-top { background-color: #ffcccc; color: #a94442; }
        .tag-bot { background-color: #d0e9c6; color: #3c763d; }
        .tag-wyckoff { background-color: #d9edf7; color: #31708f; border: 1px solid #bce8f1; }
        
        .grid-container { display: flex; flex-wrap: wrap; gap: 20px; }
        .col { flex: 1; min-width: 300px; }
        
        /* Sortable Arrows */
        th.asc::after { content: " ▲"; color: #777; font-size: 0.8em;}
        th.desc::after { content: " ▼"; color: #777; font-size: 0.8em;}
    </style>
    """

def get_nav_bar(active_page):
    return f"""
    <div class="nav-bar">
        <a href="index.html" class="{'active' if active_page == 'index' else ''}">DeMark Dashboard</a>
        <a href="wyckoff.html" class="{'active' if active_page == 'wyckoff' else ''}">Wyckoff Scans (SOS)</a>
    </div>
    """

def get_js_sort():
    return """
    <script>
    document.querySelectorAll("table").forEach(table => {
        table.querySelectorAll("th").forEach((header, i) => {
            header.addEventListener("click", () => {
                const tbody = table.querySelector("tbody") || table;
                const rows = Array.from(tbody.querySelectorAll("tr:nth-child(n+2)"));
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
    </script>
    """

def write_demark_html(daily, weekly, daily_sec, weekly_sec, fg_data, sector_res, report_date):
    fg_val, fg_prev, fg_date = fg_data
    
    # HTML Construction
    html = f"""<html><head><title>DeMark Dashboard</title>{get_common_css()}</head><body>
    {get_nav_bar('index')}
    
    <h1>📉 DeMark Signals Dashboard</h1>
    <p>{report_date}</p>
    
    <div style="background: #fff; padding: 15px; border-left: 5px solid #333; margin-bottom: 20px;">
        <strong>Fear & Greed:</strong> <span style="font-size: 1.2em; font-weight:bold;">{fg_val}</span> (Prev: {fg_prev})
    </div>
    <img src="fg_trend.png" style="max-width: 100%; height: auto; margin-bottom: 20px;">
    
    <h2>Daily Signals</h2>
    <div class="grid-container">
        <div class="col">
            <h3>Bottoms (Buy)</h3>
            {generate_table(daily["Bottoms"], "bot")}
        </div>
        <div class="col">
            <h3>Tops (Sell)</h3>
            {generate_table(daily["Tops"], "top")}
        </div>
    </div>
    
    <h2>Weekly Signals</h2>
    <div class="grid-container">
        <div class="col">
            <h3>Bottoms (Buy)</h3>
            {generate_table(weekly["Bottoms"], "bot")}
        </div>
        <div class="col">
            <h3>Tops (Sell)</h3>
            {generate_table(weekly["Tops"], "top")}
        </div>
    </div>

    <h2>Sector Trend Chart</h2>
    <img src="sector_trends.png" style="max-width: 100%;">

    {get_js_sort()}
    </body></html>"""
    
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)

def write_wyckoff_html(wyckoff_results, report_date):
    
    # Generate Table Rows
    rows_html = ""
    for t, price, sec, ind, pct in wyckoff_results:
        link = f"https://www.tradingview.com/chart/?symbol={t}"
        rows_html += f"""
        <tr>
            <td><a href="{link}" target="_blank" style="text-decoration:none; color:#2980b9; font-weight:bold;">{t}</a></td>
            <td>{price:.2f}</td>
            <td style="color: {'green' if pct > 0 else 'red'}">{pct:+.2f}%</td>
            <td>{sec}</td>
            <td>{ind}</td>
            <td><span class="tag tag-wyckoff">Breakout + Streak</span></td>
        </tr>
        """
        
    if not rows_html:
        rows_html = "<tr><td colspan='6'>No Wyckoff candidates found today.</td></tr>"

    html = f"""<html><head><title>Wyckoff Screener</title>{get_common_css()}</head><body>
    {get_nav_bar('wyckoff')}
    
    <h1>💪 Wyckoff "Sign of Strength" Screener</h1>
    <p>{report_date}</p>
    
    <div style="background: #d9edf7; padding: 15px; border: 1px solid #bce8f1; color: #31708f; border-radius: 4px; margin-bottom: 20px;">
        <strong>Criteria:</strong> Close > Max(Previous 30 Days) <strong>AND</strong> 5 Consecutive Up-Close Days.
        <br><em>This indicates a breakout from a range with aggressive momentum.</em>
    </div>
    
    <table>
        <thead>
            <tr>
                <th>Ticker</th>
                <th>Price</th>
                <th>Change %</th>
                <th>Sector</th>
                <th>Industry</th>
                <th>Pattern</th>
            </tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>
    
    {get_js_sort()}
    </body></html>"""
    
    with open("docs/wyckoff.html", "w", encoding="utf-8") as f:
        f.write(html)

def generate_table(signals, type_tag):
    if not signals: return "<p>None found.</p>"
    
    rows = ""
    for t, price, sig, ind in signals:
        css_class = "tag-bot" if type_tag == "bot" else "tag-top"
        rows += f"<tr><td><b>{t}</b></td><td>{price}</td><td><span class='tag {css_class}'>{sig}</span></td><td>{ind}</td></tr>"
    
    return f"<table><thead><tr><th>Ticker</th><th>Price</th><th>Signal</th><th>Industry</th></tr></thead><tbody>{rows}</tbody></table>"

# ==========================================
# 6. MAIN EXECUTION
# ==========================================

def main():
    start_time = time.time()
    print("⏳ Starting Dashboard Generator")

    # 1. Load Maps
    maps = {}
    industries = {}
    for f in ["sp_cache.csv", "russell_cache.csv", "nasdaq_cache.csv", "NDQ_cache.csv", "AMEX_cache.csv", "NYSE_cache.csv"]:
        m, i = fetch_tickers_and_sectors_from_csv(f)
        maps.update(m)
        industries.update(i)
        
    # Sector ETFs
    sec_map, sec_ind = fetch_tickers_and_sectors_from_csv("sectors_cache.csv")

    # 2. Fetch Data & Run DeMark
    print("\n--- Running DeMark Scans ---")
    
    # Daily
    daily_res, daily_sec_counts, daily_date = scan_timeframe(maps, industries, "1D", "1d")
    
    # Weekly
    weekly_res, weekly_sec_counts, weekly_date = scan_timeframe(maps, industries, "1W", "1wk")
    
    # Sector ETFs
    sec_res, _, _ = scan_timeframe(sec_map, sec_ind, "Sector", "1d")

    # 3. Run Wyckoff Scan (Uses the cache generated by Daily DeMark scan)
    print("\n--- Running Wyckoff Scans ---")
    wyckoff_res = scan_wyckoff_signals(maps, industries)
    print(f"Found {len(wyckoff_res)} Wyckoff candidates.")

    # 4. Aux Data (Fear Greed)
    fg_data = get_fear_and_greed()
    plot_fear_greed_trend()
    plot_sector_trends(daily_sec_counts, weekly_sec_counts)

    # 5. Generate Reports
    print("\n--- Generating HTML ---")
    report_date = f"Report generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    
    write_demark_html(daily_res, weekly_res, daily_sec_counts, weekly_sec_counts, fg_data, sec_res, report_date)
    write_wyckoff_html(wyckoff_res, report_date)

    print(f"\n✅ All Done! Open docs/index.html or docs/wyckoff.html")
    print(f"Total time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()
