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
from collections import defaultdict


def fetch_tickers_and_sectors_from_csv(cache_file):
    mapping = {}
    industry_map = {}
    if os.path.exists(cache_file):
        with open(cache_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ticker = row.get('Ticker')
                sector = row.get('Sector')
                industry = row.get('Industry')
                if ticker:
                    mapping[ticker.strip()] = sector.strip() if sector else "Unknown"
                    industry_map[ticker.strip()] = industry.strip() if industry else "Unknown"
        print(f"✅ Loaded {len(mapping)} tickers & sectors from {cache_file}")
    else:
        print(f"❌ Cache file {cache_file} not found!")
    return mapping, industry_map



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



def load_or_fetch_price_data(tickers, interval, period, cache_key):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    cache_file = f"price_cache_{cache_key}_{today}.pkl"

    if os.path.exists(cache_file):
        print(f"📦 Using cached data: {cache_file}")
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    print(f"🌐 Fetching fresh data for {cache_key}...")
    all_data = {}
    batch_size = 50

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        t = Ticker(batch)
        batch_data = t.history(interval=interval, period=period)

        if isinstance(batch_data, pd.DataFrame):
            for ticker in batch:
                if (ticker,) in batch_data.index:
                    all_data[ticker] = batch_data.xs(ticker, level=0)
        else:
            print(f"⚠️ Unexpected format in batch {batch}: {type(batch_data)}")

        time.sleep(1.5)

    with open(cache_file, "wb") as f:
        pickle.dump(all_data, f)

    return all_data


def scan_timeframe(ticker_sector_map, ticker_industry_map, interval_label, interval):
    results = {"Tops": [], "Bottoms": []}
    sector_counts = {"Tops": defaultdict(int), "Bottoms": defaultdict(int)}
    tickers = list(ticker_sector_map.keys())
    print(f"\n🔍 Scanning {len(tickers)} tickers on {interval_label} timeframe...")

    period = '2y' if interval == '1wk' else '6mo'
    price_data = load_or_fetch_price_data(tickers, interval, period, interval_label)

    for ticker, df in price_data.items():
        try:
            if df.empty:
                continue

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            if interval == '1wk':
                last_date = df['date'].iloc[-1].date()
                today = datetime.utcnow().date()
                if last_date >= today - timedelta(days=today.weekday()):
                    df = df.iloc[:-1]

            DM9Top, DM13Top, DM9Bot, DM13Bot = compute_dm_signals(df)

            sector = ticker_sector_map.get(ticker, "Unknown")
            industry = ticker_industry_map.get(ticker, "Unknown")

            if DM9Top or DM13Top:
                signal = "DM13 Top" if DM13Top else "DM9 Top"
                results["Tops"].append((ticker, signal, industry))
                sector_counts["Tops"][sector] += 1

            if DM9Bot or DM13Bot:
                signal = "DM13 Bot" if DM13Bot else "DM9 Bot"
                results["Bottoms"].append((ticker, signal, industry))
                sector_counts["Bottoms"][sector] += 1

        except Exception as e:
            print(f"⚠️ Skipping {ticker} [{interval_label}] due to error: {e}")

    results["Tops"] = sorted(results["Tops"], key=lambda x: x[0])
    results["Bottoms"] = sorted(results["Bottoms"], key=lambda x: x[0])

    return results, sector_counts


def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://edition.cnn.com/",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        print("🔍 Raw Fear & Greed Data:", data)

        fg_data = data.get("fear_and_greed", {})
        fg_value = round(fg_data.get("score", 0))
        fg_previous = round(fg_data.get("previous_close", 0))
        timestamp = fg_data.get("timestamp")

        if isinstance(timestamp, str):
            try:
                date_obj = datetime.fromisoformat(timestamp)
            except ValueError:
                date_obj = datetime.strptime(timestamp.split("+")[0], "%Y-%m-%dT%H:%M:%S")
            date = date_obj.strftime("%Y-%m-%d")
        else:
            date = datetime.utcnow().strftime("%Y-%m-%d")

        # log to CSV
        with open("fear_and_greed_history.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["Date", "Index", "Previous Close"])
            writer.writerow([date, fg_value, fg_previous])

        return fg_value, fg_previous, date
    except Exception as e:
        print(f"⚠️ Error fetching Fear & Greed Index: {e}")
        return "N/A", "N/A", "N/A"


def count_signals_by_sector(daily_results, weekly_results, daily_sectors, weekly_sectors):
    sector_counts = defaultdict(int)

    for signal_list, sector_map in [
        (daily_results["Bottoms"], daily_sectors),
        (daily_results["Tops"], daily_sectors),
        (weekly_results["Bottoms"], weekly_sectors),
        (weekly_results["Tops"], weekly_sectors),
    ]:
        for ticker, _ in signal_list:
            sector = sector_map.get(ticker, "Unknown")
            sector_counts[sector] += 1

    return dict(sorted(sector_counts.items(), key=lambda x: x[1], reverse=True))


def plot_sector_trends(daily_sectors, weekly_sectors):
    from collections import defaultdict

    all_sectors = set(daily_sectors["Tops"].keys()) | set(daily_sectors["Bottoms"].keys()) | \
                  set(weekly_sectors["Tops"].keys()) | set(weekly_sectors["Bottoms"].keys())

    sectors = sorted(all_sectors)
    daily_counts = []
    weekly_counts = []

    for sector in sectors:
        daily_total = daily_sectors["Tops"].get(sector, 0) + daily_sectors["Bottoms"].get(sector, 0)
        weekly_total = weekly_sectors["Tops"].get(sector, 0) + weekly_sectors["Bottoms"].get(sector, 0)
        daily_counts.append(daily_total)
        weekly_counts.append(weekly_total)

    x = range(len(sectors))
    width = 0.35

    plt.figure(figsize=(14, 8))
    plt.barh([i - width/2 for i in x], daily_counts, height=width, label="Daily", color="lightcoral")
    plt.barh([i + width/2 for i in x], weekly_counts, height=width, label="Weekly", color="skyblue")
    plt.yticks(x, sectors)
    plt.xlabel("Number of Signals")
    plt.title("Sector Signal Trends: Daily vs Weekly")
    plt.legend()
    plt.tight_layout()

    os.makedirs("docs", exist_ok=True)
    plt.savefig("docs/sector_trends.png", bbox_inches="tight")
    if os.path.exists("docs/sector_trends.png"):
        print("✅ sector_trends.png exists.")
    else:
        print("❌ Failed to save sector_trends.png.")
    plt.close()
    

def sector_counts_to_html(title, sector_counts):
    if not sector_counts:
        return "<p>No sector data.</p>"

    html = f"<h3>{title}</h3><table><tr><th>Sector</th><th>Count</th></tr>"
    for sector, count in sorted(sector_counts.items(), key=lambda x: x[1], reverse=True):
        html += f"<tr><td>{sector}</td><td>{count}</td></tr>"
    html += "</table>"
    return html


def signals_to_html_table(signals):
    if not signals:
        return "<p>No signals.</p>"

    signals_sorted = sorted(signals, key=lambda x: x[0])
    html = "<table><tr><th>Ticker</th><th>Signal</th><th>Industry</th></tr>"
    for ticker, signal, industry in signals_sorted:
        if signal == "DM9 Top":
            style = "background-color: #f8d7da;"
        elif signal == "DM13 Top":
            style = "background-color: #f5c6cb; font-weight: bold;"
        elif signal == "DM9 Bot":
            style = "background-color: #d4edda;"
        elif signal == "DM13 Bot":
            style = "background-color: #c3e6cb; font-weight: bold;"
        else:
            style = ""
        html += f"<tr><td>{ticker}</td><td style='{style}'>{signal}</td><td>{industry}</td></tr>"
    html += "</table>"
    return html


def build_sector_signal_grid_html(sector_results):
    # Predefined display names in grid layout
    grid_labels = [
        ["Technology", "Financials", "Communications", "Cons. Discretionary", "Energy", "Real Estate"],
        ["Healthcare", "Regional Banks", "Industrials", "Cons. Staples", "Utilities", "Home Builders"],
        ["Materials", "Gold", "Silver", "Bitcoin", "Ethereum", "Memes"]
    ]

    # Build sector signal mapping
    sector_signals = {}
    for signal_type, entries in sector_results.items():
        for ticker, signal, industry in entries:
            sector = industry  # Assuming industry field in sectors_cache maps to grid label
            current = sector_signals.get(sector)
            if current is None or ("DM13" in signal and "DM9" in current):
                sector_signals[sector] = signal

    # Render HTML grid
    html = '<h2>Sector Signal Grid</h2><table class="signal-grid">'
    for row in grid_labels:
        html += "<tr>"
        for label in row:
            signal = sector_signals.get(label)
            if signal == "DM9 Top":
                style = "background-color: #f8d7da;"
            elif signal == "DM13 Top":
                style = "background-color: #f5c6cb; font-weight: bold;"
            elif signal == "DM9 Bot":
                style = "background-color: #d4edda;"
            elif signal == "DM13 Bot":
                style = "background-color: #c3e6cb; font-weight: bold;"
            else:
                style = "background-color: #f0f0f0;"
            html += f'<td style="{style}">{label}</td>'
        html += "</tr>"
    html += "</table>"
    return html


def write_html_report(daily_results, weekly_results, daily_sectors, weekly_sectors, fg_index, fg_prev, fg_date, total_tickers, sector_results):
    # Determine color for Fear & Greed index
    if fg_index != "N/A":
        fg_value = float(fg_index)
        if fg_value >= 60:
            fg_color = "#dc3545"  # Red (Greed)
        elif fg_value >= 30:
            fg_color = "#ffc107"  # Yellow (Neutral)
        else:
            fg_color = "#28a745"  # Green (Fear)
    else:
        fg_color = "#6c757d"  # Gray fallback

    # Ticker counts
  #  try:
  #      from ticker_cache import sp500_map, russell_map, nasdaq_map  # or however your maps are imported
  #      total_tickers = len(sp500_map) + len(russell_map) + len(nasdaq_map)
  #  except:
  #      total_tickers = 1  # Fallback to avoid ZeroDivisionError

    # Signal counts
    daily_bottoms = len(daily_results["Bottoms"])
    weekly_bottoms = len(weekly_results["Bottoms"])
    daily_tops = len(daily_results["Tops"])
    weekly_tops = len(weekly_results["Tops"])

    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>DeMark Signal Report</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
            }}
            h1 {{
                color: #333;
            }}
            .fg-box {{
                background-color: {fg_color};
                color: white;
                padding: 10px;
                margin-bottom: 20px;
                border-radius: 5px;
                display: inline-block;
            }}
            .summary-table {{
                border-collapse: collapse;
                margin: 20px 0;
                width: 60%;
            }}
            .summary-table th, .summary-table td {{
                border: 1px solid #ccc;
                padding: 6px 10px;
                text-align: center;
            }}
            .summary-table th {{
                background-color: #f0f0f0;
            }}
            .row {{
                display: flex;
                justify-content: space-between;
                margin-bottom: 30px;
            }}
            .column {{
                flex: 1;
                margin: 0 10px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 6px 8px;
                text-align: left;
            }}
            th {{
                background-color: #f0f0f0;
            }}
             .signal-grid {{
                border-collapse: collapse;
                margin-bottom: 30px;
            }}
            .signal-grid td {{
                border: 1px solid #ccc;
                padding: 12px 14px;
                text-align: center;
                min-width: 100px;
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <h1>🧭 DeMark Signal Report</h1>
        <div class="fg-box">
            <strong>CNN Fear & Greed Index:</strong> {fg_index} (Prev: {fg_prev}) on {fg_date}
        </div>

        <h2>Signal Summary</h2>
        <table class="summary-table">
            <tr>
                <th>Totals</th>
                <th>Daily</th>
                <th>Weekly</th>
            </tr>
            <tr>
                <td><strong>Bottoms</strong></td>
                <td>{daily_bottoms}</td>
                <td>{weekly_bottoms}</td>
            </tr>
            <tr>
                <td><strong>Tops</strong></td>
                <td>{daily_tops}</td>
                <td>{weekly_tops}</td>
            </tr>
        </table>
    """

    html += build_sector_signal_grid_html(sector_results)

    
    # Row 1: Bottoms
    html += f"""
    <div class="row">
        <div class="column">
            <h2>Daily Bottoms</h2>
            {signals_to_html_table(daily_results["Bottoms"])}
            {sector_counts_to_html("Daily Bottoms by Sector", daily_sectors["Bottoms"])}
        </div>
        <div class="column">
            <h2>Weekly Bottoms</h2>
            {signals_to_html_table(weekly_results["Bottoms"])}
            {sector_counts_to_html("Weekly Bottoms by Sector", weekly_sectors["Bottoms"])}
        </div>
    </div>
    """

    # Row 2: Tops
    html += f"""
    <div class="row">
        <div class="column">
            <h2>Daily Tops</h2>
            {signals_to_html_table(daily_results["Tops"])}
            {sector_counts_to_html("Daily Tops by Sector", daily_sectors["Tops"])}
        </div>
        <div class="column">
            <h2>Weekly Tops</h2>
            {signals_to_html_table(weekly_results["Tops"])}
            {sector_counts_to_html("Weekly Tops by Sector", weekly_sectors["Tops"])}
        </div>
    </div>
    """

    # Sector trend chart
    html += """
    <h2 style="margin-top: 40px;">Sector Signal Trends</h2>
    <img src="sector_trends.png" alt="Sector Trends" style="max-width: 100%;">
    </body>
    </html>
    """

    # Write HTML
    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)

def main():
    start_time = time.time()
    print("⏳ Starting DM Scanner")

    # Step 1: Load ticker-sector maps
    t0 = time.time()
    sp500_map, sp500_industry = fetch_tickers_and_sectors_from_csv("sp_cache.csv")
    russell_map, russell_industry = fetch_tickers_and_sectors_from_csv("russell_cache.csv")
    nasdaq_map, nasdaq_industry = fetch_tickers_and_sectors_from_csv("nasdaq_cache.csv")
    total_tickers = len(sp500_map) + len(russell_map) + len(nasdaq_map)

    all_map = {**sp500_map, **russell_map, **nasdaq_map}
    all_industry_map = {**sp500_industry, **russell_industry, **nasdaq_industry}
    print(f"📁 Loaded ticker maps in {time.time() - t0:.2f} seconds")

    # Step 1b: Load Sector ETF tickers
    sector_map, sector_industry = fetch_tickers_and_sectors_from_csv("sectors_cache.csv")
    sector_results, _ = scan_timeframe(sector_map, sector_industry, "Sector", "1d")

    # Step 2: Timestamp + Fear & Greed
    t1 = time.time()
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    fg_val, fg_prev, fg_date = get_fear_and_greed()
    print(f"📊 Retrieved Fear & Greed Index in {time.time() - t1:.2f} seconds")

    # Step 3: Daily signals
    t2 = time.time()
    daily_results, daily_sectors = scan_timeframe(all_map, all_industry_map, "1D", "1d")
    print(f"📉 Scanned Daily signals in {time.time() - t2:.2f} seconds")

    # Step 4: Weekly signals
    t3 = time.time()
    weekly_results, weekly_sectors = scan_timeframe(all_map, all_industry_map, "1W", "1wk")
    print(f"📈 Scanned Weekly signals in {time.time() - t3:.2f} seconds")

    # Step 5: Display results
    def print_section(title, signals):
        print(f"\n🔸 {title}\n" + "-" * 40)
        if signals:
            df = pd.DataFrame(signals, columns=["Ticker", "Signal", "Industry"])
            print(df.to_string(index=False))
        else:
            print("None")

    print(f"\n📋 DeMark Signals as of {now_str}\n" + "=" * 40)
    print_section("Daily Bottoms", daily_results["Bottoms"])
    print_section("Weekly Bottoms", weekly_results["Bottoms"])
    print_section("Daily Tops", daily_results["Tops"])
    print_section("Weekly Tops", weekly_results["Tops"])

    # Count signals by sector and plot chart
    plot_sector_trends(daily_sectors, weekly_sectors)

    # Step 6: HTML output
    t4 = time.time()
    write_html_report(daily_results, weekly_results, daily_sectors, weekly_sectors, fg_val, fg_prev, fg_date, total_tickers, sector_results)
    print(f"📝 HTML report written in {time.time() - t4:.2f} seconds")

    # Total runtime
    total_time = time.time() - start_time
    print(f"\n✅ Script completed in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()

