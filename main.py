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


# Ensure cache folder exists early
os.makedirs("cache", exist_ok=True)


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


def is_friday_after_close():
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    return now.weekday() == 4 and now.time() > datetime.strptime("16:30", "%H:%M").time()


def load_or_fetch_price_data(tickers, interval, period, cache_key):
    cache_dir = "cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"price_cache_{cache_key}.pkl")

    # Detect if today is Saturday or Sunday (UTC)
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

    print(f"💾 Saved fresh data to cache: {cache_file}")
    return all_data


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
            if df.empty:
                continue

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            last_close = float(df['close'].iloc[-1])

            if interval == '1wk':
                last_date = pd.to_datetime(df['date'].iloc[-1])
                if getattr(last_date, "tzinfo", None) is not None:
                    # make naive for consistent comparisons/formatting
                    try:
                        last_date = last_date.tz_convert(None)
                    except Exception:
                        last_date = last_date.tz_localize(None)

                # drop in-progress week if present
                today = datetime.utcnow()
                start_of_week = today - timedelta(days=today.weekday())  # Monday UTC
                if last_date >= start_of_week and len(df) > 1:
                    df = df.iloc[:-1]

                # set candle_date to last fully completed weekly bar
                if not candle_date and not df.empty:
                    candle_date = pd.to_datetime(df['date'].iloc[-1])
                    if getattr(candle_date, "tzinfo", None) is not None:
                        try:
                            candle_date = candle_date.tz_convert(None)
                        except Exception:
                            candle_date = candle_date.tz_localize(None)
                    candle_date = candle_date.strftime("%Y-%m-%d")

            else:
                # DAILY: set candle_date from the last completed daily bar
                if not candle_date and not df.empty:
                    last_date = pd.to_datetime(df['date'].iloc[-1])
                    if getattr(last_date, "tzinfo", None) is not None:
                        try:
                            last_date = last_date.tz_convert(None)
                        except Exception:
                            last_date = last_date.tz_localize(None)
                    candle_date = last_date.strftime("%Y-%m-%d")

            DM9Top, DM13Top, DM9Bot, DM13Bot = compute_dm_signals(df)
            sector = ticker_sector_map.get(ticker, "Unknown")
            if interval_label == "Sector":
                industry = ticker_sector_map.get(ticker, "Unknown")  # use Sector as Industry for sector ETFs
            else:
                industry = ticker_industry_map.get(ticker, "Unknown")

            if DM9Top or DM13Top:
                signal = "DM13 Top" if DM13Top else "DM9 Top"
                results["Tops"].append((ticker, last_close, signal, industry))
                sector_counts["Tops"][sector] += 1

            if DM9Bot or DM13Bot:
                signal = "DM13 Bot" if DM13Bot else "DM9 Bot"
                results["Bottoms"].append((ticker, last_close, signal, industry))
                sector_counts["Bottoms"][sector] += 1

        except Exception as e:
            print(f"⚠️ Skipping {ticker} [{interval_label}] due to error: {e}")

    results["Tops"] = sorted(results["Tops"], key=lambda x: x[0])
    results["Bottoms"] = sorted(results["Bottoms"], key=lambda x: x[0])

    if not candle_date:
        candle_date = datetime.utcnow().strftime("%Y-%m-%d")

    return results, sector_counts, candle_date


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

        # print("🔍 Raw Fear & Greed Data:", data)

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
        for ticker, *_ in signal_list:
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


def signals_to_html_table(signals, sortable=False):
    if not signals:
        return "<p>No signals.</p>"

    signals_sorted = sorted(signals, key=lambda x: x[0])

    # Add `sortable` class if requested
    table_class = "sortable" if sortable else ""
    html = f"<table class='{table_class}'>" if table_class else "<table>"

    html += "<tr><th>Ticker</th><th>Close Price</th><th>Signal</th><th>Industry</th></tr>"

    for ticker, close_price, signal, industry in signals_sorted:
        # Safe formatting of close price
        if isinstance(close_price, (int, float)):
            price_str = f"{close_price:.2f}"
        else:
            price_str = str(close_price) if close_price is not None else "N/A"

        if signal == "DM9 Top":
            style = "background-color: #ffb3b3;"
        elif signal == "DM13 Top":
            style = "background-color: #ff8080; font-weight: bold;"
        elif signal == "DM9 Bot":
            style = "background-color: #d4edda;"
        elif signal == "DM13 Bot":
            style = "background-color: #c3e6cb; font-weight: bold;"
        else:
            style = ""

        html += (
            f"<tr>"
            f"<td>{ticker}</td>"
            f"<td>{price_str}</td>"
            f"<td style='{style}'>{signal}</td>"
            f"<td>{industry}</td>"
            f"</tr>"
        )

    html += "</table>"
    return html


def build_sector_signal_grid_html(sector_results):
    grid_labels = [
        ["Technology", "Financials", "Communications", "Discretionary", "Real Estate", "Home Builders"],
        ["Biotechnology", "Regional Banks", "Healthcare", "Staples", "Energy", "Utilities"],
        ["Materials", "Industrials", "Gold", "Silver", "Bitcoin", "Ethereum"]
    ]

    # Flatten grid labels into a set for matching
    expected_labels = {label for row in grid_labels for label in row}
    sector_signals = {}

    for signal_type, entries in sector_results.items():
        for ticker, signal, sector, _ in entries:
            if sector in expected_labels:
                current = sector_signals.get(sector)
                if current is None or ("DM13" in signal and "DM9" in current):
                    sector_signals[sector] = signal

    # Render HTML grid with <div>
    html = '<h2>Sector Signal Grid</h2><div class="sector-grid">'
    for row in grid_labels:
        for label in row:
            signal = sector_signals.get(label)
            if signal == "DM9 Top":
                style = "background-color: #ffb3b3;"
            elif signal == "DM13 Top":
                style = "background-color: #ff8080; font-weight: bold;"
            elif signal == "DM9 Bot":
                style = "background-color: #d4edda;"
            elif signal == "DM13 Bot":
                style = "background-color: #c3e6cb; font-weight: bold;"
            else:
                style = "background-color: #f0f0f0;"
            html += f'<div class="sector-cell" style="{style}">{label}</div>'
    html += "</div>"
    return html


def plot_fear_greed_trend(csv_path="fear_and_greed_history.csv",
                          out_path="docs/fg_trend.png",
                          lookback_days=120):
    try:
        if not os.path.exists(csv_path):
            return None

        df = pd.read_csv(csv_path)
        if df.empty or "Date" not in df.columns or "Index" not in df.columns:
            return None

        # Make Date UTC-aware for safe comparisons
        df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
        df = df.dropna(subset=["Date", "Index"]).sort_values("Date")

        # Use UTC-aware cutoff
        cutoff = pd.Timestamp.now(tz=pytz.UTC) - pd.Timedelta(days=lookback_days)
        df = df[df["Date"] >= cutoff]
        if df.empty:
            return None

        # For plotting, remove tz info (matplotlib is happier with naive)
        df_plot = df.copy()
        df_plot["Date"] = df_plot["Date"].dt.tz_localize(None)

        plt.figure(figsize=(9, 4.4))
        plt.plot(df_plot["Date"], df_plot["Index"])
        plt.title("CNN Fear & Greed (last ~120 days)")
        plt.xlabel("Date")
        plt.ylabel("Index")
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        plt.gcf().autofmt_xdate(rotation=45)
        plt.tight_layout()
        os.makedirs("docs", exist_ok=True)
        plt.savefig(out_path, bbox_inches="tight")
        plt.close()

        return out_path if os.path.exists(out_path) else None
    except Exception as e:
        print(f"⚠️ Could not plot Fear & Greed trend: {e}")
        return None


def write_html_report(daily_results, weekly_results, daily_sectors, weekly_sectors,
                      fg_index, fg_prev, fg_date, total_tickers, sector_results,
                      weekly_date, fg_plot_path=None, report_date_str=None, stale_threshold_seconds=3600):
    
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

    # Signal counts
    daily_bottoms = len(daily_results["Bottoms"])
    weekly_bottoms = len(weekly_results["Bottoms"])
    daily_tops = len(daily_results["Tops"])
    weekly_tops = len(weekly_results["Tops"])
                          
    # Get current timestamp for staleness checking
    import time
    page_load_time = int(time.time())

    # Opening HTML + CSS
    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>US DM Dashboard</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
            }}
            h1 {{
                color: #333;
                display: flex;
                align-items: baseline;
                gap: 12px;
            }}
            .date-subtitle {{
                margin-top: 6px;
                font-size: 0.95em;
                color: #333;
                margin-bottom: 12px;
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
                width: 100%; /* full width on mobile by default */
            }}
            .summary-table th,
            .summary-table td {{
                border: 1px solid #ccc;
                padding: 6px 10px;
                text-align: center;
            }}
            .summary-table th {{
                background-color: #f0f0f0;
            }}
            .row {{
                display: flex;
                flex-direction: column;  /* default mobile = stacked */
                margin-bottom: 30px;
            }}
            .column {{
                flex: 1;
                margin: 10px 0;
                width: 100%;
            }}
            .column table {{
                width: 100% !important;
                max-width: 100%;
                box-sizing: border-box;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
                font-size: 1.1em;
                display: block;
                white-space: normal;
            }}
            table tbody {{
                display: table;
                width: 100%;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 6px 6px;       /* tighter cells for mobile */
                text-align: left;
            }}
            th {{
                background-color: #f0f0f0;
            }}
            table, th, td {{
                font-size: 1.1em;
                line-height: 1.5;
                -webkit-text-size-adjust: 100%;
            }}
            .sector-grid {{
                display: flex;
                flex-wrap: wrap;
                margin-bottom: 30px;
                width: 100%;
            }}
            .sector-cell {{
                border: 1px solid #ccc;
                padding: 12px 14px;
                text-align: center;
                word-wrap: break-word;
                font-weight: bold;
                flex: 0 0 33.33%;   /* 3 columns on mobile/tablet */
                box-sizing: border-box;
            }}
            .sortable th {{
                background-color: #f0f0f0;
                cursor: pointer;
                color: #007bff;
                text-decoration: underline;
            }}
            .sortable th:hover {{
                color: #0056b3;
            }}
            .sortable th.asc::after {{
                content: " ▲";
                font-size: 0.9em;
                color: #333;
            }}
            .sortable th.desc::after {{
                content: " ▼";
                font-size: 0.9em;
                color: #333;
            }}
            /* Desktop overrides for larger screens */
            @media (min-width: 64em) {{   /* ~1024px if base font size = 16px */
                .row {{
                    flex-direction: row;   /* side-by-side columns */
                }}
                .column {{
                    margin: 0 10px;
                }}
                table, th, td {{
                    font-size: 1.1em;        /* normal size */
                    line-height: 1.5;
                    white-space: normal;
                }}
                .summary-table {{
                    width: 60%;            /* narrower summary table */
                }}
                .sector-cell {{
                    flex: 0 0 16.66%;      /* 6 columns on wide screens */
                }}
            }}
        </style>
    </head>
    <body>
        <div class="refresh-indicator" id="refreshTimer">Page loaded at <span id="loadTime"></span></div>
        <h1>📈 US DM Dashboard 📉 </h1>
        {f'<div class="date-subtitle">{report_date_str}</div>' if report_date_str else ''}
        <div class="fg-box">
            <strong>CNN Fear & Greed Index:</strong> {fg_index} (Prev: {fg_prev}) on {fg_date}
        </div>
        {f'<img src="fg_trend.png" alt="Fear & Greed Trend" style="max-width: 480px; display:block; margin:6px 0 16px 0;">' if fg_plot_path else ''}

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

    # Sector grid
    html += build_sector_signal_grid_html(sector_results)

    # Bottoms section
    html += f"""
    <div class="row">
        <div class="column">
            <h2>Daily Bottoms</h2>
            {signals_to_html_table(daily_results["Bottoms"], sortable=True)}
            {sector_counts_to_html("Daily Bottoms by Sector", daily_sectors["Bottoms"])}
        </div>
        <div class="column">
            <h2>Weekly Bottoms</h2>
            {signals_to_html_table(weekly_results["Bottoms"], sortable=True)}
            <p><em>Weekly signals last updated on {weekly_date}</em></p>
            {sector_counts_to_html("Weekly Bottoms by Sector", weekly_sectors["Bottoms"])}
        </div>
    </div>
    """

    # Tops section
    html += f"""
    <div class="row">
        <div class="column">
            <h2>Daily Tops</h2>
            {signals_to_html_table(daily_results["Tops"], sortable=True)}
            {sector_counts_to_html("Daily Tops by Sector", daily_sectors["Tops"])}
        </div>
        <div class="column">
            <h2>Weekly Tops</h2>
            {signals_to_html_table(weekly_results["Tops"], sortable=True)}
            <p><em>Weekly signals last updated on {weekly_date}</em></p>
            {sector_counts_to_html("Weekly Tops by Sector", weekly_sectors["Tops"])}
        </div>
    </div>
    """

    # JavaScript (plain string, no f!)
    html += """
    <script>
    (function () {
        document.querySelectorAll("table.sortable").forEach(table => {
          const headers = table.querySelectorAll("th");
          headers.forEach((header, i) => {
            header.addEventListener("click", () => {
              const tbody = table.tBodies[0] || table;
              // only grab rows after the header row
              const rows = Array.from(tbody.querySelectorAll("tr:nth-child(n+2)"));

              const wasAsc = header.classList.contains("asc");
              const wasDesc = header.classList.contains("desc");
              const asc = wasDesc || (!wasAsc && !wasDesc);

              headers.forEach(h => h.classList.remove("asc", "desc"));
              header.classList.add(asc ? "asc" : "desc");

              rows.sort((a, b) => {
                const aText = a.cells[i]?.innerText.trim() ?? "";
                const bText = b.cells[i]?.innerText.trim() ?? "";
                const aNum = parseFloat(aText.replace(/[^0-9.\-]/g, ""));
                const bNum = parseFloat(bText.replace(/[^0-9.\-]/g, ""));
                if (!Number.isNaN(aNum) && !Number.isNaN(bNum)) {
                  return asc ? (aNum - bNum) : (bNum - aNum);
                }
                return asc
                  ? aText.localeCompare(bText, undefined, { numeric: true, sensitivity: "base" })
                  : bText.localeCompare(aText, undefined, { numeric: true, sensitivity: "base" });
              });

              rows.forEach(r => tbody.appendChild(r));
            });
          });
        });
    })();
    </script>
    """

    # Sector trends
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
    NDQ_map, NDQ_industry = fetch_tickers_and_sectors_from_csv("NDQ_cache.csv")
    AMEX_map, AMEX_industry = fetch_tickers_and_sectors_from_csv("AMEX_cache.csv")
    NYSE_map, NYSE_industry = fetch_tickers_and_sectors_from_csv("NYSE_cache.csv")
    total_tickers = (
        len(sp500_map) 
        + len(russell_map) 
        + len(nasdaq_map) 
        + len(NDQ_map) 
        + len(AMEX_map) 
        + len(NYSE_map)
    )

    all_map = {**sp500_map, **russell_map, **nasdaq_map, **NDQ_map, **AMEX_map, **NYSE_map}
    all_industry_map = {**sp500_industry, **russell_industry, **nasdaq_industry, **NDQ_industry, **AMEX_industry, **NYSE_industry}
    print(f"📁 Loaded ticker maps in {time.time() - t0:.2f} seconds")

    # Step 1b: Load Sector ETF tickers
    sector_map, sector_industry = fetch_tickers_and_sectors_from_csv("sectors_cache.csv")
    sector_results, _, _ = scan_timeframe(sector_map, sector_industry, "Sector", "1d")

    # 🛠️ DEBUG: Show tickers and signals detected in sector scan
    print("\n🔍 Sector Signal Results:")
    for s in sector_results["Bottoms"]:
        print(f"✅ Bottom: {s}")
    for s in sector_results["Tops"]:
        print(f"🔺 Top:    {s}")

    # Step 2: Timestamp + Fear & Greed
    t1 = time.time()
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    fg_val, fg_prev, fg_date = get_fear_and_greed()
    fg_plot_path = plot_fear_greed_trend()
    print(f"📊 Retrieved Fear & Greed Index in {time.time() - t1:.2f} seconds")

    # Step 3: Daily signals
    t2 = time.time()
    daily_results, daily_sectors, daily_date = scan_timeframe(all_map, all_industry_map, "1D", "1d")
    print(f"📉 Scanned Daily signals in {time.time() - t2:.2f} seconds")

    # Step 4: Weekly signals
    t3 = time.time()
    # WEEKLY_CACHE_FILE = "cache/weekly_dm_cache.pkl"
    # if is_friday_after_close() or not os.path.exists(WEEKLY_CACHE_FILE):
    weekly_results, weekly_sectors, weekly_date = scan_timeframe(all_map, all_industry_map, "1W", "1wk")
        # with open(WEEKLY_CACHE_FILE, "wb") as f:
            # pickle.dump((weekly_results, weekly_sectors, weekly_date), f)
    # else:
        # with open(WEEKLY_CACHE_FILE, "rb") as f:
            # weekly_results, weekly_sectors, weekly_date = pickle.load(f)
    print(f"📈 Scanned Weekly signals in {time.time() - t3:.2f} seconds")

    daily_dt = datetime.strptime(daily_date, "%Y-%m-%d")
    report_date_str = f"Signals triggered on {daily_dt.strftime('%A, %b %d, %Y')} (as of NY market close)"
    
    # Step 5: Display results
    def print_section(title, signals):
        print(f"\n🔸 {title}\n" + "-" * 40)
        if signals:
            df = pd.DataFrame(signals, columns=["Ticker", "Last Close", "Signal", "Industry"])
            df = df[["Ticker", "Last Close", "Signal", "Industry"]]
            print(df.to_string(index=False))
        else:
            print("None")

    print(f"\n📋 DeMark Signals as of {now_str}\n" + "=" * 40)
    print_section("Daily Bottoms", daily_results["Bottoms"])
    print_section("Weekly Bottoms", weekly_results["Bottoms"])
    print_section("Daily Tops", daily_results["Tops"])
    print_section("Weekly Tops", weekly_results["Tops"])

    # 🔎 Print sector summary explicitly
    print_section("Sector Bottoms", sector_results["Bottoms"])
    print_section("Sector Tops", sector_results["Tops"])

    # Count signals by sector and plot chart
    plot_sector_trends(daily_sectors, weekly_sectors)

    # Step 6: HTML output
    t4 = time.time()
    write_html_report(
        daily_results, weekly_results, daily_sectors, weekly_sectors, fg_val, fg_prev, fg_date, total_tickers, sector_results, weekly_date,
        fg_plot_path=fg_plot_path,
        report_date_str = report_date_str
    )
    print(f"📝 HTML report written in {time.time() - t4:.2f} seconds")

    # Total runtime
    total_time = time.time() - start_time
    print(f"\n✅ Script completed in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()

