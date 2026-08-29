import pandas as pd
from datetime import datetime, timedelta
import os
import pickle
import json
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
os.makedirs("data", exist_ok=True)

def get_market_hours_banner():
    """Next NYSE regular-session open/close (9:30am-4:00pm ET, weekdays),
    converted to Adelaide local time. Label auto-switches ACST/ACDT since
    South Australia observes daylight saving and the US doesn't share the
    same DST calendar."""
    ny_tz = pytz.timezone("America/New_York")
    adl_tz = pytz.timezone("Australia/Adelaide")
    now_ny = datetime.now(ny_tz)
    open_ny = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    close_ny = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
    if now_ny > close_ny:
        open_ny += timedelta(days=1)
        close_ny += timedelta(days=1)
    while open_ny.weekday() >= 5:  # skip weekends (Sat=5, Sun=6)
        open_ny += timedelta(days=1)
        close_ny += timedelta(days=1)
    open_adl, close_adl = open_ny.astimezone(adl_tz), close_ny.astimezone(adl_tz)
    fmt = lambda dt: dt.strftime("%I:%M%p").lstrip("0").lower()
    return f"Market open {fmt(open_adl)}\u2013{fmt(close_adl)} {open_adl.strftime('%Z')}"

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

    # Require a reasonable fraction of the universe to have actually come
    # back before trusting this fetch enough to overwrite the cache. A
    # totally empty result was already guarded against; this also catches a
    # PARTIALLY failed fetch (e.g. many batches erroring out) that would
    # otherwise silently commit an incomplete snapshot as if it were a
    # complete, fresh update.
    if len(all_data) < 0.5 * len(tickers):
        # Live fetch returned too little (network issue, rate limit, partial
        # outage, etc.) — don't clobber a good cache with an incomplete one;
        # fall back to whatever's cached.
        if os.path.exists(cache_file):
            with open(cache_file, "rb") as f: return pickle.load(f)
        return all_data

    with open(cache_file, "wb") as f: pickle.dump(all_data, f)
    return all_data

def _drop_incomplete_bar(df):
    """Drop the most recent bar if it's still in-progress. Yahoo returns a
    live-updating snapshot for the current week/month before it actually
    closes (identifiable by an intraday timestamp, e.g. '2026-08-27
    16:00:03-04:00', vs the clean calendar dates on real closed bars) --
    reading it as-is means today's still-forming candle can spuriously
    register as a confirmed DM9/13 signal that might not survive to the
    real close. Assumes the script runs daily. Every reader of weekly/
    monthly price data must apply this -- previously only scan_timeframe()
    did, while build_confluence_scores() (which powers the Home page's Top
    Setups) read the raw cache directly and could report a signal as
    'today' that hadn't actually confirmed yet.

    Weekend exception: markets are closed on Sat/Sun, so nothing can
    genuinely be "still forming" then -- the most recent bar is necessarily
    already closed (e.g. a delayed/manual run landing on a Saturday, after
    GitHub Actions pushed a scheduled Friday run back past midnight). The
    plain 'always drop the last row' version wrongly discarded a
    already-closed week/month in exactly that case. This only changes
    weekend behavior; normal weekday runs are unaffected."""
    if len(df) <= 1:
        return df
    if pd.Timestamp.now().weekday() >= 5:  # Sat=5, Sun=6
        return df
    return df.iloc[:-1]

# ==========================================
# 2. SIGNAL LOGIC
# ==========================================

def _compute_dm_series(df):
    """Shared TD Sequential computation. Returns (TDUp, TDDn) full arrays so
    callers can inspect either just the latest bar (compute_dm_signals) or
    scan back for the most recent occurrence (compute_dm_recency)."""
    close = df["close"].values
    if len(close) < 20: return None, None
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
    return TDUp, TDDn

def compute_dm_signals(df):
    TDUp, TDDn = _compute_dm_series(df)
    if TDUp is None: return False, False, False, False
    return TDUp[-1] == 9, TDUp[-1] == 13, TDDn[-1] == 9, TDDn[-1] == 13

def compute_dm_recency(df, lookback):
    """
    Scans back `lookback` bars (not counting today) for the most recent
    occurrence of each DM signal type. Returns a dict of days_since (0 =
    today) for each of top9/top13/bot9/bot13, or None if not seen within
    the lookback window.
    """
    TDUp, TDDn = _compute_dm_series(df)
    result = {"top9": None, "top13": None, "bot9": None, "bot13": None}
    if TDUp is None: return result
    n = len(TDUp)
    today = n - 1
    earliest = max(0, today - lookback)
    for i in range(today, earliest - 1, -1):
        days_since = today - i
        if result["top9"] is None and TDUp[i] == 9: result["top9"] = days_since
        if result["top13"] is None and TDUp[i] == 13: result["top13"] = days_since
        if result["bot9"] is None and TDDn[i] == 9: result["bot9"] = days_since
        if result["bot13"] is None and TDDn[i] == 13: result["bot13"] = days_since
    return result

# --- Wyckoff LPS tuning knobs ---
# Backtested against AMPL + full universe count on 2026-08-11: these settings catch
# the SOS on 2026-07-01 ($8.09 support) and correctly flag the reaction on 2026-07-06
# (10.1% off support, volume contracted to <80% of 20d avg, closed green), while
# keeping full-universe fires to ~4.6% of names (~88/1917) rather than 0 (old settings,
# too strict) or 145+ (looser settings, basically flags any green day in an uptrend).
WYCKOFF_SOS_LOOKBACK = 12       # bars to search back for a qualifying SOS breakout
WYCKOFF_SOS_VOL_MULT = 1.3      # breakout volume must be >= this x the 20d avg
WYCKOFF_PRIOR_HIGH_WINDOW = 15  # breakout must clear the prior N-day high
WYCKOFF_MAX_DIST_FROM_SUPPORT = 0.12  # LPS must be within 12% of the SOS support line
WYCKOFF_VOL_CONTRACTION_MULT = 0.8    # LPS day volume must be below 80% of the 20d avg
WYCKOFF_MIN_DAYS_SINCE_SOS = 1  # need at least 1 closed day of pullback after SOS

def compute_wyckoff_signals(df):
    """
    Detects a Wyckoff Last Point of Support (LPS) setup rather than the SOS
    breakout itself:
      1. Find a recent Sign-of-Strength: a close above the prior N-day high on
         volume expansion (>= WYCKOFF_SOS_VOL_MULT x the 20d average volume).
      2. Confirm that breakout level has held as support ever since (no closes
         back below the breakout day's low).
      3. Only fire TODAY if today itself is the reaction/pullback bar: price
         sitting near that support shelf, volume contracting below its 20d
         average, and the candle closing back above its open (buyers stepping
         back in) -- this is the LPS entry, not the original breakout candle.

    Returns (is_lps, days_since_sos, sos_close, dist_from_support_pct)
    or (False, None, None, None) if no setup is present.
    """
    required_cols = {'close', 'low', 'high', 'open', 'volume'}
    if len(df) < 40 or not required_cols.issubset(df.columns):
        return False, None, None, None

    close, low, open_, vol = df['close'], df['low'], df['open'], df['volume']
    vol_avg20 = vol.rolling(20).mean()
    prior_high = close.shift(1).rolling(WYCKOFF_PRIOR_HIGH_WINDOW).max()
    breakout_mask = (close > prior_high) & (vol >= WYCKOFF_SOS_VOL_MULT * vol_avg20)

    n = len(df)
    today = n - 1
    earliest = max(today - WYCKOFF_SOS_LOOKBACK, WYCKOFF_PRIOR_HIGH_WINDOW)

    sos_idx = None
    for i in range(today - WYCKOFF_MIN_DAYS_SINCE_SOS, earliest - 1, -1):
        if bool(breakout_mask.iloc[i]):
            sos_idx = i
            break
    if sos_idx is None:
        return False, None, None, None

    days_since = today - sos_idx
    sos_support = float(low.iloc[sos_idx])
    sos_close = float(close.iloc[sos_idx])

    # Support must have held: no CLOSE back below the breakout day's low since.
    post_breakout_closes = close.iloc[sos_idx + 1: today + 1]
    if (post_breakout_closes < sos_support).any():
        return False, None, None, None

    dist_from_support = (float(close.iloc[today]) - sos_support) / sos_support
    is_near_support = 0 <= dist_from_support <= WYCKOFF_MAX_DIST_FROM_SUPPORT
    is_low_volume = bool(vol.iloc[today] < WYCKOFF_VOL_CONTRACTION_MULT * vol_avg20.iloc[today])
    is_reaction_up = bool(close.iloc[today] > open_.iloc[today])

    is_lps = is_near_support and is_low_volume and is_reaction_up
    return is_lps, days_since, sos_close, dist_from_support * 100

# ==========================================
# 3. SCANNERS
# ==========================================

def scan_timeframe(ticker_map, industry_map, label, interval):
    results = {"Tops": [], "Bottoms": []}
    sector_counts = {"Tops": defaultdict(int), "Bottoms": defaultdict(int)}
    tickers = list(ticker_map.keys())
    # DM9/13 counting only cares about the CURRENT unbroken streak, not deep
    # history: _compute_dm_series' "val_reset" subtraction is empirically a
    # no-op here (verified: TDUp comes out identical to the raw running-streak
    # counter even across an internal reset), so the count is just "how many
    # consecutive bars in a row satisfy the 4-bars-back comparison." The code
    # does have a hard floor of 20 bars (_compute_dm_series returns nothing
    # below that) -- at exactly 20 bars the max achievable streak is 16,
    # comfortably covering DM13 (needs >=17 bars to ever be reachable).
    #   - monthly: 2y (~24 bars) -- deliberately tight-ish margin since real
    #     16+ month uninterrupted streaks are essentially unheard of.
    #   - weekly: 1y (~52 bars) -- more margin than monthly since a 16+ week
    #     uninterrupted streak (quarterly momentum) is meaningfully more
    #     plausible in real data; verified DM13 still detects correctly at
    #     even 24 bars, so 52 has a comfortable safety buffer.
    #   - daily: 3mo (~62 bars). price_cache_1D.pkl also feeds the Wyckoff LPS
    #     scanner, which needs more lead-in than DM9/13 alone (its own
    #     len(df)<40 floor, plus rolling(20)/rolling(15) windows across its
    #     12-day SOS search range). Verified empirically against the real
    #     production cache (1,975 tickers, 47 live LPS signals): truncating
    #     to Wyckoff's own 40-bar floor produces ZERO mismatches vs the full
    #     ~125-bar baseline; bypassing that floor to find the true structural
    #     minimum, divergence only starts appearing at 30 bars. So the code's
    #     40-bar floor already has a real ~6-8 bar margin, and 62 bars (what
    #     '3mo' empirically yields, calibrated off this same cache's actual
    #     6mo->125-bar ratio) clears that floor by ~22 bars, and the true
    #     empirical minimum by ~28-30 -- confirmed 0 mismatches at exactly 62.
    PERIOD_BY_INTERVAL = {'1mo': '2y', '1wk': '1y', '1d': '3mo'}
    period = PERIOD_BY_INTERVAL.get(interval, '6mo')
    data = load_or_fetch_price_data(tickers, interval, period, label)
    candle_date = None
    
    for ticker, df in data.items():
        try:
            if df.empty: continue
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            
            # --- FIX FOR FALSE WEEKLY/MONTHLY SIGNALS ---
            if interval in ('1wk', '1mo'):
                df = _drop_incomplete_bar(df)
            
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
            is_lps, days_since, sos_close, dist_pct = compute_wyckoff_signals(df)
            if is_lps:
                p = float(df['close'].iloc[-1])
                pct = ((p - df['close'].iloc[-2]) / df['close'].iloc[-2]) * 100
                res.append((t, p, ticker_map.get(t, "Unknown"), industry_map.get(t, "Unknown"),
                            pct, days_since, sos_close, dist_pct))
        except: pass
    # Sort Descending (Z-A) by default
    return sorted(res, key=lambda x: x[0], reverse=True)

# ==========================================
# 3b. CONFLUENCE SCORING
# ==========================================

# --- Confluence tuning knobs ---
CONF_DAILY_LOOKBACK = 10    # trading days a daily DM signal still "counts" for
CONF_WEEKLY_LOOKBACK = 6    # weekly bars a weekly DM signal still "counts" for
CONF_MONTHLY_LOOKBACK = 3   # monthly bars a monthly DM signal still "counts" for (~1 quarter)
# NOTE: monthly13/monthly9 set equal to weekly for now as a placeholder, not a
# considered judgment -- confluence weighting overall is flagged for a proper
# empirical pass later via analyze_signals.py once signal_log.csv has enough
# history (including monthly rows) to compare hit rates across signal types.
CONF_PTS = {"monthly13": 3.0, "monthly9": 2.0, "weekly13": 3.0, "weekly9": 2.0, "daily13": 2.0, "daily9": 1.0, "wyckoff": 2.0}
CONF_STACK_BONUS = 1.5      # bonus per additional distinct signal type, same direction
# Tradeability multiplier: penalize sub-$1 (illiquid/manipulated), boost the
# $1-$50 range (sizeable for a small account), neutral above that.
CONF_PRICE_BANDS = [(1.0, 0.5), (20.0, 1.2), (50.0, 1.1), (float("inf"), 1.0)]

def _price_multiplier(price):
    for ceiling, mult in CONF_PRICE_BANDS:
        if price < ceiling: return mult
    return 1.0

def _decay(days_since, lookback):
    # Linear decay: today (0) = full weight, at the lookback edge = 0.5 weight.
    if days_since is None: return 0.0
    return 1.0 - 0.5 * (days_since / max(lookback, 1))

def build_confluence_scores(maps, inds, wyckoff_results, top_n=15):
    """
    Combines the daily DM cache, weekly DM cache, and today's Wyckoff LPS
    results into a single ranked "Top Setups" list. Bullish components
    (DM bottoms + Wyckoff LPS) and bearish components (DM tops) are scored
    separately per ticker; the stronger direction wins and is reported.
    """
    daily_cache = os.path.join("cache", "price_cache_1D.pkl")
    weekly_cache = os.path.join("cache", "price_cache_1W.pkl")
    monthly_cache = os.path.join("cache", "price_cache_1M.pkl")
    if not (os.path.exists(daily_cache) and os.path.exists(weekly_cache)):
        return []
    with open(daily_cache, "rb") as f: daily_data = pickle.load(f)
    with open(weekly_cache, "rb") as f: weekly_data = pickle.load(f)
    # Monthly is treated as optional/non-fatal: a ticker missing from it (e.g.
    # not enough price history for a meaningful monthly count, or the monthly
    # scan hasn't run yet) still scores normally on daily+weekly -- it just
    # doesn't get a monthly component, same graceful-degradation pattern as
    # elsewhere in this file rather than dropping the ticker entirely.
    monthly_data = {}
    if os.path.exists(monthly_cache):
        with open(monthly_cache, "rb") as f: monthly_data = pickle.load(f)

    wyckoff_by_ticker = {row[0]: row for row in wyckoff_results}  # row[5]=days_since (always 0/today)

    results = []
    tickers = set(daily_data.keys()) & set(weekly_data.keys())
    for t in tickers:
        try:
            ddf = daily_data[t].reset_index(); ddf.columns = [c.lower() for c in ddf.columns]
            wdf = weekly_data[t].reset_index(); wdf.columns = [c.lower() for c in wdf.columns]
            wdf = _drop_incomplete_bar(wdf)
            if len(ddf) < 20 or len(wdf) < 20: continue

            d_rec = compute_dm_recency(ddf, CONF_DAILY_LOOKBACK)
            w_rec = compute_dm_recency(wdf, CONF_WEEKLY_LOOKBACK)
            m_rec = {"top9": None, "top13": None, "bot9": None, "bot13": None}
            if t in monthly_data:
                try:
                    mdf = monthly_data[t].reset_index(); mdf.columns = [c.lower() for c in mdf.columns]
                    mdf = _drop_incomplete_bar(mdf)
                    if len(mdf) >= 20:
                        m_rec = compute_dm_recency(mdf, CONF_MONTHLY_LOOKBACK)
                except Exception:
                    pass
            has_wyckoff = t in wyckoff_by_ticker

            bull_components, bear_components = [], []

            if m_rec["bot13"] is not None:
                bull_components.append(("Monthly DM13 Bottom", m_rec["bot13"], CONF_PTS["monthly13"] * _decay(m_rec["bot13"], CONF_MONTHLY_LOOKBACK)))
            elif m_rec["bot9"] is not None:
                bull_components.append(("Monthly DM9 Bottom", m_rec["bot9"], CONF_PTS["monthly9"] * _decay(m_rec["bot9"], CONF_MONTHLY_LOOKBACK)))
            if m_rec["top13"] is not None:
                bear_components.append(("Monthly DM13 Top", m_rec["top13"], CONF_PTS["monthly13"] * _decay(m_rec["top13"], CONF_MONTHLY_LOOKBACK)))
            elif m_rec["top9"] is not None:
                bear_components.append(("Monthly DM9 Top", m_rec["top9"], CONF_PTS["monthly9"] * _decay(m_rec["top9"], CONF_MONTHLY_LOOKBACK)))

            if w_rec["bot13"] is not None:
                bull_components.append(("Weekly DM13 Bottom", w_rec["bot13"], CONF_PTS["weekly13"] * _decay(w_rec["bot13"], CONF_WEEKLY_LOOKBACK)))
            elif w_rec["bot9"] is not None:
                bull_components.append(("Weekly DM9 Bottom", w_rec["bot9"], CONF_PTS["weekly9"] * _decay(w_rec["bot9"], CONF_WEEKLY_LOOKBACK)))
            if w_rec["top13"] is not None:
                bear_components.append(("Weekly DM13 Top", w_rec["top13"], CONF_PTS["weekly13"] * _decay(w_rec["top13"], CONF_WEEKLY_LOOKBACK)))
            elif w_rec["top9"] is not None:
                bear_components.append(("Weekly DM9 Top", w_rec["top9"], CONF_PTS["weekly9"] * _decay(w_rec["top9"], CONF_WEEKLY_LOOKBACK)))

            if d_rec["bot13"] is not None:
                bull_components.append(("Daily DM13 Bottom", d_rec["bot13"], CONF_PTS["daily13"] * _decay(d_rec["bot13"], CONF_DAILY_LOOKBACK)))
            elif d_rec["bot9"] is not None:
                bull_components.append(("Daily DM9 Bottom", d_rec["bot9"], CONF_PTS["daily9"] * _decay(d_rec["bot9"], CONF_DAILY_LOOKBACK)))
            if d_rec["top13"] is not None:
                bear_components.append(("Daily DM13 Top", d_rec["top13"], CONF_PTS["daily13"] * _decay(d_rec["top13"], CONF_DAILY_LOOKBACK)))
            elif d_rec["top9"] is not None:
                bear_components.append(("Daily DM9 Top", d_rec["top9"], CONF_PTS["daily9"] * _decay(d_rec["top9"], CONF_DAILY_LOOKBACK)))

            if has_wyckoff:
                bull_components.append(("Wyckoff LPS", 0, CONF_PTS["wyckoff"]))

            if not bull_components and not bear_components: continue

            bull_raw = sum(c[2] for c in bull_components)
            bear_raw = sum(c[2] for c in bear_components)
            bull_score = bull_raw + CONF_STACK_BONUS * max(0, len(bull_components) - 1) if bull_components else 0
            bear_score = bear_raw + CONF_STACK_BONUS * max(0, len(bear_components) - 1) if bear_components else 0

            direction = "Bullish" if bull_score >= bear_score else "Bearish"
            components = bull_components if direction == "Bullish" else bear_components
            raw_score = bull_score if direction == "Bullish" else bear_score
            if raw_score <= 0: continue

            price = float(ddf["close"].iloc[-1])
            weighted_score = raw_score * _price_multiplier(price)

            results.append({
                "ticker": t, "price": price, "direction": direction,
                "score": round(weighted_score, 2), "raw_score": round(raw_score, 2),
                "sector": maps.get(t, "Unknown"), "industry": inds.get(t, "Unknown"),
                "components": components,
            })
        except Exception:
            continue

    results.sort(key=lambda r: r["score"], reverse=True)
    return results[:top_n] if top_n else results

# ==========================================
# 3b. SIGNAL LOGGING / BACKTEST TRACK RECORD
# ==========================================

SIGNAL_LOG_PATH = "data/signal_log.csv"
RETURN_HORIZONS = [5, 20, 60]  # trading days forward
SIGNAL_LOG_FIELDS = [
    "date", "ticker", "direction", "price_at_trigger", "sector", "industry",
    "raw_score", "price_multiplier", "weighted_score", "components",
    "return_5d", "return_20d", "return_60d",
]

def log_signals(all_scores, trigger_date):
    """
    Append every scored ticker (not just the Top 15 shown on Home) to the
    persistent signal log, so enough sample size accumulates over time to
    backtest each signal type / confluence-score bracket empirically.
    `components` is logged as JSON (list of [signal_name, days_since, points])
    so the exact reasoning behind a score is preserved even if the scoring
    formula (CONF_PTS, decay, stack bonus) changes later.
    Idempotent: safe to call more than once for the same trading day (e.g. a
    manual re-run) -- won't duplicate rows already logged for that date.
    """
    if not all_scores or not trigger_date or trigger_date == "N/A":
        return
    file_exists = os.path.exists(SIGNAL_LOG_PATH)
    if file_exists:
        with open(SIGNAL_LOG_PATH, newline="", encoding="utf-8") as f:
            if any(row["date"] == trigger_date for row in csv.DictReader(f)):
                return
    with open(SIGNAL_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SIGNAL_LOG_FIELDS)
        if not file_exists:
            writer.writeheader()
        for r in all_scores:
            writer.writerow({
                "date": trigger_date, "ticker": r["ticker"], "direction": r["direction"],
                "price_at_trigger": r["price"], "sector": r["sector"], "industry": r["industry"],
                "raw_score": r["raw_score"], "price_multiplier": _price_multiplier(r["price"]),
                "weighted_score": r["score"], "components": json.dumps(r["components"]),
                "return_5d": "", "return_20d": "", "return_60d": "",
            })

def backfill_signal_returns():
    """
    Fills in forward returns for older log rows once enough trading days have
    passed. Looks up each ticker's forward closing price in the *current*
    daily price cache -- since that cache is a rolling ~6mo window refreshed
    nightly, any trigger date recent enough for its horizon to have just
    elapsed (max 60 trading days ~ 3 months) is always still covered.
    Rewrites the whole file only when at least one cell was actually filled.
    """
    if not os.path.exists(SIGNAL_LOG_PATH):
        return
    daily_cache_path = os.path.join("cache", "price_cache_1D.pkl")
    if not os.path.exists(daily_cache_path):
        return
    with open(daily_cache_path, "rb") as f:
        daily_data = pickle.load(f)

    with open(SIGNAL_LOG_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    changed = False
    date_index_cache = {}
    for row in rows:
        if all(row.get(f"return_{h}d") for h in RETURN_HORIZONS):
            continue  # fully backfilled already, skip the work
        t = row["ticker"]
        if t not in daily_data:
            continue
        if t not in date_index_cache:
            df = daily_data[t].reset_index()
            df.columns = [c.lower() for c in df.columns]
            # The cache window spans several months, which can cross a US
            # DST change -- yahooquery then attaches different UTC offsets
            # (EST vs EDT) to different rows, which pd.to_datetime can't
            # unify without utc=True. errors="coerce" additionally makes a
            # single unparseable row a NaT (skipped later) rather than a
            # crash that would block write_reports() from ever running.
            df["date"] = (
                pd.to_datetime(df["date"], utc=True, errors="coerce")
                .dt.tz_convert(None)
                .dt.strftime("%Y-%m-%d")
            )
            date_index_cache[t] = df
        df = date_index_cache[t]
        matches = df.index[df["date"] == row["date"]]
        if len(matches) == 0:
            continue  # trigger date has rolled out of the cache window
        trigger_idx = matches[0]
        base_price = float(row["price_at_trigger"])
        for h in RETURN_HORIZONS:
            col = f"return_{h}d"
            if row.get(col):
                continue
            target_idx = trigger_idx + h
            if target_idx >= len(df):
                continue  # horizon hasn't elapsed yet
            fwd_price = float(df["close"].iloc[target_idx])
            row[col] = round((fwd_price - base_price) / base_price * 100, 2)
            changed = True

    if changed:
        with open(SIGNAL_LOG_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SIGNAL_LOG_FIELDS)
            writer.writeheader()
            writer.writerows(rows)

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
            --bg-color: #f6f5f2;
            --bg-elevated: #ffffff;
            --text-color: #1b1f24;
            --text-dim: #6b7280;
            --table-bg: #ffffff;
            --th-bg: #f0f0f0;
            --border-color: #dde1e6;
            --link-color: #007bff;
            --fg-box-text: #ffffff;
            --bull: #1f9d55;
            --bear: #c73e3e;
            --amber: #b5750a;
            --font-display: 'Space Grotesk', system-ui, sans-serif;
            --font-mono: 'IBM Plex Mono', 'SF Mono', Consolas, monospace;
        }
        
        [data-theme="dark"] {
            --bg-color: #0b0e11;
            --bg-elevated: #12161b;
            --text-color: #e6e9ec;
            --text-dim: #8b95a1;
            --table-bg: #12161b;
            --th-bg: #1a1f26;
            --border-color: #232830;
            --link-color: #66b3ff;
            --bull: #3ddc84;
            --bear: #ff5c5c;
            --amber: #f5a623;
        }

        /* Default to the OS/browser's color-scheme preference when the person
           hasn't made an explicit choice via the toggle. :root:not([data-theme="light"])
           means an explicit manual "light" choice still wins over a dark system
           preference; the [data-theme="dark"] rule above separately covers an
           explicit manual "dark" choice overriding a light system preference. */
        @media (prefers-color-scheme: dark) {
            :root:not([data-theme="light"]) {
                --bg-color: #0b0e11;
                --bg-elevated: #12161b;
                --text-color: #e6e9ec;
                --text-dim: #8b95a1;
                --table-bg: #12161b;
                --th-bg: #1a1f26;
                --border-color: #232830;
                --link-color: #66b3ff;
                --bull: #3ddc84;
                --bear: #ff5c5c;
                --amber: #f5a623;
            }
        }

        /* position: relative so the absolutely-positioned .theme-toggle anchors
           to body's own box (top:0/right:0 = flush with body's content edge)
           rather than the raw viewport — keeps it aligned consistently across
           the 20px desktop and 10px mobile body margins below. */
        body { position: relative; font-family: var(--font-display); margin: 20px; font-size: 16px; background-color: var(--bg-color); color: var(--text-color); transition: background 0.3s, color 0.3s; }
        h1 { display: flex; align-items: baseline; gap: 12px; letter-spacing: -0.01em; }
        
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
        th, td { border: 1px solid var(--border-color); padding: 8px 10px; text-align: left; background-color: var(--table-bg); color: var(--text-color); }
        
        /* Sortable Headers Only */
        table.sortable th { cursor: pointer; color: var(--text-color); font-family: var(--font-mono); font-size: 0.85em; text-transform: uppercase; letter-spacing: 0.04em; text-decoration: none; border-bottom: 2px solid var(--link-color); background-color: var(--th-bg); }
        table.sortable th:hover { color: var(--link-color); }
        
        /* Nav links (top-left) with the market-hours banner wrapping to its own
           line underneath on narrow screens — .page-header/.topbar are plain
           in-flow flex, no absolute positioning, so nothing here ever overlaps
           the nav links. */
        .page-header { display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 8px 16px; margin-bottom: 20px; }
        .topbar { display: flex; align-items: center; gap: 10px; flex-shrink: 0; }
        .market-banner { font-family: var(--font-mono); font-size: 0.78em; color: var(--text-dim); background: var(--bg-elevated); border: 1px solid var(--border-color); border-radius: 12px; padding: 4px 10px; white-space: nowrap; }
        /* Dark-mode toggle is deliberately taken out of the .page-header flex
           flow entirely and pinned to the page's top-right corner instead, so
           it stays there regardless of how nav/banner wrap on narrow screens.
           It's a single small icon (not the full banner), so unlike the old
           topbar-was-absolute bug, there's no realistic width for it to
           overlap the nav links. */
        .theme-toggle { position: absolute; top: 0; right: 0; cursor: pointer; font-size: 24px; user-select: none; }
        .section-card { background-color: var(--bg-elevated); border: 1px solid var(--border-color); border-radius: 8px; padding: 16px 20px; margin-bottom: 20px; }
        .section-card table { margin-top: 6px; }
        /* Second+ h3 in a card (e.g. "... by Sector" titles following a signals
           table) need real top spacing to separate them from the table above;
           only the first h3 in the card should sit flush with no top margin. */
        .section-card h3 { font-family: var(--font-display); margin-top: 24px; margin-bottom: 8px; }
        .section-card h3:first-child { margin-top: 0; }
        
        .nav-bar { margin-bottom: 0; }
        .nav-link { font-size: 1.1em; font-weight: bold; margin-right: 20px; text-decoration: none; color: var(--link-color); }
        .nav-link:hover { text-decoration: underline; opacity: 0.8; }
        .active-link { color: var(--text-color); text-decoration: none; cursor: default; }
        
        .update-footer { margin-top: 50px; font-size: 0.85em; color: #888; text-align: center; border-top: 1px solid var(--border-color); padding-top: 10px; }

        /* Arrows Hidden By Default */
        .sortable th::after { content: ""; margin-left: 5px; }
        .sortable th.asc::after { content: " ▲"; font-size: 0.8em; }
        .sortable th.desc::after { content: " ▼"; font-size: 0.8em; }

        /* --- Front page: regime strip + top setups --- */
        .ticker-mono { font-family: var(--font-mono); }

        .regime-strip { background-color: var(--bg-elevated); border: 1px solid var(--border-color); border-radius: 8px; padding: 16px 20px; margin-bottom: 28px; }
        .regime-row { display: flex; align-items: center; gap: 14px; margin: 10px 0; flex-wrap: wrap; }
        .regime-label { font-family: var(--font-mono); font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-dim); width: 90px; flex-shrink: 0; }
        .regime-value { font-family: var(--font-mono); font-weight: 600; }
        .breadth-bar { flex: 1; min-width: 140px; height: 10px; border-radius: 5px; overflow: hidden; display: flex; background: var(--border-color); }
        .breadth-bull-seg { background-color: var(--bull); height: 100%; }
        .breadth-bear-seg { background-color: var(--bear); height: 100%; }
        .breadth-count { font-family: var(--font-mono); font-size: 0.85em; color: var(--text-dim); white-space: nowrap; }

        .setups-heading { font-family: var(--font-display); font-size: 1.3em; font-weight: 700; margin: 6px 0 14px 0; }
        .setups-columns { display: flex; flex-direction: column; gap: 20px; }
        .setups-col { flex: 1; }
        .setups-col-title { font-family: var(--font-mono); font-size: 0.85em; text-transform: uppercase; letter-spacing: 0.08em; padding-bottom: 8px; margin-bottom: 10px; border-bottom: 2px solid var(--border-color); }
        .setups-col-title.bull { color: var(--bull); border-bottom-color: var(--bull); }
        .setups-col-title.bear { color: var(--bear); border-bottom-color: var(--bear); }

        .setup-card { background-color: var(--bg-elevated); border: 1px solid var(--border-color); border-radius: 6px; padding: 10px 14px; margin-bottom: 8px; }
        .setup-top-row { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; }
        .setup-ticker { font-family: var(--font-mono); font-weight: 700; font-size: 1.05em; text-decoration: none; }
        .setup-ticker.bull { color: var(--bull); }
        .setup-ticker.bear { color: var(--bear); }
        .setup-price { font-family: var(--font-mono); color: var(--text-dim); font-size: 0.9em; }
        .setup-industry { font-family: var(--font-mono); font-size: 0.75em; color: var(--text-dim); margin: 1px 0 8px 0; text-transform: uppercase; letter-spacing: 0.03em; }
        .setup-score { font-family: var(--font-mono); font-weight: 600; font-size: 0.95em; }
        .score-bar-track { height: 5px; border-radius: 3px; background: var(--border-color); margin: 6px 0 8px 0; overflow: hidden; }
        .score-bar-fill { height: 100%; border-radius: 3px; }
        .score-bar-fill.bull { background-color: var(--bull); }
        .score-bar-fill.bear { background-color: var(--bear); }
        .setup-chips { display: flex; flex-wrap: wrap; gap: 5px; }
        .chip { font-family: var(--font-mono); font-size: 0.72em; padding: 2px 7px; border-radius: 10px; background: var(--th-bg); color: var(--text-dim); border: 1px solid var(--border-color); white-space: nowrap; }
        .setups-empty { color: var(--text-dim); font-size: 0.9em; font-style: italic; }

        /* Mobile Fixes */
        @media (max-width: 37.5em) {
            body { margin: 10px; }
            table { width: 100% !important; }
            html { -webkit-text-size-adjust: none; text-size-adjust: none; }
            .fg-chart { display: none !important; }
            th, td, a { font-size: 14px !important; line-height: 1.4; padding: 8px 8px; }
            td:nth-child(4) { white-space: normal; overflow-wrap: break-word; word-wrap: break-word; min-width: 60px; }
            .market-banner { font-size: 0.68em; padding: 3px 7px; }
            .section-card { padding: 12px 14px; }
        }

        @media (min-width: 64em) {
            .row { flex-direction: row; }
            .column { margin: 0 10px; }
            .summary-table { width: 60%; }
            .setups-columns { flex-direction: row; }
        }
    </style>
    <script>
    document.addEventListener("DOMContentLoaded", function() {
        // Dark Mode Logic: defaults to the OS/browser color-scheme preference
        // (handled by CSS media query) until the person clicks the toggle,
        // at which point that explicit choice is saved and takes over.
        const toggle = document.getElementById('theme-toggle');
        const systemDarkQuery = window.matchMedia('(prefers-color-scheme: dark)');
        let savedTheme = localStorage.getItem('theme'); // 'dark' | 'light' | null (= follow system)

        function effectiveTheme() {
            return savedTheme || (systemDarkQuery.matches ? 'dark' : 'light');
        }
        function syncIcon() {
            toggle.textContent = effectiveTheme() === 'dark' ? '☀️' : '🌙';
        }

        if (savedTheme) {
            document.documentElement.setAttribute('data-theme', savedTheme);
        }
        syncIcon();

        toggle.addEventListener('click', () => {
            savedTheme = effectiveTheme() === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-theme', savedTheme);
            localStorage.setItem('theme', savedTheme);
            syncIcon();
        });

        // If the person hasn't made a manual choice, keep following the OS
        // setting live (e.g. it flips at sunset) without needing a reload.
        systemDarkQuery.addEventListener('change', () => {
            if (!savedTheme) syncIcon();
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

# Normalization ceiling for the confluence score bar (theoretical rough max:
# monthly13 + weekly13 + daily13 + wyckoff = 10.0 raw, + stack bonus 1.5x3
# additional signal types = 4.5, = 14.5, x1.2 top price multiplier = 17.4).
# Note: bearish max is still lower (~13.2) since Wyckoff LPS is bull-only --
# the known asymmetric-ceiling issue persists, just less extreme than before
# (monthly adds equally to both sides, narrowing the bull/bear gap somewhat).
CONF_SCORE_BAR_MAX = 17.4

def gen_setup_cards(setups, direction):
    cls = "bull" if direction == "Bullish" else "bear"
    if not setups:
        return f'<p class="setups-empty">No {direction.lower()} setups today.</p>'
    cards = ""
    for r in setups:
        pct = min(100, round(r["score"] / CONF_SCORE_BAR_MAX * 100))
        chips = ""
        for comp in r["components"]:
            label = comp[0]
            if label == "Wyckoff LPS":
                chips += f'<span class="chip">Wyckoff LPS · today</span>'
            else:
                days = comp[1]
                # days_since is a BAR count, not a day count -- 5 for a
                # weekly signal means 5 weeks, not 5 days. Unit follows the
                # timeframe named at the start of the label.
                if label.startswith("Weekly"):
                    unit = "w"
                elif label.startswith("Monthly"):
                    unit = "mo"
                else:
                    unit = "d"
                when = "today" if days == 0 else f"{days}{unit} ago"
                chips += f'<span class="chip">{label} · {when}</span>'
        link = f"https://www.tradingview.com/chart/?symbol={r['ticker']}"
        cards += f"""
        <div class="setup-card">
            <div class="setup-top-row">
                <a href="{link}" target="_blank" class="setup-ticker {cls}">{r['ticker']}</a>
                <span class="setup-price">${r['price']:.2f}</span>
                <span class="setup-score {cls}" style="color:var(--{'bull' if cls=='bull' else 'bear'});">{r['score']:.2f}</span>
            </div>
            <div class="setup-industry">{r['industry']}</div>
            <div class="score-bar-track"><div class="score-bar-fill {cls}" style="width:{pct}%;"></div></div>
            <div class="setup-chips">{chips}</div>
        </div>"""
    return cards

def write_reports(daily, weekly, monthly, d_sec, w_sec, m_sec, fg, wyckoff, top_setups, date_str):
    f_val, f_prev, f_date = fg
    f_col = "#dc3545" if isinstance(f_val, int) and f_val >= 60 else "#ffc107" if isinstance(f_val, int) and f_val >= 45 else "#28a745"
    style = get_shared_style(f_col)
    meta = ('<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">'
            '<link rel="preconnect" href="https://fonts.googleapis.com">'
            '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
            '<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=IBM+Plex+Mono:wght@400;600&display=swap" rel="stylesheet">')
    updated_at = f'<div class="update-footer">Last updated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}</div>'
    
    # Market-hours banner: sits in-flow next to nav, wraps below it on mobile.
    toggle = (
        f'<div class="topbar">'
        f'<span class="market-banner">{get_market_hours_banner()}</span>'
        f'</div>'
    )
    # Dark-mode toggle: rendered separately (not nested in .page-header) since
    # it's position:absolute and pinned to the page's top-right corner —
    # independent of how nav/banner lay out or wrap.
    theme_toggle = '<div id="theme-toggle" class="theme-toggle">🌙</div>'
    nav = lambda active: (
        '<div class="nav-bar">'
        f'<a href="index.html" class="nav-link{" active-link" if active=="home" else ""}">Home</a>'
        f'<a href="demark.html" class="nav-link{" active-link" if active=="demark" else ""}">DeMark</a>'
        f'<a href="wyckoff.html" class="nav-link{" active-link" if active=="wyckoff" else ""}">Wyckoff</a>'
        '</div>'
    )

    # --- HOME (new front page) ---
    d_bot, d_top = len(daily["Bottoms"]), len(daily["Tops"])
    total_bt = d_bot + d_top
    bull_pct = round(d_bot / total_bt * 100) if total_bt else 50

    bullish_setups = [r for r in top_setups if r["direction"] == "Bullish"]
    bearish_setups = [r for r in top_setups if r["direction"] == "Bearish"]

    html_home = f"""<html><head>{meta}<title>Dashboard</title>{style}</head><body>
    {theme_toggle}
    <div class="page-header">{nav("home")}{toggle}</div>
    <h1>US Signals Dashboard</h1><div class="date-subtitle">{date_str}</div>

    <div class="regime-strip">
        <div class="regime-row">
            <span class="regime-label">Sentiment</span>
            <span class="regime-value" style="color:{f_col};">CNN Fear &amp; Greed: {f_val} (prev {f_prev})</span>
        </div>
        <div class="regime-row">
            <span class="regime-label">Breadth</span>
            <div class="breadth-bar"><div class="breadth-bull-seg" style="width:{bull_pct}%;"></div><div class="breadth-bear-seg" style="width:{100-bull_pct}%;"></div></div>
            <span class="breadth-count">{d_bot} bottoms / {d_top} tops (daily)</span>
        </div>
        <img src="fg_trend.png" class="fg-chart" style="max-width: 420px; display:block; margin:10px 0 0 0; border-radius:4px;">
    </div>

    <div class="setups-heading">Top Setups</div>
    <div class="setups-columns">
        <div class="setups-col">
            <div class="setups-col-title bull">Bullish</div>
            {gen_setup_cards(bullish_setups, "Bullish")}
        </div>
        <div class="setups-col">
            <div class="setups-col-title bear">Bearish</div>
            {gen_setup_cards(bearish_setups, "Bearish")}
        </div>
    </div>
    <p style="opacity:0.6; font-size:0.85em; margin-top:20px;">Score = weighted sum of confirming signals (weekly/daily DeMark + Wyckoff LPS) across timeframes, with a stacking bonus for multiple distinct signal types and a tradeability multiplier by price. See <a href="demark.html" class="nav-link" style="font-size:1em;">DeMark</a> and <a href="wyckoff.html" class="nav-link" style="font-size:1em;">Wyckoff</a> tabs for the full underlying scans.</p>
    {updated_at}</body></html>"""
    with open("docs/index.html", "w", encoding="utf-8") as f: f.write(html_home)

    # --- DEMARK HTML (formerly index.html) ---
    html_dm = f"""<html><head>{meta}<title>DeMark</title>{style}</head><body>
    {theme_toggle}
    <div class="page-header">{nav("demark")}{toggle}</div>
    <h1>DeMark Signals</h1><div class="date-subtitle">{date_str}</div>
    
    <div class="section-card">
    <h2 style="margin-top:0;">Signal Summary</h2>
    <table class="summary-table">
        <tr><th>Period</th><th>Bottoms</th><th>Tops</th></tr>
        <tr><td><strong>Daily</strong></td><td>{len(daily["Bottoms"])}</td><td>{len(daily["Tops"])}</td></tr>
        <tr><td><strong>Weekly</strong></td><td>{len(weekly["Bottoms"])}</td><td>{len(weekly["Tops"])}</td></tr>
        <tr><td><strong>Monthly</strong></td><td>{len(monthly["Bottoms"])}</td><td>{len(monthly["Tops"])}</td></tr>
    </table>
    </div>
    
    <div class="row">
        <div class="column"><div class="section-card"><h3>Daily Bottoms</h3>{gen_table(daily["Bottoms"])}{gen_sec_table("Daily Bottoms by Sector", d_sec["Bottoms"])}</div></div>
        <div class="column"><div class="section-card"><h3>Daily Tops</h3>{gen_table(daily["Tops"])}{gen_sec_table("Daily Tops by Sector", d_sec["Tops"])}</div></div>
    </div>
    <div class="row">
        <div class="column"><div class="section-card"><h3>Weekly Bottoms</h3>{gen_table(weekly["Bottoms"])}{gen_sec_table("Weekly Bottoms by Sector", w_sec["Bottoms"])}</div></div>
        <div class="column"><div class="section-card"><h3>Weekly Tops</h3>{gen_table(weekly["Tops"])}{gen_sec_table("Weekly Tops by Sector", w_sec["Tops"])}</div></div>
    </div>
    <div class="row">
        <div class="column"><div class="section-card"><h3>Monthly Bottoms</h3>{gen_table(monthly["Bottoms"])}{gen_sec_table("Monthly Bottoms by Sector", m_sec["Bottoms"])}</div></div>
        <div class="column"><div class="section-card"><h3>Monthly Tops</h3>{gen_table(monthly["Tops"])}{gen_sec_table("Monthly Tops by Sector", m_sec["Tops"])}</div></div>
    </div>
    {updated_at}</body></html>"""
    with open("docs/demark.html", "w", encoding="utf-8") as f: f.write(html_dm)

    # --- WYCKOFF HTML ---
    w_rows = ""
    for t, p, sec, ind, pct, days_since, sos_close, dist_pct in wyckoff:
        lk = f"<a href='https://www.tradingview.com/chart/?symbol={t}' target='_blank' style='text-decoration:none; color:var(--link-color); font-weight:bold;'>{t}</a>"
        w_rows += (
            f"<tr><td>{lk}</td><td>{p:.2f}</td>"
            f"<td style='color:{'green' if pct>0 else 'red'}'>{pct:+.2f}%</td>"
            f"<td>{ind}</td>"
            f"<td>{days_since}d ago @ {sos_close:.2f}</td>"
            f"<td>{dist_pct:+.1f}%</td>"
            f"<td style='background-color:#d4edda; color:#000;'>LPS</td></tr>"
        )

    html_w = f"""<html><head>{meta}<title>Wyckoff</title>{style}</head><body>
    {theme_toggle}
    <div class="page-header">{nav("wyckoff")}{toggle}</div>
    <h1>Wyckoff LPS</h1><div class="date-subtitle">{date_str}</div>
    <p style="opacity:0.75; font-size:0.9em; margin-top:-8px;">Last Point of Support: a pullback to a prior volume-confirmed breakout, on light volume, reacting back up.</p>
    <div class="section-card">
    <table class="sortable"><thead><tr><th>Ticker</th><th>Price</th><th>%</th><th>Industry</th><th>SOS Breakout</th><th>Dist. from Support</th><th>Pattern</th></tr></thead><tbody>{w_rows if w_rows else "<tr><td colspan='7'>None</td></tr>"}</tbody></table>
    </div>
    {updated_at}</body></html>"""
    with open("docs/wyckoff.html", "w", encoding="utf-8") as f: f.write(html_w)

def main():
    maps, inds = {}, {}
    for f in ["sp_cache.csv", "russell_cache.csv", "nasdaq_cache.csv", "NDQ_cache.csv", "AMEX_cache.csv", "NYSE_cache.csv"]:
        m, i = fetch_tickers_and_sectors_from_csv(f); maps.update(m); inds.update(i)
    
    # Run Scans
    daily, d_s, d_date = scan_timeframe(maps, inds, "1D", "1d")
    weekly, w_s, _ = scan_timeframe(maps, inds, "1W", "1wk")
    # Monthly is new/less proven than daily+weekly, so it's wrapped the same
    # defensive way as signal logging below: a failure here must never be
    # able to block daily/weekly/Wyckoff from publishing.
    try:
        monthly, m_s, _ = scan_timeframe(maps, inds, "1M", "1mo")
    except Exception as e:
        print(f"WARNING: monthly scan failed, continuing without it: {e}")
        monthly, m_s = {"Tops": [], "Bottoms": []}, {"Tops": defaultdict(int), "Bottoms": defaultdict(int)}
    wyckoff = scan_wyckoff(maps, inds)
    all_scores = build_confluence_scores(maps, inds, wyckoff, top_n=None)
    top_setups = all_scores[:15]
    fg = get_fear_and_greed()
    
    # Generate Graph
    plot_fear_greed_history()

    # Signal log: append today's scored tickers, then backfill forward
    # returns for any older rows that just reached a 5/20/60-day horizon.
    # Wrapped defensively: this is a nice-to-have for future backtesting,
    # and must never be able to stop write_reports() from running and
    # updating the live site, no matter what goes wrong inside it.
    try:
        log_signals(all_scores, d_date)
        backfill_signal_returns()
    except Exception as e:
        print(f"WARNING: signal logging/backfill failed, continuing without it: {e}")

    try:
        ds = f"Signals triggered on {datetime.strptime(d_date, '%Y-%m-%d').strftime('%A, %b %d, %Y')} (as of NY close)"
    except: ds = f"Signals triggered on {d_date} (as of NY close)"
    
    write_reports(daily, weekly, monthly, d_s, w_s, m_s, fg, wyckoff, top_setups, ds)

if __name__ == "__main__": main()
