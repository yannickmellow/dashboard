"""
Signal backtest analysis.

Reads data/signal_log.csv (built up nightly by main.py's log_signals() +
backfill_signal_returns()) and computes, per signal type and per confluence-
score bucket:
  - sample size (total logged, and how many have each horizon filled in yet)
  - hit rate: % where price moved in the predicted direction
  - average / median forward return

Run locally, any time, once enough history has accumulated:
    python analyze_signals.py

Nothing here writes back to the log -- read-only analysis.
"""
import json
import pandas as pd

LOG_PATH = "data/signal_log.csv"
HORIZONS = [5, 20, 60]


def load_log():
    df = pd.read_csv(LOG_PATH)
    df["components"] = df["components"].apply(json.loads)
    return df


def explode_by_signal_type(df):
    """
    One row per (setup, contributing signal type), so each signal type's own
    track record can be measured even when it fired alongside others.

    Caveat: a setup's overall forward return gets attributed to *every*
    signal type that contributed to it. So a triple-confirmed setup's return
    counts toward all three buckets -- inflating apparent sample size versus
    "pure" single-signal setups. This is the only way to get workable sample
    sizes early on; worth re-running with an is-the-only-contributor filter
    once volume is high enough to support it.
    """
    rows = []
    for _, r in df.iterrows():
        for name, days_since, points in r["components"]:
            row = {
                "date": r["date"], "ticker": r["ticker"], "direction": r["direction"],
                "signal_type": name, "weighted_score": r["weighted_score"],
            }
            for h in HORIZONS:
                row[f"return_{h}d"] = r.get(f"return_{h}d")
            rows.append(row)
    return pd.DataFrame(rows)


def adjusted_returns(df):
    """Flip sign on Bearish rows so positive always = 'the trade worked'."""
    sign = df["direction"].map({"Bullish": 1, "Bearish": -1})
    for h in HORIZONS:
        df[f"adj_return_{h}d"] = df[f"return_{h}d"] * sign
    return df


def score_bucket(score):
    # Boundaries rescaled to match CONF_SCORE_BAR_MAX=17.4 (bumped from 12.0
    # when monthly signals were added a 4th stackable bull component) --
    # the old "9-12 (max confluence)" label was stale: 12 hasn't been the
    # real ceiling for a while, and would silently compress genuinely
    # maxed-out setups into a bucket that understates how strong they are.
    if score < 4: return "0-4 (weak)"
    if score < 8: return "4-8 (moderate)"
    if score < 12: return "8-12 (strong)"
    return "12+ (max confluence)"


def summarize(df, group_col):
    out = []
    for key, g in df.groupby(group_col):
        row = {group_col: key, "n_logged": len(g)}
        for h in HORIZONS:
            col = f"adj_return_{h}d"
            valid = g[col].dropna()
            row[f"n_{h}d"] = len(valid)
            row[f"hit_rate_{h}d_%"] = round((valid > 0).mean() * 100, 1) if len(valid) else None
            row[f"avg_return_{h}d_%"] = round(valid.mean(), 2) if len(valid) else None
            row[f"median_return_{h}d_%"] = round(valid.median(), 2) if len(valid) else None
        out.append(row)
    return pd.DataFrame(out).sort_values("n_logged", ascending=False)


def main():
    df = load_log()
    print(f"Loaded {len(df)} logged setups from {LOG_PATH}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}\n")

    exploded = adjusted_returns(explode_by_signal_type(df))
    by_signal = summarize(exploded, "signal_type")
    print("=" * 100)
    print("BY SIGNAL TYPE  (a setup's return counts toward every signal type that contributed to it)")
    print("=" * 100)
    print(by_signal.to_string(index=False))
    print()

    df = adjusted_returns(df)
    df["score_bucket"] = df["weighted_score"].apply(score_bucket)
    by_bucket = summarize(df, "score_bucket")
    print("=" * 100)
    print("BY CONFLUENCE SCORE BUCKET  (whole-setup level -- does a higher score actually predict a better outcome?)")
    print("=" * 100)
    print(by_bucket.to_string(index=False))


if __name__ == "__main__":
    main()
