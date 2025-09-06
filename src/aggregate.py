import pandas as pd
from pathlib import Path

def _weight_series(df):
    if "% to NAV" in df.columns and df["% to NAV"].notna().any():
        w = df["% to NAV"].fillna(0.0) / 100.0
    else:
        mv = df["Market/Fair Value (Rs. in Lacs)"].fillna(0.0)
        denom = mv.sum()
        w = mv / denom if denom else mv
    return w

def weighted_avg_yield(df):
    w = _weight_series(df)
    mask = df["YIELD"].notna()
    if mask.sum() == 0:
        return None, 0.0
    w2 = w[mask]
    w2 = w2 / w2.sum() if w2.sum() else w2
    y = (df.loc[mask, "YIELD"] * w2).sum()
    coverage = float((w[mask]).sum()) * 100.0
    return float(y), coverage

def by_bucket(df, col, value_col="Market/Fair Value (Rs. in Lacs)"):
    mv = df[value_col].fillna(0.0)
    total = mv.sum()
    grp = df.groupby(col, dropna=False)[value_col].sum().reset_index()
    grp["% of Portfolio (by value)"] = (grp[value_col] / total * 100.0) if total else 0.0
    return grp.sort_values("% of Portfolio (by value)", ascending=False)

def top_issuers(df, n=10):
    grp = df.groupby("Issuer Name", dropna=False)["Market/Fair Value (Rs. in Lacs)"].sum().reset_index()
    grp = grp.sort_values("Market/Fair Value (Rs. in Lacs)", ascending=False).head(n)
    total = df["Market/Fair Value (Rs. in Lacs)"].sum()
    grp["% to Portfolio"] = grp["Market/Fair Value (Rs. in Lacs)"] / total * 100.0 if total else 0.0
    return grp
