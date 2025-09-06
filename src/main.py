import argparse, json
from pathlib import Path
import pandas as pd
import yaml

from ingest import load_sheet, find_header_row, extract_isin_based_data
from normalize import canonicalize_df
from enrich import enrich
from aggregate import weighted_avg_yield, by_bucket, top_issuers
from utils import is_valid_isin
from visualize import add_charts_to_excel

def run(as_of: str, raw_dir: Path, out_dir: Path, config_dir: Path):
    with open(config_dir/"sheet_hints.yml","r",encoding="utf-8") as f:
        hints = yaml.safe_load(f)

    interim_dir = raw_dir.parent.parent/"interim"
    processed_dir = raw_dir.parent.parent/"processed"
    interim_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    merged = []
    logs = []

    amc_tokens = {"ABSLF":"ABSLF","ICICI":"ICICI","KOTAK":"KOTAK","HDFC":"HDFC","NIPPON":"NIPPON","NIMF":"NIPPON","SBI":"SBI"}

    for p in sorted(raw_dir.iterdir()):
        if not p.suffix.lower() in [".xls",".xlsx",".xlsm"]:
            continue
        if p.name.startswith("~$"):
            continue

        amc = None
        up = p.name.upper()
        for key,val in amc_tokens.items():
            if key in up:
                amc = val; break
        if not amc or amc not in hints:
            continue

        sheet_hint = hints[amc]["sheet"]
        forced_header_row = hints[amc].get("header_row", None)

        df0, chosen_sheet, sheet_names = load_sheet(p, sheet_hint)
        hdr = forced_header_row if forced_header_row is not None else find_header_row(df0)

        headers, table = extract_isin_based_data(df0, hdr)

        canon, header_map, unit_hint = canonicalize_df(table, headers, amc, config_dir)
        canon.to_parquet(interim_dir/f"{amc}.parquet", index=False)

        canon["AMC"] = amc
        canon = enrich(canon, config_dir)
        merged.append(canon)

        logs.append({
            "amc": amc, "file": p.name, "chosen_sheet": chosen_sheet, "header_row": hdr,
            "unit_hint": unit_hint, "rows": len(canon), "sheet_names": sheet_names, "header_map": header_map
        })

    if not merged:
        raise RuntimeError("No workbooks ingested — check raw directory and filenames.")

    merged_df = pd.concat(merged, ignore_index=True, sort=False)
    
    # Filter out rows with invalid ISINs (headers, totals, etc.)
    print(f"Before ISIN filtering: {len(merged_df)} rows")
    valid_isin_mask = merged_df['ISIN'].apply(is_valid_isin)
    merged_df = merged_df[valid_isin_mask].reset_index(drop=True)
    print(f"After ISIN filtering: {len(merged_df)} rows")
    
    merged_path = processed_dir/"merged_holdings.parquet"
    merged_df.to_parquet(merged_path, index=False)

    writer = pd.ExcelWriter(out_dir/"aggregates.xlsx", engine="openpyxl")
    merged_df.to_excel(writer, sheet_name="Holdings", index=False)

    amcs = sorted(merged_df["AMC"].dropna().unique().tolist())
    summary_rows = []
    for amc in amcs:
        sub = merged_df[merged_df["AMC"]==amc].copy()
        wytm, cov_yield = weighted_avg_yield(sub)
        mv = sub["Market/Fair Value (Rs. in Lacs)"].fillna(0.0)
        total = mv.sum()
        with_mat = mv[sub["Maturity_Final"].notna()].sum()
        cov_mat = (with_mat/total*100.0) if total else 0.0

        by_bucket(sub, "Maturity Bucket").to_excel(writer, sheet_name=f"{amc}_Maturity", index=False)
        by_bucket(sub, "Instrument Type").to_excel(writer, sheet_name=f"{amc}_Instrument", index=False)
        by_bucket(sub, "Rating_canonical").to_excel(writer, sheet_name=f"{amc}_Rating", index=False)

        y = sub.copy()
        y["Yield Bucket"] = pd.cut(y["YIELD"], bins=[-1,7,8,9,10,1e9], labels=["<7","7-8","8-9","9-10",">=10"])
        by_bucket(y[y["YIELD"].notna()], "Yield Bucket").to_excel(writer, sheet_name=f"{amc}_Yield", index=False)

        top_issuers(sub).to_excel(writer, sheet_name=f"{amc}_Issuers", index=False)

        summary_rows.append({
            "AMC": amc,
            "Weighted Avg Yield (%)": wytm,
            "Yield Coverage (% of value)": cov_yield,
            "Maturity Coverage (% of value)": cov_mat
        })

    by_bucket(merged_df, "Maturity Bucket").to_excel(writer, sheet_name="All_Maturity", index=False)
    by_bucket(merged_df, "Instrument Type").to_excel(writer, sheet_name="All_Instrument", index=False)
    by_bucket(merged_df, "Rating_canonical").to_excel(writer, sheet_name="All_Rating", index=False)
    y = merged_df.copy()
    y["Yield Bucket"] = pd.cut(y["YIELD"], bins=[-1,7,8,9,10,1e9], labels=["<7","7-8","8-9","9-10",">=10"])
    by_bucket(y[y["YIELD"].notna()], "Yield Bucket").to_excel(writer, sheet_name="All_Yield", index=False)
    top_issuers(merged_df).to_excel(writer, sheet_name="All_Issuers", index=False)

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_excel(writer, sheet_name="Summary", index=False)

    writer.close()

    (out_dir/"analysis.md").write_text(
"""# Corporate Bond Funds — Portfolio Comparison (as of 2025-07-31)

## Executive Summary
- Weighted average yields and coverage are in the `Summary` sheet.
- Sovereign (G-Sec/SDL/T-Bill) vs Corporate mix is in `*_Instrument` tabs.
- Maturity ladder highlights rate risk; Perpetual/NA excluded from tenor risk.
- Rating mix shows credit quality; split ratings mapped to the lower grade.
- Issuer concentration lists Top-10 by value.

## Data Notes
- Values normalized to Rs. in Lacs. `% to Net Assets` treated as `% to NAV`.
- Maturity taken from explicit columns when available, else parsed from instrument names.
- AT1/Tier-2/Perpetual instruments excluded from maturity buckets; call dates not treated as maturity.
- Weighted-average yield computed over rows with YIELD; weights renormalized; coverage reported.
""", encoding="utf-8"
    )

    (out_dir/"run_log.json").write_text(json.dumps(logs, indent=2), encoding="utf-8")

    # Add charts and visualizations to Excel
    try:
        add_charts_to_excel(out_dir/"aggregates.xlsx")
        print("Charts and dashboard added to Excel output")
    except Exception as e:
        print(f"Warning: Could not add charts to Excel: {e}")

    return {
        "merged_path": str(merged_path),
        "aggregates_path": str(out_dir/"aggregates.xlsx"),
        "analysis_path": str(out_dir/"analysis.md"),
        "run_log": str(out_dir/"run_log.json")
    }

def main():
    ap = argparse.ArgumentParser(description="Corporate Bond Fund Portfolio Pipeline")
    ap.add_argument("--asof", default="2025-07-31")
    ap.add_argument("--raw-dir", default=str(Path(__file__).resolve().parents[1]/"data"/"raw"/"2025-07-31"))
    ap.add_argument("--out-dir", default=str(Path(__file__).resolve().parents[1]/"output"/"2025-07-31"))
    ap.add_argument("--config-dir", default=str(Path(__file__).resolve().parents[1]/"config"))
    args = ap.parse_args()

    res = run(args.asof, Path(args.raw_dir), Path(args.out_dir), Path(args.config_dir))
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()
