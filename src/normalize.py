import re, yaml
from pathlib import Path
import pandas as pd
from utils import norm_alias, parse_numeric, detect_unit_from_headers, to_lacs, clean_text

def load_schema(config_dir: Path):
    with open(config_dir/"schema_map.yml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def canonicalize_df(df: pd.DataFrame, headers: list, amc: str, config_dir: Path):
    cfg = load_schema(config_dir)
    canon_cols = cfg["canonical"]
    aliases_common = {k: [norm_alias(a) for a in v] for k, v in cfg["aliases_common"].items()}
    aliases_amc = {k: [norm_alias(a) for a in v] for k, v in cfg.get("aliases_by_amc",{}).get(amc, {}).items()}

    header_map = {}
    headers_norm = [norm_alias(h) for h in headers]

    for c in canon_cols:
        match_idx = None
        for i, h in enumerate(headers):
            if norm_alias(h) == norm_alias(c):
                match_idx = i; break
        if match_idx is None:
            for i, h in enumerate(headers):
                hn = headers_norm[i]
                if hn in aliases_amc.get(c, []):
                    match_idx = i; break
        if match_idx is None:
            for i, h in enumerate(headers):
                hn = headers_norm[i]
                if hn in aliases_common.get(c, []):
                    match_idx = i; break
        header_map[c] = headers[match_idx] if match_idx is not None else None

    out = pd.DataFrame()
    for c in canon_cols:
        src_col = header_map.get(c)
        out[c] = df[src_col] if src_col in df.columns else None

    unit_hint = detect_unit_from_headers(headers)
    out["Market/Fair Value (Rs. in Lacs)"] = out["Market/Fair Value (Rs. in Lacs)"].map(parse_numeric).map(lambda x: to_lacs(x, unit_hint))
    out["% to NAV"] = out["% to NAV"].map(parse_numeric)

    out["ISIN"] = out["ISIN"].map(clean_text)
    out["Name of the Instrument"] = out["Name of the Instrument"].map(clean_text)
    out["Industry / Rating"] = out["Industry / Rating"].map(clean_text)
    out["YIELD"] = out["YIELD"].map(parse_numeric)
    out["Quantity"] = out["Quantity"].map(parse_numeric)

    # Ensure string columns are properly converted to avoid parquet issues
    string_cols = ["ISIN", "Name of the Instrument", "Industry / Rating"]
    for col in string_cols:
        out[col] = out[col].fillna("").astype(str)

    for extra in ["Coupon (%)","Coupon","Yield to Call","~YTC (AT1/Tier 2 bonds)","Maturity","Maturity Date","Industry","Rating"]:
        if extra in df.columns and extra not in out.columns:
            out[extra] = df[extra]
            # Handle NaN values and convert to appropriate types
            if extra in ["Industry", "Rating"]:
                out[extra] = out[extra].fillna("").astype(str)
            elif extra in ["Coupon (%)", "Coupon", "Yield to Call", "~YTC (AT1/Tier 2 bonds)"]:
                out[extra] = out[extra].map(parse_numeric)
            else:  # For date columns and others
                out[extra] = out[extra].fillna("")

    return out, header_map, unit_hint
