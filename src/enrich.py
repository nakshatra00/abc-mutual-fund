import yaml, re
import pandas as pd
from datetime import datetime
from pathlib import Path
from utils import clean_text, parse_date_from_text

def load_buckets(config_dir: Path):
    with open(config_dir/"buckets.yml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_rating_map(config_dir: Path):
    with open(config_dir/"rating_map.yml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def classify_instrument(name: str) -> str:
    n = clean_text(name).lower()
    if any(k in n for k in ["treps","repo","reverse repo","cblo"]):
        return "Overnight"
    if any(k in n for k in ["t-bill","tbill","91dtb","182dtb","364dtb","t bill"]):
        return "T-Bill"
    if any(k in n for k in ["sdl","state dev loan","govt of","government of","state development loan"]):
        return "SDL"
    if any(k in n for k in ["g-sec","gs "," goi","government stock","frb goi"," frb "]):
        return "G-Sec"
    if any(k in n for k in ["cp "]):
        return "CP"
    if re.search(r"\bcp\b", n): return "CP"
    if any(k in n for k in [" cd "]):
        return "CD"
    if re.search(r"\bcd\b", n): return "CD"
    if any(k in n for k in ["perpetual","perp","at1","tier 2","tier-2","subordinated"]):
        return "AT1/Tier-2"
    return "Corporate Bond"

def normalize_rating(rating_raw: str, name: str, rating_cfg: dict) -> str:
    n = clean_text(name).lower()
    r = clean_text(rating_raw).lower()
    if any(tok in n for tok in rating_cfg.get("sovereign_tokens", [])):
        return "SOVEREIGN"
    m = re.search(r"(sov|sovereign|aaa|aa\+|aa-|aa|a\+|a-|a|bbb\+|bbb-|bbb|a1\+|a1|a2\+|a2|a3)", r, flags=re.I)
    if m:
        key = m.group(1).lower()
        return rating_cfg["map"].get(key, key.upper())
    parts = re.split(r"[ /|,]+", r)
    order = rating_cfg.get("order", [])
    grades = [rating_cfg["map"].get(p.lower(), p.upper()) for p in parts if p]
    def rank(g):
        try: return order.index(g)
        except: return len(order)+1
    if grades:
        return sorted(grades, key=rank)[-1]
    return ""

def extract_issuer(name: str) -> str:
    s = clean_text(name)
    m = re.search(r"govt of ([A-Za-z ]+)", s, flags=re.I)
    if m:
        return f"Government of {m.group(1).strip()} (SDL)"
    if re.search(r"\b(goi|government of india|g-sec|gs|t-?bill)\b", s, flags=re.I):
        return "Government of India"
    s2 = re.split(r"\b(NCD|BOND|DEBENTURE|SR|SERIES|TRANCHE|AT1|TIER|CP|CD|SECURED|UNSECURED)\b", s, flags=re.I)[0]
    return s2.strip()

def enrich(df: pd.DataFrame, config_dir: Path):
    buckets = load_buckets(config_dir)
    rating_cfg = load_rating_map(config_dir)
    as_of = datetime.strptime(buckets["as_of"], "%Y-%m-%d").date()

    # Simple maturity processing - single column approach
    # First try to find existing maturity column
    mat_col = None
    for col in df.columns:
        if "maturity" in col.lower():
            mat_col = col; break
    
    # Extract maturity date from column if available, otherwise from name
    if mat_col and df[mat_col].notna().any():
        df["Maturity Date"] = pd.to_datetime(df[mat_col], dayfirst=True, errors="coerce").dt.date
    else:
        df["Maturity Date"] = None
    
    # For rows without maturity from column, try parsing from instrument name
    missing_maturity = df["Maturity Date"].isna()
    if missing_maturity.any():
        df.loc[missing_maturity, "Maturity Date"] = df.loc[missing_maturity, "Name of the Instrument"].map(parse_date_from_text)

    # Add fund name based on AMC
    fund_names = {
        'ABSLF': 'Aditya Birla Sun Life Corporate Bond Fund',
        'HDFC': 'HDFC Corporate Bond Fund', 
        'ICICI': 'ICICI Prudential Corporate Bond Fund',
        'KOTAK': 'Kotak Corporate Bond Fund',
        'NIPPON': 'Nippon India Corporate Bond Fund',
        'SBI': 'SBI Corporate Bond Fund'
    }
    df["Fund Name"] = df["AMC"].map(fund_names)

    df["Instrument Type"] = df["Name of the Instrument"].map(classify_instrument)
    df["Issuer Name"] = df["Name of the Instrument"].map(extract_issuer)
    df["Rating_canonical"] = [normalize_rating(r, n, rating_cfg) for r, n in zip(df["Industry / Rating"], df["Name of the Instrument"])]

    def bucket_years(row):
        itype = row["Instrument Type"]
        dt = row["Maturity Date"]
        if itype in ["Overnight"]:
            return "<1"
        if itype in ["T-Bill","CP","CD"] and (dt is None or pd.isna(dt)):
            return "<1"
        if pd.isna(dt) or dt is None:
            return "Perpetual/NA"
        yrs = (dt - as_of).days / 365.25
        if yrs < 1: return "<1"
        if yrs < 3: return "1-3"
        if yrs < 5: return "3-5"
        if yrs < 7: return "5-7"
        if yrs < 10: return "7-10"
        return ">10"

    df["Maturity Bucket"] = df.apply(bucket_years, axis=1)
    return df
