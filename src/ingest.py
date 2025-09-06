import re
from pathlib import Path
import pandas as pd
from utils import clean_text

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())

def read_xls_dispatch(path: Path):
    with open(path, "rb") as f:
        head = f.read(8)
    if head.startswith(b"PK\x03\x04"):
        xl = pd.ExcelFile(path, engine="openpyxl")
        return xl, "openpyxl"
    if head.startswith(b"<?xml") or head.startswith(b"\xef\xbb\xbf<?xml"):
        xl = pd.ExcelFile(path)
        return xl, None
    xl = pd.ExcelFile(path)
    return xl, None

def load_sheet(path: Path, sheet_name: str):
    if path.suffix.lower() == ".xls":
        xl, _ = read_xls_dispatch(path)
    else:
        xl = pd.ExcelFile(path, engine="openpyxl")
    chosen = None
    n_hint = _norm(sheet_name)
    for s in xl.sheet_names:
        if _norm(s) == n_hint:
            chosen = s
            break
    if not chosen:
        words = [w for w in sheet_name.lower().split() if w]
        for s in xl.sheet_names:
            if all(w in s.lower() for w in words):
                chosen = s
                break
    if not chosen:
        chosen = xl.sheet_names[0]
    df0 = xl.parse(chosen, header=None, dtype=object)
    return df0, chosen, xl.sheet_names

def find_header_row(df0):
    for i in range(min(50, len(df0))):
        row_vals = [clean_text(x).lower() for x in df0.iloc[i].tolist()]
        if any(v.startswith("isin") for v in row_vals):
            return i
        row_txt = " ".join(row_vals)
        if "% to nav" in row_txt or "% to net assets" in row_txt:
            return i
    for i in range(min(50, len(df0))):
        if df0.iloc[i].notna().sum() >= 4:
            return i
    return 0

def extract_isin_based_data(df0, header_row):
    """
    Simplified extraction: find headers, then scan all rows for valid ISINs
    """
    from utils import is_valid_isin, clean_text
    
    # Get headers for schema mapping
    headers = [clean_text(c) for c in df0.iloc[header_row].values]
    
    # Find ISIN column index
    isin_col_idx = None
    for i, header in enumerate(headers):
        if 'isin' in header.lower():
            isin_col_idx = i
            break
    
    if isin_col_idx is None:
        return headers, pd.DataFrame()  # Return empty if no ISIN column found
    
    # Scan all rows for valid ISINs
    valid_rows = []
    for row_idx in range(len(df0)):
        if row_idx == header_row:  # Skip header row
            continue
            
        # Check if this row has a valid ISIN
        if isin_col_idx < len(df0.columns):
            isin_value = df0.iloc[row_idx, isin_col_idx]
            if is_valid_isin(isin_value):
                # Extract the entire row
                row_data = {}
                for col_idx, header in enumerate(headers):
                    if col_idx < len(df0.columns):
                        row_data[header] = df0.iloc[row_idx, col_idx]
                    else:
                        row_data[header] = None
                valid_rows.append(row_data)
    
    # Convert to DataFrame
    result_df = pd.DataFrame(valid_rows)
    
    return headers, result_df
