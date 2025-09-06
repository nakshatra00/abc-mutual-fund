# src/gen_data_description.py
# Auto-generates a Markdown "Data Description" page by inspecting each AMC workbook.
# - Handles .xlsx/.xlsm via openpyxl
# - Handles legacy .xls via xlrd==1.2.0
# - Detects mislabelled .xls that are actually .xlsx (ZIP) or Excel 2003 XML
# - Parses Excel 2003 XML (SpreadsheetML) directly with ElementTree
# - Uses strict sheet selection from your hints (e.g., HDFCMO for HDFC)
# - Cleans headers (e.g., removes _x000D_) and maps to canonical schema

import re
import os
import sys
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

RAW_DIR_DEFAULT = "data/raw/2025-07-31"
OUT_MD_DEFAULT  = "output/2025-07-31/data_description.md"

# ----------------------------- Canonical schema ------------------------------

CANON: Dict[str, List[str]] = {
    "ISIN": ["isin", "isin no", "isin code", "isin number"],
    "Name of the Instrument": [
        "name of the instrument",
        "security name",
        "name of issuer",
        "issuer",
        "instrument",
        "company/issuer/instrument name",
        "name of instrument",
    ],
    "Industry / Rating": [
        "industry / rating",
        "industry",
        "rating",
        "credit rating",
        "rating / industry",
        "industry/rating",
        "industry/ rating",
    ],
    "Quantity": ["quantity", "qty", "units", "no. of units", "quantity (face)"],
    "Market/Fair Value (Rs. in Lacs)": [
        "market value",
        "fair value",
        "current value",
        "book value",
        "exposure/market value(rs.lakh)",
        "value (rs. in lacs)",
        "value (rs. in crores)",
        "market/fair value (rs.in lacs)",
        "market value (rs.in lacs)",
    ],
    "% to NAV": ["% to nav", "% to aum", "% of net assets", "% to net assets"],
    "YIELD": ["yield", "ytm", "yield (%)", "yield to maturity", "yield of the instrument"],
}

def _norm_alias(s: str) -> str:
    s = s.lower()
    s = s.replace("_x000d_", " ")
    s = s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    s = re.sub(r"[^a-z0-9 %/().*-]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

ALIAS_TO_CANON: Dict[str, str] = {}
for canon, aliases in CANON.items():
    for a in aliases:
        ALIAS_TO_CANON[_norm_alias(a)] = canon

# ----------------------------- AMC hints -------------------------------------

SHEET_HINTS = {
    "ABSLF": "BSLIF",                       # Aditya Birla Sun Life
    "KOTAK": "KCB",
    "SBI":   "SCBF",
    "NIPPON":"SD",                          # Nippon India / NIMF
    "NIMF":  "SD",
    "ICICI": "ICICI Prudential Corporate Bond Fund",
    # Per your instruction, HDFC uses HDFCMO (avoid the Derivative sheet)
    "HDFC":  "HDFCMO",
}

AMC_PATTERNS = [
    ("ABSLF",  re.compile(r"(ABSLF|ADITYA\s*BIRLA|BIRLA)", re.I)),
    ("KOTAK",  re.compile(r"KOTAK", re.I)),
    ("SBI",    re.compile(r"\bSBI\b|State\s*Bank", re.I)),
    ("NIPPON", re.compile(r"(NIPPON|RELIANCE|NIMF)", re.I)),
    ("ICICI",  re.compile(r"\bICICI\b", re.I)),
    ("HDFC",   re.compile(r"\bHDFC\b", re.I)),
]

ENGINE_BY_EXT = {
    ".xlsx": "openpyxl",
    ".xlsm": "openpyxl",
    ".xls":  None,  # handled via dispatcher
}

# ----------------------------- Utilities -------------------------------------

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())

def detect_amc(fname: str) -> Optional[str]:
    for amc, pat in AMC_PATTERNS:
        if pat.search(fname):
            return amc
    return None

def _clean_header_token(x) -> str:
    s = "" if x is None or (isinstance(x, float) and math.isnan(x)) else str(x)
    s = s.replace("_x000D_", " ")
    s = s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def find_header_row(df: pd.DataFrame) -> Optional[int]:
    max_scan = min(50, len(df))
    for i in range(max_scan):
        row_vals = [str(x).strip().lower() for x in df.iloc[i].tolist()]
        if any(rv.startswith("isin") for rv in row_vals):
            return i
        row_txt = " ".join(row_vals)
        if ("% to nav" in row_txt) or ("% to net assets" in row_txt):
            return i
    for i in range(max_scan):
        if df.iloc[i].notna().sum() >= 4:
            return i
    return None

def guess_unit_note_from_rows(rows: List[List[str]]) -> str:
    text = " ".join([str(x).lower() for r in rows for x in r if isinstance(x, (str, int, float))])
    if "crore" in text or "crores" in text:
        return "Rs. in Crores (convert ×100 to Lacs)"
    if "lakh" in text or "lac" in text or "lacs" in text:
        return "Rs. in Lacs"
    return "Unknown (assume plain INR unless a nearby note says otherwise)"

def alias_mapping(raw_cols: List[str]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {k: None for k in CANON.keys()}
    for raw in raw_cols:
        c_clean = _norm_alias(raw)
        if c_clean in ALIAS_TO_CANON:
            canon = ALIAS_TO_CANON[c_clean]
            if not mapping[canon]:
                mapping[canon] = raw
            continue
        for alias, canon in ALIAS_TO_CANON.items():
            if alias in c_clean and not mapping[canon]:
                mapping[canon] = raw
                break
    return mapping

def pick_sheet(sheet_names: List[str], hint: Optional[str]) -> str:
    if hint:
        n_hint = _norm(hint)
        for s in sheet_names:
            if _norm(s) == n_hint:
                return s
        hint_words = [w for w in hint.lower().split() if w]
        for s in sheet_names:
            s_low = s.lower()
            if all(w in s_low for w in hint_words):
                return s
    for s in sheet_names:
        if re.search(r"corp.*bond|bond.*corp", s, re.I):
            return s
    return sheet_names[0]

# ------------------------ Excel 2003 XML (SpreadsheetML) ----------------------

def parse_excel_2003_xml(path: Path, sheet_hint: Optional[str]) -> Tuple[pd.DataFrame, List[str], str]:
    """
    Minimal parser for Excel 2003 XML (SpreadsheetML). Extracts the specified
    worksheet (by hint/heuristic), returns DataFrame of raw cell strings.
    """
    import xml.etree.ElementTree as ET

    tree = ET.parse(str(path))
    root = tree.getroot()

    # Determine namespaces
    ns = {}
    if root.tag.startswith("{"):
        uri = root.tag.split("}")[0][1:]
        ns["ss"] = uri  # often 'urn:schemas-microsoft-com:office:spreadsheet'
    # Fallback common ns
    ns.setdefault("ss", "urn:schemas-microsoft-com:office:spreadsheet")

    # Collect worksheets
    ws_list = []
    for ws in root.findall(".//{urn:schemas-microsoft-com:office:spreadsheet}Worksheet"):
        name = ws.attrib.get("{urn:schemas-microsoft-com:office:spreadsheet}Name") or ws.attrib.get("Name") or ""
        ws_list.append((name, ws))
    sheet_names = [n for n, _ in ws_list]
    if not ws_list:
        # Try generic lookup with any prefix (namespaced)
        for ws in root.iter():
            if ws.tag.endswith("Worksheet"):
                name = ws.attrib.get("{urn:schemas-microsoft-com:office:spreadsheet}Name") or ws.attrib.get("Name") or ""
                ws_list.append((name, ws))
        sheet_names = [n for n, _ in ws_list]

    # Choose sheet
    chosen_name = pick_sheet(sheet_names, sheet_hint)
    chosen_ws = None
    for n, ws in ws_list:
        if _norm(n) == _norm(chosen_name):
            chosen_ws = ws
            break
    if chosen_ws is None and ws_list:
        chosen_name, chosen_ws = ws_list[0]  # fallback

    # Extract rows/cells
    rows = []
    max_cols = 0

    def _cell_text(cell) -> str:
        # Data may be nested: <Cell><Data>value</Data></Cell>
        txt = ""
        for child in cell.iter():
            if child.tag.endswith("Data") and (child.text is not None):
                txt = child.text
        return "" if txt is None else str(txt)

    # Find the Table element within worksheet
    table = None
    for el in chosen_ws.iter():
        if el.tag.endswith("Table"):
            table = el
            break
    if table is None:
        return pd.DataFrame([]), sheet_names, chosen_name

    current_row_idx = 0
    for r in table:
        if not r.tag.endswith("Row"):
            continue
        row_vals = []
        col_idx = 0
        for cell in r:
            if not cell.tag.endswith("Cell"):
                continue
            # Handle ss:Index (sparse columns)
            idx_attr = cell.attrib.get("{urn:schemas-microsoft-com:office:spreadsheet}Index") or cell.attrib.get("Index")
            if idx_attr:
                idx = int(idx_attr) - 1  # 1-based in SpreadsheetML
                while col_idx < idx:
                    row_vals.append("")
                    col_idx += 1
            row_vals.append(_cell_text(cell))
            col_idx += 1
        max_cols = max(max_cols, len(row_vals))
        rows.append(row_vals)

    # Normalize row widths
    norm_rows = [row + [""] * (max_cols - len(row)) for row in rows]
    df = pd.DataFrame(norm_rows)
    return df, sheet_names, chosen_name

# ----------------------------- XLS dispatch ----------------------------------

def sniff_xls_flavor(path: Path) -> str:
    """
    For .xls extension, sniff the actual content:
    - return 'zipxlsx' if it's actually a ZIP (xlsx/xlsm) mislabelled
    - return 'xml2003' if SpreadsheetML (starts with <?xml)
    - return 'biff' for real legacy .xls BIFF
    """
    with open(path, "rb") as f:
        head = f.read(8)
    if head.startswith(b"PK\x03\x04"):
        return "zipxlsx"
    if head.startswith(b"\xff\xd8") or head.startswith(b"\x89PNG"):
        return "unknown"
    # XML 2003 often starts with '<?xml'
    if head.startswith(b"<?xml") or head.startswith(b"\xef\xbb\xbf<?xml"):
        return "xml2003"
    # Heuristic: BIFF8 XLS usually starts with D0 CF 11 E0 (OLE header)
    if head.startswith(b"\xD0\xCF\x11\xE0"):
        return "biff"
    # Fallback: try BIFF
    return "biff"

def read_xls_dispatch(path: Path, sheet_hint: Optional[str]) -> Tuple[pd.DataFrame, List[str], str]:
    flavor = sniff_xls_flavor(path)
    if flavor == "zipxlsx":
        # Mislabelled xlsx—use openpyxl through pandas
        xl = pd.ExcelFile(path, engine="openpyxl")
        sheet_to_use = pick_sheet(xl.sheet_names, sheet_hint)
        df0 = xl.parse(sheet_to_use, header=None, dtype=object)
        return df0, xl.sheet_names, sheet_to_use
    if flavor == "xml2003":
        # Parse SpreadsheetML
        return parse_excel_2003_xml(path, sheet_hint)
    # Legacy BIFF via xlrd==1.2.0
    import xlrd  # ensure version 1.2.0
    book = xlrd.open_workbook(str(path), on_demand=True)
    sheet_names = [sh.name for sh in book.sheets()]
    sheet_to_use = pick_sheet(sheet_names, sheet_hint)
    sh = book.sheet_by_name(sheet_to_use)
    rows = [[sh.cell_value(r, c) for c in range(sh.ncols)] for r in range(sh.nrows)]
    return pd.DataFrame(rows), sheet_names, sheet_to_use

# ------------------------------ Describe -------------------------------------

def describe_workbook(path: Path, amc: str, sheet_hint: Optional[str]) -> Dict:
    ext = path.suffix.lower()

    if ext == ".xls":
        df0, sheet_names, sheet_to_use = read_xls_dispatch(path, sheet_hint)
    else:
        engine = ENGINE_BY_EXT.get(ext, None)
        try:
            xl = pd.ExcelFile(path, engine=engine)
        except Exception:
            xl = pd.ExcelFile(path)
        sheet_to_use = pick_sheet(xl.sheet_names, sheet_hint)
        df0 = xl.parse(sheet_to_use, header=None, dtype=object)
        sheet_names = xl.sheet_names

    hdr_row = find_header_row(df0) or 0
    raw_cols = [_clean_header_token(c) for c in df0.iloc[hdr_row].tolist()]
    raw_cols = [c for c in raw_cols if c]

    sample_rows = []
    for i in range(hdr_row, min(hdr_row + 3, len(df0))):
        sample_rows.append([_clean_header_token(x) for x in df0.iloc[i].tolist()])
    unit_note = guess_unit_note_from_rows(sample_rows)

    mapping = alias_mapping(raw_cols)

    section_tokens: List[str] = []
    for j in range(hdr_row + 1, min(hdr_row + 50, len(df0))):
        try:
            v = _clean_header_token(df0.iloc[j, 0])
        except Exception:
            v = ""
        if any(tok in v.lower() for tok in ["corporate bond", "debenture", "money market", "treps", "g-sec", "total", "grand total"]):
            section_tokens.append(v)

    return {
        "amc": amc,
        "file": path.name,
        "sheet": sheet_to_use,
        "header_row": hdr_row,
        "raw_columns": raw_cols,
        "unit_note": unit_note,
        "alias_mapping": mapping,
        "sections_seen": section_tokens[:6],
        "sheet_names": sheet_names,
    }

# ----------------------------- Markdown emit ---------------------------------

def emit_markdown(descriptions: List[Dict]) -> str:
    lines: List[str] = []
    lines.append("# Data Description — Corporate Bond Fund Portfolios (As of **31-Jul-2025**)\n")
    lines.append("> Auto-generated from the Excel files in `data/raw/2025-07-31`.\n")

    for desc in descriptions:
        lines.append(f"## {desc['amc']}")
        lines.append(f"- **File:** `{desc['file']}`")
        lines.append(f"- **Sheet used:** `{desc['sheet']}`")
        lines.append(f"- **Header row index:** `{desc['header_row']}`")
        lines.append(f"- **Unit note (guess):** {desc['unit_note']}\n")

        lines.append("**Raw column headers**")
        if desc["raw_columns"]:
            cols = ", ".join([f"`{c}`" for c in desc["raw_columns"]])
            lines.append(cols)
        else:
            lines.append("_(none detected)_")
        lines.append("")

        lines.append("**Alias → Canonical mapping (what we’ll use in code)**")
        for canon in CANON.keys():
            raw = desc["alias_mapping"].get(canon) or "_missing_"
            lines.append(f"- **{canon}** ← {raw}")
        lines.append("\n---\n")

    return "\n".join(lines)

# --------------------------------- CLI ---------------------------------------

def main(raw_dir: str, out_md: str):
    raw_dir_p = Path(raw_dir)
    out_path = Path(out_md)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    descriptions: List[Dict] = []
    for p in sorted(raw_dir_p.iterdir()):
        # Skip temp/lock files, non-Excel
        if p.name.startswith("~$"):
            continue
        if p.suffix.lower() not in (".xls", ".xlsx", ".xlsm"):
            continue

        amc = detect_amc(p.name)
        if not amc:
            continue
        sheet_hint = SHEET_HINTS.get(amc)

        try:
            desc = describe_workbook(p, amc, sheet_hint)
            descriptions.append(desc)
        except Exception as e:
            descriptions.append({
                "amc": amc,
                "file": p.name,
                "sheet": "(error)",
                "header_row": None,
                "raw_columns": [],
                "unit_note": f"ERROR: {e}",
                "alias_mapping": {k: None for k in CANON.keys()},
                "sections_seen": [],
                "sheet_names": [],
            })

    md = emit_markdown(descriptions)
    out_path.write_text(md, encoding="utf-8")
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    raw_dir = sys.argv[1] if len(sys.argv) > 1 else RAW_DIR_DEFAULT
    out_md  = sys.argv[2] if len(sys.argv) > 2 else OUT_MD_DEFAULT
    main(raw_dir, out_md)
