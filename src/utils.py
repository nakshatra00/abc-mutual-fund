"""
MUTUAL FUND DATA PROCESSING UTILITIES
====================================

PURPOSE:
Common utility functions for data validation, cleaning, and standardization
across all mutual fund extractors and analysis modules.

CORE FUNCTIONS:
- ISIN validation and formatting
- Text cleaning and normalization
- Date parsing and standardization
- Numeric value processing
- Data type validation

ISIN VALIDATION:
- Format: IN + 10 alphanumeric characters (total 12)
- Example: INE261F08EK5 (valid), ABC123 (invalid)
- Handles pandas NaN and various input types

TEXT CLEANING:
- Removes Excel artifacts (_x000D_, line breaks)
- Standardizes spacing and formatting
- Handles None/NaN values gracefully

USAGE:
Used by all extractor modules for consistent data processing
"""

import re
import math
from datetime import datetime
from dateutil import parser as dparser
import pandas as pd

ISIN_RE = re.compile(r"^IN[A-Z0-9]{10}$")

def is_valid_isin(isin):
    """Validate Indian ISIN format: IN followed by 10 alphanumeric characters"""
    if not isin or pd.isna(isin):
        return False
    isin_str = str(isin).strip().upper()
    return bool(ISIN_RE.match(isin_str)) and len(isin_str) == 12

def clean_text(s):
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = s.replace("_x000D_", " ").replace("\r\n"," ").replace("\n"," ").replace("\r"," ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_alias(s):
    s = clean_text(s).lower()
    s = re.sub(r"[^a-z0-9 %/().:*+-]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_numeric(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).replace(",", "").replace("(", "-").replace(")", "")
    s = s.strip()
    try:
        return float(s)
    except Exception:
        return None

def parse_date_from_text(text):
    text = clean_text(text)
    if not text:
        return None
    
    # Handle HDFC pattern: "MAT DDMMYY" like "MAT 181139"
    mat_pattern = re.search(r"MAT\s+(\d{6})", text, flags=re.I)
    if mat_pattern:
        date_str = mat_pattern.group(1)
        try:
            # Parse DDMMYY format
            day = int(date_str[:2])
            month = int(date_str[2:4])
            year = int(date_str[4:6])
            # Convert 2-digit year to 4-digit (assume 20xx for years < 50, 19xx for >= 50)
            if year < 50:
                year += 2000
            else:
                year += 1900
            return datetime(year, month, day).date()
        except Exception:
            pass
    
    # Handle explicit maturity tags
    tagged = re.search(r"(Maturity|Mat\.?|MAT)\s*[:\-]?\s*([0-9A-Za-z ,/\-]+)", text, flags=re.I)
    if tagged:
        cand = tagged.group(2)
        try:
            return dparser.parse(cand, dayfirst=True, fuzzy=True).date()
        except Exception:
            pass
    
    # Handle dates in parentheses like (15/09/2028)
    paren_date = re.search(r"\((\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\)", text)
    if paren_date:
        try:
            return dparser.parse(paren_date.group(1), dayfirst=True, fuzzy=True).date()
        except Exception:
            pass
            
    # Original patterns
    pats = [
        r"\b\d{1,2}[-/](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*[-/]\d{2,4}\b",
        r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b",
        r"\b\d{1,2}(st|nd|rd|th)?\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*[, ]+\d{2,4}\b"
    ]
    for p in pats:
        m = re.search(p, text, flags=re.I)
        if m:
            try:
                return dparser.parse(m.group(0), dayfirst=True, fuzzy=True).date()
            except Exception:
                continue
    if re.search(r"\b(Call|YTC)\b", text, flags=re.I):
        return None
    return None

def years_between(as_of, dt):
    return (dt - as_of).days / 365.25

def isin_valid(x):
    s = clean_text(x)
    return bool(ISIN_RE.match(s))

def detect_unit_from_headers(headers):
    head = " ".join([h.lower() for h in headers])
    if "crore" in head or "crores" in head:
        return "crore"
    if "lac" in head or "lakh" in head or "lacs" in head:
        return "lac"
    return "unknown"

def to_lacs(value, unit_hint="lac"):
    if value is None:
        return None
    if unit_hint == "crore":
        return value * 100.0
    if unit_hint == "lac":
        return value
    return value / 100000.0
