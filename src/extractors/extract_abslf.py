#!/usr/bin/env python3
"""
ABSLF (ADITYA BIRLA SUN LIFE) CORPORATE BOND FUND EXTRACTOR
==========================================================

PURPOSE:
Extracts and standardizes portfolio data from ABSLF SEBI monthly reports.
Handles ABSLF-specific Excel format with embedded formulas and merged cells.

DATA SOURCE: ABSLF_SEBI_Monthly_Portfolio <date>.xlsm
SHEET: BSLIF
FORMAT SPECIFICS:
- Header row: 4 (Excel numbering)
- Maturity dates in parentheses: (15/09/2028)
- Yield values in decimal format (converted to percentage)
- Market values include line breaks (_x000D_)

KEY CHALLENGES:
- Yield conversion from decimal (0.065) to percentage (6.5%)
- NAV percentage validation and decimal conversion
- Maturity date parsing from instrument names
- Excel formula artifacts in column names

OUTPUT: Standardized CSV with ISIN, yields, ratings, maturities
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

def parse_date_from_name(name):
    """Extract maturity date from ABSLF instrument name format: (DD/MM/YYYY)"""
    if not name:
        return None
    
    name = str(name).strip()
    
    # ABSLF uses format like (15/09/2028), (10/02/2026)
    date_patterns = [
        r'\((\d{1,2}[/-]\d{1,2}[/-]\d{4})\)',  # (15/09/2028)
        r'\((\d{1,2}[/-]\d{1,2}[/-]\d{2})\)',   # (15/09/28)
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, name)
        if matches:
            try:
                date_str = matches[0]
                # Try different date formats
                for fmt in ['%d/%m/%Y', '%d-%m-%Y', '%d/%m/%y', '%d-%m-%y']:
                    try:
                        return datetime.strptime(date_str, fmt).date()
                    except:
                        continue
            except:
                continue
    
    return None

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def extract_abslf_data():
    """Extract ABSLF data with detailed verification"""
    print("🔍 EXTRACTING ABSLF DATA")
    print("=" * 40)
    
    # File configuration
    file_path = Path("data/raw/2025-07-31/ABSLF_SEBI_Monthly_Portfolio 31 JULY 2025.xlsm")
    sheet_name = "BSLIF"
    header_row = 3  # 0-indexed, so row 4 in Excel
    
    print(f"📁 File: {file_path.name}")
    print(f"📋 Sheet: {sheet_name}")
    print(f"📍 Header Row: {header_row + 1} (Excel numbering)")
    
    # Read Excel file
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        print(f"📊 Raw sheet shape: {df.shape}")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    # Extract headers and data
    headers = df.iloc[header_row].fillna("").astype(str).str.strip()
    print(f"📋 Headers: {list(headers)}")
    
    data_df = df.iloc[header_row + 1:].reset_index(drop=True)
    data_df.columns = headers
    
    # Check for ISIN column
    if 'ISIN' not in data_df.columns:
        print(f"❌ No ISIN column found!")
        return
    
    print(f"📊 Data rows before filtering: {len(data_df)}")
    
    # Filter valid ISINs
    valid_mask = data_df['ISIN'].apply(is_valid_isin)
    data_df = data_df[valid_mask].reset_index(drop=True)
    print(f"✅ Valid ISINs: {len(data_df)}")
    
    # Check market value column
    value_cols = [col for col in data_df.columns if 'market' in col.lower() or 'value' in col.lower()]
    print(f"💰 Value columns found: {value_cols}")
    
    if not value_cols:
        print("❌ No market value column found!")
        return
    
    # Use first value column
    value_col = value_cols[0]
    print(f"💰 Using value column: '{value_col}'")
    
    # Check % to Net Assets column
    nav_col = '% to Net Assets'
    if nav_col in data_df.columns:
        sample_nav = pd.to_numeric(data_df[nav_col], errors='coerce').dropna().head(3)
        print(f"📊 Sample % to NAV values: {list(sample_nav)}")
        
        # Check if values are in decimal format (0.045 = 4.5%) or percentage format (4.5)
        if len(sample_nav) > 0 and sample_nav.max() < 1:
            print("📊 Converting decimal format to percentage (multiplying by 100)")
            nav_values = pd.to_numeric(data_df[nav_col], errors='coerce') * 100
        else:
            nav_values = pd.to_numeric(data_df[nav_col], errors='coerce')
    else:
        print(f"❌ Column '{nav_col}' not found!")
        nav_values = None

    # Standardize columns
    yield_col = 'Yield'
    if yield_col in data_df.columns:
        sample_yield = pd.to_numeric(data_df[yield_col], errors='coerce').dropna().head(3)
        print(f"📊 Sample Yield values: {list(sample_yield)}")
        
        # Check if values are in decimal format (0.065 = 6.5%) or percentage format (6.5)
        if len(sample_yield) > 0 and sample_yield.max() < 1:
            print("📊 Converting yield decimal format to percentage (multiplying by 100)")
            yield_values = pd.to_numeric(data_df[yield_col], errors='coerce') * 100
        else:
            print("📊 Yield values already in percentage format")
            yield_values = pd.to_numeric(data_df[yield_col], errors='coerce')
    else:
        print(f"❌ Column '{yield_col}' not found!")
        yield_values = None
    
    standardized = pd.DataFrame({
        'Fund Name': 'Aditya Birla Sun Life Corporate Bond Fund',
        'AMC': 'ABSLF',
        'ISIN': data_df['ISIN'],
        'Instrument Name': data_df.get('Name of the Instrument', ''),
        'Market Value (Lacs)': pd.to_numeric(data_df[value_col], errors='coerce'),
        '% to NAV': nav_values,
        'Yield': yield_values,
        'Rating': data_df.get('Rating', ''),
        'Quantity': pd.to_numeric(data_df.get('Quantity', ''), errors='coerce')
    })
    
    # Parse maturity dates
    print("📅 Parsing maturity dates...")
    standardized['Maturity Date'] = standardized['Instrument Name'].apply(parse_date_from_name)
    
    maturity_count = standardized['Maturity Date'].notna().sum()
    maturity_pct = (maturity_count / len(standardized)) * 100
    print(f"📅 Maturity coverage: {maturity_count}/{len(standardized)} ({maturity_pct:.1f}%)")
    
    # Check total portfolio value
    total_value = standardized['Market Value (Lacs)'].sum()
    print(f"💰 Total Portfolio Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
    
    # Save individual CSV
    output_path = Path("output/2025-07-31/individual_extracts/ABSLF_verified.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    standardized.to_csv(output_path, index=False)
    print(f"💾 Saved: {output_path}")
    
    # Show verification samples
    print(f"\n🔍 VERIFICATION SAMPLES:")
    print("-" * 40)
    for i, (_, row) in enumerate(standardized.head(5).iterrows()):
        print(f"{i+1}. {row['Instrument Name'][:50]}...")
        print(f"   ISIN: {row['ISIN']}")
        print(f"   Maturity: {row['Maturity Date']}")
        print(f"   Value: ₹{row['Market Value (Lacs)']:,.2f} Lacs")
        print(f"   % to NAV: {row['% to NAV']:.2f}%")
        print(f"   Yield: {row['Yield']}")
    
    # Value distribution check
    print(f"\n📊 VALUE DISTRIBUTION CHECK:")
    print(f"   Min: ₹{standardized['Market Value (Lacs)'].min():,.0f}")
    print(f"   Max: ₹{standardized['Market Value (Lacs)'].max():,.0f}")
    print(f"   Mean: ₹{standardized['Market Value (Lacs)'].mean():,.0f}")
    print(f"   Median: ₹{standardized['Market Value (Lacs)'].median():,.0f}")
    
    return standardized

if __name__ == "__main__":
    extract_abslf_data()
