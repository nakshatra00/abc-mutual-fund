#!/usr/bin/env python3
"""
HDFC CORPORATE BOND FUND EXTRACTOR
=================================

PURPOSE:
Extracts portfolio data from HDFC AMC's monthly corporate bond fund reports.
Handles HDFC-specific format including unique maturity date coding system.

DATA SOURCE: Monthly HDFC Corporate Bond Fund - <date>.xlsx
SHEET: Portfolio (typically first sheet)
FORMAT SPECIFICS:
- Header row: Dynamic detection
- Maturity format: "MAT 181139" = 18/11/2039
- Standard Excel .xlsx format
- Consistent column naming patterns

KEY CHALLENGES:
- Decoding HDFC's proprietary maturity date format (DDMMYY)
- Dynamic header row detection
- ISIN validation and filtering
- Handling merged cells and formatting artifacts

MATURITY PARSING:
MAT 181139 -> 18/11/39 -> 18/11/2039 (assumes 20xx century)
MAT 251225 -> 25/12/25 -> 25/12/2025

OUTPUT: Standardized CSV with decoded maturity dates
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

def parse_date_from_name(name):
    """Decode HDFC maturity format: MAT DDMMYY -> DD/MM/20YY"""
    if not name:
        return None
    
    name = str(name).strip()
    
    # HDFC uses MAT format like "MAT 181139" = 18/11/39 = 18/11/2039
    mat_pattern = r'MAT\s+(\d{2})(\d{2})(\d{2})'
    matches = re.findall(mat_pattern, name)
    
    if matches:
        try:
            dd, mm, yy = matches[0]
            year = int(f"20{yy}")
            date_str = f"{dd}/{mm}/{year}"
            return datetime.strptime(date_str, '%d/%m/%Y').date()
        except:
            pass
    
    return None

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def extract_hdfc_data():
    """Extract HDFC data with detailed verification"""
    print("🔍 EXTRACTING HDFC DATA")
    print("=" * 40)
    
    # File configuration
    file_path = Path("data/raw/2025-07-31/Monthly HDFC Corporate Bond Fund - 31 July 2025.xlsx")
    sheet_name = "HDFCMO"
    header_row = 4  # 0-indexed, so row 5 in Excel
    
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
    
    # Standardize columns
    standardized = pd.DataFrame()
    standardized['Fund Name'] = 'HDFC Corporate Bond Fund'
    standardized['AMC'] = 'HDFC'
    standardized['ISIN'] = data_df['ISIN']
    standardized['Instrument Name'] = data_df.get('Name Of the Instrument', '')
    standardized['Market Value (Lacs)'] = pd.to_numeric(data_df[value_col], errors='coerce')
    standardized['% to NAV'] = pd.to_numeric(data_df.get('% to NAV', ''), errors='coerce')
    standardized['Yield'] = pd.to_numeric(data_df.get('Yield', ''), errors='coerce')
    standardized['Rating'] = data_df.get('Industry+ /Rating', '')
    standardized['Quantity'] = pd.to_numeric(data_df.get('Quantity', ''), errors='coerce')
    
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
    output_path = Path("output/2025-07-31/individual_extracts/HDFC_verified.csv")
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
        print(f"   Yield: {row['Yield']}")
    
    # Show MAT parsing examples
    print(f"\n📅 MAT PARSING EXAMPLES:")
    print("-" * 40)
    mat_examples = standardized[standardized['Maturity Date'].notna()].head(3)
    for _, row in mat_examples.iterrows():
        print(f"'{row['Instrument Name'][:30]}...' → {row['Maturity Date']}")
    
    return standardized

if __name__ == "__main__":
    extract_hdfc_data()
