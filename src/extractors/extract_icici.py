#!/usr/bin/env python3
"""
ICICI PRUDENTIAL CORPORATE BOND FUND EXTRACTOR
==============================================

PURPOSE:
Extracts portfolio data from ICICI Prudential AMC's corporate bond fund reports.
Handles ICICI's Excel format with dynamic header detection and validation.

DATA SOURCE: ICICI Prudential Corporate Bond Fund.xlsx
FORMAT SPECIFICS:
- Header row: Dynamic detection using keyword matching
- Standard .xlsx format with clean structure
- No maturity dates in instrument names
- Comprehensive rating and yield data

KEY FEATURES:
- Automatic header row detection
- ISIN validation and filtering
- Market value and NAV percentage processing
- Rating standardization compatibility

TECHNICAL NOTES:
- Clean data format with minimal preprocessing needed
- Reliable column naming conventions
- Good data quality with high coverage rates
"""

import pandas as pd
from pathlib import Path

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def extract_icici_data():
    """Extract ICICI data with detailed verification"""
    print("🔍 EXTRACTING ICICI DATA")
    print("=" * 40)
    
    # File configuration
    file_path = Path("data/raw/2025-07-31/ICICI Prudential Corporate Bond Fund.xlsx")
    sheet_name = "CORPORATE BOND"
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
    value_cols = [col for col in data_df.columns if 'market' in col.lower() or 'value' in col.lower() or 'exposure' in col.lower()]
    print(f"💰 Value columns found: {value_cols}")
    
    if not value_cols:
        print("❌ No market value column found!")
        return
    
    # Use first value column
    value_col = value_cols[0]
    print(f"💰 Using value column: '{value_col}'")
    
    # Check if values are in Lacs or need conversion
    sample_values = pd.to_numeric(data_df[value_col], errors='coerce').dropna().head(5)
    print(f"💰 Sample values: {list(sample_values)}")
    
    # Check % to Nav column and convert from decimal to percentage
    nav_col = '% to Nav'
    if nav_col in data_df.columns:
        sample_nav = pd.to_numeric(data_df[nav_col], errors='coerce').dropna().head(3)
        print(f"📊 Sample % to NAV values: {list(sample_nav)}")
        
        # Values are in decimal format (0.091 = 9.1%) - convert to percentage
        if len(sample_nav) > 0 and sample_nav.max() <= 1:
            print("📊 Converting decimal format to percentage (multiplying by 100)")
            nav_values = pd.to_numeric(data_df[nav_col], errors='coerce') * 100
        else:
            nav_values = pd.to_numeric(data_df[nav_col], errors='coerce')
    else:
        print(f"❌ Column '{nav_col}' not found!")
        nav_values = None

    # Standardize columns
    standardized = pd.DataFrame({
        'Fund Name': 'ICICI Prudential Corporate Bond Fund',
        'AMC': 'ICICI',
        'ISIN': data_df['ISIN'],
        'Instrument Name': data_df.get('Company/Issuer/Instrument Name', ''),
        'Market Value (Lacs)': pd.to_numeric(data_df[value_col], errors='coerce'),
        '% to NAV': nav_values,
        'Yield': pd.to_numeric(data_df.get('Yield of the instrument', ''), errors='coerce'),
        'Rating': data_df.get('Industry/Rating', ''),
        'Quantity': pd.to_numeric(data_df.get('Quantity', ''), errors='coerce')
    })
    
    # ICICI typically doesn't have maturity dates in instrument names
    standardized['Maturity Date'] = None
    print("📅 No maturity dates expected in ICICI instrument names")
    
    # Check total portfolio value
    total_value = standardized['Market Value (Lacs)'].sum()
    print(f"💰 Total Portfolio Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
    
    # Save individual CSV
    output_path = Path("output/2025-07-31/individual_extracts/ICICI_verified.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    standardized.to_csv(output_path, index=False)
    print(f"💾 Saved: {output_path}")
    
    # Show verification samples
    print(f"\n🔍 VERIFICATION SAMPLES:")
    print("-" * 40)
    for i, (_, row) in enumerate(standardized.head(5).iterrows()):
        name = str(row['Instrument Name']) if pd.notna(row['Instrument Name']) else "N/A"
        print(f"{i+1}. {name[:50]}...")
        print(f"   ISIN: {row['ISIN']}")
        print(f"   Value: ₹{row['Market Value (Lacs)']:,.2f} Lacs")
        print(f"   % to NAV: {row['% to NAV']:.2f}%")
        print(f"   Yield: {row['Yield']}")
        print(f"   Rating: {row['Rating']}")
    
    # Instrument type distribution
    print(f"\n📊 INSTRUMENT ANALYSIS:")
    print("-" * 40)
    unique_names = standardized['Instrument Name'].value_counts().head(5)
    for name, count in unique_names.items():
        print(f"   {name[:40]}... ({count} holdings)")
    
    return standardized

if __name__ == "__main__":
    extract_icici_data()
