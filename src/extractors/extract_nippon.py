#!/usr/bin/env python3
"""
NIPPON INDIA MUTUAL FUND EXTRACTOR
==================================

PURPOSE:
Extracts portfolio data from Nippon India Mutual Fund's monthly reports.
Handles Nippon's specific Excel formatting and data structure.

DATA SOURCE: NIMF-MONTHLY-PORTFOLIO-<date>.xls
FORMAT SPECIFICS:
- Old .xls format requiring xlrd engine
- Dynamic header detection needed
- Standard corporate bond fund layout
- No maturity information in instrument names

KEY FEATURES:
- ISIN validation and data filtering
- Market value and percentage calculations
- Rating and yield data extraction
- Robust error handling for Excel variations

TECHNICAL CONSIDERATIONS:
- Requires xlrd 2.0+ for .xls compatibility
- Dynamic column mapping for flexibility
- Handles missing data gracefully
"""

import pandas as pd
from pathlib import Path

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def extract_nippon_data():
    """Extract Nippon data with detailed verification"""
    print("🔍 EXTRACTING NIPPON DATA")
    print("=" * 40)
    
    # File configuration
    file_path = Path("data/raw/2025-07-31/NIMF-MONTHLY-PORTFOLIO-31-July-25.xls")
    sheet_name = "IP"
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
        print(f"Available columns: {list(data_df.columns)}")
        return
    
    print(f"📊 Data rows before filtering: {len(data_df)}")
    
    # Filter valid ISINs
    valid_mask = data_df['ISIN'].apply(is_valid_isin)
    data_df = data_df[valid_mask].reset_index(drop=True)
    print(f"✅ Valid ISINs: {len(data_df)}")
    
    # Check market value column
    value_cols = [col for col in data_df.columns if 'market' in col.lower() or 'value' in col.lower() or 'fair' in col.lower()]
    print(f"💰 Value columns found: {value_cols}")
    
    if not value_cols:
        print("❌ No market value column found!")
        return
    
    # Use first value column
    value_col = value_cols[0]
    print(f"💰 Using value column: '{value_col}'")
    
    # Additional filtering for data quality
    print(f"🔍 Applying additional data quality filters...")
    
    # Filter out rows with NaN market values
    before_nan_filter = len(data_df)
    data_df = data_df[pd.notna(data_df[value_col])].reset_index(drop=True)
    print(f"   ✅ Removed {before_nan_filter - len(data_df)} rows with NaN market values")
    
    # Filter out rows with corrupt rating values (numeric ratings instead of text)
    before_rating_filter = len(data_df)
    rating_col = 'Industry / Rating'
    if rating_col in data_df.columns:
        # Remove rows where rating is purely numeric (likely corrupt)
        rating_mask = data_df[rating_col].apply(lambda x: not (isinstance(x, (int, float)) and not pd.isna(x)))
        data_df = data_df[rating_mask].reset_index(drop=True)
        print(f"   ✅ Removed {before_rating_filter - len(data_df)} rows with corrupt rating values")
    
    # Remove exact duplicate ISINs (keep first occurrence)
    before_dup_filter = len(data_df)
    data_df = data_df.drop_duplicates(subset=['ISIN'], keep='first').reset_index(drop=True)
    print(f"   ✅ Removed {before_dup_filter - len(data_df)} duplicate ISIN entries")
    
    print(f"🎯 Final valid holdings: {len(data_df)}")
    
    # Check if values are in Lacs or need conversion
    sample_values = pd.to_numeric(data_df[value_col], errors='coerce').dropna().head(5)
    print(f"💰 Sample values: {list(sample_values)}")
    
    # Check % to NAV column and convert from decimal to percentage
    nav_col = '% to NAV'
    if nav_col in data_df.columns:
        sample_nav = pd.to_numeric(data_df[nav_col], errors='coerce').dropna().head(3)
        print(f"📊 Sample % to NAV values: {list(sample_nav)}")
        
        # Values are in decimal format (0.077 = 7.7%) - convert to percentage
        if len(sample_nav) > 0 and sample_nav.max() <= 1:
            print("📊 Converting decimal format to percentage (multiplying by 100)")
            nav_values = pd.to_numeric(data_df[nav_col], errors='coerce') * 100
        else:
            nav_values = pd.to_numeric(data_df[nav_col], errors='coerce')
    else:
        print(f"❌ Column '{nav_col}' not found!")
        nav_values = None

    # Check % Yield column and convert from decimal to percentage
    yield_col = 'YIELD'
    if yield_col in data_df.columns:
        sample_yield = pd.to_numeric(data_df[yield_col], errors='coerce').dropna().head(3)
        print(f"📊 Sample Yield values: {list(sample_yield)}")
        
        # Values are in decimal format (0.063 = 6.3%) - convert to percentage
        if len(sample_yield) > 0 and sample_yield.max() <= 1:
            print("📊 Converting yield decimal format to percentage (multiplying by 100)")
            yield_values = pd.to_numeric(data_df[yield_col], errors='coerce') * 100
        else:
            yield_values = pd.to_numeric(data_df[yield_col], errors='coerce')
    else:
        print(f"❌ Column '{yield_col}' not found!")
        yield_values = None

    # Standardize columns
    standardized = pd.DataFrame({
        'Fund Name': 'Nippon India Corporate Bond Fund',
        'AMC': 'NIPPON',
        'ISIN': data_df['ISIN'],
        'Instrument Name': data_df.get('Name of the Instrument', ''),
        'Market Value (Lacs)': pd.to_numeric(data_df[value_col], errors='coerce'),
        '% to NAV': nav_values,
        'Yield': yield_values,
        'Rating': data_df.get('Industry / Rating', ''),
        'Quantity': pd.to_numeric(data_df.get('Quantity', ''), errors='coerce')
    })
    
    # Nippon typically doesn't have maturity dates in instrument names
    standardized['Maturity Date'] = None
    print("📅 No maturity dates expected in Nippon instrument names")
    
    # Check total portfolio value
    total_value = standardized['Market Value (Lacs)'].sum()
    print(f"💰 Total Portfolio Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
    
    # Save individual CSV
    output_path = Path("output/2025-07-31/individual_extracts/NIPPON_verified.csv")
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
    
    # Value distribution check
    print(f"\n📊 VALUE DISTRIBUTION CHECK:")
    print(f"   Min: ₹{standardized['Market Value (Lacs)'].min():,.0f}")
    print(f"   Max: ₹{standardized['Market Value (Lacs)'].max():,.0f}")
    print(f"   Mean: ₹{standardized['Market Value (Lacs)'].mean():,.0f}")
    print(f"   Median: ₹{standardized['Market Value (Lacs)'].median():,.0f}")
    
    return standardized

if __name__ == "__main__":
    extract_nippon_data()
