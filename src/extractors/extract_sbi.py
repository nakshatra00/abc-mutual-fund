#!/usr/bin/env python3
"""
SBI Corporate Bond Fund Data Extractor
Dedicated extractor for SBI Corporate Bond Fund
"""

import pandas as pd
from pathlib import Path

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def extract_sbi_data():
    """Extract SBI data with detailed verification"""
    print("🔍 EXTRACTING SBI DATA")
    print("=" * 40)
    
    # File configuration
    file_path = Path("data/raw/2025-07-31/SBI_All-Schemes-Monthly-Portfolio---as-on-31st-July-2025.xlsx")
    sheet_name = "SCBF"
    header_row = 5  # 0-indexed, so row 6 in Excel
    
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
    value_cols = [col for col in data_df.columns if 'market' in col.lower() or 'value' in col.lower()]
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
    
    # Standardize columns
    standardized = pd.DataFrame()
    standardized['Fund Name'] = 'SBI Corporate Bond Fund'
    standardized['AMC'] = 'SBI'
    standardized['ISIN'] = data_df['ISIN']
    standardized['Instrument Name'] = data_df.get('Name of the Instrument / Issuer', '')
    standardized['Market Value (Lacs)'] = pd.to_numeric(data_df[value_col], errors='coerce')
    standardized['% to NAV'] = pd.to_numeric(data_df.get('% to AUM', ''), errors='coerce')
    standardized['Yield'] = pd.to_numeric(data_df.get('YTM %', ''), errors='coerce')
    standardized['Rating'] = data_df.get('Rating / Industry^', '')
    standardized['Quantity'] = pd.to_numeric(data_df.get('Quantity', ''), errors='coerce')
    
    # SBI typically doesn't have maturity dates in instrument names
    standardized['Maturity Date'] = None
    print("📅 No maturity dates expected in SBI instrument names")
    
    # Check total portfolio value
    total_value = standardized['Market Value (Lacs)'].sum()
    print(f"💰 Total Portfolio Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
    
    # Save individual CSV
    output_path = Path("output/2025-07-31/individual_extracts/SBI_verified.csv")
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
    extract_sbi_data()
