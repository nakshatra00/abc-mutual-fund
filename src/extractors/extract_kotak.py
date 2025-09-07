#!/usr/bin/env python3
"""
Kotak Corporate Bond Fund Data Extractor
Fixed extractor with correct column mapping
"""

import pandas as pd
from pathlib import Path

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def extract_kotak_data():
    """Extract Kotak data with correct column mapping"""
    print("🔍 EXTRACTING KOTAK DATA")
    print("=" * 40)
    
    # File configuration
    file_path = Path("data/raw/2025-07-31/Kotak_ConsolidatedSEBIPortfolioJuly2025.xls")
    sheet_name = "KCB"
    header_row = 1  # 0-indexed, so row 2 in Excel
    data_start_row = 5  # Data actually starts at row 6
    
    print(f"📁 File: {file_path.name}")
    print(f"📋 Sheet: {sheet_name}")
    print(f"📍 Header Row: {header_row + 1} (Excel numbering)")
    print(f"📍 Data Start Row: {data_start_row + 1} (Excel numbering)")
    
    # Read Excel file
    try:
        # For .xls files, try with openpyxl engine first, then xlrd
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine='openpyxl')
        except:
            # If openpyxl fails, try with xlrd
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine='xlrd')
        print(f"📊 Raw sheet shape: {df.shape}")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        print("💡 Try: pip install --upgrade xlrd openpyxl")
        return
    
    # Extract headers and data starting from actual data rows
    headers = df.iloc[header_row].fillna("").astype(str).str.strip()
    print(f"📋 Headers: {list(headers)}")
    
    data_df = df.iloc[data_start_row:].reset_index(drop=True)
    print(f"📊 Data rows before filtering: {len(data_df)}")
    
    # Filter valid ISINs using direct column access (Column 3 has ISINs)
    valid_mask = data_df.iloc[:, 3].apply(is_valid_isin)
    filtered_data = data_df[valid_mask].reset_index(drop=True)
    print(f"✅ Valid ISINs: {len(filtered_data)}")
    
    # Map columns correctly based on actual structure
    # Column 0: Empty, Column 1: Some rate, Column 2: Instrument Name, Column 3: ISIN, etc.
    print(f"🔍 Sample instrument names: {list(filtered_data.iloc[:, 2].dropna().head(3))}")
    print(f"🔍 Sample ISINs: {list(filtered_data.iloc[:, 3].dropna().head(3))}")
    print(f"💰 Sample values: {list(pd.to_numeric(filtered_data.iloc[:, 7], errors='coerce').dropna().head(3))}")
    
    # Standardize columns using correct mapping
    standardized = pd.DataFrame({
        'Fund Name': 'Kotak Corporate Bond Fund',
        'AMC': 'KOTAK', 
        'ISIN': filtered_data.iloc[:, 3],                                          # Column 3: ISIN
        'Instrument Name': filtered_data.iloc[:, 2],                               # Column 2: Instrument Name
        'Market Value (Lacs)': pd.to_numeric(filtered_data.iloc[:, 7], errors='coerce'),  # Column 7: Market Value
        '% to NAV': pd.to_numeric(filtered_data.iloc[:, 8], errors='coerce'),     # Column 8: % to Net Assets
        'Yield': pd.to_numeric(filtered_data.iloc[:, 5], errors='coerce'),        # Column 5: Yield
        'Rating': filtered_data.iloc[:, 4],                                       # Column 4: Rating
        'Quantity': pd.to_numeric(filtered_data.iloc[:, 6], errors='coerce')      # Column 6: Quantity
    })
    
    # Kotak typically doesn't have maturity dates in instrument names
    standardized['Maturity Date'] = None
    print("📅 No maturity dates expected in Kotak instrument names")
    
    # Check total portfolio value
    total_value = standardized['Market Value (Lacs)'].sum()
    print(f"💰 Total Portfolio Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
    
    # Save individual CSV
    output_path = Path("output/2025-07-31/individual_extracts/KOTAK_verified.csv")
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
    extract_kotak_data()
