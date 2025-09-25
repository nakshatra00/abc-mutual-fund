#!/usr/bin/env python3
"""
UTI MUTUAL FUND EXTRACTOR
=========================

PURPOSE:
Extracts portfolio data from UTI Mutual Fund's consolidated portfolio report.
Handles UTI's unique single-file multi-scheme format with row-based scheme detection.

DATA SOURCE: UTI.xls (single file with multiple schemes)
FORMAT SPECIFICS:
- Single sheet "EXPOSURE" with all schemes
- Row-based scheme headers: "SCHEME: UTI - Fund Name"
- Each scheme has its own data section with consistent column headers
- Target specific schemes: Corporate Bond Fund and Money Market Fund

KEY CHALLENGES:
- Single file with 29+ different schemes
- Row-based scheme detection and boundary identification
- Dynamic header row detection per scheme
- ISIN validation across varied data quality

TECHNICAL FEATURES:
- Scheme-specific data extraction
- Robust ISIN validation and filtering  
- Standardized output format matching other extractors
- Focus on primary target schemes
"""

import pandas as pd
from pathlib import Path

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def find_target_scheme(df, target_scheme_name):
    """Find the target scheme in UTI file"""
    for idx, row in df.iterrows():
        for col in df.columns:
            cell_value = str(row[col]) if pd.notna(row[col]) else ''
            if 'SCHEME:' in cell_value.upper() and target_scheme_name.lower() in cell_value.lower():
                return {
                    'start_row': idx,
                    'header_row': idx + 3,  # Headers are 3 rows after scheme
                    'data_start_row': idx + 5,  # Data starts 5 rows after scheme
                }
    return None

def extract_uti_data():
    """Extract UTI Corporate Bond Fund data"""
    print("🔍 EXTRACTING UTI DATA")
    print("=" * 40)
    
    # File configuration
    file_path = Path("ingestion/uti/UTI.xls")
    sheet_name = "EXPOSURE"
    target_scheme = "UTI - Corporate Bond Fund"
    
    print(f"📁 File: {file_path.name}")
    print(f"📋 Sheet: {sheet_name}")
    print(f"🎯 Target Scheme: {target_scheme}")
    
    # Read Excel file
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine='xlrd')
        print(f"📊 Raw sheet shape: {df.shape}")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    # Find target scheme
    scheme_info = find_target_scheme(df, target_scheme)
    if not scheme_info:
        print(f"❌ Target scheme '{target_scheme}' not found!")
        return
    
    print(f"📍 Found scheme at row {scheme_info['start_row']}")
    
    # Extract headers and data
    headers = df.iloc[scheme_info['header_row']].fillna("").astype(str).str.strip()
    print(f"📋 Headers: {list(headers)}")
    
    # Find next scheme to determine data boundary
    next_scheme_row = None
    for idx in range(scheme_info['data_start_row'], len(df)):
        for col in df.columns:
            cell_value = str(df.iloc[idx, col]) if pd.notna(df.iloc[idx, col]) else ''
            if 'SCHEME:' in cell_value.upper():
                next_scheme_row = idx
                break
        if next_scheme_row:
            break
    
    end_row = next_scheme_row if next_scheme_row else len(df)
    data_df = df.iloc[scheme_info['data_start_row']:end_row].reset_index(drop=True)
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
    
    # Standardize columns
    standardized = pd.DataFrame()
    standardized['Fund Name'] = 'UTI Corporate Bond Fund'
    standardized['AMC'] = 'UTI'
    standardized['ISIN'] = data_df['ISIN']
    standardized['Instrument Name'] = data_df.get('NAME OF THE INSTRUMENT', '')
    standardized['Market Value (Lacs)'] = pd.to_numeric(data_df[value_col], errors='coerce')
    standardized['% to NAV'] = pd.to_numeric(data_df.get('% TO NAV', ''), errors='coerce')
    standardized['Yield'] = pd.to_numeric(data_df.get('Yield', ''), errors='coerce')
    standardized['Rating'] = data_df.get('RATING/INDUSTRY', '')
    standardized['Quantity'] = pd.to_numeric(data_df.get('QUANTITY', ''), errors='coerce')
    standardized['Maturity Date'] = None  # UTI doesn't include maturity dates
    
    # Check total portfolio value
    total_value = standardized['Market Value (Lacs)'].sum()
    print(f"💰 Total Portfolio Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
    
    # Save individual CSV
    output_path = Path("output/2025-07-31/individual_extracts/UTI_verified.csv")
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
    extract_uti_data()

import pandas as pd
import re
from pathlib import Path

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def find_scheme_boundaries(df):
    """Find all scheme boundaries in the UTI file"""
    schemes = []
    
    for idx, row in df.iterrows():
        for col in df.columns:
            cell_value = str(row[col]) if pd.notna(row[col]) else ''
            if 'SCHEME:' in cell_value.upper():
                scheme_name = cell_value.replace('SCHEME:', '').strip()
                schemes.append({
                    'name': scheme_name,
                    'start_row': idx,
                    'header_row': idx + 3,  # Headers are typically 3 rows after scheme
                    'data_start_row': idx + 5,  # Data starts 5 rows after scheme
                })
                break
    
    # Calculate end rows for each scheme
    for i, scheme in enumerate(schemes):
        if i < len(schemes) - 1:
            scheme['end_row'] = schemes[i + 1]['start_row'] - 1
        else:
            scheme['end_row'] = len(df) - 1
    
    return schemes

def categorize_scheme(scheme_name):
    """Categorize UTI scheme into fund types"""
    scheme_lower = scheme_name.lower()
    
    # Money Market Fund categories
    money_market_keywords = [
        'money market', 'liquid', 'overnight', 'ultra short', 
        'low duration', 'short duration'
    ]
    
    # Corporate Bond Fund categories  
    corporate_bond_keywords = [
        'corporate bond', 'banking', 'psu', 'credit risk',
        'medium duration', 'medium to long', 'dynamic bond',
        'gilt', 'floater'
    ]
    
    for keyword in money_market_keywords:
        if keyword in scheme_lower:
            return 'money-market'
    
    for keyword in corporate_bond_keywords:
        if keyword in scheme_lower:
            return 'corporate-bond'
    
    # Default to None for unmatched schemes
    return None

def extract_scheme_data(df, scheme_info):
    """Extract data for a specific scheme"""
    try:
        # Get headers
        headers = df.iloc[scheme_info['header_row']].fillna('').astype(str)
        
        # Extract data section
        data_section = df.iloc[scheme_info['data_start_row']:scheme_info['end_row']].copy()
        data_section.columns = headers
        
        # Find ISIN column (column 7 based on analysis)
        isin_col = None
        for col in data_section.columns:
            if 'ISIN' in str(col).upper():
                isin_col = col
                break
        
        if isin_col is None:
            print(f"❌ No ISIN column found for scheme: {scheme_info['name']}")
            return None
        
        # Filter for valid ISINs
        valid_mask = data_section[isin_col].apply(is_valid_isin)
        filtered_data = data_section[valid_mask].copy()
        
        if len(filtered_data) == 0:
            print(f"⚠️  No valid data for scheme: {scheme_info['name']}")
            return None
        
        # Standardize column names and extract data
        standardized = pd.DataFrame()
        standardized['Fund Name'] = scheme_info['name']
        standardized['AMC'] = 'UTI'
        standardized['ISIN'] = filtered_data[isin_col]
        
        # Map UTI columns to standard format
        name_col = filtered_data.columns[0] if len(filtered_data.columns) > 0 else None
        rating_col = filtered_data.columns[1] if len(filtered_data.columns) > 1 else None
        quantity_col = filtered_data.columns[2] if len(filtered_data.columns) > 2 else None
        market_value_col = filtered_data.columns[3] if len(filtered_data.columns) > 3 else None
        nav_percent_col = filtered_data.columns[4] if len(filtered_data.columns) > 4 else None
        yield_col = filtered_data.columns[9] if len(filtered_data.columns) > 9 else None
        
        standardized['Instrument Name'] = filtered_data[name_col] if name_col else ''
        standardized['Market Value (Lacs)'] = pd.to_numeric(filtered_data[market_value_col], errors='coerce') if market_value_col else 0
        standardized['% to NAV'] = pd.to_numeric(filtered_data[nav_percent_col], errors='coerce') if nav_percent_col else 0
        standardized['Yield'] = pd.to_numeric(filtered_data[yield_col], errors='coerce') if yield_col else 0
        standardized['Rating'] = filtered_data[rating_col] if rating_col else ''
        standardized['Quantity'] = pd.to_numeric(filtered_data[quantity_col], errors='coerce') if quantity_col else 0
        standardized['Maturity Date'] = None  # UTI doesn't typically include maturity dates
        
        return standardized
        
    except Exception as e:
        print(f"❌ Error extracting scheme {scheme_info['name']}: {e}")
        return None

def extract_uti_data(fund_types=['corporate-bond', 'money-market']):
    """Extract UTI data for specified fund types"""
    print("🔍 EXTRACTING UTI DATA")
    print("=" * 40)
    
    # File configuration
    file_path = Path("ingestion/uti/UTI.xls")
    sheet_name = "EXPOSURE"
    
    print(f"📁 File: {file_path.name}")
    print(f"📋 Sheet: {sheet_name}")
    
    # Read Excel file
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=str)
        print(f"📊 Raw sheet shape: {df.shape}")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return {}
    
    # Find all schemes
    all_schemes = find_scheme_boundaries(df)
    print(f"📋 Total schemes found: {len(all_schemes)}")
    
    # Categorize and filter schemes
    extracted_data = {}
    for fund_type in fund_types:
        extracted_data[fund_type] = []
    
    schemes_processed = 0
    schemes_by_type = {'corporate-bond': 0, 'money-market': 0, 'other': 0}
    
    for scheme_info in all_schemes:
        fund_type = categorize_scheme(scheme_info['name'])
        schemes_by_type[fund_type if fund_type else 'other'] += 1
        
        if fund_type in fund_types:
            print(f"📊 Processing {fund_type}: {scheme_info['name']}")
            scheme_data = extract_scheme_data(df, scheme_info)
            
            if scheme_data is not None and len(scheme_data) > 0:
                extracted_data[fund_type].append(scheme_data)
                schemes_processed += 1
                print(f"✅ Extracted {len(scheme_data)} holdings")
            else:
                print(f"⚠️  No valid data extracted")
    
    print(f"\n📊 SCHEME CATEGORIZATION:")
    print(f"   Corporate Bond: {schemes_by_type['corporate-bond']}")
    print(f"   Money Market: {schemes_by_type['money-market']}")
    print(f"   Other/Unmatched: {schemes_by_type['other']}")
    print(f"   Processed: {schemes_processed}")
    
    # Consolidate data and save individual extracts
    consolidated_data = {}
    for fund_type in fund_types:
        if extracted_data[fund_type]:
            consolidated_df = pd.concat(extracted_data[fund_type], ignore_index=True)
            consolidated_data[fund_type] = consolidated_df
            
            # Save individual extract
            output_path = Path(f"output/2025-07-31/individual_extracts/UTI_{fund_type.replace('-', '_')}_verified.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            consolidated_df.to_csv(output_path, index=False)
            
            total_value = consolidated_df['Market Value (Lacs)'].sum()
            print(f"\n💰 {fund_type.upper()} SUMMARY:")
            print(f"   Total Holdings: {len(consolidated_df)}")
            print(f"   Total Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
            print(f"   Schemes: {len(extracted_data[fund_type])}")
            print(f"   Saved: {output_path}")
            
            # Show sample holdings
            print(f"\n🔍 SAMPLE HOLDINGS ({fund_type}):")
            print("-" * 40)
            for i, (_, row) in enumerate(consolidated_df.head(3).iterrows()):
                name = str(row['Instrument Name']) if pd.notna(row['Instrument Name']) else "N/A"
                print(f"{i+1}. {name[:50]}...")
                print(f"   Fund: {row['Fund Name'][:30]}...")
                print(f"   ISIN: {row['ISIN']}")
                print(f"   Value: ₹{row['Market Value (Lacs)']:,.2f} Lacs")
                print(f"   Yield: {row['Yield']:.2f}%" if pd.notna(row['Yield']) else "   Yield: N/A")
    
    return consolidated_data

if __name__ == "__main__":
    # Extract both fund types
    data = extract_uti_data(['corporate-bond', 'money-market'])
    
    if data:
        print(f"\n🎉 UTI EXTRACTION COMPLETED!")
        print(f"   Fund types extracted: {list(data.keys())}")
        for fund_type, df in data.items():
            print(f"   {fund_type}: {len(df)} total holdings")
    else:
        print(f"\n❌ UTI EXTRACTION FAILED!")