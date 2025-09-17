#!/usr/bin/env python3
"""
GENERIC MUTUAL FUND EXTRACTOR
============================

PURPOSE:
Universal extraction engine that uses YAML configuration files to extract
data from any AMC's Excel files. Replaces individual extractor scripts.

USAGE:
python extract.py --date 2025-07-31 --fund-type corporate-bond --amc ABSLF,HDFC
python extract.py --date 2025-08-31 --fund-type money-market --amc all

FEATURES:
- YAML-driven configuration for each AMC
- Support for multiple fund types (corporate-bond, money-market, etc.)
- Flexible column mapping and data processing
- Automatic file pattern matching
- Temporal tracking with "As Of Date" column

CONFIGURATION:
Each AMC has a YAML file in Ingestion_Auto/ folder that defines:
- File patterns for different fund types
- Sheet names and header rows
- Column mappings
- Data processing rules
- Validation requirements
"""

import pandas as pd
import yaml
import argparse
import re
from pathlib import Path
from datetime import datetime
import glob

def load_amc_config(amc_name):
    """Load YAML configuration for specified AMC"""
    config_path = Path(f"Ingestion_Auto/{amc_name.lower()}.yml")
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def find_files(date, fund_type, amc_config):
    """Find raw files matching AMC's pattern for the fund type"""
    raw_dir = Path(f"data/raw/{date}/{fund_type}")
    
    if not raw_dir.exists():
        return []
    
    pattern = amc_config['file_patterns'][fund_type]
    
    # Search for files matching the pattern
    files = list(raw_dir.glob(pattern))
    return files



def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def read_excel_with_config(file_path, config, fund_type):
    """Read Excel file using AMC-specific configuration"""
    fund_config = config['fund_types'][fund_type]
    file_handling = config.get('file_handling', {})
    
    # Try reading with appropriate engine
    engines = [file_handling.get('excel_engine', 'openpyxl')]
    if file_handling.get('fallback_engine'):
        engines.append(file_handling['fallback_engine'])
    
    for engine in engines:
        try:
            df = pd.read_excel(
                file_path, 
                sheet_name=fund_config['sheet_name'], 
                header=None,
                engine=engine
            )
            break
        except Exception as e:
            if engine == engines[-1]:  # Last engine failed
                raise e
            continue
    
    return df, fund_config

def extract_data_from_file(file_path, config, fund_type, as_of_date):
    """Extract data from a single file using configuration"""
    amc_name = config['amc_name']
    print(f"\n📊 Processing {amc_name}: {file_path.name}")
    
    # Read Excel file
    df, fund_config = read_excel_with_config(file_path, config, fund_type)
    print(f"📊 Raw sheet shape: {df.shape}")
    
    # Extract headers and data
    header_row = fund_config['header_row'] - 1  # Convert to 0-indexed
    data_start_row = fund_config.get('data_start_row', header_row + 1) - 1
    
    # Handle positional mapping (like Kotak)
    if config.get('file_handling', {}).get('uses_positional_mapping', False):
        # Use positional column mapping
        data_df = df.iloc[data_start_row:].reset_index(drop=True)
        
        # Filter valid ISINs using position
        isin_col = config['column_mappings']['isin_col']
        valid_mask = data_df.iloc[:, isin_col].apply(is_valid_isin)
        data_df = data_df[valid_mask].reset_index(drop=True)
        
        # Create standardized dataset using positional mapping
        standardized = pd.DataFrame({
            'Fund Name': fund_config['fund_name'],
            'AMC': amc_name,
            'ISIN': data_df.iloc[:, config['column_mappings']['isin_col']],
            'Instrument Name': data_df.iloc[:, config['column_mappings']['instrument_name_col']],
            'Market Value (Lacs)': pd.to_numeric(data_df.iloc[:, config['column_mappings']['market_value_col']], errors='coerce'),
            '% to NAV': pd.to_numeric(data_df.iloc[:, config['column_mappings']['nav_percentage_col']], errors='coerce'),
            'Yield': pd.to_numeric(data_df.iloc[:, config['column_mappings']['yield_col']], errors='coerce'),
            'Rating': data_df.iloc[:, config['column_mappings']['rating_col']],
            'Quantity': pd.to_numeric(data_df.iloc[:, config['column_mappings']['quantity_col']], errors='coerce'),
            'As Of Date': as_of_date
        })
        
    else:
        # Use named column mapping
        headers = df.iloc[header_row].fillna("").astype(str).str.strip()
        data_df = df.iloc[header_row + 1:].reset_index(drop=True)
        data_df.columns = headers
        
        # Check for required columns
        column_mappings = config['column_mappings']
        isin_col = column_mappings['isin']
        
        if isin_col not in data_df.columns:
            print(f"❌ Required column '{isin_col}' not found!")
            print(f"Available columns: {list(data_df.columns)}")
            return None
        
        # Filter valid ISINs
        valid_mask = data_df[isin_col].apply(is_valid_isin)
        data_df = data_df[valid_mask].reset_index(drop=True)
        
        # Find market value column (flexible matching)
        market_value_col = column_mappings.get('market_value')
        if market_value_col not in data_df.columns:
            # Try to find similar column
            value_cols = [col for col in data_df.columns if 'market' in col.lower() or 'value' in col.lower() or 'exposure' in col.lower()]
            if value_cols:
                market_value_col = value_cols[0]
                print(f"💰 Using market value column: '{market_value_col}'")
            else:
                print(f"❌ No market value column found!")
                return None
        
        # Create standardized dataset
        standardized = pd.DataFrame({
            'Fund Name': fund_config['fund_name'],
            'AMC': amc_name,
            'ISIN': data_df[isin_col],
            'Instrument Name': data_df.get(column_mappings.get('instrument_name', ''), ''),
            'Market Value (Lacs)': pd.to_numeric(data_df[market_value_col], errors='coerce'),
            '% to NAV': pd.to_numeric(data_df.get(column_mappings.get('nav_percentage', ''), 0), errors='coerce'),
            'Yield': pd.to_numeric(data_df.get(column_mappings.get('yield', ''), 0), errors='coerce'),
            'Rating': data_df.get(column_mappings.get('rating', ''), ''),
            'Quantity': pd.to_numeric(data_df.get(column_mappings.get('quantity', ''), 0), errors='coerce'),
            'As Of Date': as_of_date
        })
    
    # Apply data processing rules
    processing = config.get('processing', {})
    
    # Yield conversion
    if processing.get('yield_conversion') == 'decimal_to_percentage':
        sample_yield = standardized['Yield'].dropna()
        if len(sample_yield) > 0 and sample_yield.max() < 1:
            standardized['Yield'] = standardized['Yield'] * 100
            print("📊 Converted yield from decimal to percentage")
    
    # NAV conversion  
    if processing.get('nav_conversion') == 'decimal_to_percentage':
        sample_nav = standardized['% to NAV'].dropna()
        if len(sample_nav) > 0 and sample_nav.max() < 1:
            standardized['% to NAV'] = standardized['% to NAV'] * 100
            print("📊 Converted NAV from decimal to percentage")
    
    # No maturity date extraction - will be sourced from master sheet
    
    # Summary
    total_value = standardized['Market Value (Lacs)'].sum()
    print(f"✅ {amc_name}: {len(standardized)} holdings, ₹{total_value/100:,.0f} Cr")
    
    return standardized

def save_individual_extract(df, amc_name, date, fund_type):
    """Save individual AMC extract to CSV"""
    output_dir = Path(f"output/{date}/{fund_type}/individual_extracts")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"{amc_name}_verified.csv"
    df.to_csv(output_file, index=False)
    print(f"💾 Saved: {output_file}")
    
    return output_file

def main():
    parser = argparse.ArgumentParser(description='Extract mutual fund data using YAML configs')
    parser.add_argument('--date', required=True, help='Date in YYYY-MM-DD format (e.g., 2025-07-31)')
    parser.add_argument('--fund-type', required=True, help='Fund type: corporate-bond, money-market, etc.')
    parser.add_argument('--amc', required=True, help='AMC names comma-separated or "all" (e.g., ABSLF,HDFC or all)')
    
    args = parser.parse_args()
    
    # Available AMCs
    available_amcs = ['ABSLF', 'HDFC', 'ICICI', 'KOTAK', 'NIPPON', 'SBI']
    
    # Parse AMC selection
    if args.amc.lower() == 'all':
        selected_amcs = available_amcs
    else:
        selected_amcs = [amc.strip().upper() for amc in args.amc.split(',')]
        # Validate AMC names
        invalid_amcs = [amc for amc in selected_amcs if amc not in available_amcs]
        if invalid_amcs:
            print(f"❌ Invalid AMCs: {invalid_amcs}")
            print(f"Available AMCs: {available_amcs}")
            return
    
    print(f"🎯 EXTRACTING {args.fund_type.upper()} FUNDS")
    print(f"📅 Date: {args.date}")
    print(f"🏢 AMCs: {selected_amcs}")
    print("=" * 50)
    
    success_count = 0
    
    for amc in selected_amcs:
        try:
            # Load AMC configuration
            config = load_amc_config(amc)
            
            # Find files for this AMC and fund type
            files = find_files(args.date, args.fund_type, config)
            
            if not files:
                print(f"❌ {amc}: No files found matching pattern")
                continue
                
            if len(files) > 1:
                print(f"⚠️  {amc}: Multiple files found, using first: {files[0].name}")
            
            # Extract data
            df = extract_data_from_file(files[0], config, args.fund_type, args.date)
            
            if df is not None and len(df) > 0:
                # Save individual extract
                save_individual_extract(df, amc, args.date, args.fund_type)
                success_count += 1
            else:
                print(f"❌ {amc}: No valid data extracted")
                
        except Exception as e:
            print(f"❌ {amc}: Extraction failed - {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n🎯 EXTRACTION SUMMARY: {success_count}/{len(selected_amcs)} AMCs processed successfully")

if __name__ == "__main__":
    main()
