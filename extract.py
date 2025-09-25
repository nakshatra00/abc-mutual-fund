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

def find_uti_scheme(df, target_scheme_name):
    """Find UTI target scheme and return scheme boundaries"""
    for idx, row in df.iterrows():
        for col in df.columns:
            cell_value = str(row[col]) if pd.notna(row[col]) else ''
            if 'SCHEME:' in cell_value.upper() and target_scheme_name.lower() in cell_value.lower():
                # Find next scheme to determine boundary
                next_scheme_row = None
                for next_idx in range(idx + 5, len(df)):
                    for next_col in df.columns:
                        next_cell = str(df.iloc[next_idx, next_col]) if pd.notna(df.iloc[next_idx, next_col]) else ''
                        if 'SCHEME:' in next_cell.upper():
                            next_scheme_row = next_idx
                            break
                    if next_scheme_row:
                        break
                
                return {
                    'start_row': idx,
                    'header_row': idx + 3,
                    'data_start_row': idx + 5,
                    'end_row': next_scheme_row if next_scheme_row else len(df)
                }
    return None

def extract_data_from_file(file_path, config, fund_type, as_of_date, use_enhanced=True):
    """Main extraction function for a single file"""
    
    # Use enhanced extractor if available and configured
    if use_enhanced and config.get('use_enhanced_extraction', True):
        try:
            from src.enhanced_extractor import extract_enhanced_data
            return extract_enhanced_data(file_path, config, fund_type, as_of_date)
        except ImportError:
            print("⚠️ Enhanced extractor not available, falling back to basic extraction")
        except Exception as e:
            print(f"⚠️ Enhanced extraction failed: {e}, falling back to basic extraction")
    
    # Basic extraction (ISIN-only) - legacy method
    amc_name = config['amc_name']
    print(f"\n📊 Processing {amc_name}: {file_path.name} (Basic Mode)")
    
    # Read Excel file
    df, fund_config = read_excel_with_config(file_path, config, fund_type)
    print(f"📊 Raw sheet shape: {df.shape}")
    
    # Special handling for UTI which has multiple schemes in single file
    if amc_name == 'UTI' and config.get('file_handling', {}).get('uses_scheme_detection'):
        target_scheme = fund_config.get('target_scheme')
        if not target_scheme:
            print(f"❌ No target scheme specified for UTI {fund_type}")
            return None
        
        # Find the specific scheme
        scheme_info = find_uti_scheme(df, target_scheme)
        if not scheme_info:
            print(f"❌ Target scheme '{target_scheme}' not found in UTI file")
            return None
        
        # Extract data for this scheme
        headers = df.iloc[scheme_info['header_row']].fillna("").astype(str).str.strip()
        data_df = df.iloc[scheme_info['data_start_row']:scheme_info['end_row']].reset_index(drop=True)
        data_df.columns = headers
        
        print(f"📊 UTI Scheme: {target_scheme}")
        print(f"📊 Data rows: {scheme_info['data_start_row']} to {scheme_info['end_row']}")
        
    else:
        # Regular processing for single-scheme files
        header_row = fund_config['header_row'] - 1  # Convert to 0-based index
        headers = df.iloc[header_row].fillna("").astype(str).str.strip()
        
        # Extract data from header row onwards
        data_df = df.iloc[header_row + 1:].reset_index(drop=True)
        data_df.columns = headers
    
    # Get column mappings
    column_mappings = config['column_mappings']
    isin_col = column_mappings['isin']
    name_col = column_mappings['instrument_name']
    value_col = column_mappings.get('market_value')
    
    # For debugging: show available columns
    print(f"📊 Available columns: {list(data_df.columns)}")
    
    # Find market value column if not exact match
    if value_col not in data_df.columns:
        # Try to find similar columns
        value_cols = [col for col in data_df.columns if 'market' in col.lower() or 'value' in col.lower() or 'exposure' in col.lower()]
        if value_cols:
            value_col = value_cols[0]
            print(f"💰 Using market value column: '{value_col}'")
        else:
            print(f"❌ No market value column found!")
            return None
    
    # Extract valid holdings (has ISIN and name) - BASIC MODE: ISIN-only
    has_isin_mask = data_df[isin_col].notna() & (data_df[isin_col].astype(str).str.strip() != '')
    has_name_mask = data_df[name_col].notna() & (data_df[name_col].astype(str).str.strip() != '')
    
    valid_holdings = data_df[has_isin_mask & has_name_mask].copy()
    
    print(f"� Valid ISIN holdings found: {len(valid_holdings)}")
    
    if len(valid_holdings) == 0:
        print(f"❌ No valid holdings found in {amc_name} {fund_type}")
        return None
    
    # Create standardized output (basic format for backward compatibility)
    output_data = pd.DataFrame()
    output_data['Fund Name'] = fund_config['fund_name']
    output_data['AMC'] = amc_name
    output_data['ISIN'] = valid_holdings[isin_col]
    output_data['Instrument Name'] = valid_holdings[name_col]
    output_data['Market Value (Lacs)'] = pd.to_numeric(valid_holdings[value_col], errors='coerce').fillna(0)
    output_data['% to NAV'] = pd.to_numeric(valid_holdings.get(column_mappings.get('nav_percentage', ''), 0), errors='coerce').fillna(0)
    output_data['Yield'] = pd.to_numeric(valid_holdings.get(column_mappings.get('yield', ''), 0), errors='coerce').fillna(0)
    output_data['Rating'] = valid_holdings.get(column_mappings.get('rating', ''), '')
    output_data['Quantity'] = pd.to_numeric(valid_holdings.get(column_mappings.get('quantity', ''), 0), errors='coerce').fillna(0)
    output_data['As Of Date'] = as_of_date
    
    # Summary
    total_value = output_data['Market Value (Lacs)'].sum()
    total_nav_pct = output_data['% to NAV'].sum()
    
    print(f"� Total ISIN Holdings: {len(output_data)}")
    print(f"💰 Total Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
    print(f"� NAV Coverage: {total_nav_pct:.1f}% (ISIN-only)")
    
    return {'main_holdings': output_data, 'total_value': total_value}

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
    available_amcs = ['ABSLF', 'HDFC', 'ICICI', 'KOTAK', 'NIPPON', 'SBI', 'UTI']
    
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
            
            # Extract data (enhanced or basic)
            result = extract_data_from_file(files[0], config, args.fund_type, args.date)
            
            if result is not None:
                # Handle both enhanced and basic extraction results
                if isinstance(result, dict) and 'main_holdings' in result:
                    # Enhanced extraction result
                    try:
                        from src.enhanced_extractor import save_enhanced_extract
                        save_enhanced_extract(result, amc, args.date, args.fund_type)
                        success_count += 1
                        print(f"✅ {amc}: Enhanced extraction successful")
                    except ImportError:
                        # Fallback to basic save
                        if len(result['main_holdings']) > 0:
                            save_individual_extract(result['main_holdings'], amc, args.date, args.fund_type)
                            success_count += 1
                            print(f"✅ {amc}: Basic save successful")
                elif hasattr(result, '__len__') and len(result) > 0:
                    # Basic extraction result (DataFrame)
                    save_individual_extract(result, amc, args.date, args.fund_type)
                    success_count += 1
                    print(f"✅ {amc}: Basic extraction successful")
                else:
                    print(f"❌ {amc}: No valid data extracted")
            else:
                print(f"❌ {amc}: Extraction returned None")
                
        except Exception as e:
            print(f"❌ {amc}: Extraction failed - {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n🎯 EXTRACTION SUMMARY: {success_count}/{len(selected_amcs)} AMCs processed successfully")

if __name__ == "__main__":
    main()
