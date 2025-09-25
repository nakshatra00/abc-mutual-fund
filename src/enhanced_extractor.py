#!/usr/bin/env python3
"""
ENHANCED PORTFOLIO EXTRACTOR
============================

PURPOSE:
Enhanced extraction engine that captures ALL portfolio components:
- ISIN-based securities (existing)
- Cash and cash equivalents
- Derivatives and structured products
- Summary totals for validation
- Special situations (defaults, segregated assets)

FEATURES:
- Comprehensive security type classification
- Portfolio completeness validation
- NAV reconciliation checks
- Quality metrics and warnings
"""

import pandas as pd
import yaml
import re
from pathlib import Path

def is_valid_isin(isin):
    """Check if ISIN is valid"""
    if not isin or pd.isna(isin):
        return False
    isin = str(isin).strip().upper()
    return len(isin) == 12 and isin[:2].isalpha()

def classify_security_type(instrument_name, isin, config):
    """Classify security based on name and ISIN"""
    if not instrument_name or pd.isna(instrument_name):
        return 'unknown'
    
    name = str(instrument_name).strip().upper()
    security_types = config.get('security_types', {})
    
    # Check for summary rows first (these should be excluded from totals)
    summary_keywords = security_types.get('summary_rows', {}).get('keywords', [])
    if any(keyword.upper() in name for keyword in summary_keywords):
        return 'summary_row'
    
    # Check for cash equivalents
    cash_keywords = security_types.get('cash_equivalents', {}).get('keywords', [])
    if any(keyword.upper() in name for keyword in cash_keywords):
        return 'cash_equivalent'
    
    # Check for derivatives
    derivative_keywords = security_types.get('derivatives', {}).get('keywords', [])
    if any(keyword.upper() in name for keyword in derivative_keywords):
        return 'derivative'
    
    # Check for special situations
    special_keywords = security_types.get('special_situations', {}).get('keywords', [])
    if any(keyword.upper() in name for keyword in special_keywords):
        return 'special_situation'
    
    # Check if it has valid ISIN
    if is_valid_isin(isin):
        return 'isin_security'
    
    # Non-ISIN instrument that doesn't match other categories
    return 'other_security'

def extract_enhanced_data(file_path, config, fund_type, as_of_date):
    """Enhanced data extraction capturing all portfolio components"""
    amc_name = config['amc_name']
    print(f"\n📊 Enhanced Processing {amc_name}: {file_path.name}")
    
    # Read Excel file (reuse existing logic)
    from extract import read_excel_with_config, find_uti_scheme
    
    df, fund_config = read_excel_with_config(file_path, config, fund_type)
    print(f"📊 Raw sheet shape: {df.shape}")
    
    # Special handling for UTI multi-scheme file
    if amc_name == 'UTI' and config.get('file_handling', {}).get('uses_scheme_detection'):
        target_scheme = fund_config.get('target_scheme')
        if not target_scheme:
            print(f"❌ No target scheme specified for UTI {fund_type}")
            return None
            
        scheme_info = find_uti_scheme(df, target_scheme)
        if not scheme_info:
            print(f"❌ Target scheme '{target_scheme}' not found in UTI file")
            return None
            
        headers = df.iloc[scheme_info['header_row']].fillna("").astype(str).str.strip()
        data_df = df.iloc[scheme_info['data_start_row']:scheme_info['end_row']].reset_index(drop=True)
        data_df.columns = headers
    else:
        # Normal processing for other AMCs
        header_row = fund_config['header_row'] - 1
        headers = df.iloc[header_row].fillna("").astype(str).str.strip()
        data_df = df.iloc[header_row + 1:].reset_index(drop=True)
        data_df.columns = headers
    
    # Get column mappings
    column_mappings = config['column_mappings']
    isin_col = column_mappings['isin']
    name_col = column_mappings['instrument_name']
    value_col = column_mappings.get('market_value')
    
    # Find market value column if not exact match
    if value_col not in data_df.columns:
        value_cols = [col for col in data_df.columns if 'market' in col.lower() or 'value' in col.lower() or 'exposure' in col.lower()]
        if value_cols:
            value_col = value_cols[0]
            print(f"💰 Using market value column: '{value_col}'")
        else:
            print(f"❌ No market value column found!")
            return None
    
    # Enhanced extraction - capture ALL rows with instrument names
    has_name_mask = data_df[name_col].notna() & (data_df[name_col].astype(str).str.strip() != '')
    all_entries = data_df[has_name_mask].copy()
    
    print(f"📊 Total entries found: {len(all_entries)}")
    
    # Classify each entry
    all_entries['Security_Type'] = all_entries.apply(
        lambda row: classify_security_type(
            row[name_col], 
            row.get(isin_col, ''), 
            config
        ), axis=1
    )
    
    # Count by type
    type_counts = all_entries['Security_Type'].value_counts()
    print(f"📊 Security type breakdown:")
    for sec_type, count in type_counts.items():
        print(f"   {sec_type}: {count}")
    
    # Create comprehensive dataset
    enhanced_data = pd.DataFrame()
    enhanced_data['Fund Name'] = fund_config['fund_name']
    enhanced_data['AMC'] = amc_name
    enhanced_data['Security Type'] = all_entries['Security_Type']
    enhanced_data['ISIN'] = all_entries.get(isin_col, '')
    enhanced_data['Instrument Name'] = all_entries[name_col]
    enhanced_data['Market Value (Lacs)'] = pd.to_numeric(all_entries[value_col], errors='coerce').fillna(0)
    enhanced_data['% to NAV'] = pd.to_numeric(all_entries.get(column_mappings.get('nav_percentage', ''), 0), errors='coerce').fillna(0)
    enhanced_data['Yield'] = pd.to_numeric(all_entries.get(column_mappings.get('yield', ''), 0), errors='coerce').fillna(0)
    enhanced_data['Rating'] = all_entries.get(column_mappings.get('rating', ''), '')
    enhanced_data['Quantity'] = pd.to_numeric(all_entries.get(column_mappings.get('quantity', ''), 0), errors='coerce').fillna(0)
    enhanced_data['As Of Date'] = as_of_date
    
    # Perform validation checks
    validation_results = perform_validation_checks(enhanced_data, config)
    
    # Filter out summary rows for main analysis
    main_holdings = enhanced_data[enhanced_data['Security Type'] != 'summary_row'].copy()
    summary_rows = enhanced_data[enhanced_data['Security Type'] == 'summary_row'].copy()
    
    print(f"📊 Main holdings: {len(main_holdings)}")
    print(f"📊 Summary rows: {len(summary_rows)}")
    
    # Summary by security type (excluding summary rows)
    total_value_by_type = main_holdings.groupby('Security Type')['Market Value (Lacs)'].sum()
    total_value = main_holdings['Market Value (Lacs)'].sum()
    
    print(f"\n💰 Portfolio Breakdown by Security Type:")
    for sec_type, value in total_value_by_type.items():
        pct = (value / total_value * 100) if total_value > 0 else 0
        print(f"   {sec_type}: ₹{value:,.0f} Lacs ({pct:.1f}%)")
    
    print(f"\n💰 Total Portfolio Value: ₹{total_value:,.0f} Lacs (₹{total_value/100:,.0f} Crores)")
    
    # Display validation results
    print(f"\n🔍 VALIDATION RESULTS:")
    for check, result in validation_results.items():
        status = "✅" if result['passed'] else "⚠️"
        print(f"   {status} {check}: {result['message']}")
    
    return {
        'main_holdings': main_holdings,
        'summary_rows': summary_rows,
        'validation_results': validation_results,
        'total_value': total_value
    }

def perform_validation_checks(data, config):
    """Perform comprehensive validation checks"""
    validation_config = config.get('validation', {}).get('completeness', {})
    results = {}
    
    # Exclude summary rows from main calculations
    main_data = data[data['Security Type'] != 'summary_row']
    
    # 1. Portfolio coverage check
    total_nav_pct = main_data['% to NAV'].sum()
    min_coverage = validation_config.get('min_portfolio_coverage', 95.0)
    
    results['portfolio_coverage'] = {
        'passed': total_nav_pct >= min_coverage,
        'message': f"{total_nav_pct:.1f}% coverage (min: {min_coverage}%)",
        'value': total_nav_pct
    }
    
    # 2. Cash component check
    cash_data = main_data[main_data['Security Type'] == 'cash_equivalent']
    cash_pct = cash_data['% to NAV'].sum()
    min_cash = validation_config.get('min_cash_expected', 0.5)
    max_cash = validation_config.get('max_cash_expected', 20.0)
    
    cash_ok = min_cash <= cash_pct <= max_cash
    results['cash_component'] = {
        'passed': cash_ok or cash_pct == 0,  # Allow zero if no cash detected
        'message': f"{cash_pct:.1f}% cash (expected: {min_cash}-{max_cash}%)",
        'value': cash_pct
    }
    
    # 3. ISIN securities percentage
    isin_data = main_data[main_data['Security Type'] == 'isin_security']
    isin_pct = isin_data['% to NAV'].sum()
    
    results['isin_securities'] = {
        'passed': isin_pct > 50.0,  # Expect majority to be ISIN securities
        'message': f"{isin_pct:.1f}% ISIN securities",
        'value': isin_pct
    }
    
    # 4. Unaccounted exposure
    unaccounted = 100.0 - total_nav_pct
    max_unaccounted = validation_config.get('nav_reconciliation_tolerance', 2.0)
    
    results['unaccounted_exposure'] = {
        'passed': abs(unaccounted) <= max_unaccounted,
        'message': f"{unaccounted:.1f}% unaccounted (tolerance: ±{max_unaccounted}%)",
        'value': abs(unaccounted)
    }
    
    return results

def save_enhanced_extract(data_dict, amc_name, date, fund_type):
    """Save enhanced extract with all components"""
    output_dir = Path(f"output/{date}/{fund_type}/individual_extracts")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save main holdings (compatible with existing system)
    main_file = output_dir / f"{amc_name}_verified.csv"
    data_dict['main_holdings'].to_csv(main_file, index=False)
    print(f"💾 Saved main holdings: {main_file}")
    
    # Save comprehensive extract
    all_data = pd.concat([data_dict['main_holdings'], data_dict['summary_rows']], ignore_index=True)
    comprehensive_file = output_dir / f"{amc_name}_comprehensive.csv"
    all_data.to_csv(comprehensive_file, index=False)
    print(f"💾 Saved comprehensive extract: {comprehensive_file}")
    
    # Save validation report
    validation_file = output_dir / f"{amc_name}_validation.json"
    import json
    
    # Convert numpy types to Python native types for JSON serialization
    def convert_numpy_types(obj):
        if hasattr(obj, 'item'):
            return obj.item()
        elif isinstance(obj, dict):
            return {k: convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(item) for item in obj]
        else:
            return obj
    
    serializable_results = convert_numpy_types(data_dict['validation_results'])
    
    with open(validation_file, 'w') as f:
        json.dump(serializable_results, f, indent=2)
    print(f"💾 Saved validation report: {validation_file}")
    
    return main_file