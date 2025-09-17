#!/usr/bin/env python3
"""
SIMPLE CONSOLIDATION SCRIPT
===========================

PURPOSE:
Loads individual AMC CSV files and combines them into a single consolidated dataset.
No analysis, no reports - just pure consolidation with rating standardization.

USAGE:
python consolidate.py --date 2025-07-31 --fund-type corporate-bond
python consolidate.py --date 2025-08-31 --fund-type money-market

FEATURES:
- CLI-driven with argparse
- Loads all individual CSV extracts
- Applies rating standardization
- Saves consolidated CSV
- Minimal logging and no analysis

OUTPUT:
- Single consolidated CSV file with all AMCs combined
- Standardized rating column
- "As Of Date" column for temporal analysis
"""

import pandas as pd
import argparse
from pathlib import Path
import sys

# Import rating standardizer
sys.path.append('src')
from rating_standardizer import standardize_ratings

def load_individual_extracts(date, fund_type):
    """Load all individual AMC CSV files"""
    extract_dir = Path(f"output/{date}/{fund_type}/individual_extracts")
    
    if not extract_dir.exists():
        print(f"❌ Extract directory not found: {extract_dir}")
        return []
    
    # Find all CSV files
    csv_files = list(extract_dir.glob("*_verified.csv"))
    
    if not csv_files:
        print(f"❌ No individual extract files found in: {extract_dir}")
        return []
    
    all_funds = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            
            # Ensure required columns exist
            if 'AMC' not in df.columns or 'Market Value (Lacs)' not in df.columns:
                print(f"⚠️  Skipping {csv_file.name}: Missing required columns")
                continue
            
            all_funds.append(df)
            amc_name = df['AMC'].iloc[0] if len(df) > 0 else "Unknown"
            total_value = df['Market Value (Lacs)'].sum()
            
            print(f"✅ Loaded {amc_name}: {len(df)} holdings, ₹{total_value/100:,.0f} Cr")
            
        except Exception as e:
            print(f"❌ Error loading {csv_file.name}: {e}")
    
    return all_funds

def consolidate_funds(fund_dataframes):
    """Combine all fund dataframes into single consolidated dataset"""
    if not fund_dataframes:
        print("❌ No fund data to consolidate")
        return None
    
    # Combine all funds
    consolidated = pd.concat(fund_dataframes, ignore_index=True)
    
    print(f"\n📊 CONSOLIDATED DATASET:")
    print("-" * 30)
    print(f"Total Holdings: {len(consolidated):,}")
    print(f"Total Portfolio Value: ₹{consolidated['Market Value (Lacs)'].sum()/100:,.0f} Crores")
    print(f"AMCs Included: {', '.join(consolidated['AMC'].unique())}")
    
    # Add standardized rating column
    print(f"\n⭐ STANDARDIZING RATINGS:")
    print("-" * 30)
    consolidated = standardize_ratings(consolidated, 'Rating', create_new_column=True, print_summary=True)
    
    return consolidated

def save_consolidated_data(consolidated_df, date, fund_type):
    """Save consolidated dataset to CSV"""
    output_dir = Path(f"output/{date}/{fund_type}/consolidated")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename
    output_file = output_dir / f"{fund_type}_consolidated_{date}.csv"
    
    # Save CSV
    consolidated_df.to_csv(output_file, index=False)
    
    print(f"\n💾 CONSOLIDATED DATA SAVED:")
    print(f"   📁 {output_file}")
    print(f"   📊 {len(consolidated_df):,} holdings across {consolidated_df['AMC'].nunique()} AMCs")
    
    return output_file

def main():
    parser = argparse.ArgumentParser(description='Consolidate individual AMC extracts')
    parser.add_argument('--date', required=True, help='Date in YYYY-MM-DD format (e.g., 2025-07-31)')
    parser.add_argument('--fund-type', required=True, help='Fund type: corporate-bond, money-market, etc.')
    
    args = parser.parse_args()
    
    print(f"🎯 CONSOLIDATING {args.fund_type.upper()} FUNDS")
    print(f"📅 Date: {args.date}")
    print("=" * 50)
    
    # Load individual extracts
    fund_dataframes = load_individual_extracts(args.date, args.fund_type)
    
    if not fund_dataframes:
        print("❌ No data to consolidate. Run extraction first:")
        print(f"   python extract.py --date {args.date} --fund-type {args.fund_type} --amc all")
        return
    
    # Consolidate data
    consolidated = consolidate_funds(fund_dataframes)
    
    if consolidated is None:
        return
    
    # Save consolidated data
    save_consolidated_data(consolidated, args.date, args.fund_type)

if __name__ == "__main__":
    main()
