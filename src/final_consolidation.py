#!/usr/bin/env python3
"""
CORPORATE BOND FUNDS - FINAL CONSOLIDATION PIPELINE
==================================================

PURPOSE:
Master orchestration script that runs all 6 fund extractors and creates
the final consolidated dataset for analysis. This is the main entry point
for the entire data processing pipeline.

PROCESS FLOW:
1. Execute all individual fund extractors (ABSLF, HDFC, ICICI, KOTAK, NIPPON, SBI)
2. Load and validate individual fund CSV files
3. Standardize credit ratings across different agencies
4. Combine into single consolidated dataset
5. Generate summary statistics and quality metrics

INPUT: Raw Excel files in data/raw/2025-07-31/
OUTPUT: Corporate_Bond_Funds_Consolidated_Analysis.csv

KEY FEATURES:
- Dynamic extractor loading using importlib
- Error handling for missing/failed extractions
- Cross-fund rating standardization
- Portfolio value calculations and summaries
- Comprehensive logging and progress tracking

DEPENDENCIES: All extractor modules, rating_standardizer.py
"""

import pandas as pd
from pathlib import Path
from rating_standardizer import standardize_ratings
import sys

def run_all_extractors():
    """Execute all fund extractor scripts in sequence with error handling"""
    print("🚀 RUNNING ALL EXTRACTORS")
    print("=" * 50)
    
    # Import sys to add the src directory to the path
    import sys
    from pathlib import Path
    
    # Add src directory to Python path so we can import from extractors
    src_path = Path(__file__).parent
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    extractors = [
        ('ABSLF', 'extractors.extract_abslf', 'extract_abslf_data'),
        ('HDFC', 'extractors.extract_hdfc', 'extract_hdfc_data'),
        ('ICICI', 'extractors.extract_icici', 'extract_icici_data'),
        ('KOTAK', 'extractors.extract_kotak', 'extract_kotak_data'),
        ('NIPPON', 'extractors.extract_nippon', 'extract_nippon_data'),
        ('SBI', 'extractors.extract_sbi', 'extract_sbi_data')
    ]
    
    success_count = 0
    for fund_name, module_name, function_name in extractors:
        try:
            print(f"\n📊 Processing {fund_name}...")
            
            # Import the module
            module = __import__(module_name, fromlist=[function_name])
            
            # Get and call the extraction function
            extraction_function = getattr(module, function_name)
            extraction_function()
            
            print(f"✅ {fund_name} extraction completed")
            success_count += 1
            
        except Exception as e:
            print(f"❌ {fund_name} extraction failed: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print(f"\n🎯 EXTRACTION SUMMARY: {success_count}/{len(extractors)} funds processed successfully")
    return success_count > 0

def consolidate_all_funds():
    """Consolidate all 6 fund extracts into final dataset"""
    print("🎯 CONSOLIDATING ALL 6 CORPORATE BOND FUNDS")
    print("=" * 50)
    
    # Define all fund files
    extract_dir = Path("output/2025-07-31/individual_extracts")
    fund_files = {
        'ABSLF': extract_dir / "ABSLF_verified.csv",
        'HDFC': extract_dir / "HDFC_verified.csv", 
        'ICICI': extract_dir / "ICICI_verified.csv",
        'KOTAK': extract_dir / "KOTAK_verified.csv",
        'NIPPON': extract_dir / "NIPPON_verified.csv",
        'SBI': extract_dir / "SBI_verified.csv"
    }
    
    all_funds = []
    fund_summary = []
    
    for fund_name, file_path in fund_files.items():
        if file_path.exists():
            df = pd.read_csv(file_path)
            # Ensure AMC field is populated correctly
            df['AMC'] = fund_name
            df['Fund Name'] = f"{fund_name} Corporate Bond Fund"
            all_funds.append(df)
            
            total_value = df['Market Value (Lacs)'].sum()
            holdings_count = len(df)
            maturity_coverage = df['Maturity Date'].notna().sum() / len(df) * 100
            
            fund_summary.append({
                'Fund': fund_name,
                'Holdings': holdings_count,
                'Total Value (Lacs)': total_value,
                'Total Value (Crores)': total_value / 100,
                'Maturity Coverage %': maturity_coverage,
                'File': file_path.name
            })
            
            print(f"✅ {fund_name}: {holdings_count} holdings, ₹{total_value/100:,.0f} Cr ({maturity_coverage:.1f}% maturity)")
        else:
            print(f"❌ {fund_name}: File not found - {file_path}")
    
    if not all_funds:
        print("❌ No fund files found!")
        return
    
    # Combine all funds
    consolidated = pd.concat(all_funds, ignore_index=True)
    
    # Add standardized rating column
    print(f"\n⭐ STANDARDIZING RATINGS:")
    print("-" * 30)
    consolidated = standardize_ratings(consolidated, 'Rating', create_new_column=True, print_summary=True)
    
    print(f"\n📊 CONSOLIDATED DATASET:")
    print("-" * 30)
    print(f"Total Holdings: {len(consolidated):,}")
    print(f"Total Portfolio Value: ₹{consolidated['Market Value (Lacs)'].sum():,.0f} Lacs")
    print(f"Total Portfolio Value: ₹{consolidated['Market Value (Lacs)'].sum()/100:,.0f} Crores")
    
    # Fund distribution
    print(f"\n📊 FUND DISTRIBUTION:")
    print("-" * 30)
    fund_dist = consolidated.groupby('AMC').agg({
        'Market Value (Lacs)': ['count', 'sum']
    }).round(2)
    fund_dist.columns = ['Holdings', 'Total Value (Lacs)']
    fund_dist['Total Value (Crores)'] = fund_dist['Total Value (Lacs)'] / 100
    fund_dist['% of Total'] = fund_dist['Total Value (Lacs)'] / consolidated['Market Value (Lacs)'].sum() * 100
    
    for amc, row in fund_dist.iterrows():
        print(f"{amc}: {row['Holdings']} holdings, ₹{row['Total Value (Crores)']:,.0f} Cr ({row['% of Total']:.1f}%)")
    
    # Maturity analysis
    print(f"\n📅 MATURITY ANALYSIS:")
    print("-" * 30)
    total_with_maturity = consolidated['Maturity Date'].notna().sum()
    overall_coverage = total_with_maturity / len(consolidated) * 100
    print(f"Holdings with Maturity: {total_with_maturity:,} / {len(consolidated):,} ({overall_coverage:.1f}%)")
    
    # Rating quality analysis
    print(f"\n⭐ RATING QUALITY ANALYSIS:")
    print("-" * 30)
    if 'Standardized Rating' in consolidated.columns:
        # Analyze by rating grade
        rating_value = consolidated.groupby('Standardized Rating')['Market Value (Lacs)'].agg(['count', 'sum']).round(2)
        rating_value.columns = ['Holdings', 'Total Value (Lacs)']
        rating_value['Total Value (Crores)'] = rating_value['Total Value (Lacs)'] / 100
        rating_value['% of Portfolio'] = rating_value['Total Value (Lacs)'] / consolidated['Market Value (Lacs)'].sum() * 100
        
        # Show top rating categories by value
        rating_value_sorted = rating_value.sort_values('Total Value (Lacs)', ascending=False).head(5)
        for rating, row in rating_value_sorted.iterrows():
            if pd.notna(rating):
                print(f"{rating:>12}: {row['Holdings']} holdings, ₹{row['Total Value (Crores)']:,.0f} Cr ({row['% of Portfolio']:.1f}%)")
    
    # Save consolidated dataset
    output_file = Path("output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv")
    consolidated.to_csv(output_file, index=False)
    print(f"\n💾 FINAL DATASET SAVED:")
    print(f"   📁 {output_file}")
    print(f"   📊 {len(consolidated):,} holdings across 6 AMCs")
    print(f"   💰 ₹{consolidated['Market Value (Lacs)'].sum()/100:,.0f} Crores total portfolio value")
    
    # Save fund summary
    summary_df = pd.DataFrame(fund_summary)
    summary_file = Path("output/2025-07-31/Fund_Summary_Report.csv")
    summary_df.to_csv(summary_file, index=False)
    print(f"   📋 {summary_file}")
    
    # Top holdings analysis
    print(f"\n🏆 TOP 10 HOLDINGS (By Value):")
    print("-" * 40)
    top_holdings = consolidated.nlargest(10, 'Market Value (Lacs)')
    for i, (_, holding) in enumerate(top_holdings.iterrows()):
        name = str(holding['Instrument Name'])[:40] if pd.notna(holding['Instrument Name']) else "N/A"
        print(f"{i+1:2d}. {name}... ({holding['AMC']})")
        print(f"     ₹{holding['Market Value (Lacs)']:,.0f} Lacs | {holding['ISIN']}")
    
    return consolidated, fund_summary

def main():
    """Main execution function"""
    print("🎯 CORPORATE BOND FUNDS - COMPLETE ANALYSIS PIPELINE")
    print("=" * 60)
    
    # Step 1: Run all extractors
    extraction_success = run_all_extractors()
    
    if not extraction_success:
        print("❌ Extraction phase failed - cannot proceed with consolidation")
        return
    
    print("\n" + "=" * 60)
    
    # Step 2: Consolidate all funds
    consolidate_all_funds()

if __name__ == "__main__":
    main()
