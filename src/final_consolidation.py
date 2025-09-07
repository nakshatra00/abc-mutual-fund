#!/usr/bin/env python3
"""
Final Consolidation of All 6 Corporate Bond Funds
Creates the comprehensive analysis dataset
"""

import pandas as pd
from pathlib import Path
from rating_standardizer import standardize_ratings

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

if __name__ == "__main__":
    consolidate_all_funds()
