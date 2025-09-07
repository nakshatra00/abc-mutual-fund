#!/usr/bin/env python3
"""
Mutual Fund Portfolio Analysis Engine
Generates comprehensive PDF reports from consolidated portfolio data
"""

import pandas as pd
import numpy as np
import argparse
import os
from datetime import datetime
import re
from pathlib import Path
import json

class PortfolioAnalyzer:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.df = None
        self.output_dir = None
        self.prepared_data_dir = None
        
    def load_data(self):
        """Load and prepare the consolidated CSV data"""
        self.df = pd.read_csv(self.csv_path)
        
        # Create output directories
        base_dir = Path(self.csv_path).parent
        self.output_dir = base_dir / "reports"
        self.prepared_data_dir = base_dir / "prepared_data"
        
        self.output_dir.mkdir(exist_ok=True)
        self.prepared_data_dir.mkdir(exist_ok=True)
        
        print(f"Loaded {len(self.df)} records from {len(self.df['Fund Name'].unique())} funds")
        
    def extract_issuer_name(self, instrument_name):
        """Extract issuer name from instrument description"""
        if pd.isna(instrument_name):
            return "Unknown"
            
        # Remove percentage and maturity info
        cleaned = re.sub(r'\d+\.\d+%\s*', '', str(instrument_name))
        cleaned = re.sub(r'\(\d{2}/\d{2}/\d{4}\)', '', cleaned)
        cleaned = re.sub(r'\*+', '', cleaned).strip()
        
        # Handle Government bonds
        if 'GOI' in cleaned or 'Government' in cleaned:
            return 'Government of India'
            
        # Extract company name (usually the main part)
        words = cleaned.split()
        if len(words) > 3:
            return ' '.join(words[:3])
        return cleaned
    
    def classify_instrument_type(self, instrument_name):
        """Classify instrument type from name"""
        if pd.isna(instrument_name):
            return "Other"
            
        name_lower = str(instrument_name).lower()
        
        if 'goi' in name_lower or 'government' in name_lower:
            return 'Government Bond'
        elif 'bank' in name_lower:
            return 'Bank Bond'
        elif 'finance' in name_lower or 'housing' in name_lower:
            return 'NBFC Bond'
        elif 'power' in name_lower or 'energy' in name_lower:
            return 'Power/Utility Bond'
        else:
            return 'Corporate Bond'
    
    def calculate_maturity_bucket(self, maturity_date):
        """Calculate maturity bucket from date"""
        if pd.isna(maturity_date):
            return "No Maturity Info"
            
        try:
            mat_date = pd.to_datetime(maturity_date)
            today = pd.Timestamp.now()
            years_to_maturity = (mat_date - today).days / 365.25
            
            if years_to_maturity <= 1:
                return "0-1 Years"
            elif years_to_maturity <= 3:
                return "1-3 Years"
            elif years_to_maturity <= 5:
                return "3-5 Years"
            elif years_to_maturity <= 10:
                return "5-10 Years"
            else:
                return "10+ Years"
        except:
            return "No Maturity Info"
    
    def get_yield_bucket(self, yield_val):
        """Categorize yield into buckets"""
        if pd.isna(yield_val):
            return "No Yield Info"
            
        try:
            y = float(yield_val)
            if y < 5:
                return "<5%"
            elif y < 6:
                return "5-6%"
            elif y < 7:
                return "6-7%"
            elif y < 8:
                return "7-8%"
            else:
                return ">8%"
        except:
            return "No Yield Info"
    
    def prepare_analysis_data(self):
        """Prepare all analysis datasets"""
        # Add derived columns
        self.df['Issuer'] = self.df['Instrument Name'].apply(self.extract_issuer_name)
        self.df['Instrument_Type'] = self.df['Instrument Name'].apply(self.classify_instrument_type)
        self.df['Maturity_Bucket'] = self.df['Maturity Date'].apply(self.calculate_maturity_bucket)
        self.df['Yield_Bucket'] = self.df['Yield'].apply(self.get_yield_bucket)
        
        # Clean data
        self.df['Market Value (Lacs)'] = pd.to_numeric(self.df['Market Value (Lacs)'], errors='coerce')
        self.df['% to NAV'] = pd.to_numeric(self.df['% to NAV'], errors='coerce')
        self.df['Yield'] = pd.to_numeric(self.df['Yield'], errors='coerce')
        
        print("✓ Data preparation completed")
    
    def calculate_weighted_avg_yield(self):
        """Calculate weighted average yield by fund and overall"""
        fund_yields = []
        
        for fund in self.df['Fund Name'].unique():
            fund_data = self.df[self.df['Fund Name'] == fund].copy()
            fund_data = fund_data.dropna(subset=['Market Value (Lacs)', 'Yield'])
            
            if len(fund_data) > 0:
                weighted_yield = np.average(fund_data['Yield'], 
                                          weights=fund_data['Market Value (Lacs)'])
                total_value = fund_data['Market Value (Lacs)'].sum()
                
                fund_yields.append({
                    'Fund': fund,
                    'Weighted_Avg_Yield': weighted_yield,
                    'Total_Value_Lacs': total_value,
                    'Holdings_Count': len(fund_data)
                })
        
        yield_summary = pd.DataFrame(fund_yields)
        yield_summary = yield_summary.sort_values('Weighted_Avg_Yield', ascending=False)
        yield_summary.to_csv(self.prepared_data_dir / 'fund_yield_summary.csv', index=False)
        
        # Overall weighted average
        overall_data = self.df.dropna(subset=['Market Value (Lacs)', 'Yield'])
        overall_yield = np.average(overall_data['Yield'], 
                                 weights=overall_data['Market Value (Lacs)'])
        
        return yield_summary, overall_yield
    
    def analyze_rating_distribution(self):
        """Analyze rating distribution"""
        rating_analysis = self.df.groupby(['Fund Name', 'Standardized Rating']).agg({
            'Market Value (Lacs)': 'sum',
            '% to NAV': 'sum',
            'ISIN': 'count'
        }).reset_index()
        
        rating_analysis.columns = ['Fund', 'Rating', 'Market_Value_Lacs', 'NAV_Percentage', 'Holdings_Count']
        rating_analysis = rating_analysis.sort_values(['Fund', 'Rating'])
        rating_analysis.to_csv(self.prepared_data_dir / 'rating_distribution.csv', index=False)
        
        # Overall rating distribution
        overall_ratings = self.df.groupby('Standardized Rating').agg({
            'Market Value (Lacs)': 'sum',
            '% to NAV': 'sum',
            'ISIN': 'count'
        }).reset_index()
        overall_ratings.columns = ['Rating', 'Market_Value_Lacs', 'NAV_Percentage', 'Holdings_Count']
        overall_ratings = overall_ratings.sort_values('Market_Value_Lacs', ascending=False)
        overall_ratings.to_csv(self.prepared_data_dir / 'overall_rating_distribution.csv', index=False)
        
        return rating_analysis, overall_ratings
    
    def analyze_top_holdings(self):
        """Analyze top holdings by fund and overall"""
        # Top holdings by fund
        fund_top_holdings = []
        for fund in self.df['Fund Name'].unique():
            fund_data = self.df[self.df['Fund Name'] == fund].copy()
            top_10 = fund_data.nlargest(10, 'Market Value (Lacs)')
            top_10['Rank'] = range(1, len(top_10) + 1)
            fund_top_holdings.append(top_10[['Fund Name', 'Rank', 'ISIN', 'Instrument Name', 
                                           'Market Value (Lacs)', '% to NAV', 'Yield', 'Standardized Rating']])
        
        all_fund_holdings = pd.concat(fund_top_holdings, ignore_index=True)
        all_fund_holdings.to_csv(self.prepared_data_dir / 'top_holdings_by_fund.csv', index=False)
        
        # Overall top holdings
        overall_top = self.df.nlargest(20, 'Market Value (Lacs)')
        overall_top['Rank'] = range(1, len(overall_top) + 1)
        overall_top.to_csv(self.prepared_data_dir / 'top_holdings_overall.csv', index=False)
        
        return all_fund_holdings, overall_top
    
    def analyze_issuer_concentration(self):
        """Analyze issuer concentration"""
        issuer_analysis = self.df.groupby(['Fund Name', 'Issuer']).agg({
            'Market Value (Lacs)': 'sum',
            '% to NAV': 'sum',
            'ISIN': 'count'
        }).reset_index()
        
        issuer_analysis.columns = ['Fund', 'Issuer', 'Market_Value_Lacs', 'NAV_Percentage', 'Holdings_Count']
        issuer_analysis = issuer_analysis.sort_values(['Fund', 'Market_Value_Lacs'], ascending=[True, False])
        issuer_analysis.to_csv(self.prepared_data_dir / 'issuer_analysis.csv', index=False)
        
        # Top issuers overall
        top_issuers = self.df.groupby('Issuer').agg({
            'Market Value (Lacs)': 'sum',
            '% to NAV': 'sum',
            'ISIN': 'count'
        }).reset_index()
        top_issuers.columns = ['Issuer', 'Market_Value_Lacs', 'NAV_Percentage', 'Holdings_Count']
        top_issuers = top_issuers.sort_values('Market_Value_Lacs', ascending=False).head(15)
        top_issuers.to_csv(self.prepared_data_dir / 'top_issuers.csv', index=False)
        
        return issuer_analysis, top_issuers
    
    def analyze_yield_buckets(self):
        """Analyze yield distribution"""
        yield_analysis = self.df.groupby(['Fund Name', 'Yield_Bucket']).agg({
            'Market Value (Lacs)': 'sum',
            '% to NAV': 'sum',
            'ISIN': 'count'
        }).reset_index()
        
        yield_analysis.columns = ['Fund', 'Yield_Bucket', 'Market_Value_Lacs', 'NAV_Percentage', 'Holdings_Count']
        
        # Define bucket order
        bucket_order = ['<5%', '5-6%', '6-7%', '7-8%', '>8%', 'No Yield Info']
        yield_analysis['Bucket_Order'] = yield_analysis['Yield_Bucket'].map(
            {bucket: i for i, bucket in enumerate(bucket_order)}
        )
        yield_analysis = yield_analysis.sort_values(['Fund', 'Bucket_Order'])
        yield_analysis.to_csv(self.prepared_data_dir / 'yield_analysis.csv', index=False)
        
        return yield_analysis
    
    def analyze_maturity_buckets(self):
        """Analyze maturity distribution (mainly ABSLF)"""
        maturity_analysis = self.df.groupby(['Fund Name', 'Maturity_Bucket']).agg({
            'Market Value (Lacs)': 'sum',
            '% to NAV': 'sum',
            'ISIN': 'count'
        }).reset_index()
        
        maturity_analysis.columns = ['Fund', 'Maturity_Bucket', 'Market_Value_Lacs', 'NAV_Percentage', 'Holdings_Count']
        
        # Define bucket order
        bucket_order = ['0-1 Years', '1-3 Years', '3-5 Years', '5-10 Years', '10+ Years', 'No Maturity Info']
        maturity_analysis['Bucket_Order'] = maturity_analysis['Maturity_Bucket'].map(
            {bucket: i for i, bucket in enumerate(bucket_order)}
        )
        maturity_analysis = maturity_analysis.sort_values(['Fund', 'Bucket_Order'])
        maturity_analysis.to_csv(self.prepared_data_dir / 'maturity_analysis.csv', index=False)
        
        return maturity_analysis
    
    def create_overview_metrics(self):
        """Create overview metrics for all funds"""
        fund_metrics = []
        
        for fund in self.df['Fund Name'].unique():
            fund_data = self.df[self.df['Fund Name'] == fund].copy()
            
            metrics = {
                'Fund_Name': fund,
                'Total_Holdings': len(fund_data),
                'Total_Value_Lacs': fund_data['Market Value (Lacs)'].sum(),
                'Avg_Yield': fund_data['Yield'].mean(),
                'Weighted_Avg_Yield': np.average(fund_data['Yield'].dropna(), 
                                               weights=fund_data[fund_data['Yield'].notna()]['Market Value (Lacs)']),
                'Top_Rating_Pct': fund_data[fund_data['Standardized Rating'] == 'AAA']['Market Value (Lacs)'].sum() / fund_data['Market Value (Lacs)'].sum() * 100,
                'Top_10_Concentration': fund_data.nlargest(10, 'Market Value (Lacs)')['% to NAV'].sum()
            }
            fund_metrics.append(metrics)
        
        overview_df = pd.DataFrame(fund_metrics)
        overview_df = overview_df.sort_values('Total_Value_Lacs', ascending=False)
        overview_df.to_csv(self.prepared_data_dir / 'overview_metrics.csv', index=False)
        
        return overview_df
    
    def generate_all_analysis(self):
        """Run all analysis functions"""
        print("🔄 Starting comprehensive analysis...")
        
        self.load_data()
        self.prepare_analysis_data()
        
        print("📊 Calculating metrics...")
        yield_summary, overall_yield = self.calculate_weighted_avg_yield()
        rating_dist, overall_ratings = self.analyze_rating_distribution()
        fund_holdings, overall_holdings = self.analyze_top_holdings()
        issuer_analysis, top_issuers = self.analyze_issuer_concentration()
        yield_buckets = self.analyze_yield_buckets()
        maturity_buckets = self.analyze_maturity_buckets()
        overview_metrics = self.create_overview_metrics()
        
        # Create summary statistics
        summary_stats = {
            'analysis_date': datetime.now().isoformat(),
            'total_funds': len(self.df['Fund Name'].unique()),
            'total_holdings': len(self.df),
            'total_value_lacs': float(self.df['Market Value (Lacs)'].sum()),
            'overall_weighted_yield': float(overall_yield),
            'funds_analyzed': list(self.df['Fund Name'].unique())
        }
        
        with open(self.prepared_data_dir / 'analysis_summary.json', 'w') as f:
            json.dump(summary_stats, f, indent=2)
        
        print(f"✅ Analysis complete! Processed {len(self.df)} holdings from {len(self.df['Fund Name'].unique())} funds")
        print(f"📁 Prepared data saved to: {self.prepared_data_dir}")
        print(f"🎯 Overall Weighted Average Yield: {overall_yield:.2f}%")
        
        return {
            'prepared_data_dir': self.prepared_data_dir,
            'output_dir': self.output_dir,
            'summary_stats': summary_stats
        }

def main():
    parser = argparse.ArgumentParser(description='Analyze mutual fund portfolios and generate reports')
    parser.add_argument('csv_path', help='Path to the consolidated portfolio CSV file')
    parser.add_argument('--output', '-o', help='Output directory (optional)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_path):
        print(f"❌ Error: CSV file not found: {args.csv_path}")
        return 1
    
    try:
        analyzer = PortfolioAnalyzer(args.csv_path)
        results = analyzer.generate_all_analysis()
        
        print(f"\n🎉 Analysis completed successfully!")
        print(f"📊 Next: Run Quarto to generate PDF reports from prepared data")
        print(f"📁 Data location: {results['prepared_data_dir']}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())