#!/usr/bin/env python3
"""
Quarto Data Preparation Script
Transforms raw consolidated CSV into report-ready analytical datasets
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
import json
from datetime import datetime

class QuartoDataPrep:
    """Prepare analytical datasets for Quarto reports"""
    
    def __init__(self, report_date="2025-07-31", base_path=None):
        self.report_date = report_date
        # Setup paths
        if base_path:
            self.base_path = Path(base_path)
        else:
            # If running from quarto-reports directory, go up one level
            self.base_path = Path.cwd().parent if Path.cwd().name == "quarto-reports" else Path.cwd()
        
        self.input_path = self.base_path / "output" / report_date / "Corporate_Bond_Funds_Consolidated_Analysis.csv"
        self.output_dir = self.base_path / "quarto-reports" / "prepared_data"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def load_data(self):
        """Load and validate consolidated data"""
        print(f"📊 Loading data from {self.input_path}")
        self.df = pd.read_csv(self.input_path)
        
        # Basic validation
        print(f"✅ Loaded {len(self.df):,} holdings across {self.df['AMC'].nunique()} AMCs")
        print(f"💰 Total portfolio value: ₹{self.df['Market Value (Lacs)'].sum():,.0f} Lacs")
        return self.df
    
    def enrich_data(self):
        """Add derived columns to raw data"""
        print("🔧 Enriching data with derived columns...")
        
        # Convert to working dataframe
        enriched = self.df.copy()
        
        # 1. Market Value in Crores
        enriched['Market_Value_Crores'] = enriched['Market Value (Lacs)'] / 100
        
        # 2. Extract issuer names from instrument names
        enriched['Issuer_Clean'] = enriched['Instrument Name'].apply(self._extract_issuer)
        
        # 3. Classify instrument types
        enriched['Instrument_Type'] = enriched['Instrument Name'].apply(self._classify_instrument)
        
        # 4. Yield buckets
        enriched['Yield_Bucket'] = enriched['Yield'].apply(self._assign_yield_bucket)
        
        # 5. Rating quality score for calculations
        enriched['Rating_Quality_Score'] = enriched['Standardized Rating'].apply(self._rating_to_score)
        
        # 6. Maturity years (where available)
        enriched['Maturity_Years'] = pd.to_datetime(enriched['Maturity Date'], errors='coerce').apply(
            lambda x: (x - pd.Timestamp('2025-07-31')).days / 365.25 if pd.notna(x) else None
        )
        
        # 7. Fund name clean
        enriched['Fund_Clean'] = enriched['AMC']
        
        # Save enriched dataset
        output_file = self.output_dir / "holdings_enriched.csv"
        enriched.to_csv(output_file, index=False)
        print(f"💾 Saved enriched data: {output_file}")
        
        self.enriched_df = enriched
        return enriched
    
    def create_amc_summary(self):
        """Create AMC-level summary statistics"""
        print("📊 Creating AMC summary...")
        
        amc_summary = []
        
        for amc in self.enriched_df['AMC'].unique():
            amc_data = self.enriched_df[self.enriched_df['AMC'] == amc].copy()
            
            # Calculate weighted average yield
            total_value = amc_data['Market Value (Lacs)'].sum()
            wa_yield = (amc_data['Yield'] * amc_data['Market Value (Lacs)']).sum() / total_value
            
            # Rating distribution
            aaa_sovereign_pct = (
                amc_data[amc_data['Standardized Rating'].isin(['AAA', 'SOVEREIGN'])]['Market Value (Lacs)'].sum() 
                / total_value * 100
            )
            
            # Top 5 issuer concentration
            issuer_concentration = amc_data.groupby('Issuer_Clean')['Market Value (Lacs)'].sum().sort_values(ascending=False)
            top5_concentration = issuer_concentration.head(5).sum() / total_value * 100
            
            # HHI calculation
            issuer_weights = issuer_concentration / total_value
            hhi = (issuer_weights ** 2).sum()
            
            # Coverage metrics
            yield_coverage = amc_data['Yield'].notna().sum() / len(amc_data) * 100
            rating_coverage = amc_data['Standardized Rating'].notna().sum() / len(amc_data) * 100
            maturity_coverage = amc_data['Maturity Date'].notna().sum() / len(amc_data) * 100
            
            amc_summary.append({
                'AMC': amc,
                'Total_Holdings': len(amc_data),
                'AUM_Crores': total_value / 100,
                'AUM_Lacs': total_value,
                'WA_Yield': wa_yield,
                'AAA_Plus_Pct': aaa_sovereign_pct,
                'Top5_Issuer_Concentration': top5_concentration,
                'HHI_Score': hhi,
                'Yield_Coverage': yield_coverage,
                'Rating_Coverage': rating_coverage,
                'Maturity_Coverage': maturity_coverage,
                'Avg_Yield': amc_data['Yield'].mean(),
                'Min_Yield': amc_data['Yield'].min(),
                'Max_Yield': amc_data['Yield'].max()
            })
        
        amc_df = pd.DataFrame(amc_summary)
        
        # Save AMC summary
        output_file = self.output_dir / "amc_summary.csv"
        amc_df.to_csv(output_file, index=False)
        print(f"💾 Saved AMC summary: {output_file}")
        
        return amc_df
    
    def create_issuer_analysis(self):
        """Create cross-fund issuer analysis"""
        print("🏢 Creating issuer analysis...")
        
        issuer_analysis = []
        
        # Group by issuer across all funds
        issuer_groups = self.enriched_df.groupby('Issuer_Clean')
        
        for issuer, group in issuer_groups:
            if issuer == 'Unknown':
                continue
                
            total_exposure = group['Market Value (Lacs)'].sum()
            funds_list = sorted(group['AMC'].unique())
            
            # Weighted average yield for this issuer
            wa_yield = (group['Yield'] * group['Market Value (Lacs)']).sum() / total_exposure
            
            issuer_analysis.append({
                'Issuer': issuer,
                'Total_Exposure_Crores': total_exposure / 100,
                'Total_Exposure_Lacs': total_exposure,
                'Funds_Count': len(funds_list),
                'WA_Yield': wa_yield,
                'Funds_List': ','.join(funds_list),
                'Holdings_Count': len(group),
                'Avg_Holding_Size_Crores': (total_exposure / 100) / len(group)
            })
        
        issuer_df = pd.DataFrame(issuer_analysis).sort_values('Total_Exposure_Crores', ascending=False)
        
        # Save issuer analysis
        output_file = self.output_dir / "issuer_analysis.csv"
        issuer_df.to_csv(output_file, index=False)
        print(f"💾 Saved issuer analysis: {output_file}")
        
        return issuer_df
    
    def create_rating_distribution(self):
        """Create rating distribution by AMC"""
        print("⭐ Creating rating distribution...")
        
        rating_dist = []
        
        for amc in self.enriched_df['AMC'].unique():
            amc_data = self.enriched_df[self.enriched_df['AMC'] == amc]
            total_value = amc_data['Market Value (Lacs)'].sum()
            
            # Get rating distribution
            rating_breakdown = amc_data.groupby('Standardized Rating')['Market Value (Lacs)'].sum()
            
            for rating, value in rating_breakdown.items():
                if pd.notna(rating):
                    rating_dist.append({
                        'AMC': amc,
                        'Rating': rating,
                        'Market_Value_Crores': value / 100,
                        'Market_Value_Lacs': value,
                        'Percentage': (value / total_value) * 100,
                        'Holdings_Count': len(amc_data[amc_data['Standardized Rating'] == rating])
                    })
        
        rating_df = pd.DataFrame(rating_dist)
        
        # Save rating distribution
        output_file = self.output_dir / "rating_distribution.csv"
        rating_df.to_csv(output_file, index=False)
        print(f"💾 Saved rating distribution: {output_file}")
        
        return rating_df
    
    def create_yield_buckets(self):
        """Create yield bucket analysis"""
        print("📈 Creating yield bucket analysis...")
        
        yield_buckets = []
        
        for amc in self.enriched_df['AMC'].unique():
            amc_data = self.enriched_df[self.enriched_df['AMC'] == amc]
            total_value = amc_data['Market Value (Lacs)'].sum()
            
            # Get yield bucket distribution
            bucket_breakdown = amc_data.groupby('Yield_Bucket')['Market Value (Lacs)'].sum()
            
            for bucket, value in bucket_breakdown.items():
                yield_buckets.append({
                    'AMC': amc,
                    'Yield_Bucket': bucket,
                    'Market_Value_Crores': value / 100,
                    'Market_Value_Lacs': value,
                    'Percentage': (value / total_value) * 100,
                    'Holdings_Count': len(amc_data[amc_data['Yield_Bucket'] == bucket])
                })
        
        yield_df = pd.DataFrame(yield_buckets)
        
        # Save yield buckets
        output_file = self.output_dir / "yield_buckets.csv"
        yield_df.to_csv(output_file, index=False)
        print(f"💾 Saved yield buckets: {output_file}")
        
        return yield_df
    
    def create_top_holdings(self):
        """Create top holdings analysis"""
        print("🏆 Creating top holdings...")
        
        # Overall top 20 holdings
        overall_top = self.enriched_df.nlargest(20, 'Market Value (Lacs)')[
            ['AMC', 'ISIN', 'Instrument Name', 'Issuer_Clean', 'Market Value (Lacs)', 
             'Market_Value_Crores', '% to NAV', 'Yield', 'Standardized Rating', 'Maturity Date']
        ].copy()
        overall_top['Rank'] = range(1, len(overall_top) + 1)
        
        # Save overall top holdings
        output_file = self.output_dir / "top_holdings_overall.csv"
        overall_top.to_csv(output_file, index=False)
        
        # Top 10 holdings per fund
        fund_tops = []
        for amc in self.enriched_df['AMC'].unique():
            amc_data = self.enriched_df[self.enriched_df['AMC'] == amc]
            top_10 = amc_data.nlargest(10, 'Market Value (Lacs)')[
                ['AMC', 'ISIN', 'Instrument Name', 'Issuer_Clean', 'Market Value (Lacs)', 
                 'Market_Value_Crores', '% to NAV', 'Yield', 'Standardized Rating', 'Maturity Date']
            ].copy()
            top_10['Fund_Rank'] = range(1, len(top_10) + 1)
            fund_tops.append(top_10)
        
        fund_tops_df = pd.concat(fund_tops, ignore_index=True)
        
        # Save fund-wise top holdings
        output_file = self.output_dir / "top_holdings_by_fund.csv"
        fund_tops_df.to_csv(output_file, index=False)
        print(f"💾 Saved top holdings: {output_file}")
        
        return overall_top, fund_tops_df
    
    def create_concentration_metrics(self):
        """Create detailed concentration analysis"""
        print("📊 Creating concentration metrics...")
        
        concentration_data = []
        
        for amc in self.enriched_df['AMC'].unique():
            amc_data = self.enriched_df[self.enriched_df['AMC'] == amc]
            total_value = amc_data['Market Value (Lacs)'].sum()
            
            # Issuer concentration
            issuer_conc = amc_data.groupby('Issuer_Clean')['Market Value (Lacs)'].sum().sort_values(ascending=False)
            
            # ISIN concentration
            isin_conc = amc_data.groupby('ISIN')['Market Value (Lacs)'].sum().sort_values(ascending=False)
            
            concentration_data.append({
                'AMC': amc,
                'Top1_Issuer_Pct': (issuer_conc.iloc[0] / total_value * 100) if len(issuer_conc) > 0 else 0,
                'Top3_Issuer_Pct': (issuer_conc.head(3).sum() / total_value * 100),
                'Top5_Issuer_Pct': (issuer_conc.head(5).sum() / total_value * 100),
                'Top10_Issuer_Pct': (issuer_conc.head(10).sum() / total_value * 100),
                'Top1_ISIN_Pct': (isin_conc.iloc[0] / total_value * 100) if len(isin_conc) > 0 else 0,
                'Top5_ISIN_Pct': (isin_conc.head(5).sum() / total_value * 100),
                'Top10_ISIN_Pct': (isin_conc.head(10).sum() / total_value * 100),
                'Issuer_HHI': ((issuer_conc / total_value) ** 2).sum(),
                'ISIN_HHI': ((isin_conc / total_value) ** 2).sum(),
                'Unique_Issuers': len(issuer_conc),
                'Unique_ISINs': len(isin_conc)
            })
        
        concentration_df = pd.DataFrame(concentration_data)
        
        # Save concentration metrics
        output_file = self.output_dir / "concentration_metrics.csv"
        concentration_df.to_csv(output_file, index=False)
        print(f"💾 Saved concentration metrics: {output_file}")
        
        return concentration_df
    
    def create_data_quality_report(self):
        """Create data quality and coverage report"""
        print("✅ Creating data quality report...")
        
        # Overall statistics
        total_holdings = len(self.enriched_df)
        total_value = self.enriched_df['Market Value (Lacs)'].sum()
        
        # Coverage analysis
        coverage_stats = {}
        for col in ['Yield', 'Standardized Rating', 'Maturity Date', '% to NAV']:
            if col in self.enriched_df.columns:
                coverage_stats[col] = {
                    'coverage_pct': float(self.enriched_df[col].notna().sum() / total_holdings * 100),
                    'missing_count': int(self.enriched_df[col].isna().sum())
                }
        
        # AMC-level coverage
        amc_coverage = {}
        for amc in self.enriched_df['AMC'].unique():
            amc_data = self.enriched_df[self.enriched_df['AMC'] == amc]
            nav_sum = amc_data['% to NAV'].sum()
            
            amc_coverage[amc] = {
                'holdings_count': int(len(amc_data)),
                'nav_sum': float(nav_sum),
                'nav_sum_ok': bool(92 <= nav_sum <= 110),
                'yield_coverage': float(amc_data['Yield'].notna().sum() / len(amc_data) * 100),
                'rating_coverage': float(amc_data['Standardized Rating'].notna().sum() / len(amc_data) * 100)
            }
        
        quality_report = {
            'report_date': self.report_date,
            'generated_at': datetime.now().isoformat(),
            'total_holdings': int(total_holdings),
            'total_value_lacs': float(total_value),
            'total_value_crores': float(total_value / 100),
            'amc_count': int(self.enriched_df['AMC'].nunique()),
            'amc_list': list(self.enriched_df['AMC'].unique()),
            'coverage_stats': coverage_stats,
            'amc_coverage': amc_coverage,
            'data_files_created': [
                'holdings_enriched.csv',
                'amc_summary.csv', 
                'issuer_analysis.csv',
                'rating_distribution.csv',
                'yield_buckets.csv',
                'top_holdings_overall.csv',
                'top_holdings_by_fund.csv',
                'concentration_metrics.csv'
            ]
        }
        
        # Save quality report with proper type conversion
        output_file = self.output_dir / "data_quality_report.json"
        with open(output_file, 'w') as f:
            json.dump(quality_report, f, indent=2, default=self._convert_to_json_serializable)
        print(f"💾 Saved data quality report: {output_file}")
        
        return quality_report
    
    def _convert_to_json_serializable(self, obj):
        """Convert numpy types to JSON serializable types"""
        import numpy as np
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    # Helper methods
    def _extract_issuer(self, instrument_name):
        """Extract issuer name from instrument description"""
        if pd.isna(instrument_name):
            return 'Unknown'
        
        name = str(instrument_name).strip()
        
        # Government securities
        if any(x in name.lower() for x in ['government', 'goi', 'g-sec', 'treasury', 'tbill']):
            return 'Government of India'
        
        # Common issuer patterns
        issuer_patterns = {
            'nabard': 'NABARD',
            'sidbi': 'SIDBI', 
            'national bank for agriculture': 'NABARD',
            'small industries development': 'SIDBI',
            'rural electrification': 'REC',
            'power finance corporation': 'PFC',
            'lic housing': 'LIC Housing Finance',
            'bajaj finance': 'Bajaj Finance',
            'state bank of india': 'SBI',
            'reliance industries': 'Reliance Industries'
        }
        
        name_lower = name.lower()
        for pattern, issuer in issuer_patterns.items():
            if pattern in name_lower:
                return issuer
        
        # Extract from percentage patterns like "7.48% Issuer Name"
        match = re.search(r'^\d+\.?\d*%\s+(.+?)(?:\s+\(|\s+\*\*|$)', name)
        if match:
            extracted = match.group(1).strip()
            # Limit length
            if len(extracted) > 50:
                extracted = extracted[:50]
            return extracted
        
        # Fallback: take first few words
        words = name.split()[:4]
        return ' '.join(words) if words else 'Unknown'
    
    def _classify_instrument(self, instrument_name):
        """Classify instrument type"""
        if pd.isna(instrument_name):
            return 'Unknown'
        
        name = str(instrument_name).lower()
        
        if any(x in name for x in ['government', 'goi', 'g-sec', 'treasury', 'tbill']):
            return 'Government Securities'
        elif any(x in name for x in ['tier ii', 'tier 2', 'at1', 'perpetual']):
            return 'Bank Capital'
        elif any(x in name for x in ['ncd', 'debenture', 'bond']):
            return 'Corporate Bond'
        elif any(x in name for x in ['cp', 'commercial paper']):
            return 'Commercial Paper'
        else:
            return 'Corporate Bond'  # Default
    
    def _assign_yield_bucket(self, yield_val):
        """Assign yield to bucket"""
        if pd.isna(yield_val):
            return 'Unknown'
        elif yield_val <= 6:
            return '≤6%'
        elif yield_val <= 7:
            return '6-7%'
        elif yield_val <= 8:
            return '7-8%'
        elif yield_val <= 9:
            return '8-9%'
        else:
            return '>9%'
    
    def _rating_to_score(self, rating):
        """Convert rating to numerical score for calculations"""
        rating_scores = {
            'SOVEREIGN': 100,
            'AAA': 95,
            'AA+': 90,
            'AA': 85,
            'AA-': 80,
            'A+': 75,
            'A': 70,
            'A-': 65,
            'BBB+': 60,
            'BBB': 55,
            'BBB-': 50,
            'A1+': 95,
            'A1': 90,
            'A2+': 85,
            'A2': 80,
            'A3': 75
        }
        return rating_scores.get(rating, 0)
    
    def create_missing_report_files(self):
        """Create additional files needed by the Quarto reports"""
        print("📄 Creating additional report files...")
        
        # 1. Create overview_metrics.csv
        overview_metrics = pd.DataFrame({
            'Report_Date': [self.report_date],
            'Total_Holdings': [len(self.enriched_df)],
            'Total_AUM_Cr': [self.enriched_df['Market Value (Lacs)'].sum() / 100],
            'Weighted_Avg_Yield': [
                (self.enriched_df['Yield'] * self.enriched_df['Market Value (Lacs)']).sum() / 
                self.enriched_df['Market Value (Lacs)'].sum()
            ],
            'Number_of_AMCs': [self.enriched_df['AMC'].nunique()]
        })
        overview_metrics.to_csv(self.output_dir / "overview_metrics.csv", index=False)
        
        # 2. Create top_issuers.csv from issuer_analysis.csv
        if (self.output_dir / "issuer_analysis.csv").exists():
            issuer_df = pd.read_csv(self.output_dir / "issuer_analysis.csv")
            top_issuers = issuer_df.rename(columns={
                'Total_Exposure': 'Total_Amount_Cr',
                'Portfolio_Pct': 'Percentage_of_Total'
            }).head(50)
            top_issuers.to_csv(self.output_dir / "top_issuers.csv", index=False)
        
        # 3. Create yield_analysis.csv from yield_buckets.csv
        if (self.output_dir / "yield_buckets.csv").exists():
            yield_df = pd.read_csv(self.output_dir / "yield_buckets.csv")
            yield_analysis = yield_df.rename(columns={
                'Yield_Bucket': 'Yield_Bucket',
                'Amount_Cr': 'Amount_Cr',
                'Percentage': 'Percentage',
                'Holdings_Count': 'Holdings_Count'
            })
            yield_analysis.to_csv(self.output_dir / "yield_analysis.csv", index=False)
        
        # 4. Create data_quality_summary.csv from quality report
        data_quality_summary = pd.DataFrame({
            'Total_Holdings': [len(self.enriched_df)],
            'Successful_Analysis': [self.enriched_df['Issuer_Clean'].notna().sum()],
            'Coverage_Percentage': [
                (self.enriched_df['Issuer_Clean'].notna().sum() / len(self.enriched_df)) * 100
            ],
            'Rating_Coverage': [
                (self.enriched_df['Standardized Rating'].notna().sum() / len(self.enriched_df)) * 100
            ]
        })
        data_quality_summary.to_csv(self.output_dir / "data_quality_summary.csv", index=False)
        
        print("💾 Created additional report files")

    def run_full_prep(self):
        """Run complete data preparation pipeline"""
        print("🚀 Starting Quarto data preparation pipeline...")
        print("=" * 60)
        
        # Load and validate data
        self.load_data()
        
        # Create all analytical datasets
        self.enrich_data()
        self.create_amc_summary()
        self.create_issuer_analysis()
        self.create_rating_distribution()
        self.create_yield_buckets()
        self.create_top_holdings()
        self.create_concentration_metrics()
        quality_report = self.create_data_quality_report()
        
        # Create additional files needed by reports
        self.create_missing_report_files()
        
        print("\n" + "=" * 60)
        print("✅ Data preparation completed successfully!")
        print(f"📁 Output directory: {self.output_dir}")
        print(f"📊 Processed {quality_report['total_holdings']:,} holdings")
        print(f"💰 Total value: ₹{quality_report['total_value_crores']:,.0f} Crores")
        print(f"📋 Created {len(quality_report['data_files_created'])} analytical datasets")
        
        return quality_report

def main():
    """Main execution"""
    prep = QuartoDataPrep(report_date="2025-07-31")
    prep.run_full_prep()

if __name__ == "__main__":
    main()
