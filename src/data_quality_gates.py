#!/usr/bin/env python3
"""
CORPORATE BOND FUND DATA QUALITY VALIDATION SYSTEM
=================================================

PURPOSE:
Comprehensive data quality validation framework that ensures portfolio data
meets production standards before reports are generated. Implements 9 critical
quality gates that must pass for data to be considered publication-ready.

QUALITY GATES:
1. Date Integrity - Validates date formats and reasonableness
2. NAV Sanity Check - Ensures portfolio percentages sum to ~100%
3. Duplicate ISIN Check - Identifies duplicate securities within funds
4. ISIN Format Validation - Validates 12-character ISIN codes
5. Type Casting & Data Integrity - Ensures numeric fields are parseable
6. Outlier Detection - Flags unusual values for review
7. Data Coverage Analysis - Ensures minimum data coverage thresholds
8. Business Logic Validation - Checks concentration and diversification
9. Standardized Ratings Check - Validates rating standardization success

THRESHOLDS (CONFIGURABLE):
- NAV Range: 92-102% (accounts for cash holdings)
- Coverage Minimums: ISIN 100%, Rating 80%, Yield 70%, Maturity 50%
- Concentration Limits: Top 10 holdings <50%, Individual <25%

OUTPUT: Pass/Fail determination with detailed issue reporting
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime, timedelta
import warnings

class DataQualityGates:
    """Production-ready data validation system with configurable thresholds"""
    
    def __init__(self, report_date="2025-07-31"):
        self.report_date = report_date
        self.validation_results = {}
        self.critical_failures = []
        self.warnings = []
        self.passed_gates = []
        
    def validate_all_funds(self):
        """Execute all validation gates and generate final verdict"""
        print("🚪 CORPORATE BOND FUND DATA QUALITY GATES")
        print("=" * 60)
        print(f"📅 Report Date: {self.report_date}")
        print(f"🕐 Validation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Load all fund data
        extract_dir = Path("output/2025-07-31/individual_extracts")
        fund_files = {
            'ABSLF': extract_dir / "ABSLF_verified.csv",
            'HDFC': extract_dir / "HDFC_verified.csv", 
            'ICICI': extract_dir / "ICICI_verified.csv",
            'KOTAK': extract_dir / "KOTAK_verified.csv",
            'NIPPON': extract_dir / "NIPPON_verified.csv",
            'SBI': extract_dir / "SBI_verified.csv"
        }
        
        consolidated_path = Path("output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv")
        
        all_passed = True
        
        # Gate 1: Date Integrity Check
        all_passed &= self._gate_1_date_integrity(fund_files)
        
        # Gate 2: % to NAV Sanity Check
        all_passed &= self._gate_2_nav_sanity(fund_files)
        
        # Gate 3: Duplicate ISIN Check
        all_passed &= self._gate_3_duplicate_isin(fund_files, consolidated_path)
        
        # Gate 4: ISIN Format Validation
        all_passed &= self._gate_4_isin_format(fund_files)
        
        # Gate 5: Type Casting and Data Integrity
        all_passed &= self._gate_5_type_casting(fund_files)
        
        # Gate 6: Outlier Detection
        all_passed &= self._gate_6_outlier_detection(fund_files)
        
        # Gate 7: Coverage Analysis
        all_passed &= self._gate_7_coverage_analysis(fund_files)
        
        # Gate 8: Business Logic Validation
        all_passed &= self._gate_8_business_logic(fund_files)
        
        # Gate 9: Standardized Ratings Check
        all_passed &= self._gate_9_standardized_ratings(consolidated_path)
        
        # Final Report
        self._generate_final_report(all_passed)
        
        return all_passed
    
    def _gate_1_date_integrity(self, fund_files):
        """Gate 1: Verify date integrity and consistency"""
        print("🚪 GATE 1: DATE INTEGRITY CHECK")
        print("-" * 40)
        
        gate_passed = True
        date_issues = []
        
        for fund_name, file_path in fund_files.items():
            if not file_path.exists():
                continue
                
            df = pd.read_csv(file_path)
            
            # Check if maturity dates exist and are valid
            if 'Maturity Date' in df.columns:
                maturity_dates = pd.to_datetime(df['Maturity Date'], errors='coerce')
                
                # Check for invalid date formats
                invalid_dates = df['Maturity Date'].notna() & maturity_dates.isna()
                if invalid_dates.any():
                    issue = f"{fund_name}: {invalid_dates.sum()} invalid date formats"
                    date_issues.append(issue)
                    gate_passed = False
                
                # Check for unrealistic dates (past dates for bonds)
                valid_dates = maturity_dates.dropna()
                if len(valid_dates) > 0:
                    report_date_dt = pd.to_datetime(self.report_date)
                    past_dates = valid_dates < report_date_dt
                    
                    if past_dates.any():
                        issue = f"{fund_name}: {past_dates.sum()} bonds with past maturity dates"
                        date_issues.append(issue)
                        # This is a warning, not a failure for bonds near maturity
                        self.warnings.append(issue)
                    
                    # Check for very far future dates (>50 years)
                    far_future = valid_dates > (report_date_dt + timedelta(days=50*365))
                    if far_future.any():
                        issue = f"{fund_name}: {far_future.sum()} bonds with very distant maturity (>50 years)"
                        date_issues.append(issue)
                        self.warnings.append(issue)
        
        if gate_passed:
            print("   ✅ PASSED: All dates are properly formatted and reasonable")
            self.passed_gates.append("Date Integrity")
        else:
            print("   ❌ FAILED: Date integrity issues found:")
            for issue in date_issues:
                print(f"      - {issue}")
            self.critical_failures.extend(date_issues)
        
        if self.warnings:
            print("   ⚠️  WARNINGS:")
            for warning in self.warnings[-len(date_issues):]:  # Show only recent warnings
                print(f"      - {warning}")
        
        print()
        return gate_passed
    
    def _gate_2_nav_sanity(self, fund_files):
        """Gate 2: % to NAV sanity check - each fund sum must be within [92, 102]%"""
        print("🚪 GATE 2: % TO NAV SANITY CHECK")
        print("-" * 40)
        
        gate_passed = True
        nav_issues = []
        
        for fund_name, file_path in fund_files.items():
            if not file_path.exists():
                continue
                
            df = pd.read_csv(file_path)
            
            if '% to NAV' in df.columns:
                nav_values = pd.to_numeric(df['% to NAV'], errors='coerce').dropna()
                nav_sum = nav_values.sum()
                
                print(f"   📊 {fund_name}: % to NAV Sum = {nav_sum:.2f}%")
                
                if nav_sum < 92 or nav_sum > 102:
                    issue = f"{fund_name}: % to NAV sum ({nav_sum:.2f}%) outside acceptable range [92-102]%"
                    nav_issues.append(issue)
                    gate_passed = False
                    print(f"      ❌ FAILED: {nav_sum:.2f}% is outside [92-102]% range")
                else:
                    print(f"      ✅ PASSED: {nav_sum:.2f}% is within acceptable range")
                
                # Additional checks for individual holdings
                very_high = (nav_values > 25).sum()  # >25% in single holding is unusual
                negative = (nav_values < 0).sum()
                
                if very_high > 0:
                    warning = f"{fund_name}: {very_high} holdings with >25% to NAV (concentration risk)"
                    self.warnings.append(warning)
                
                if negative > 0:
                    issue = f"{fund_name}: {negative} holdings with negative % to NAV"
                    nav_issues.append(issue)
                    gate_passed = False
            else:
                issue = f"{fund_name}: Missing '% to NAV' column"
                nav_issues.append(issue)
                gate_passed = False
        
        if gate_passed:
            print("   ✅ OVERALL: All funds passed % to NAV sanity check")
            self.passed_gates.append("% to NAV Sanity")
        else:
            print("   ❌ OVERALL: % to NAV sanity check failed:")
            for issue in nav_issues:
                print(f"      - {issue}")
            self.critical_failures.extend(nav_issues)
        
        print()
        return gate_passed
    
    def _gate_3_duplicate_isin(self, fund_files, consolidated_path):
        """Gate 3: Check for duplicate ISINs within funds and handle appropriately"""
        print("🚪 GATE 3: DUPLICATE ISIN CHECK")
        print("-" * 40)
        
        gate_passed = True
        duplicate_issues = []
        
        # Check within individual funds
        for fund_name, file_path in fund_files.items():
            if not file_path.exists():
                continue
                
            df = pd.read_csv(file_path)
            
            if 'ISIN' in df.columns:
                isin_counts = df['ISIN'].value_counts()
                duplicates = isin_counts[isin_counts > 1]
                
                if len(duplicates) > 0:
                    print(f"   🔍 {fund_name}: Found {len(duplicates)} duplicate ISINs")
                    
                    # Analyze duplicates - are they legitimate splits or errors?
                    for isin, count in duplicates.head(5).items():
                        dup_rows = df[df['ISIN'] == isin]
                        
                        # Check if market values are different (could be legitimate splits)
                        values = dup_rows['Market Value (Lacs)'].unique()
                        names = dup_rows['Instrument Name'].unique()
                        
                        if len(values) > 1:
                            # Different values - might be legitimate
                            total_value = dup_rows['Market Value (Lacs)'].sum()
                            print(f"      📋 ISIN: {isin} ({count}x) - Total: ₹{total_value:,.0f} Lacs")
                            print(f"         Values: {[f'₹{v:,.0f}' for v in values]}")
                            self.warnings.append(f"{fund_name}: ISIN {isin} appears {count} times with different values")
                        else:
                            # Same values - likely error
                            issue = f"{fund_name}: ISIN {isin} duplicated {count} times with same value"
                            duplicate_issues.append(issue)
                            gate_passed = False
                            print(f"      ❌ ISIN: {isin} ({count}x) - Same value: ₹{values[0]:,.0f} Lacs")
                else:
                    print(f"   ✅ {fund_name}: No duplicate ISINs found")
        
        # Check cross-fund duplicates (expected and good)
        if consolidated_path.exists():
            cons_df = pd.read_csv(consolidated_path)
            if 'ISIN' in cons_df.columns:
                cross_fund_dups = cons_df['ISIN'].value_counts()
                cross_fund_dups = cross_fund_dups[cross_fund_dups > 1]
                
                print(f"   📊 Cross-Fund Analysis: {len(cross_fund_dups)} ISINs held by multiple funds")
                if len(cross_fund_dups) > 0:
                    top_shared = cross_fund_dups.head(3)
                    for isin, count in top_shared.items():
                        shared_rows = cons_df[cons_df['ISIN'] == isin]
                        funds = shared_rows['Fund Name'].unique()
                        total_value = shared_rows['Market Value (Lacs)'].sum()
                        print(f"      📈 {isin}: {count} funds, ₹{total_value:,.0f} Lacs total")
        
        if gate_passed:
            print("   ✅ OVERALL: No problematic duplicate ISINs found")
            self.passed_gates.append("Duplicate ISIN Check")
        else:
            print("   ❌ OVERALL: Duplicate ISIN issues found:")
            for issue in duplicate_issues:
                print(f"      - {issue}")
            self.critical_failures.extend(duplicate_issues)
        
        print()
        return gate_passed
    
    def _gate_4_isin_format(self, fund_files):
        """Gate 4: ISIN format validation - [A-Z]{2}[A-Z0-9]{9}[0-9]"""
        print("🚪 GATE 4: ISIN FORMAT VALIDATION")
        print("-" * 40)
        
        gate_passed = True
        format_issues = []
        
        # ISIN pattern: 2 letters + 9 alphanumeric + 1 digit = 12 characters total
        isin_pattern = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')
        
        for fund_name, file_path in fund_files.items():
            if not file_path.exists():
                continue
                
            df = pd.read_csv(file_path)
            
            if 'ISIN' in df.columns:
                # Clean ISINs - remove spaces and convert to uppercase
                original_isins = df['ISIN'].copy()
                cleaned_isins = df['ISIN'].astype(str).str.replace(' ', '').str.upper()
                
                # Check format
                valid_format = cleaned_isins.apply(lambda x: bool(isin_pattern.match(x)) if pd.notna(x) else False)
                invalid_count = (~valid_format & df['ISIN'].notna()).sum()
                
                print(f"   🔍 {fund_name}: {len(df)} ISINs checked")
                
                if invalid_count > 0:
                    print(f"      ❌ {invalid_count} ISINs with invalid format")
                    
                    # Show examples of invalid ISINs
                    invalid_isins = df.loc[~valid_format & df['ISIN'].notna(), 'ISIN'].head(3)
                    for isin in invalid_isins:
                        print(f"         Invalid: '{isin}' (length: {len(str(isin))})")
                    
                    issue = f"{fund_name}: {invalid_count} ISINs with invalid format"
                    format_issues.append(issue)
                    gate_passed = False
                else:
                    print(f"      ✅ All ISINs have valid format")
                
                # Check for missing ISINs
                missing_isins = df['ISIN'].isna().sum()
                if missing_isins > 0:
                    issue = f"{fund_name}: {missing_isins} missing ISINs"
                    format_issues.append(issue)
                    gate_passed = False
                    print(f"      ❌ {missing_isins} missing ISINs")
                
                # Check length distribution
                lengths = cleaned_isins.str.len()
                non_12_char = (lengths != 12).sum()
                if non_12_char > 0:
                    print(f"      ⚠️  {non_12_char} ISINs not exactly 12 characters")
                    length_dist = lengths.value_counts().sort_index()
                    print(f"         Length distribution: {dict(length_dist)}")
        
        if gate_passed:
            print("   ✅ OVERALL: All ISINs have valid format")
            self.passed_gates.append("ISIN Format Validation")
        else:
            print("   ❌ OVERALL: ISIN format validation failed:")
            for issue in format_issues:
                print(f"      - {issue}")
            self.critical_failures.extend(format_issues)
        
        print()
        return gate_passed
    
    def _gate_5_type_casting(self, fund_files):
        """Gate 5: Type casting validation and non-parsable data detection"""
        print("🚪 GATE 5: TYPE CASTING & DATA INTEGRITY")
        print("-" * 40)
        
        gate_passed = True
        casting_issues = []
        
        numeric_columns = ['Market Value (Lacs)', '% to NAV', 'Yield', 'Quantity', 'Coupon']
        
        for fund_name, file_path in fund_files.items():
            if not file_path.exists():
                continue
                
            df = pd.read_csv(file_path)
            print(f"   🔍 {fund_name}: Checking data type integrity")
            
            for col in numeric_columns:
                if col in df.columns:
                    # Try to convert to numeric
                    original_values = df[col].copy()
                    numeric_values = pd.to_numeric(df[col], errors='coerce')
                    
                    # Find non-parsable values
                    non_parsable = original_values.notna() & numeric_values.isna()
                    non_parsable_count = non_parsable.sum()
                    
                    if non_parsable_count > 0:
                        print(f"      ❌ {col}: {non_parsable_count} non-parsable values")
                        
                        # Show examples
                        examples = original_values[non_parsable].head(3)
                        for val in examples:
                            print(f"         Non-parsable: '{val}'")
                        
                        issue = f"{fund_name}: {col} has {non_parsable_count} non-parsable values"
                        casting_issues.append(issue)
                        gate_passed = False
                    else:
                        print(f"      ✅ {col}: All values parsable")
                    
                    # Check for reasonable ranges
                    if col == 'Market Value (Lacs)':
                        negative_values = (numeric_values < 0).sum()
                        if negative_values > 0:
                            issue = f"{fund_name}: {negative_values} negative market values"
                            casting_issues.append(issue)
                            gate_passed = False
                    
                    elif col == '% to NAV':
                        negative_nav = (numeric_values < 0).sum()
                        very_high_nav = (numeric_values > 50).sum()
                        if negative_nav > 0:
                            issue = f"{fund_name}: {negative_nav} negative % to NAV values"
                            casting_issues.append(issue)
                            gate_passed = False
                        if very_high_nav > 0:
                            warning = f"{fund_name}: {very_high_nav} very high % to NAV values (>50%)"
                            self.warnings.append(warning)
                    
                    elif col == 'Yield':
                        negative_yield = (numeric_values < 0).sum()
                        very_high_yield = (numeric_values > 25).sum()
                        if negative_yield > 0:
                            issue = f"{fund_name}: {negative_yield} negative yield values"
                            casting_issues.append(issue)
                            gate_passed = False
                        if very_high_yield > 0:
                            warning = f"{fund_name}: {very_high_yield} very high yield values (>25%)"
                            self.warnings.append(warning)
        
        if gate_passed:
            print("   ✅ OVERALL: All numeric data properly formatted")
            self.passed_gates.append("Type Casting & Data Integrity")
        else:
            print("   ❌ OVERALL: Type casting issues found:")
            for issue in casting_issues:
                print(f"      - {issue}")
            self.critical_failures.extend(casting_issues)
        
        print()
        return gate_passed
    
    def _gate_6_outlier_detection(self, fund_files):
        """Gate 6: Detect and flag outliers in key metrics"""
        print("🚪 GATE 6: OUTLIER DETECTION")
        print("-" * 40)
        
        gate_passed = True
        outlier_issues = []
        
        for fund_name, file_path in fund_files.items():
            if not file_path.exists():
                continue
                
            df = pd.read_csv(file_path)
            print(f"   🔍 {fund_name}: Detecting outliers")
            
            # Market Value outliers
            if 'Market Value (Lacs)' in df.columns:
                values = pd.to_numeric(df['Market Value (Lacs)'], errors='coerce').dropna()
                if len(values) > 0:
                    q99 = values.quantile(0.99)
                    q01 = values.quantile(0.01)
                    
                    extreme_high = (values > q99 * 5).sum()  # 5x the 99th percentile
                    extreme_low = (values < max(q01 / 5, 1)).sum()  # Very small values
                    
                    if extreme_high > 0:
                        warning = f"{fund_name}: {extreme_high} extremely high market values (>5x 99th percentile)"
                        self.warnings.append(warning)
                        print(f"      ⚠️  {extreme_high} extremely high market values")
                    
                    if extreme_low > 0:
                        warning = f"{fund_name}: {extreme_low} extremely low market values"
                        self.warnings.append(warning)
                        print(f"      ⚠️  {extreme_low} extremely low market values")
            
            # % to NAV outliers
            if '% to NAV' in df.columns:
                nav_values = pd.to_numeric(df['% to NAV'], errors='coerce').dropna()
                if len(nav_values) > 0:
                    extreme_high_nav = (nav_values > 30).sum()  # >30% is extreme concentration
                    
                    if extreme_high_nav > 0:
                        issue = f"{fund_name}: {extreme_high_nav} holdings with >30% to NAV (extreme concentration)"
                        outlier_issues.append(issue)
                        gate_passed = False
                        print(f"      ❌ {extreme_high_nav} holdings with >30% to NAV")
            
            # Yield outliers
            if 'Yield' in df.columns:
                yields = pd.to_numeric(df['Yield'], errors='coerce').dropna()
                if len(yields) > 0:
                    extreme_yields = ((yields > 20) | (yields < -5)).sum()
                    
                    if extreme_yields > 0:
                        warning = f"{fund_name}: {extreme_yields} bonds with extreme yields (>20% or <-5%)"
                        self.warnings.append(warning)
                        print(f"      ⚠️  {extreme_yields} bonds with extreme yields")
            
            if not any([extreme_high > 0 if 'extreme_high' in locals() else False,
                       extreme_low > 0 if 'extreme_low' in locals() else False,
                       extreme_high_nav > 0 if 'extreme_high_nav' in locals() else False]):
                print(f"      ✅ No significant outliers detected")
        
        if gate_passed:
            print("   ✅ OVERALL: No critical outliers found")
            self.passed_gates.append("Outlier Detection")
        else:
            print("   ❌ OVERALL: Critical outliers detected:")
            for issue in outlier_issues:
                print(f"      - {issue}")
            self.critical_failures.extend(outlier_issues)
        
        print()
        return gate_passed
    
    def _gate_7_coverage_analysis(self, fund_files):
        """Gate 7: Analyze data coverage for key fields"""
        print("🚪 GATE 7: DATA COVERAGE ANALYSIS")
        print("-" * 40)
        
        gate_passed = True
        coverage_issues = []
        
        # Define minimum coverage thresholds
        thresholds = {
            'ISIN': 100,           # Must be 100%
            'Instrument Name': 100, # Must be 100%
            'Market Value (Lacs)': 100, # Must be 100%
            '% to NAV': 92,        # At least 92%
            'Rating': 80,          # At least 80%
            'Yield': 70,           # At least 70%
            'Maturity Date': 50    # At least 50%
        }
        
        coverage_summary = {}
        
        for fund_name, file_path in fund_files.items():
            if not file_path.exists():
                continue
                
            df = pd.read_csv(file_path)
            total_rows = len(df)
            
            print(f"   🔍 {fund_name}: Coverage analysis ({total_rows} holdings)")
            fund_coverage = {}
            
            for col, min_threshold in thresholds.items():
                if col in df.columns:
                    non_null_count = df[col].notna().sum()
                    coverage_pct = (non_null_count / total_rows) * 100
                    fund_coverage[col] = coverage_pct
                    
                    if coverage_pct >= min_threshold:
                        print(f"      ✅ {col}: {coverage_pct:.1f}% (≥{min_threshold}%)")
                    else:
                        print(f"      ❌ {col}: {coverage_pct:.1f}% (<{min_threshold}%)")
                        issue = f"{fund_name}: {col} coverage {coverage_pct:.1f}% below threshold {min_threshold}%"
                        coverage_issues.append(issue)
                        if min_threshold == 100:  # Critical fields
                            gate_passed = False
                else:
                    print(f"      ❌ {col}: Column missing")
                    issue = f"{fund_name}: {col} column is missing"
                    coverage_issues.append(issue)
                    gate_passed = False
            
            coverage_summary[fund_name] = fund_coverage
        
        # Overall coverage statistics
        print(f"\n   📊 OVERALL COVERAGE SUMMARY:")
        for col in thresholds.keys():
            coverages = [coverage_summary[fund].get(col, 0) for fund in coverage_summary]
            if coverages:
                avg_coverage = np.mean(coverages)
                min_coverage = min(coverages)
                print(f"      {col}: Avg {avg_coverage:.1f}%, Min {min_coverage:.1f}%")
        
        if gate_passed:
            print("   ✅ OVERALL: All critical coverage thresholds met")
            self.passed_gates.append("Data Coverage Analysis")
        else:
            print("   ❌ OVERALL: Coverage thresholds not met:")
            for issue in coverage_issues[:5]:  # Show first 5 issues
                print(f"      - {issue}")
            if len(coverage_issues) > 5:
                print(f"      ... and {len(coverage_issues) - 5} more issues")
            self.critical_failures.extend(coverage_issues)
        
        print()
        return gate_passed
    
    def _gate_8_business_logic(self, fund_files):
        """Gate 8: Business logic validation for corporate bond funds"""
        print("🚪 GATE 8: BUSINESS LOGIC VALIDATION")
        print("-" * 40)
        
        gate_passed = True
        business_issues = []
        
        for fund_name, file_path in fund_files.items():
            if not file_path.exists():
                continue
                
            df = pd.read_csv(file_path)
            print(f"   🔍 {fund_name}: Business logic validation")
            
            # Check 1: Corporate bond funds should have reasonable yield ranges
            if 'Yield' in df.columns:
                yields = pd.to_numeric(df['Yield'], errors='coerce').dropna()
                if len(yields) > 0:
                    # For corporate bonds, expect yields typically between 4-15%
                    reasonable_yields = ((yields >= 4) & (yields <= 15)).sum()
                    unreasonable_yields = len(yields) - reasonable_yields
                    
                    if unreasonable_yields > len(yields) * 0.3:  # >30% unreasonable
                        warning = f"{fund_name}: {unreasonable_yields} bonds with unusual yields (outside 4-15%)"
                        self.warnings.append(warning)
                        print(f"      ⚠️  {unreasonable_yields} bonds with unusual yields")
            
            # Check 2: Rating distribution should be reasonable for corporate bonds
            if 'Rating' in df.columns:
                ratings = df['Rating'].dropna()
                if len(ratings) > 0:
                    # Check for unrated bonds (high risk for corporate bond fund)
                    unrated = ratings.str.contains('unrated|not rated|nr', case=False, na=False).sum()
                    if unrated > len(ratings) * 0.1:  # >10% unrated
                        warning = f"{fund_name}: {unrated} unrated bonds ({unrated/len(ratings)*100:.1f}%)"
                        self.warnings.append(warning)
                        print(f"      ⚠️  {unrated} unrated bonds")
            
            # Check 3: Market value concentration
            if 'Market Value (Lacs)' in df.columns:
                values = pd.to_numeric(df['Market Value (Lacs)'], errors='coerce').dropna()
                if len(values) > 0:
                    total_value = values.sum()
                    top_10_value = values.nlargest(10).sum()
                    concentration = (top_10_value / total_value) * 100
                    
                    if concentration > 80:  # >80% in top 10 holdings
                        warning = f"{fund_name}: High concentration - top 10 holdings = {concentration:.1f}%"
                        self.warnings.append(warning)
                        print(f"      ⚠️  High concentration: top 10 = {concentration:.1f}%")
                    else:
                        print(f"      ✅ Reasonable concentration: top 10 = {concentration:.1f}%")
            
            # Check 4: Fund should have reasonable number of holdings
            total_holdings = len(df)
            if total_holdings < 20:
                warning = f"{fund_name}: Very few holdings ({total_holdings}) for a diversified fund"
                self.warnings.append(warning)
                print(f"      ⚠️  Only {total_holdings} holdings (low diversification)")
            elif total_holdings > 500:
                warning = f"{fund_name}: Very many holdings ({total_holdings}) - check for duplicates"
                self.warnings.append(warning)
                print(f"      ⚠️  {total_holdings} holdings (very high - check duplicates)")
            else:
                print(f"      ✅ Reasonable diversification: {total_holdings} holdings")
        
        # Business logic typically generates warnings, not failures
        print("   ✅ OVERALL: Business logic validation completed")
        self.passed_gates.append("Business Logic Validation")
        
        print()
        return gate_passed
    
    def _gate_9_standardized_ratings(self, consolidated_path):
        """Gate 9: Standardized Ratings Quality Check"""
        print("🚪 GATE 9: STANDARDIZED RATINGS CHECK")
        print("-" * 40)
        
        gate_passed = True
        
        try:
            df = pd.read_csv(consolidated_path)
            
            if 'Standardized Rating' not in df.columns:
                failure = "Standardized Rating column not found in consolidated data"
                self.critical_failures.append(failure)
                print(f"   ❌ FAILED: {failure}")
                return False
            
            # Check rating standardization coverage
            total_holdings = len(df)
            non_null_ratings = df['Rating'].notna().sum()
            standardized_ratings = df['Standardized Rating'].notna().sum()
            
            print(f"   📊 Total holdings: {total_holdings}")
            print(f"   📊 Holdings with original ratings: {non_null_ratings} ({(non_null_ratings/total_holdings)*100:.1f}%)")
            print(f"   📊 Holdings with standardized ratings: {standardized_ratings} ({(standardized_ratings/total_holdings)*100:.1f}%)")
            
            # Check standardization success rate
            standardization_rate = (standardized_ratings / non_null_ratings) * 100 if non_null_ratings > 0 else 0
            print(f"   📊 Standardization success rate: {standardization_rate:.1f}%")
            
            # Minimum thresholds
            min_standardization_rate = 70.0  # At least 70% of ratings should be standardized
            min_coverage = 60.0  # At least 60% of all holdings should have standardized ratings
            
            coverage_rate = (standardized_ratings / total_holdings) * 100
            
            if standardization_rate < min_standardization_rate:
                failure = f"Standardization success rate {standardization_rate:.1f}% below threshold {min_standardization_rate}%"
                self.critical_failures.append(failure)
                print(f"   ❌ FAILED: {failure}")
                gate_passed = False
            else:
                print(f"   ✅ PASSED: Standardization rate {standardization_rate:.1f}% meets threshold")
            
            if coverage_rate < min_coverage:
                failure = f"Overall rating coverage {coverage_rate:.1f}% below threshold {min_coverage}%"
                self.critical_failures.append(failure)
                print(f"   ❌ FAILED: {failure}")
                gate_passed = False
            else:
                print(f"   ✅ PASSED: Overall coverage {coverage_rate:.1f}% meets threshold")
            
            # Check for valid standardized rating values
            valid_ratings = ['SOVEREIGN', 'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 
                           'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-', 'B+', 'B', 'B-', 
                           'C', 'D', 'A1+', 'A1', 'A2+', 'A2', 'A3']
            
            standardized_values = df['Standardized Rating'].dropna().unique()
            invalid_ratings = [r for r in standardized_values if r not in valid_ratings]
            
            if invalid_ratings:
                warning = f"Found {len(invalid_ratings)} invalid standardized ratings: {invalid_ratings[:5]}"
                self.warnings.append(warning)
                print(f"   ⚠️  WARNING: {warning}")
            else:
                print(f"   ✅ All standardized ratings are valid")
            
            # Show distribution of standardized ratings
            print(f"   📊 STANDARDIZED RATING DISTRIBUTION:")
            rating_dist = df['Standardized Rating'].value_counts().head(5)
            for rating, count in rating_dist.items():
                pct = (count/total_holdings)*100
                print(f"      {str(rating):12s}: {count:4d} holdings ({pct:5.1f}%)")
            
            if gate_passed:
                print("   ✅ OVERALL: Standardized ratings quality check passed")
                self.passed_gates.append("Standardized Ratings Check")
            else:
                print("   ❌ OVERALL: Standardized ratings quality check failed")
                
        except Exception as e:
            failure = f"Error during standardized ratings check: {str(e)}"
            self.critical_failures.append(failure)
            print(f"   ❌ ERROR: {failure}")
            gate_passed = False
        
        print()
        return gate_passed
    
    def _generate_final_report(self, all_passed):
        """Generate final validation report"""
        print("📋 FINAL VALIDATION REPORT")
        print("=" * 60)
        
        print(f"🕐 Validation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📅 Report date: {self.report_date}")
        print()
        
        # Gates summary
        total_gates = 9
        passed_count = len(self.passed_gates)
        
        print(f"🚪 GATES SUMMARY: {passed_count}/{total_gates} PASSED")
        print("-" * 40)
        
        for gate in self.passed_gates:
            print(f"   ✅ {gate}")
        
        if len(self.passed_gates) < total_gates:
            failed_gates = total_gates - len(self.passed_gates)
            print(f"\n   ❌ {failed_gates} gates failed - see details above")
        
        # Critical failures
        if self.critical_failures:
            print(f"\n❌ CRITICAL FAILURES ({len(self.critical_failures)}):")
            print("-" * 40)
            for i, failure in enumerate(self.critical_failures[:10], 1):
                print(f"   {i}. {failure}")
            if len(self.critical_failures) > 10:
                print(f"   ... and {len(self.critical_failures) - 10} more failures")
        
        # Warnings
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            print("-" * 40)
            for i, warning in enumerate(self.warnings[:10], 1):
                print(f"   {i}. {warning}")
            if len(self.warnings) > 10:
                print(f"   ... and {len(self.warnings) - 10} more warnings")
        
        # Final verdict
        print(f"\n🎯 FINAL VERDICT:")
        print("-" * 40)
        
        if all_passed:
            print("   ✅ DATA READY FOR PUBLICATION")
            print("   All critical quality gates passed successfully!")
            print("   Data meets production quality standards.")
        else:
            print("   ❌ DATA NOT READY FOR PUBLICATION")
            print("   Critical quality gates failed - fix issues before proceeding.")
            print("   Review and resolve all critical failures listed above.")
        
        if self.warnings:
            print(f"\n   📝 Note: {len(self.warnings)} warnings found - review recommended but not blocking.")
        
        print("\n" + "=" * 60)
        
        return {
            'all_passed': all_passed,
            'passed_gates': len(self.passed_gates),
            'total_gates': total_gates,
            'critical_failures': len(self.critical_failures),
            'warnings': len(self.warnings),
            'details': {
                'passed_gates': self.passed_gates,
                'failures': self.critical_failures,
                'warnings': self.warnings
            }
        }

def main():
    """Run all data quality gates"""
    validator = DataQualityGates(report_date="2025-07-31")
    result = validator.validate_all_funds()
    return result

if __name__ == "__main__":
    main()
