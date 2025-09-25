#!/usr/bin/env python3
"""
PORTFOLIO VALIDATION FRAMEWORK
==============================

PURPOSE:
Comprehensive validation framework for mutual fund portfolio data:
- Completeness checks (NAV coverage, missing components)
- Quality metrics (data consistency, value distributions)
- Business rule validation (regulatory compliance)
- Comparative analysis (benchmark vs actual)

FEATURES:
- Detailed validation reports
- Visual validation indicators
- Quality score calculations
- Risk flag identification
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any

class PortfolioValidator:
    """Comprehensive portfolio validation framework"""
    
    def __init__(self):
        self.validation_rules = {
            'completeness': {
                'min_nav_coverage': 95.0,      # Minimum % of NAV captured
                'max_nav_variance': 2.0,       # Maximum variance from 100%
                'expected_cash_range': (0.5, 20.0),  # Expected cash % range
                'min_holdings_count': 10,      # Minimum number of holdings
            },
            'quality': {
                'max_zero_values': 0.05,       # Max % of zero values allowed
                'min_top10_concentration': 30.0,  # Min top 10 holdings %
                'max_single_holding': 20.0,    # Max single holding %
                'expected_yield_range': (0.0, 25.0),  # Expected yield range
            },
            'business': {
                'max_unrated_exposure': 15.0,  # Max % unrated securities
                'min_investment_grade': 70.0,  # Min % investment grade
                'max_cash_equivalent': 25.0,   # Max cash equivalent %
                'min_diversification_score': 0.6,  # Min diversification
            }
        }
    
    def validate_portfolio(self, data: pd.DataFrame, config: Dict) -> Dict:
        """Run comprehensive portfolio validation"""
        print(f"\n🔍 PORTFOLIO VALIDATION: {data['AMC'].iloc[0]} {data['Fund Name'].iloc[0]}")
        
        results = {
            'fund_info': {
                'amc': data['AMC'].iloc[0],
                'fund_name': data['Fund Name'].iloc[0],
                'total_holdings': len(data),
                'as_of_date': data['As Of Date'].iloc[0]
            },
            'completeness': self._validate_completeness(data),
            'quality': self._validate_quality(data),
            'business': self._validate_business_rules(data),
            'summary': {}
        }
        
        # Calculate overall scores
        results['summary'] = self._calculate_summary_scores(results)
        
        return results
    
    def _validate_completeness(self, data: pd.DataFrame) -> Dict:
        """Validate portfolio completeness"""
        rules = self.validation_rules['completeness']
        results = {}
        
        # NAV Coverage
        total_nav_pct = data['% to NAV'].sum()
        nav_variance = abs(100.0 - total_nav_pct)
        
        results['nav_coverage'] = {
            'value': total_nav_pct,
            'passed': total_nav_pct >= rules['min_nav_coverage'],
            'severity': 'critical' if total_nav_pct < 90.0 else 'warning' if total_nav_pct < 95.0 else 'pass',
            'message': f"NAV Coverage: {total_nav_pct:.1f}% (min: {rules['min_nav_coverage']}%)"
        }
        
        results['nav_variance'] = {
            'value': nav_variance,
            'passed': nav_variance <= rules['max_nav_variance'],
            'severity': 'critical' if nav_variance > 5.0 else 'warning' if nav_variance > 2.0 else 'pass',
            'message': f"NAV Variance: ±{nav_variance:.1f}% (max: ±{rules['max_nav_variance']}%)"
        }
        
        # Holdings count
        holdings_count = len(data)
        results['holdings_count'] = {
            'value': holdings_count,
            'passed': holdings_count >= rules['min_holdings_count'],
            'severity': 'warning' if holdings_count < rules['min_holdings_count'] else 'pass',
            'message': f"Holdings Count: {holdings_count} (min: {rules['min_holdings_count']})"
        }
        
        # Security type breakdown (if available)
        if 'Security Type' in data.columns:
            type_breakdown = data.groupby('Security Type')['% to NAV'].sum()
            cash_pct = type_breakdown.get('cash_equivalent', 0.0)
            isin_pct = type_breakdown.get('isin_security', 0.0)
            
            results['cash_component'] = {
                'value': cash_pct,
                'passed': rules['expected_cash_range'][0] <= cash_pct <= rules['expected_cash_range'][1] or cash_pct == 0,
                'severity': 'warning' if cash_pct > rules['expected_cash_range'][1] else 'pass',
                'message': f"Cash Component: {cash_pct:.1f}% (expected: {rules['expected_cash_range'][0]}-{rules['expected_cash_range'][1]}%)"
            }
            
            results['isin_coverage'] = {
                'value': isin_pct,
                'passed': isin_pct > 50.0,
                'severity': 'warning' if isin_pct < 70.0 else 'pass',
                'message': f"ISIN Securities: {isin_pct:.1f}% of portfolio"
            }
        
        return results
    
    def _validate_quality(self, data: pd.DataFrame) -> Dict:
        """Validate data quality metrics"""
        rules = self.validation_rules['quality']
        results = {}
        
        # Zero values check
        total_values = len(data)
        zero_values = (data['Market Value (Lacs)'] == 0).sum()
        zero_pct = zero_values / total_values if total_values > 0 else 0
        
        results['zero_values'] = {
            'value': zero_pct,
            'passed': zero_pct <= rules['max_zero_values'],
            'severity': 'warning' if zero_pct > rules['max_zero_values'] else 'pass',
            'message': f"Zero Values: {zero_pct:.1%} ({zero_values}/{total_values})"
        }
        
        # Concentration analysis
        data_sorted = data.sort_values('% to NAV', ascending=False)
        top10_pct = data_sorted.head(10)['% to NAV'].sum()
        top1_pct = data_sorted.iloc[0]['% to NAV'] if len(data_sorted) > 0 else 0
        
        results['top10_concentration'] = {
            'value': top10_pct,
            'passed': top10_pct >= rules['min_top10_concentration'],
            'severity': 'warning' if top10_pct < rules['min_top10_concentration'] else 'pass',
            'message': f"Top 10 Concentration: {top10_pct:.1f}% (min: {rules['min_top10_concentration']}%)"
        }
        
        results['single_holding_risk'] = {
            'value': top1_pct,
            'passed': top1_pct <= rules['max_single_holding'],
            'severity': 'warning' if top1_pct > rules['max_single_holding'] else 'pass',
            'message': f"Largest Holding: {top1_pct:.1f}% (max: {rules['max_single_holding']}%)"
        }
        
        # Yield analysis (if available)
        if 'Yield' in data.columns and data['Yield'].notna().any():
            valid_yields = data[data['Yield'] > 0]['Yield']
            if len(valid_yields) > 0:
                avg_yield = valid_yields.mean()
                results['yield_reasonableness'] = {
                    'value': avg_yield,
                    'passed': rules['expected_yield_range'][0] <= avg_yield <= rules['expected_yield_range'][1],
                    'severity': 'warning' if not (rules['expected_yield_range'][0] <= avg_yield <= rules['expected_yield_range'][1]) else 'pass',
                    'message': f"Average Yield: {avg_yield:.2f}% (expected: {rules['expected_yield_range'][0]}-{rules['expected_yield_range'][1]}%)"
                }
        
        return results
    
    def _validate_business_rules(self, data: pd.DataFrame) -> Dict:
        """Validate business and regulatory rules"""
        rules = self.validation_rules['business']
        results = {}
        
        # Rating analysis (if available)
        if 'Rating' in data.columns:
            total_exposure = data['% to NAV'].sum()
            unrated_mask = data['Rating'].isna() | (data['Rating'].astype(str).str.strip() == '') | (data['Rating'].astype(str).str.upper() == 'UNRATED')
            unrated_pct = data[unrated_mask]['% to NAV'].sum()
            
            results['unrated_exposure'] = {
                'value': unrated_pct,
                'passed': unrated_pct <= rules['max_unrated_exposure'],
                'severity': 'warning' if unrated_pct > rules['max_unrated_exposure'] else 'pass',
                'message': f"Unrated Exposure: {unrated_pct:.1f}% (max: {rules['max_unrated_exposure']}%)"
            }
            
            # Investment grade analysis (simplified)
            ig_patterns = ['AAA', 'AA', 'A+', 'A', 'A-', 'BBB']
            ig_mask = data['Rating'].astype(str).str.upper().str.contains('|'.join(ig_patterns), na=False)
            ig_pct = data[ig_mask]['% to NAV'].sum()
            
            results['investment_grade'] = {
                'value': ig_pct,
                'passed': ig_pct >= rules['min_investment_grade'],
                'severity': 'warning' if ig_pct < rules['min_investment_grade'] else 'pass',
                'message': f"Investment Grade: {ig_pct:.1f}% (min: {rules['min_investment_grade']}%)"
            }
        
        # Cash equivalent limits (if security types available)
        if 'Security Type' in data.columns:
            cash_equiv_pct = data[data['Security Type'] == 'cash_equivalent']['% to NAV'].sum()
            results['cash_limits'] = {
                'value': cash_equiv_pct,
                'passed': cash_equiv_pct <= rules['max_cash_equivalent'],
                'severity': 'warning' if cash_equiv_pct > rules['max_cash_equivalent'] else 'pass',
                'message': f"Cash Equivalents: {cash_equiv_pct:.1f}% (max: {rules['max_cash_equivalent']}%)"
            }
        
        # Diversification score (Herfindahl-Hirschman Index)
        weights = data['% to NAV'] / 100.0
        hhi = (weights ** 2).sum()
        diversification_score = 1 - hhi if hhi <= 1 else 0
        
        results['diversification'] = {
            'value': diversification_score,
            'passed': diversification_score >= rules['min_diversification_score'],
            'severity': 'warning' if diversification_score < rules['min_diversification_score'] else 'pass',
            'message': f"Diversification Score: {diversification_score:.2f} (min: {rules['min_diversification_score']})"
        }
        
        return results
    
    def _calculate_summary_scores(self, results: Dict) -> Dict:
        """Calculate overall validation scores"""
        all_checks = []
        
        # Collect all validation results
        for category in ['completeness', 'quality', 'business']:
            if category in results:
                for check, result in results[category].items():
                    all_checks.append(result)
        
        if not all_checks:
            return {'overall_score': 0, 'total_checks': 0, 'passed_checks': 0}
        
        # Count passed checks
        passed_checks = sum(1 for check in all_checks if check.get('passed', False))
        total_checks = len(all_checks)
        overall_score = passed_checks / total_checks * 100
        
        # Count by severity
        critical_issues = sum(1 for check in all_checks if check.get('severity') == 'critical' and not check.get('passed', False))
        warning_issues = sum(1 for check in all_checks if check.get('severity') == 'warning' and not check.get('passed', False))
        
        return {
            'overall_score': overall_score,
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'critical_issues': critical_issues,
            'warning_issues': warning_issues,
            'quality_grade': self._get_quality_grade(overall_score, critical_issues)
        }
    
    def _get_quality_grade(self, score: float, critical_issues: int) -> str:
        """Convert score to quality grade"""
        if critical_issues > 0:
            return 'F'  # Fail if any critical issues
        elif score >= 95:
            return 'A+'
        elif score >= 90:
            return 'A'
        elif score >= 85:
            return 'B+'
        elif score >= 80:
            return 'B'
        elif score >= 75:
            return 'C+'
        elif score >= 70:
            return 'C'
        else:
            return 'D'
    
    def print_validation_report(self, results: Dict):
        """Print formatted validation report"""
        fund_info = results['fund_info']
        summary = results['summary']
        
        print(f"\n" + "="*60)
        print(f"📊 VALIDATION REPORT: {fund_info['amc']} - {fund_info['fund_name']}")
        print(f"📅 As of: {fund_info['as_of_date']} | Holdings: {fund_info['total_holdings']}")
        print(f"="*60)
        
        # Overall summary
        print(f"🎯 OVERALL QUALITY GRADE: {summary['quality_grade']}")
        print(f"📊 Score: {summary['overall_score']:.1f}% ({summary['passed_checks']}/{summary['total_checks']} checks passed)")
        
        if summary['critical_issues'] > 0:
            print(f"🚨 Critical Issues: {summary['critical_issues']}")
        if summary['warning_issues'] > 0:
            print(f"⚠️  Warnings: {summary['warning_issues']}")
        
        # Category details
        categories = [
            ('completeness', '📋 COMPLETENESS CHECKS'),
            ('quality', '🔍 QUALITY CHECKS'),
            ('business', '📊 BUSINESS RULES'),
        ]
        
        for category, title in categories:
            if category in results:
                print(f"\n{title}:")
                for check_name, result in results[category].items():
                    status = "✅" if result['passed'] else "❌" if result['severity'] == 'critical' else "⚠️"
                    print(f"  {status} {result['message']}")
    
    def save_validation_report(self, results: Dict, output_path: Path):
        """Save detailed validation report"""
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"💾 Validation report saved: {output_path}")

def validate_all_extracts(date: str, fund_type: str):
    """Validate all extracted files for a given date and fund type"""
    validator = PortfolioValidator()
    extracts_dir = Path(f"output/{date}/{fund_type}/individual_extracts")
    
    if not extracts_dir.exists():
        print(f"❌ No extracts found for {date}/{fund_type}")
        return
    
    # Find all extract files
    extract_files = list(extracts_dir.glob("*_verified.csv"))
    if not extract_files:
        print(f"❌ No verified extract files found in {extracts_dir}")
        return
    
    print(f"🔍 VALIDATING {len(extract_files)} EXTRACTS FOR {fund_type.upper()}")
    print(f"📅 Date: {date}")
    print("="*70)
    
    validation_results = {}
    
    for extract_file in extract_files:
        try:
            # Load extract data
            data = pd.read_csv(extract_file)
            if len(data) == 0:
                print(f"⚠️ Empty extract: {extract_file.name}")
                continue
            
            # Run validation
            amc_name = extract_file.stem.replace('_verified', '')
            config = {}  # Could load specific config if needed
            
            results = validator.validate_portfolio(data, config)
            validator.print_validation_report(results)
            
            # Save individual validation report
            validation_file = extracts_dir / f"{amc_name}_validation_detailed.json"
            validator.save_validation_report(results, validation_file)
            
            validation_results[amc_name] = results
            
        except Exception as e:
            print(f"❌ Validation failed for {extract_file.name}: {e}")
    
    # Create summary validation report
    summary_file = extracts_dir / "validation_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(validation_results, f, indent=2, default=str)
    
    print(f"\n💾 Summary validation report: {summary_file}")
    
    return validation_results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate extracted portfolio data')
    parser.add_argument('--date', required=True, help='Date (YYYY-MM-DD)')
    parser.add_argument('--fund-type', required=True, help='Fund type')
    
    args = parser.parse_args()
    validate_all_extracts(args.date, args.fund_type)