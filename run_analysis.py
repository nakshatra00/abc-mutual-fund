#!/usr/bin/env python3
"""
Complete Analysis Pipeline
Runs both analysis and report generation in sequence
"""

import sys
import os
import argparse
from pathlib import Path

# Import our analysis modules
from analysis_engine import PortfolioAnalyzer
from generate_reports import ReportGenerator

def main():
    parser = argparse.ArgumentParser(description='Complete mutual fund analysis and report generation pipeline')
    parser.add_argument('csv_path', help='Path to the consolidated portfolio CSV file')
    parser.add_argument('--analysis-only', action='store_true', help='Run only the analysis, skip report generation')
    parser.add_argument('--reports-only', action='store_true', help='Run only report generation (requires existing analysis)')
    parser.add_argument('--overview-only', action='store_true', help='Generate only the overview report')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_path):
        print(f"❌ Error: CSV file not found: {args.csv_path}")
        return 1
    
    csv_path = Path(args.csv_path)
    print(f"🚀 Starting mutual fund analysis pipeline for: {csv_path}")
    
    try:
        # Step 1: Run Analysis (unless reports-only)
        if not args.reports_only:
            print("\n" + "="*60)
            print("📊 STEP 1: RUNNING PORTFOLIO ANALYSIS")
            print("="*60)
            
            analyzer = PortfolioAnalyzer(args.csv_path)
            analysis_results = analyzer.generate_all_analysis()
            
            if not analysis_results:
                print("❌ Analysis failed. Stopping pipeline.")
                return 1
            
            print("✅ Analysis completed successfully!")
        
        # Step 2: Generate Reports (unless analysis-only)
        if not args.analysis_only:
            print("\n" + "="*60)
            print("📋 STEP 2: GENERATING PDF REPORTS")
            print("="*60)
            
            generator = ReportGenerator(args.csv_path)
            
            if args.overview_only:
                generator.setup_directories()
                success = generator.generate_overview_report()
            else:
                success = generator.generate_all_reports()
            
            if not success:
                print("❌ Report generation failed.")
                return 1
            
            print("✅ Reports generated successfully!")
        
        # Summary
        print("\n" + "="*60)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        reports_dir = csv_path.parent / "reports"
        prepared_data_dir = csv_path.parent / "prepared_data"
        
        if not args.reports_only:
            print(f"📊 Analysis data: {prepared_data_dir}")
        
        if not args.analysis_only:
            print(f"📋 PDF reports: {reports_dir}")
            
            # List generated reports
            if reports_dir.exists():
                pdf_files = list(reports_dir.glob("*.pdf"))
                if pdf_files:
                    print(f"\n📁 Generated {len(pdf_files)} PDF reports:")
                    for pdf in pdf_files:
                        print(f"   • {pdf.name}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Pipeline failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())