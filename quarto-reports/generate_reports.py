#!/usr/bin/env python3
"""
Corporate Bond Fund Analysis - Report Generator
Automated generation of Quarto PDF reports for fund analysis
"""

import os
import sys
import subprocess
import pandas as pd
from pathlib import Path
import yaml
import shutil
from datetime import datetime
import argparse

class ReportGenerator:
    """Generate comprehensive PDF reports using Quarto"""
    
    def __init__(self, base_path="/Users/nakshatragupta/Documents/Coding/abc-mutual-fund"):
        self.base_path = Path(base_path)
        self.quarto_path = self.base_path / "quarto-reports"
        self.output_path = self.base_path / "output"
        
        # Fund mapping for individual reports
        self.fund_mapping = {
            "ABSLF_verified.csv": {"name": "Aditya Birla Sun Life Corporate Bond Fund", "amc": "ABSLF"},
            "HDFC_verified.csv": {"name": "HDFC Corporate Bond Fund", "amc": "HDFC"},
            "ICICI_verified.csv": {"name": "ICICI Prudential Corporate Bond Fund", "amc": "ICICI"},
            "KOTAK_verified.csv": {"name": "Kotak Corporate Bond Fund", "amc": "KOTAK"},
            "NIPPON_verified.csv": {"name": "Nippon India Corporate Bond Fund", "amc": "NIPPON"},
            "SBI_verified.csv": {"name": "SBI Corporate Bond Fund", "amc": "SBI"}
        }
    
    def check_dependencies(self):
        """Check if required dependencies are available"""
        print("Checking dependencies...")
        
        # Check if Quarto is installed
        try:
            result = subprocess.run(["quarto", "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✓ Quarto found: {result.stdout.strip()}")
            else:
                print("✗ Quarto not found. Please install Quarto to generate PDF reports.")
                print("  Visit: https://quarto.org/docs/get-started/")
                return False
        except FileNotFoundError:
            print("✗ Quarto not found. Please install Quarto to generate PDF reports.")
            print("  Visit: https://quarto.org/docs/get-started/")
            return False
        
        # Check Python dependencies
        required_packages = ["pandas", "numpy", "matplotlib", "seaborn"]
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package)
                print(f"✓ {package} available")
            except ImportError:
                missing_packages.append(package)
        
        if missing_packages:
            print(f"✗ Missing packages: {', '.join(missing_packages)}")
            print(f"  Install with: pip install {' '.join(missing_packages)}")
            return False
        
        return True
    
    def prepare_data(self, report_date):
        """Prepare data for reporting"""
        print(f"Preparing data for {report_date}...")
        
        # Import and run data preparation
        sys.path.insert(0, str(self.quarto_path))
        
        try:
            import data_prep
            from data_prep import QuartoDataPrep
            
            # Initialize data preparation
            prep = QuartoDataPrep(
                base_path=str(self.base_path),
                report_date=report_date
            )
            
            # Load and prepare all data
            success = prep.run_full_prep()
            
            if success is None:  # run_full_prep doesn't return a value, assume success if no exception
                print("✓ Data preparation completed successfully")
                return True
            elif success:
                print("✓ Data preparation completed successfully")
                return True
            else:
                print("✗ Data preparation failed")
                return False
                
        except Exception as e:
            print(f"✗ Error in data preparation: {str(e)}")
            return False
    
    def generate_overview_report(self, report_date, output_dir):
        """Generate the overview report"""
        print("Generating overview report...")
        
        try:
            # Set working directory to quarto-reports
            original_cwd = os.getcwd()
            os.chdir(self.quarto_path)
            
            # Create output directory
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate the overview report
            cmd = [
                "quarto", "render", "overview-report.qmd",
                "--to", "pdf",
                "--output-dir", str(output_dir),
                "-P", f"report_date:{report_date}"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✓ Overview report generated successfully")
                # Rename to more descriptive name
                old_path = output_dir / "overview-report.pdf"
                new_path = output_dir / f"Corporate_Bond_Funds_Overview_{report_date}.pdf"
                if old_path.exists():
                    shutil.move(str(old_path), str(new_path))
                    print(f"  → {new_path.name}")
                return True
            else:
                print(f"✗ Overview report generation failed:")
                print(result.stderr)
                return False
                
        except Exception as e:
            print(f"✗ Error generating overview report: {str(e)}")
            return False
        finally:
            os.chdir(original_cwd)
    
    def generate_fund_report(self, fund_file, fund_info, report_date, output_dir):
        """Generate individual fund report"""
        fund_name = fund_info["name"]
        amc_name = fund_info["amc"]
        
        print(f"Generating report for {fund_name}...")
        
        try:
            # Set working directory to quarto-reports
            original_cwd = os.getcwd()
            os.chdir(self.quarto_path)
            
            # Generate the fund report
            cmd = [
                "quarto", "render", "fund-report.qmd",
                "--to", "pdf",
                "--output-dir", str(output_dir),
                "-P", f"fund_name:{fund_name}",
                "-P", f"amc_name:{amc_name}",
                "-P", f"report_date:{report_date}",
                "-P", f"data_file:{fund_file}"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Rename to more descriptive name
                old_path = output_dir / "fund-report.pdf"
                new_path = output_dir / f"{amc_name}_Fund_Analysis_{report_date}.pdf"
                if old_path.exists():
                    shutil.move(str(old_path), str(new_path))
                    print(f"  ✓ {new_path.name}")
                return True
            else:
                print(f"  ✗ Failed to generate {fund_name} report:")
                print(result.stderr)
                return False
                
        except Exception as e:
            print(f"  ✗ Error generating {fund_name} report: {str(e)}")
            return False
        finally:
            os.chdir(original_cwd)
    
    def generate_all_reports(self, report_date):
        """Generate all reports for the specified date"""
        print(f"\\n{'='*60}")
        print(f"CORPORATE BOND FUNDS - REPORT GENERATION")
        print(f"Report Date: {report_date}")
        print(f"{'='*60}")
        
        # Check dependencies
        if not self.check_dependencies():
            return False
        
        # Prepare data
        if not self.prepare_data(report_date):
            return False
        
        # Setup output directory
        output_dir = self.output_path / report_date / "reports"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate overview report
        overview_success = self.generate_overview_report(report_date, output_dir)
        
        # Generate individual fund reports
        fund_successes = []
        individual_extracts_path = self.output_path / report_date / "individual_extracts"
        
        if individual_extracts_path.exists():
            for fund_file, fund_info in self.fund_mapping.items():
                fund_data_path = individual_extracts_path / fund_file
                if fund_data_path.exists():
                    success = self.generate_fund_report(fund_file, fund_info, report_date, output_dir)
                    fund_successes.append(success)
                else:
                    print(f"  ⚠ {fund_file} not found - skipping")
        
        # Summary
        print(f"\\n{'='*60}")
        print("REPORT GENERATION SUMMARY")
        print(f"{'='*60}")
        
        if overview_success:
            print("✓ Overview Report: Generated successfully")
        else:
            print("✗ Overview Report: Failed")
        
        successful_funds = sum(fund_successes)
        total_funds = len(fund_successes)
        print(f"✓ Individual Fund Reports: {successful_funds}/{total_funds} generated successfully")
        
        if overview_success and successful_funds > 0:
            print(f"\\n📁 Reports saved to: {output_dir}")
            print("\\n📊 Report Files:")
            for pdf_file in output_dir.glob("*.pdf"):
                print(f"  • {pdf_file.name}")
        
        return overview_success and successful_funds > 0
    
    def list_available_dates(self):
        """List available report dates"""
        print("Available report dates:")
        
        if not self.output_path.exists():
            print("  No output directory found")
            return []
        
        dates = []
        for date_dir in self.output_path.iterdir():
            if date_dir.is_dir() and date_dir.name.count('-') == 2:  # YYYY-MM-DD format
                consolidated_file = date_dir / "Corporate_Bond_Funds_Consolidated_Analysis.csv"
                if consolidated_file.exists():
                    dates.append(date_dir.name)
                    print(f"  • {date_dir.name}")
        
        return sorted(dates)

def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(description="Generate Corporate Bond Fund Analysis Reports")
    parser.add_argument("--date", "-d", 
                       help="Report date (YYYY-MM-DD format). If not specified, will list available dates.")
    parser.add_argument("--list", "-l", action="store_true",
                       help="List available report dates")
    
    args = parser.parse_args()
    
    generator = ReportGenerator()
    
    if args.list or not args.date:
        available_dates = generator.list_available_dates()
        if not args.date and available_dates:
            print(f"\\nTo generate reports, use: python generate_reports.py --date {available_dates[-1]}")
        return
    
    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("Error: Date must be in YYYY-MM-DD format")
        return
    
    # Generate reports
    success = generator.generate_all_reports(args.date)
    
    if success:
        print("\\n🎉 Report generation completed successfully!")
    else:
        print("\\n❌ Report generation failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
