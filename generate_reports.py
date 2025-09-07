#!/usr/bin/env python3
"""
Report Generation Script
Automates the creation of all PDF reports using Quarto
"""

import subprocess
import sys
import os
import pandas as pd
import argparse
from pathlib import Path
import shutil

class ReportGenerator:
    def __init__(self, csv_path):
        self.csv_path = Path(csv_path)
        self.base_dir = self.csv_path.parent  # Directory containing the CSV
        self.reports_dir = self.base_dir / "reports"
        self.prepared_data_dir = self.base_dir / "prepared_data"
        self.project_root = Path(__file__).parent  # abc-mutual-fund directory
        
    def check_dependencies(self):
        """Check if required tools are available"""
        print("🔍 Checking dependencies...")
        
        # Check for Quarto
        try:
            result = subprocess.run(['quarto', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Quarto found: {result.stdout.strip()}")
            else:
                print("❌ Quarto not found. Please install Quarto from https://quarto.org/")
                return False
        except FileNotFoundError:
            print("❌ Quarto not found. Please install Quarto from https://quarto.org/")
            return False
        
        # Check for Python packages
        required_packages = ['pandas', 'matplotlib', 'seaborn', 'numpy']
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package)
                print(f"✅ {package} available")
            except ImportError:
                missing_packages.append(package)
                print(f"❌ {package} not found")
        
        if missing_packages:
            print(f"💡 Install missing packages: pip install {' '.join(missing_packages)}")
            return False
            
        return True
    
    def setup_directories(self):
        """Ensure all required directories exist and copy QMD templates to the data directory"""
        self.reports_dir.mkdir(exist_ok=True)
        
        # Templates directory in project root
        templates_dir = self.project_root / "quarto-templates"
        
        # Copy QMD templates to the same directory as the CSV (where data will be)
        overview_target = self.base_dir / "overview_report.qmd"
        fund_target = self.base_dir / "fund_report.qmd"
        
        # Copy templates to the base directory
        if templates_dir.exists():
            shutil.copy2(templates_dir / "overview_report.qmd", overview_target)
            shutil.copy2(templates_dir / "fund_report.qmd", fund_target)
            print(f"📁 Report templates copied to: {self.base_dir}")
        else:
            print(f"⚠️  Templates directory not found: {templates_dir}")
            
        # Verify data directory exists
        if not self.prepared_data_dir.exists():
            print(f"⚠️  Prepared data directory not found: {self.prepared_data_dir}")
            print("   Make sure to run analysis_engine.py first")
    
    def get_fund_list(self):
        """Get list of unique funds from the CSV"""
        df = pd.read_csv(self.csv_path)
        funds = df['Fund Name'].unique()
        print(f"📊 Found {len(funds)} funds: {', '.join(funds)}")
        return funds
    
    def generate_overview_report(self):
        """Generate the overview PDF report"""
        print("\n🔄 Generating Overview Report...")
        
        overview_qmd = self.base_dir / "overview_report.qmd"
        
        if not overview_qmd.exists():
            print(f"❌ Overview QMD not found: {overview_qmd}")
            return False
            
        if not self.prepared_data_dir.exists():
            print(f"❌ Prepared data directory not found: {self.prepared_data_dir}")
            return False
        
        try:
            # Change to the base directory to ensure relative paths work
            original_cwd = os.getcwd()
            os.chdir(self.base_dir)
            print(f"🔄 Working directory: {self.base_dir}")
            
            # Define descriptive output name
            output_pdf_name = "Mutual_Fund_Portfolio_Overview_Analysis.pdf"
            final_output_path = self.reports_dir / output_pdf_name
            
            # Run Quarto render with specific output name
            cmd = [
                'quarto', 'render', 'overview_report.qmd',
                '--to', 'pdf',
                '--output', output_pdf_name
            ]
            
            print(f"🔄 Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Ensure reports directory exists
                if not self.reports_dir.exists():
                    self.reports_dir.mkdir(parents=True, exist_ok=True)
                
                # Move generated PDF to reports directory
                possible_pdfs = [
                    Path(output_pdf_name),  # Specified output name (relative path)
                    Path("overview_report.pdf"),  # Default name (relative path)
                ]
                
                moved = False
                for pdf_path in possible_pdfs:
                    if pdf_path.exists():
                        pdf_path.rename(final_output_path)
                        print(f"✅ Overview report generated and saved as: {final_output_path}")
                        moved = True
                        break
                
                if not moved:
                    print(f"⚠️  Quarto completed successfully but PDF not found")
                    print(f"   Expected locations: {[str(p) for p in possible_pdfs]}")
                    # List actual files in directory
                    pdf_files = list(Path(".").glob("*.pdf"))
                    print(f"   Found PDFs: {[p.name for p in pdf_files]}")
                    return True  # Still consider success if Quarto succeeded
                
                return True
            else:
                print(f"❌ Error generating overview report:")
                print(f"STDOUT: {result.stdout}")
                print(f"STDERR: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Exception during overview report generation: {str(e)}")
            return False
        finally:
            os.chdir(original_cwd)
    
    def generate_fund_report(self, fund_name):
        """Generate individual fund report"""
        print(f"🔄 Generating report for {fund_name}...")
        
        fund_qmd = self.base_dir / "fund_report.qmd"
        
        # Create a clean, safe filename from fund name
        safe_fund_name = (fund_name.replace(' ', '_')
                                  .replace('/', '_')
                                  .replace('(', '')
                                  .replace(')', '')
                                  .replace('-', '_')
                                  .replace('.', '_'))
        
        if not fund_qmd.exists():
            print(f"❌ Fund QMD not found: {fund_qmd}")
            return False
            
        if not self.prepared_data_dir.exists():
            print(f"❌ Prepared data directory not found: {self.prepared_data_dir}")
            return False
        
        try:
            # Change to the base directory
            original_cwd = os.getcwd()
            os.chdir(self.base_dir)
            print(f"🔄 Working directory: {self.base_dir}")
            print(f"🔄 Fund name: '{fund_name}' -> Safe filename: '{safe_fund_name}'")
            
            # Set environment variable for fund name (most reliable method)
            os.environ['FUND_NAME'] = fund_name
            
            # Also create a temporary file with fund name as backup
            temp_fund_file = Path("temp_fund_name.txt")  # Use relative path since we're in base_dir
            with open(temp_fund_file, 'w') as f:
                f.write(fund_name)
            
            # Define the output PDF name with fund-specific naming
            output_pdf_name = f"{safe_fund_name}_Portfolio_Analysis.pdf"
            final_output_path = self.reports_dir / output_pdf_name
            
            # Run Quarto render directly on the template
            cmd = [
                'quarto', 'render', 'fund_report.qmd',
                '--to', 'pdf',
                '--output', output_pdf_name,  # Specify exact output name
                '-P', f'fund_name="{fund_name}"'
            ]
            
            print(f"🔄 Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # Clean up
            if temp_fund_file.exists():
                temp_fund_file.unlink()
            if 'FUND_NAME' in os.environ:
                del os.environ['FUND_NAME']
            
            if result.returncode == 0:
                # Ensure reports directory exists
                if not self.reports_dir.exists():
                    self.reports_dir.mkdir(parents=True, exist_ok=True)
                
                # Check for generated PDF and move it
                possible_pdfs = [
                    Path(output_pdf_name),  # Specified output name (relative path)
                    Path("fund_report.pdf"),  # Default name (relative path)
                ]
                
                moved = False
                for pdf_path in possible_pdfs:
                    if pdf_path.exists():
                        pdf_path.rename(final_output_path)
                        print(f"✅ Fund report generated and saved as: {final_output_path}")
                        moved = True
                        break
                
                if not moved:
                    print(f"⚠️  Quarto completed successfully but PDF not found")
                    print(f"   Expected locations: {[str(p) for p in possible_pdfs]}")
                    # List actual files in directory
                    pdf_files = list(Path(".").glob("*.pdf"))
                    print(f"   Found PDFs: {[p.name for p in pdf_files]}")
                    return True  # Still consider success if Quarto succeeded
                
                return True
            else:
                print(f"❌ Error generating fund report for {fund_name}:")
                print(f"STDOUT: {result.stdout}")
                print(f"STDERR: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Exception during fund report generation: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            os.chdir(original_cwd)
    
    def generate_all_reports(self):
        """Generate all reports"""
        if not self.check_dependencies():
            return False
        
        self.setup_directories()
        
        # Check if analysis data exists
        if not self.prepared_data_dir.exists() or not (self.prepared_data_dir / "analysis_summary.json").exists():
            print("❌ Analysis data not found. Please run analysis_engine.py first.")
            return False
        
        print(f"\n🚀 Generating all reports from: {self.csv_path}")
        
        success_count = 0
        
        # Generate overview report
        if self.generate_overview_report():
            success_count += 1
        
        # Generate individual fund reports
        funds = self.get_fund_list()
        for fund in funds:
            if self.generate_fund_report(fund):
                success_count += 1
        
        total_reports = len(funds) + 1  # funds + overview
        
        print(f"\n🎉 Report generation completed!")
        print(f"📊 Successfully generated: {success_count}/{total_reports} reports")
        print(f"📁 Reports saved to: {self.reports_dir}")
        
        # List generated files
        print("\n📋 Generated Reports:")
        for pdf_file in self.reports_dir.glob("*.pdf"):
            print(f"   • {pdf_file.name}")
        
        return success_count == total_reports

def main():
    parser = argparse.ArgumentParser(description='Generate PDF reports from analyzed mutual fund data')
    parser.add_argument('csv_path', help='Path to the consolidated portfolio CSV file')
    parser.add_argument('--overview-only', action='store_true', help='Generate only the overview report')
    parser.add_argument('--fund', help='Generate report for specific fund only')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_path):
        print(f"❌ Error: CSV file not found: {args.csv_path}")
        return 1
    
    try:
        generator = ReportGenerator(args.csv_path)
        
        if args.fund:
            # Generate specific fund report
            generator.setup_directories()
            if generator.generate_fund_report(args.fund):
                print(f"✅ Report generated for {args.fund}")
                return 0
            else:
                print(f"❌ Failed to generate report for {args.fund}")
                return 1
                
        elif args.overview_only:
            # Generate only overview report
            generator.setup_directories()
            if generator.generate_overview_report():
                print("✅ Overview report generated successfully")
                return 0
            else:
                print("❌ Failed to generate overview report")
                return 1
        else:
            # Generate all reports
            if generator.generate_all_reports():
                return 0
            else:
                return 1
                
    except Exception as e:
        print(f"❌ Error during report generation: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())