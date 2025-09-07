# Mutual Fund Portfolio Analysis System

🚀 **Automated analysis and reporting system for mutual fund portfolios with professional PDF generation featuring descriptive file naming and comprehensive analysis.**

## ✨ Latest Improvements

### 🎯 **Enhanced PDF Report Naming**
Reports now generate with clear, descriptive names:
- **Overview Report**: `Mutual_Fund_Portfolio_Overview_Analysis.pdf`
- **Fund-Specific Reports**: 
  - `ABSLF_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
  - `HDFC_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
  - `ICICI_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
  - `KOTAK_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
  - `NIPPON_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
  - `SBI_Corporate_Bond_Fund_Portfolio_Analysis.pdf`

### 🔧 **Fixed Quarto Integration Issues**
- ✅ Resolved path confusion between templates and data directories
- ✅ Fixed working directory management for reliable PDF generation
- ✅ Improved fund name parameter passing mechanisms
- ✅ Enhanced error handling and debugging information
- ✅ Eliminated syntax errors in Quarto templates

## 🚀 Quick Start

### 1. Complete Analysis (Recommended)
```bash
python run_analysis.py "output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv"
```
This runs both analysis and generates all PDF reports with proper descriptive names.

### 2. Individual Steps

#### Run Analysis Only
```bash
python analysis_engine.py "output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv"
```

#### Generate Reports Only (requires existing analysis)
```bash
python generate_reports.py "output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv"
```

#### Generate Overview Report Only
```bash
python run_analysis.py "path/to/your/data.csv" --overview-only
```

## System Requirements

### Dependencies
- **Python 3.8+**
- **Quarto** (for PDF generation) - Install from https://quarto.org/
- **Python packages**: pandas, matplotlib, seaborn, numpy

### Install Python Requirements
```bash
pip install pandas matplotlib seaborn numpy
```

## What This System Does

### 📊 Analysis Engine (`analysis_engine.py`)
- **Standardizes Data**: Extracts issuer names, classifies instruments, standardizes ratings
- **Calculates Metrics**: Weighted average yields, concentration ratios, risk metrics
- **Creates Analysis Datasets**:
  - Rating distribution by fund
  - Top holdings analysis  
  - Issuer concentration metrics
  - Yield bucket analysis
  - Maturity analysis (where available)
  - Fund performance summaries

### 📋 Report Generation (`generate_reports.py`)
- **Overview Report**: Cross-fund comparative analysis with professional charts
- **Individual Fund Reports**: Detailed analysis per fund
- **Professional Visualizations**:
  - Rating distribution pie charts
  - Yield bucket analysis
  - Top holdings bar charts
  - Issuer concentration analysis
  - Risk assessment summaries

## Generated Outputs

### Analysis Data (CSV files in `prepared_data/`)
- `overview_metrics.csv` - Key metrics by fund
- `rating_distribution.csv` - Rating analysis
- `top_holdings_by_fund.csv` - Top holdings per fund
- `issuer_analysis.csv` - Issuer concentration
- `yield_analysis.csv` - Yield bucket distribution
- `maturity_analysis.csv` - Maturity buckets
- `analysis_summary.json` - Summary statistics

### PDF Reports (in `reports/`)
- `Mutual_Fund_Portfolio_Overview_Analysis.pdf` - Comprehensive cross-fund analysis
- `ABSLF_Corporate_Bond_Fund_Portfolio_Analysis.pdf` - ADITYA BIRLA SUN LIFE fund analysis
- `HDFC_Corporate_Bond_Fund_Portfolio_Analysis.pdf` - HDFC fund analysis  
- `ICICI_Corporate_Bond_Fund_Portfolio_Analysis.pdf` - ICICI PRUDENTIAL fund analysis
- `KOTAK_Corporate_Bond_Fund_Portfolio_Analysis.pdf` - KOTAK MAHINDRA fund analysis
- `NIPPON_Corporate_Bond_Fund_Portfolio_Analysis.pdf` - NIPPON INDIA fund analysis
- `SBI_Corporate_Bond_Fund_Portfolio_Analysis.pdf` - SBI fund analysis

## Key Features

### ✅ Automated Processing
- Single command runs complete analysis pipeline
- Handles multiple funds automatically
- Professional PDF formatting

### ✅ Comprehensive Analysis
- **Rating Distribution**: AAA, AA, A, etc. with percentages
- **Yield Analysis**: Bucket analysis (<5%, 5-6%, 6-7%, 7-8%, >8%)
- **Issuer Concentration**: Top issuers by exposure
- **Maturity Analysis**: Time buckets (where data available)
- **Risk Metrics**: Concentration ratios, credit quality

### ✅ Professional Reporting
- Clean, sorted visualizations
- Fund manager-ready presentations
- Executive summary sections
- Key findings and recommendations

## Input Data Requirements

Your CSV should have these columns:
- `Fund Name`
- `ISIN`
- `Instrument Name`
- `Market Value (Lacs)`
- `% to NAV`
- `Yield`
- `Standardized Rating`
- `Quantity`
- `Maturity Date` (optional)

## Example Usage

```bash
# Full analysis for your consolidated CSV
python run_analysis.py "output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv"

# Analysis only (no PDF generation)
python run_analysis.py "path/to/data.csv" --analysis-only

# Reports only (if analysis already done)
python run_analysis.py "path/to/data.csv" --reports-only

# Just the overview report
python run_analysis.py "path/to/data.csv" --overview-only
```

## Troubleshooting

### Common Issues

1. **"Quarto not found"**
   - Install Quarto from [https://quarto.org/](https://quarto.org/)
   - Ensure it's in your PATH
   - Verify with: `quarto --version`

2. **"Analysis data not found"**
   - Run `analysis_engine.py` first
   - Check that CSV path is correct
   - Verify prepared_data/ directory exists

3. **PDF generation fails**
   - Ensure all Python packages are installed: `pip install pandas matplotlib seaborn numpy`
   - Check that Quarto templates exist in quarto-templates/
   - Verify working directory permissions

4. **Fund name parameter issues**
   - Check fund names match exactly with CSV data
   - Use quotes around fund names with spaces
   - Verify FUND_NAME environment variable is not set

5. **Path resolution errors**
   - Ensure you're running from the project root directory
   - Check that CSV file path is correct (absolute or relative)
   - Verify output directory structure permissions

### Enhanced Debugging

The system now provides detailed debugging information:
- Working directory confirmation
- Fund name mapping verification  
- File path validation
- PDF generation status tracking

### Getting Help

- Check that your CSV has all required columns: Fund Name, ISIN, Instrument Name, Market Value (Lacs), % to NAV, Yield, Standardized Rating
- Verify Python packages: `python -c "import pandas, matplotlib, seaborn, numpy; print('All packages available')"`
- Ensure Quarto is properly installed: `quarto --version`
- Check file permissions in output directory

## Output Structure

After running the analysis, you'll have:

```
your-data-directory/
├── Corporate_Bond_Funds_Consolidated_Analysis.csv    (your input)
├── prepared_data/                                     (analysis outputs)
│   ├── analysis_summary.json
│   ├── overview_metrics.csv
│   ├── rating_distribution.csv
│   ├── top_holdings_by_fund.csv
│   └── [other analysis files...]
└── reports/                                          (PDF reports)
    ├── Mutual_Fund_Portfolio_Overview_Analysis.pdf
    ├── ABSLF_Corporate_Bond_Fund_Portfolio_Analysis.pdf
    ├── HDFC_Corporate_Bond_Fund_Portfolio_Analysis.pdf
    ├── ICICI_Corporate_Bond_Fund_Portfolio_Analysis.pdf
    ├── KOTAK_Corporate_Bond_Fund_Portfolio_Analysis.pdf
    ├── NIPPON_Corporate_Bond_Fund_Portfolio_Analysis.pdf
    └── SBI_Corporate_Bond_Fund_Portfolio_Analysis.pdf
```

## Customization

- **Colors/Styling**: Modify the Quarto templates (`overview_report.qmd`, `fund_report.qmd`)
- **Analysis Logic**: Edit `analysis_engine.py` for different calculations
- **Report Content**: Customize the `.qmd` files for different sections

---

**Ready to analyze your mutual fund portfolios!** 🚀