# Corporate Bond Fund Portfolio Analysis Pipeline

Comprehensive pipeline to ingest, analyze, and generate professional PDF reports for AMFI Corporate Bond Fund portfolios. Features automated data processing, standardization, analysis, and Quarto-based PDF report generation.

## 🚀 Quick Start

### Complete Analysis & Reporting
```bash
# Run full pipeline: analysis + PDF reports
python run_analysis.py "output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv"
```

### Data Processing Only
```bash
# Traditional data consolidation pipeline
python -m src.main --asof 2025-07-31 --raw-dir data/raw/2025-07-31 --out-dir output/2025-07-31 --config-dir config
```

## 📁 Project Structure
```
abc-mutual-fund/
├── config/                     # Configuration files
│   ├── buckets.yml            # Maturity/yield bucket definitions
│   ├── rating_map.yml         # Rating standardization
│   ├── schema_map.yml         # Column mapping
│   └── sheet_hints.yml        # Excel parsing hints
├── data/raw/2025-07-31/       # Raw Excel files (6 fund portfolios)
├── src/                       # Core data processing modules
│   ├── extractors/            # Fund-specific extractors
│   ├── final_consolidation.py # Data consolidation
│   └── utils.py               # Utility functions
├── output/2025-07-31/         # Processed outputs
│   ├── prepared_data/         # Analysis datasets
│   └── reports/               # Generated PDF reports
├── quarto-templates/          # Report templates
│   ├── overview_report.qmd    # Cross-fund analysis template
│   └── fund_report.qmd        # Individual fund template
├── analysis_engine.py         # Portfolio analysis engine
├── generate_reports.py        # PDF report generator
└── run_analysis.py           # Complete pipeline orchestrator
```

## 🔧 Installation

### System Requirements
- **Python 3.8+**
- **Quarto** - Install from https://quarto.org/

### Python Dependencies
```bash
pip install -r requirements.txt
```

### Verify Installation
```bash
# Check Quarto installation
quarto --version

# Check Python packages
python -c "import pandas, matplotlib, seaborn, numpy; print('All packages available')"
```

## 📊 Features

### Data Processing Pipeline
- **Multi-format Excel ingestion** (6 different fund templates)
- **Automated data standardization** and validation
- **Rating normalization** across different agencies
- **Issuer name extraction** and classification
- **Maturity bucket analysis** with intelligent parsing
- **Sovereign instrument identification**

### Analysis Engine
- **Weighted average yield calculations**
- **Credit rating distribution analysis**
- **Issuer concentration metrics**
- **Top holdings identification**
- **Risk assessment calculations**
- **Portfolio composition analysis**

### Professional PDF Reports
- **📈 Overview Report**: Cross-fund comparative analysis
- **📋 Individual Fund Reports**: Detailed per-fund analysis
- **Professional visualizations** with charts and tables
- **Executive summaries** with key findings
- **Descriptive file naming** for easy identification

## 🎯 Generated Outputs

### PDF Reports (in `reports/` directory)
- `Mutual_Fund_Portfolio_Overview_Analysis.pdf`
- `ABSLF_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
- `HDFC_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
- `ICICI_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
- `KOTAK_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
- `NIPPON_Corporate_Bond_Fund_Portfolio_Analysis.pdf`
- `SBI_Corporate_Bond_Fund_Portfolio_Analysis.pdf`

### Analysis Data (CSV files in `prepared_data/`)
- Fund-level metrics and summaries
- Rating distributions and concentrations
- Top holdings analysis
- Issuer exposure analysis
- Yield and maturity bucket analysis

## 💡 Usage Examples

```bash
# Complete analysis with all reports
python run_analysis.py "output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv"

# Overview report only
python run_analysis.py "path/to/data.csv" --overview-only

# Specific fund report
python generate_reports.py "path/to/data.csv" --fund "ABSLF Corporate Bond Fund"

# Analysis without PDF generation
python run_analysis.py "path/to/data.csv" --analysis-only

# PDF generation only (requires existing analysis)
python run_analysis.py "path/to/data.csv" --reports-only
```

## 🔍 Data Processing Notes
- Units normalized to **Rs. in Lacs**
- Maturity parsed from instrument names when missing
- AT1/Perpetual bonds excluded from maturity analysis
- Sovereign instruments (G-Sec/SDL/T-Bill) tagged with `SOVEREIGN` rating
- Weighted-average yields computed with coverage reporting
- Professional error handling and validation

## 📈 Analysis Capabilities
- **Rating Quality**: AAA/AA/A/BBB/Below IG distribution
- **Yield Analysis**: Performance across yield buckets
- **Maturity Profile**: Time-to-maturity distribution
- **Concentration Risk**: Top 10 holdings analysis
- **Issuer Exposure**: Concentration by issuer
- **Fund Comparison**: Cross-portfolio benchmarking

## 🛠️ Customization
- **Report Templates**: Edit `.qmd` files in `quarto-templates/`
- **Analysis Logic**: Modify `analysis_engine.py`
- **Data Processing**: Customize extractors in `src/extractors/`
- **Styling**: Update chart colors and formatting in templates

---
**Professional mutual fund portfolio analysis made simple!** 🎯
