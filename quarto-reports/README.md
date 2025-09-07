# Corporate Bond Fund Analysis - Quarto Reporting System

This directory contains a comprehensive Quarto-based reporting system for generating professional PDF reports from corporate bond fund analysis data.

## 📋 Overview

The reporting system automatically generates:
- **Overview Report**: Cross-fund analysis with portfolio summaries, rating distributions, and risk metrics
- **Individual Fund Reports**: Detailed analysis for each fund including holdings, concentration, and yield analysis

## 🏗️ System Architecture

```
quarto-reports/
├── _quarto.yml                 # Quarto project configuration
├── styles.scss                 # Custom CSS styling for professional reports
├── data-prep.py                # Data preparation and analytics engine
├── overview-report.qmd          # Cross-fund overview report template
├── fund-report.qmd             # Individual fund report template
├── generate_reports.py         # Automated report generation script
├── prepared_data/              # Analytics-ready datasets (auto-generated)
└── README.md                   # This file
```

## 🚀 Quick Start

### Prerequisites

1. **Install Quarto**: Download from [quarto.org](https://quarto.org/docs/get-started/)
2. **Python Dependencies**:
   ```bash
   pip install pandas numpy matplotlib seaborn
   ```

### Generate Reports

```bash
# List available report dates
python generate_reports.py --list

# Generate reports for specific date
python generate_reports.py --date 2025-07-31
```

## 📊 Report Components

### Overview Report (`overview-report.qmd`)
- **Executive Summary**: Key portfolio metrics and insights
- **Fund House Analysis**: AUM distribution and yield comparison
- **Credit Quality Analysis**: Rating distribution across all funds
- **Issuer Concentration**: Top issuers and concentration metrics
- **Yield Analysis**: Yield distribution and characteristics
- **Risk Assessment**: Portfolio concentration and quality metrics

### Individual Fund Report (`fund-report.qmd`)
- **Fund Overview**: Key performance indicators
- **Portfolio Composition**: Holdings summary and top positions
- **Credit Quality Analysis**: Rating breakdown and distribution
- **Issuer Analysis**: Concentration and exposure metrics
- **Yield Analysis**: Distribution and risk-return profile
- **Risk Assessment**: Comprehensive risk metrics
- **Data Quality**: Coverage and completeness assessment

## 🔧 Data Preparation Pipeline

The `data-prep.py` script creates analytics-ready datasets:

### Input Data
- Consolidated fund data from `output/{date}/Corporate_Bond_Funds_Consolidated_Analysis.csv`
- Individual fund extracts from `output/{date}/individual_extracts/`

### Generated Datasets
- `overview_metrics.csv`: High-level portfolio metrics
- `amc_summary.csv`: Fund house summaries
- `rating_distribution.csv`: Credit rating analysis
- `top_issuers.csv`: Issuer concentration data
- `yield_analysis.csv`: Yield bucket analysis
- `concentration_metrics.csv`: Risk concentration measures
- `data_quality_summary.csv`: Data coverage statistics

## 🎨 Styling and Formatting

### Professional Design Features
- **Corporate Color Scheme**: Consistent branding with rating-specific colors
- **Responsive Tables**: Clean, professional table formatting
- **Interactive Charts**: High-quality matplotlib/seaborn visualizations
- **KPI Dashboards**: Visual key performance indicators
- **Executive Summaries**: Highlighted insights and recommendations

### Rating Color Coding
- **SOVEREIGN**: Deep blue (#1f4e79)
- **AAA**: Blue (#2f5f8f)
- **AA**: Lighter blue (#3f6f9f)
- **A**: Green (#4f7f4f)
- **BBB**: Orange (#df8f2f)
- **OTHER**: Gray (#8f8f8f)

## 🔄 Automated Workflow

### Data Flow
1. **Source Data**: Raw fund portfolio files
2. **Processing**: Rating standardization and data cleaning
3. **Consolidation**: Cross-fund analysis and aggregation
4. **Preparation**: Analytics dataset generation
5. **Reporting**: PDF generation with Quarto

### Report Generation Process
1. **Dependency Check**: Verify Quarto and Python packages
2. **Data Preparation**: Load and process fund data
3. **Analytics Generation**: Create reporting datasets
4. **Template Rendering**: Generate PDF reports
5. **Output Organization**: Save to structured directories

## 📁 Output Structure

Reports are saved to `output/{date}/reports/`:
```
output/2025-07-31/reports/
├── Corporate_Bond_Funds_Overview_2025-07-31.pdf
├── ABSLF_Fund_Analysis_2025-07-31.pdf
├── HDFC_Fund_Analysis_2025-07-31.pdf
├── ICICI_Fund_Analysis_2025-07-31.pdf
├── KOTAK_Fund_Analysis_2025-07-31.pdf
├── NIPPON_Fund_Analysis_2025-07-31.pdf
└── SBI_Fund_Analysis_2025-07-31.pdf
```

## 🔧 Customization

### Adding New Funds
Update `fund_mapping` in `generate_reports.py`:
```python
self.fund_mapping = {
    "NEW_FUND_verified.csv": {
        "name": "New Fund Name", 
        "amc": "NEW_AMC"
    }
}
```

### Modifying Report Content
- **Overview Report**: Edit `overview-report.qmd`
- **Fund Reports**: Edit `fund-report.qmd`
- **Styling**: Modify `styles.scss`
- **Analytics**: Update `data-prep.py`

### Configuration Options
Adjust `_quarto.yml` for:
- PDF formatting options
- Font and layout settings
- Include/exclude sections
- Output formats (HTML, Word, etc.)

## 🐛 Troubleshooting

### Common Issues

**Quarto Not Found**
```bash
# Install Quarto from https://quarto.org/docs/get-started/
quarto --version  # Verify installation
```

**Missing Python Packages**
```bash
pip install pandas numpy matplotlib seaborn
```

**Data File Not Found**
- Ensure consolidated analysis has been run first
- Check date format (YYYY-MM-DD)
- Verify output directory structure

**PDF Generation Fails**
- Check LaTeX installation (required for PDF output)
- Review error messages in terminal
- Ensure data preparation completed successfully

### Debug Mode
For detailed error information:
```bash
# Run with verbose output
python generate_reports.py --date 2025-07-31 --verbose
```

## 📈 Performance Tips

### Large Datasets
- Reports handle large portfolios efficiently
- Charts automatically scale for readability
- Data preparation optimizes memory usage

### Batch Processing
- Generate all reports with single command
- Parallel processing for multiple funds
- Automated data validation and quality checks

## 🤝 Contributing

### Development Guidelines
- Follow PEP 8 for Python code
- Use meaningful variable names
- Add docstrings for new functions
- Test with different dataset sizes

### Adding Features
1. Update data preparation pipeline
2. Modify report templates
3. Test with sample data
4. Update documentation

## 📞 Support

For technical issues:
1. Check error messages carefully
2. Verify all dependencies are installed
3. Ensure data files are in correct format
4. Review troubleshooting section above

---

**Last Updated**: January 2025  
**System Version**: 1.0  
**Supported Data Format**: Corporate Bond Fund Portfolio CSV files
