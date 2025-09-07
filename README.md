# Corporate Bond Fund Portfolio Analysis Pipeline

A comprehensive automated pipeline for analyzing Corporate Bond Fund portfolios from 6 Asset Management Companies (AMCs). This system processes, standardizes, validates, and consolidates portfolio data for institutional analysis.

## 📊 Project Overview

This pipeline processes portfolio data from 6 major AMCs as of **July 31, 2025**:
- **ABSLF** (Aditya Birla Sun Life)
- **HDFC** (HDFC Asset Management)
- **ICICI** (ICICI Prudential)
- **KOTAK** (Kotak Mahindra Asset Management)
- **NIPPON** (Nippon India Mutual Fund)
- **SBI** (SBI Funds Management)

### Key Results
- **922 holdings** across 6 funds
- **₹136,755 Crores** total portfolio value
- **97.4/100** data quality score
- **96.1%** average NAV coverage

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Virtual environment (recommended)

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd abc-mutual-fund

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage
```bash
# Run individual fund extraction
python src/extractors/extract_abslf.py

# Run full consolidation
python src/final_consolidation.py

# Run data quality validation
python src/data_quality_gates.py

# Generate visualizations
python src/visualize.py
```

## 📁 Project Structure

```
abc-mutual-fund/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── config/                   # Configuration files
│   ├── buckets.yml          # Maturity bucket definitions
│   ├── rating_map.yml       # Credit rating mappings
│   ├── schema_map.yml       # Column name standardization
│   └── sheet_hints.yml      # Excel sheet parsing hints
├── data/                     # Data directory
│   └── raw/2025-07-31/      # Raw Excel files from AMCs
├── src/                      # Source code
│   ├── extractors/          # Individual fund extractors
│   │   ├── extract_abslf.py
│   │   ├── extract_hdfc.py
│   │   ├── extract_icici.py
│   │   ├── extract_kotak.py
│   │   ├── extract_nippon.py
│   │   └── extract_sbi.py
│   ├── data_quality_gates.py    # Production validation system
│   ├── final_consolidation.py   # Main consolidation script
│   ├── utils.py                 # Utility functions
│   └── visualize.py            # Data visualization tools
└── output/2025-07-31/         # Generated outputs
    ├── Corporate_Bond_Funds_Consolidated_Analysis.csv
    ├── Fund_Summary_Report.csv
    └── individual_extracts/    # Individual fund CSV files
```

## 🔄 Pipeline Workflow

### 1. Data Extraction
Each fund has a dedicated extractor that handles:
- Excel file parsing with sheet-specific logic
- Column mapping and standardization
- Percentage format conversion
- Data type validation

```bash
# Extract individual funds
python src/extractors/extract_abslf.py
python src/extractors/extract_hdfc.py
# ... etc for all 6 funds
```

### 2. Data Consolidation
```bash
# Consolidate all funds into single dataset
python src/final_consolidation.py
```

### 3. Data Quality Validation
```bash
# Run comprehensive quality checks
python src/data_quality_gates.py
```

### 4. Visualization and Analysis
```bash
# Generate analysis charts and reports
python src/visualize.py
```

## 🛠️ Configuration

### Schema Mapping (`config/schema_map.yml`)
Standardizes column names across different AMCs:
```yaml
INSTRUMENT_NAME: ["Name of the Instrument", "Scrip Name", "Security Name"]
ISIN_CODE: ["ISIN", "ISIN Code", "ISIN CODE"]
MARKET_VALUE: ["Market Value", "Market Value (Rs in Lakhs)"]
```

### Rating Mapping (`config/rating_map.yml`)
Normalizes credit ratings to standard format:
```yaml
SOVEREIGN: ["SOVEREIGN", "GOVT", "Government"]
AAA: ["AAA", "AAA+", "AAA-"]
AA: ["AA+", "AA", "AA-"]
```

### Maturity Buckets (`config/buckets.yml`)
Defines time-based categorization:
```yaml
buckets:
  - name: "0-1 Year"
    min_years: 0
    max_years: 1
  - name: "1-3 Years"
    min_years: 1
    max_years: 3
```

## 📊 Data Quality System

The pipeline includes a comprehensive 8-gate validation system:

### Validation Gates
1. **Date Integrity** - Validates date formats and ranges
2. **NAV Sanity Check** - Ensures reasonable NAV values
3. **ISIN Format** - Validates ISIN code format
4. **Type Casting** - Ensures proper data types
5. **Outlier Detection** - Identifies statistical anomalies
6. **Coverage Analysis** - Checks data completeness
7. **Business Logic** - Validates business rules
8. **Cross-Fund Validation** - Ensures consistency across funds

### Quality Metrics
- **Overall Score**: 97.4/100 (Excellent)
- **Gates Passed**: 7/8
- **NAV Coverage**: 96.1% average across funds

## 📈 Output Files

### Main Outputs
1. **`Corporate_Bond_Funds_Consolidated_Analysis.csv`**
   - Complete consolidated dataset
   - 922 holdings across 6 funds
   - Standardized columns and formats

2. **`Fund_Summary_Report.csv`**
   - Fund-level summary statistics
   - NAV coverage and data quality metrics

3. **`individual_extracts/`**
   - Separate CSV files for each fund
   - Verified and validated data

### Data Columns
- `FUND_NAME`: Asset Management Company name
- `INSTRUMENT_NAME`: Security/instrument name
- `ISIN_CODE`: International Securities Identification Number
- `RATING`: Standardized credit rating
- `MARKET_VALUE_LACS`: Market value in Lacs (₹)
- `PERCENTAGE_TO_NAV`: Percentage of Net Asset Value
- `MATURITY_DATE`: Security maturity date
- `YIELD_PERCENT`: Yield percentage
- `SECTOR`: Industry sector classification

## 🔍 Data Processing Features

### Standardization
- **Currency**: All values in ₹ Lacs
- **Percentages**: Decimal format (e.g., 0.05 for 5%)
- **Ratings**: Standardized to industry conventions
- **ISINs**: Validated format checking

### Data Enrichment
- **Sector Classification**: Automatic sector tagging
- **Maturity Buckets**: Time-based categorization
- **Yield Analysis**: Statistical yield metrics
- **Rating Distribution**: Credit quality analysis

### Quality Assurance
- **Duplicate Detection**: Cross-fund holdings analysis
- **Outlier Identification**: Statistical anomaly detection
- **Missing Data Handling**: Comprehensive coverage reporting
- **Business Logic Validation**: Domain-specific rule checking

## 🎯 Use Cases

### Investment Analysis
- Portfolio composition analysis
- Credit quality assessment
- Sector concentration analysis
- Yield curve positioning

### Risk Management
- Duration risk assessment
- Credit risk evaluation
- Concentration risk analysis
- Liquidity analysis

### Compliance Reporting
- Regulatory compliance checks
- Investment guideline validation
- Portfolio limit monitoring
- Transparency reporting

## 🧪 Testing and Validation

### Data Quality Checks
```bash
# Run full validation suite
python src/data_quality_gates.py

# Individual extractor testing
python src/extractors/extract_abslf.py --test
```

### Manual Verification
- Fund-by-fund comparison with source files
- Statistical validation of aggregated metrics
- Cross-reference with AMC published data

## 🚨 Troubleshooting

### Common Issues
1. **File Not Found**: Ensure Excel files are in `data/raw/2025-07-31/`
2. **Permission Errors**: Check file permissions and close Excel files
3. **Memory Issues**: Large files may require increased memory allocation
4. **Encoding Issues**: Ensure UTF-8 encoding for text files

### Debug Mode
```bash
# Run with verbose logging
python src/final_consolidation.py --debug

# Test individual components
python -c "from src.utils import *; test_function()"
```

## 📝 Development

### Adding New Funds
1. Create new extractor in `src/extractors/`
2. Update configuration files
3. Add to consolidation script
4. Update validation rules

### Extending Analysis
1. Add new columns to schema mapping
2. Update data quality gates
3. Enhance visualization tools
4. Update documentation

## 📞 Support

For questions or issues:
1. Check existing documentation
2. Review data quality reports
3. Examine log files for errors
4. Validate input data format

## 📄 License

This project is for internal use and analysis purposes.

---

**Last Updated**: September 7, 2025  
**Data Date**: July 31, 2025  
**Version**: 1.0.0
