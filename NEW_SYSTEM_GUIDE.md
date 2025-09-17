# New Extraction System Usage Guide

## Quick Start

### 1. Extract Data
```bash
# Extract all AMCs for corporate bond funds
python extract.py --date 2025-07-31 --fund-type corporate-bond --amc all

# Extract specific AMCs
python extract.py --date 2025-07-31 --fund-type corporate-bond --amc ABSLF,HDFC,ICICI

# Extract money market funds (when available)
python extract.py --date 2025-08-31 --fund-type money-market --amc all
```

### 2. Consolidate Data
```bash
# Consolidate all extracted corporate bond funds
python consolidate.py --date 2025-07-31 --fund-type corporate-bond

# Consolidate money market funds
python consolidate.py --date 2025-08-31 --fund-type money-market
```

## Folder Structure

```
data/raw/{date}/{fund-type}/
├── corporate-bond/
│   ├── ABSLF_SEBI_Monthly_Portfolio_31_JULY_2025.xlsm
│   ├── Monthly_HDFC_Corporate_Bond_Fund_31_July_2025.xlsx
│   └── ...
└── money-market/
    └── (future money market files)

output/{date}/{fund-type}/
├── individual_extracts/
│   ├── ABSLF_verified.csv
│   ├── HDFC_verified.csv
│   └── ...
└── consolidated/
    └── corporate-bond_consolidated_2025-07-31.csv
```

## Configuration Files

Each AMC has a YAML config in `Ingestion_Auto/`:
- `abslf.yml` - Aditya Birla Sun Life
- `hdfc.yml` - HDFC Asset Management  
- `icici.yml` - ICICI Prudential
- `kotak.yml` - Kotak Mahindra
- `nippon.yml` - Nippon India
- `sbi.yml` - SBI Mutual Fund

## Key Features

- ✅ **Temporal Tracking**: "As Of Date" column for time-series analysis
- ✅ **Flexible Fund Types**: Easy to add new fund types (equity, hybrid, etc.)
- ✅ **AMC-Specific Handling**: Each AMC's unique format handled via YAML config
- ✅ **Rating Standardization**: Automatic cross-AMC rating standardization
- ✅ **CLI-Driven**: Simple command-line interface
- ✅ **No Analysis**: Pure extraction and consolidation only

## Output Schema

All extracts follow this standardized schema:
```
Fund Name, AMC, ISIN, Instrument Name, Market Value (Lacs), 
% to NAV, Yield, Rating, Quantity, As Of Date, Maturity Date, Standardized Rating
```
