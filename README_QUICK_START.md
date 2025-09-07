# Quick Start Guide

## Complete Pipeline (Recommended)

```bash
python run_analysis.py output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv
```

## Step-by-Step Flow

### 1. Data Consolidation

```bash
python src/final_consolidation.py
```

### 2. Data Quality Check

```bash
python src/data_quality_gates.py output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv
```

### 3. Run Analysis & Generate Reports

```bash
python run_analysis.py output/2025-07-31/Corporate_Bond_Funds_Consolidated_Analysis.csv
```

## Output Location

- **Reports**: `output/2025-07-31/reports/`
- **Data**: `output/2025-07-31/prepared_data/`

## Generated Files

- Mutual_Fund_Portfolio_Overview_Analysis.pdf
- ABSLF_Corporate_Bond_Fund_Portfolio_Analysis.pdf
- HDFC_Corporate_Bond_Fund_Portfolio_Analysis.pdf
- ICICI_Corporate_Bond_Fund_Portfolio_Analysis.pdf
- KOTAK_Corporate_Bond_Fund_Portfolio_Analysis.pdf
- NIPPON_Corporate_Bond_Fund_Portfolio_Analysis.pdf
- SBI_Corporate_Bond_Fund_Portfolio_Analysis.pdf
