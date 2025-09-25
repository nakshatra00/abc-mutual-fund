# Mutual Fund Portfolio Scraper

A simple, flexible scraper system for downloading mutual fund portfolio files from different AMCs (Asset Management Companies).

## Features

- **Multi-AMC Support**: ICICI, ABSLF, HDFC (easily extensible)
- **Different Organization Strategies**: Each AMC has its own file organization approach
- **Date-based Downloads**: Specify target dates for portfolio files
- **Fund Type Classification**: Automatically sorts files into corporate-bond, money-market, gilt, and other categories

## Quick Start

```bash
# ICICI - Downloads ZIP files and sorts by fund type
python scraper/scraper.py icici --type fortnightly --date 2025-09-15
python scraper/scraper.py icici --type monthly --date 2025-08-31

# ABSLF - Downloads files and copies to all fund type folders
python scraper/scraper.py abslf --type fortnightly --date 2025-09-15
python scraper/scraper.py abslf --type monthly --date 2025-08-31

# HDFC - Downloads individual files based on manual configuration
python scraper/scraper.py hdfc --type fortnightly --date 2025-09-15
python scraper/scraper.py hdfc --type monthly --date 2025-08-31
```

## Directory Structure

```
scraper/
├── scraper.py          # Main scraper script
├── icici.yml           # ICICI configuration with URL patterns
├── abslf.yml           # ABSLF configuration  
├── hdfc.yml            # HDFC manual file configuration
└── data/
    └── raw/
        └── YYYY-MM-DD/
            ├── icici/
            │   ├── corporate-bond/
            │   ├── money-market/
            │   ├── gilt/
            │   └── other/
            ├── abslf/
            │   ├── corporate-bond/
            │   ├── money-market/
            │   └── gilt/
            └── hdfc/
                ├── corporate-bond/
                ├── money-market/
                ├── gilt/
                └── other/
```

## AMC-Specific Behaviors

### ICICI
- Downloads ZIP files containing multiple portfolio files
- Automatically sorts files into fund type folders based on filename patterns
- Supports both monthly and fortnightly downloads
- Uses dynamic URL building with date formatting

### ABSLF  
- Downloads individual portfolio files
- Copies the same file to ALL fund type folders (corporate-bond, money-market, gilt)
- Special date formatting (e.g., "15-sep-2025")
- Single file covers all fund types

### HDFC
- Uses manual file configuration in `hdfc.yml`
- Downloads individual Excel files for specific funds
- Each file is manually categorized by fund type in the config
- Easily extensible by updating the YAML configuration

## Configuration Files

Each AMC has its own YAML configuration file:

### ICICI (`icici.yml`)
- URL patterns with placeholders for dynamic date building
- Fund type patterns for automatic file classification
- Supports both ZIP extraction and file organization

### ABSLF (`abslf.yml`)
- URL patterns with ABSLF-specific date formatting
- Copy-to-all-folders strategy configuration

### HDFC (`hdfc.yml`)
- Manual file lists with specific URLs
- Per-file fund type classification
- Easy to update by adding new URLs to the manual_files section

## Adding New AMCs

1. Create a new YAML config file: `scraper/new_amc.yml`
2. Define the AMC's URL patterns or manual file lists
3. Add fund type patterns for automatic classification
4. The scraper will automatically handle the new AMC

## File Organization Strategies

1. **Pattern-Based Sorting** (ICICI): Files sorted by matching filename patterns
2. **Copy-to-All** (ABSLF): Same file copied to all fund type directories  
3. **Manual Classification** (HDFC): Files organized based on manual configuration

## Error Handling

- Graceful handling of network errors (403, 404, etc.)
- Continues processing even if some downloads fail
- Clear error messages and download summaries
- Automatic directory creation