# Mutual Fund Disclosure Scraper

This system provides automated scraping capabilities for downloading mutual fund portfolio disclosures from various Asset Management Companies (AMCs).

## Features

- **Multi-AMC Support**: Configurable scrapers for HDFC, ICICI, UTI, SBI, and other major AMCs
- **Fund Type Filtering**: Support for corporate-bond, money-market, equity, and other fund types  
- **Intelligent Discovery**: Automatic detection of disclosure files on AMC websites
- **Metadata Tracking**: Comprehensive metadata for all downloaded files
- **Rate Limiting**: Respectful scraping with configurable delays
- **Error Handling**: Robust retry logic and error recovery
- **Structured Logging**: JSON-formatted logs for monitoring and debugging
- **Validation**: File size, format, and content validation

## Quick Start

1. **Setup Environment**:
   ```bash
   python setup_scraper.py
   pip install -r requirements.txt
   ```

2. **List Available AMCs**:
   ```bash
   python run_scraper.py --list-amcs
   ```

3. **Dry Run** (see what would be scraped):
   ```bash
   python run_scraper.py --amc HDFC --dry-run
   ```

4. **Scrape Specific AMC**:
   ```bash
   python run_scraper.py --amc HDFC --fund-type corporate-bond
   ```

5. **Scrape All AMCs**:
   ```bash
   python run_scraper.py --all
   ```

## Configuration

### Global Configuration (`config/scraper_config.yml`)

Controls overall scraper behavior:
- Output directories and file management
- Rate limiting and timeout settings
- Logging configuration
- Validation rules

### AMC-Specific Configuration (`config/scrapers/amc_name.yml`)

Each AMC has its own configuration file with:
- Website URLs and navigation patterns
- File pattern matching rules
- Fund type configurations
- Parsing strategies for different file formats

## Architecture

```
src/scrapers/
├── base_scraper.py       # Abstract base classes
├── scraper_manager.py    # Orchestration and factory
├── logging_utils.py      # Logging and metadata utilities
└── __init__.py          # Package exports

config/
├── scraper_config.yml    # Global configuration
└── scrapers/            # AMC-specific configs
    ├── hdfc.yml
    ├── icici.yml
    ├── uti.yml
    └── ...

data/raw/
└── YYYY-MM-DD/          # Date-based organization
    ├── corporate-bond/
    │   ├── hdfc/
    │   │   ├── original/     # Raw downloaded files
    │   │   └── processed/    # Processed data
    │   └── ...
    └── money-market/
        └── ...
```

## Usage Examples

### Command Line Interface

```bash
# Scrape all fund types for HDFC
python run_scraper.py --amc HDFC

# Scrape corporate bond funds for multiple AMCs
python run_scraper.py --amc HDFC,ICICI,UTI --fund-type corporate-bond

# Scrape everything with custom output directory
python run_scraper.py --all --output-dir /path/to/custom/output

# List fund types for specific AMC
python run_scraper.py --list-fund-types HDFC

# Enable debug logging
python run_scraper.py --amc HDFC --log-level DEBUG
```

### Programmatic Usage

```python
import asyncio
from src.scrapers import load_scraper_manager, quick_scrape

# Quick scrape for single AMC
result = await quick_scrape("HDFC", ["corporate-bond"])

# Full manager for complex operations
manager = load_scraper_manager()
result = await manager.scrape_all(
    fund_types=["corporate-bond", "money-market"],
    amc_names=["HDFC", "ICICI"]
)
```

## Output Structure

Downloaded files are organized by date and fund type:

```
data/raw/2025-09-25/
├── corporate-bond/
│   └── hdfc/
│       ├── original/
│       │   ├── HDFC_Corporate_Bond_Portfolio_Jul2025.xlsx
│       │   └── HDFC_Corporate_Bond_Portfolio_Jul2025_metadata.json
│       └── processed/
└── scrape_summary_20250925_143022.json
```

Each downloaded file includes metadata:
- Source URL and download timestamp
- File hash and validation status  
- AMC and fund type information
- Processing status tracking

## Monitoring and Logging

The system provides comprehensive logging:

- **Console logs**: Human-readable progress and status
- **File logs**: Rotating log files with detailed information
- **JSON logs**: Structured logs for programmatic analysis
- **Session stats**: Download statistics and performance metrics

## Configuration Customization

### Adding New AMCs

1. Create new YAML config in `config/scrapers/`
2. Define website URLs and file patterns
3. Configure fund types and parsing rules
4. Test with dry run mode

### Adding New Fund Types

1. Add fund type to global `fund_types` list
2. Configure fund type in relevant AMC configs
3. Define file patterns and CSS selectors
4. Update parsing strategies if needed

## Error Handling

The scraper includes robust error handling:

- **Retry Logic**: Configurable retry attempts with exponential backoff
- **Rate Limiting**: Automatic delays to respect website policies
- **Validation**: File size, format, and content validation
- **Graceful Degradation**: Continue scraping other files on individual failures

## Integration

The scraper integrates with the existing mutual fund analysis system:

- Downloads files to the expected `data/raw/` structure
- Maintains compatibility with existing extraction pipeline
- Provides metadata for processing status tracking
- Can trigger downstream analysis automatically

## Development

To extend the scraper system:

1. Inherit from `BaseScraper` for new scraper types
2. Implement `discover_files()` and `download_file()` methods
3. Add scraper type to `ScraperFactory.SCRAPER_TYPES`
4. Create appropriate configuration templates

## Support

For issues or questions:
1. Check logs in `logs/` directory
2. Run setup script to verify configuration
3. Use dry run mode to debug discovery issues
4. Enable debug logging for detailed information