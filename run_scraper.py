#!/usr/bin/env python3
"""
Main scraper runner for mutual fund disclosures.

Usage:
    python run_scraper.py --amc HDFC --fund-type corporate-bond
    python run_scraper.py --all
    python run_scraper.py --amc HDFC,ICICI --fund-type corporate-bond,money-market
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scrapers import load_scraper_manager, ScraperManager


def setup_logging(level: str = "INFO"):
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/scraper_run.log', mode='a')
        ]
    )
    
    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)


async def main():
    """Main entry point for the scraper."""
    parser = argparse.ArgumentParser(
        description="Scrape mutual fund portfolio disclosures from AMC websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                                    # Scrape all AMCs and fund types
  %(prog)s --amc HDFC                              # Scrape all fund types for HDFC
  %(prog)s --amc HDFC --fund-type corporate-bond   # Scrape specific fund type for HDFC
  %(prog)s --amc HDFC,ICICI,UTI                    # Scrape multiple AMCs
  %(prog)s --fund-type corporate-bond,money-market # Scrape specific fund types for all AMCs
  %(prog)s --list-amcs                             # List available AMCs
  %(prog)s --dry-run --amc HDFC                    # Show what would be scraped without downloading
        """
    )
    
    parser.add_argument(
        '--amc', 
        type=str,
        help='AMC name(s) to scrape (comma-separated). Use --list-amcs to see available options.'
    )
    
    parser.add_argument(
        '--fund-type',
        type=str, 
        help='Fund type(s) to scrape (comma-separated). e.g., corporate-bond,money-market,equity'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Scrape all configured AMCs and fund types'
    )
    
    parser.add_argument(
        '--list-amcs',
        action='store_true',
        help='List all available AMCs and exit'
    )
    
    parser.add_argument(
        '--list-fund-types',
        type=str,
        metavar='AMC_NAME',
        help='List available fund types for specified AMC and exit'
    )
    
    parser.add_argument(
        '--config-dir',
        type=str,
        default='config',
        help='Path to configuration directory (default: config)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be scraped without actually downloading files'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Override output directory for downloaded files'
    )
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load scraper manager
        logger.info(f"Loading scraper configuration from {args.config_dir}")
        manager = load_scraper_manager(args.config_dir)
        
        # Override output directory if specified
        if args.output_dir:
            manager.global_config.setdefault('scraper', {})['base_output_dir'] = args.output_dir
            logger.info(f"Output directory overridden to: {args.output_dir}")
        
        # Handle list commands
        if args.list_amcs:
            amcs = manager.get_available_amcs()
            print(f"Available AMCs ({len(amcs)}):")
            for amc in sorted(amcs):
                print(f"  - {amc}")
            return
        
        if args.list_fund_types:
            fund_types = manager.get_amc_fund_types(args.list_fund_types)
            if fund_types:
                print(f"Available fund types for {args.list_fund_types.upper()}:")
                for fund_type in sorted(fund_types):
                    print(f"  - {fund_type}")
            else:
                print(f"AMC '{args.list_fund_types}' not found or has no configured fund types")
            return
        
        # Parse arguments
        amc_names = None
        if args.amc:
            amc_names = [amc.strip().upper() for amc in args.amc.split(',')]
        
        fund_types = None
        if args.fund_type:
            fund_types = [ft.strip().lower() for ft in args.fund_type.split(',')]
        
        # Validate arguments
        if not args.all and not amc_names:
            parser.error("Either --all or --amc must be specified")
        
        # Show dry run information
        if args.dry_run:
            logger.info("DRY RUN MODE - No files will be downloaded")
            
            if args.all:
                print("Would scrape ALL AMCs and fund types:")
                for amc in sorted(manager.get_available_amcs()):
                    fund_types_for_amc = manager.get_amc_fund_types(amc)
                    print(f"  {amc}: {', '.join(fund_types_for_amc)}")
            else:
                print(f"Would scrape AMCs: {', '.join(amc_names)}")
                if fund_types:
                    print(f"Fund types: {', '.join(fund_types)}")
                else:
                    print("Fund types: ALL configured for each AMC")
            return
        
        # Execute scraping
        logger.info("Starting scrape operation")
        
        if args.all:
            logger.info("Scraping all AMCs and fund types")
            results = await manager.scrape_all(fund_types)
        else:
            if len(amc_names) == 1:
                logger.info(f"Scraping {amc_names[0]}")
                results = await manager.scrape_amc(amc_names[0], fund_types)
            else:
                logger.info(f"Scraping multiple AMCs: {', '.join(amc_names)}")
                results = await manager.scrape_all(fund_types, amc_names)
        
        # Print summary
        if 'amc_results' in results:  # Bulk operation
            print(f"\n=== BULK SCRAPE SUMMARY ===")
            print(f"Total files attempted: {results.get('total_files_attempted', 0)}")
            print(f"Successful downloads: {results.get('total_successful_downloads', 0)}")
            print(f"Success rate: {results.get('overall_success_rate', 0):.1%}")
            print(f"Duration: {results.get('duration_seconds', 0):.1f} seconds")
            
            print(f"\nPer-AMC Results:")
            for amc, amc_result in results['amc_results'].items():
                if amc_result.get('success', True):
                    success_rate = amc_result.get('success_rate', 0)
                    attempted = amc_result.get('total_files_attempted', 0)
                    successful = amc_result.get('successful_downloads', 0)
                    print(f"  {amc}: {successful}/{attempted} files ({success_rate:.1%})")
                else:
                    print(f"  {amc}: ERROR - {amc_result.get('error', 'Unknown error')}")
        else:  # Single AMC operation
            amc_name = results.get('amc_name', 'Unknown')
            success_rate = results.get('success_rate', 0)
            attempted = results.get('total_files_attempted', 0)
            successful = results.get('successful_downloads', 0)
            
            print(f"\n=== SCRAPE SUMMARY FOR {amc_name} ===")
            print(f"Files attempted: {attempted}")
            print(f"Successful downloads: {successful}")
            print(f"Success rate: {success_rate:.1%}")
            
            if 'fund_type_results' in results:
                print(f"\nPer-Fund-Type Results:")
                for fund_type, fund_results in results['fund_type_results'].items():
                    successful_in_type = len([r for r in fund_results if r.get('success', False)])
                    print(f"  {fund_type}: {successful_in_type}/{len(fund_results)} files")
        
        logger.info("Scrape operation completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Scrape operation interrupted by user")
        print("\nOperation cancelled by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Scrape operation failed: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Ensure we can import our modules
    try:
        asyncio.run(main())
    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure all required packages are installed:")
        print("  pip install aiohttp beautifulsoup4 requests pandas pyyaml")
        sys.exit(1)