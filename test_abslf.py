#!/usr/bin/env python3
"""
Simple test script for ABSLF scraping.
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import requests
from bs4 import BeautifulSoup
import yaml

def setup_logging():
    """Simple logging setup."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def load_abslf_config():
    """Load ABSLF configuration."""
    with open('config/scrapers/abslf.yml', 'r') as f:
        return yaml.safe_load(f)

def test_abslf_scraping():
    """Test ABSLF website scraping."""
    logger = logging.getLogger(__name__)
    
    # Load config
    config = load_abslf_config()
    base_url = config['disclosure_sources']['website']['base_url']
    
    logger.info(f"Testing ABSLF scraping from: {base_url}")
    
    try:
        # Make request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(base_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Successfully loaded page, status: {response.status_code}")
        logger.info(f"Content length: {len(response.content)} bytes")
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all links
        all_links = soup.find_all('a', href=True)
        logger.info(f"Found {len(all_links)} total links")
        
        # Filter for Excel files
        excel_links = []
        for link in all_links:
            href = link.get('href', '')
            if any(ext in href.lower() for ext in ['.xls', '.xlsx']):
                excel_links.append({
                    'text': link.get_text(strip=True),
                    'href': href,
                    'full_url': requests.compat.urljoin(base_url, href)
                })
        
        logger.info(f"Found {len(excel_links)} Excel file links:")
        
        for i, link in enumerate(excel_links, 1):
            print(f"{i}. {link['text']}")
            print(f"   URL: {link['full_url']}")
            print()
        
        # Filter by fund types
        fund_types = config.get('fund_types', {})
        
        for fund_type, fund_config in fund_types.items():
            print(f"=== {fund_type.upper()} FUNDS ===")
            
            file_patterns = fund_config.get('file_patterns', [])
            matching_files = []
            
            for link in excel_links:
                text = link['text'].lower()
                href = link['href'].lower()
                
                # Check if link matches any pattern
                for pattern in file_patterns:
                    pattern_lower = pattern.lower().replace('*', '')
                    if pattern_lower in text or pattern_lower in href:
                        matching_files.append(link)
                        break
            
            print(f"Found {len(matching_files)} matching files:")
            for file_link in matching_files:
                print(f"  - {file_link['text']}")
                print(f"    {file_link['full_url']}")
            print()
        
        return True
        
    except Exception as e:
        logger.error(f"Error scraping ABSLF: {e}")
        return False

def download_test_file():
    """Test downloading one file."""
    logger = logging.getLogger(__name__)
    
    # This would be a real file URL from the discovery above
    # For now, just show the process
    logger.info("To test download, pick a URL from the discovery above and use:")
    logger.info("python -c \"import requests; r=requests.get('URL'); open('test.xlsx', 'wb').write(r.content)\"")

if __name__ == "__main__":
    setup_logging()
    
    try:
        print("=== ABSLF Scraper Test ===")
        print()
        
        success = test_abslf_scraping()
        
        if success:
            print("✅ Test completed successfully!")
            print("\nNext steps:")
            print("1. Pick files you want to download from the list above")
            print("2. Test download with: python run_scraper.py --amc ABSLF --dry-run")
            print("3. Actual download with: python run_scraper.py --amc ABSLF")
        else:
            print("❌ Test failed. Check the logs above.")
            sys.exit(1)
            
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install requests beautifulsoup4 pyyaml")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)