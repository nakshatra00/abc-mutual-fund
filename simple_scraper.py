#!/usr/bin/env python3
"""
Simple Mutual Fund Portfolio Scraper - Function-based approach
=============================================================

This module provides simple functions for scraping portfolio disclosures
from different AMC websites and organidef download_file(url: str, filepath: Path, session: requests.Session) -> bool:
    """Download a file from URL to filepath."""
    try:
        response = session.get(url, timeout=30, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return True
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False


def download_icici_file(file_info: Dict[str, Any], filepath: Path, config: Dict[str, Any]) -> bool:
    """Download ICICI file using Selenium (for JavaScript-triggered downloads)."""
    if not SELENIUM_AVAILABLE:
        print("❌ Selenium not available for ICICI downloads")
        return False
    
    driver = create_selenium_driver(config)
    if not driver:
        return False
    
    try:
        # Navigate to the download page
        disclosure_config = config['disclosure_types'][file_info['disclosure_type']]
        base_url = disclosure_config['base_url']
        
        print(f"🔄 ICICI: Loading download page...")
        driver.get(base_url)
        
        # Wait and find the download element by text
        wait = WebDriverWait(driver, 30)
        
        # Look for the specific download link/button
        target_text = file_info['element_text']
        xpath = f"//a[contains(text(), '{target_text}')]|//button[contains(text(), '{target_text}')]"
        
        try:
            download_element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            
            # Set up download directory (Chrome will download to default location)
            download_dir = filepath.parent
            download_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"🔄 ICICI: Clicking download for: {target_text}")
            download_element.click()
            
            # Wait for download to complete (basic approach)
            time.sleep(10)  # Give time for download to start
            
            # Look for downloaded file in download folder
            # This is a simplified approach - in production you'd want more robust download detection
            downloaded_files = list(download_dir.glob("*.zip")) + list(download_dir.glob("*.xlsx")) + list(download_dir.glob("*.xls"))
            
            if downloaded_files:
                # Move the downloaded file to our target location
                latest_file = max(downloaded_files, key=lambda x: x.stat().st_mtime)
                shutil.move(str(latest_file), str(filepath))
                print(f"✅ ICICI: Downloaded and moved to {filepath}")
                return True
            else:
                print(f"❌ ICICI: No file found after download attempt")
                return False
                
        except TimeoutException:
            print(f"❌ ICICI: Timeout finding download element for: {target_text}")
            return False
            
    except Exception as e:
        print(f"❌ ICICI: Download failed: {e}")
        return False
    finally:
        driver.quit()
    
    return Falsefund-type folders.

- ABSLF: One file with multiple sheets -> extract, rename, copy to all fund folders
- ICICI: Multiple files in ZIP -> extract each to respective fund folder  
- Other AMCs: Similar patterns based on their structure
"""

import os
import yaml
import requests
import zipfile
import shutil
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# Selenium imports (optional - install with: pip install selenium)
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
from typing import Dict, List, Any, Optional, Tuple
import re
import time


def load_config(config_path: str) -> Dict[str, Any]:
    """Load scraper configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_years_to_check(years_count: int = 2) -> List[int]:
    """Get list of years to check (current and previous years)."""
    current_year = datetime.now().year
    return [current_year - i for i in range(years_count)]


def build_abslf_urls(config: Dict[str, Any], disclosure_type: str = "monthly") -> List[Dict[str, Any]]:
    """Build URLs for ABSLF portfolio files."""
    if disclosure_type not in config['disclosure_types']:
        return []
    
    disclosure_config = config['disclosure_types'][disclosure_type]
    pattern_key = disclosure_config['pattern_key']
    url_pattern = config['url_patterns'][pattern_key]
    date_patterns = disclosure_config['date_patterns']
    
    urls = []
    years = get_years_to_check(config.get('years_to_check', 2))
    
    for year in years:
        for date_pattern in date_patterns:
            # Handle year substitution in date patterns
            formatted_date = date_pattern.format(year=year) if '{year}' in date_pattern else date_pattern
            url = url_pattern.format(year=year, date=formatted_date)
            
            urls.append({
                'url': url,
                'filename': os.path.basename(url),
                'disclosure_type': disclosure_type,
                'year': year,
                'date_pattern': formatted_date,
                'estimated_date': parse_date_pattern(formatted_date, year),
                'amc': 'abslf'
            })
    
    return urls


def parse_date_pattern(date_pattern: str, year: int) -> str:
    """Convert date pattern to YYYY-MM-DD format."""
    month_map = {
        'jan': '01', 'january': '01', 'feb': '02', 'february': '02',
        'mar': '03', 'march': '03', 'apr': '04', 'april': '04',
        'may': '05', 'jun': '06', 'june': '06',
        'jul': '07', 'july': '07', 'aug': '08', 'august': '08',
        'sep': '09', 'september': '09', 'oct': '10', 'october': '10',
        'nov': '11', 'november': '11', 'dec': '12', 'december': '12'
    }
    
    # Parse patterns like "31-january-2025" or "15-sep-2025"
    parts = date_pattern.split('-')
    if len(parts) >= 3:
        day, month_name = parts[0], parts[1]
        month_num = month_map.get(month_name.lower(), '01')
        return f"{year}-{month_num}-{day.zfill(2)}"
    elif len(parts) == 2:
        day, month_name = parts
        month_num = month_map.get(month_name.lower(), '01')
        return f"{year}-{month_num}-{day.zfill(2)}"
    
    return f"{year}-01-01"


def check_url_exists(url: str, session: requests.Session) -> bool:
    """Check if URL exists and returns a downloadable file."""
    try:
        response = session.head(url, timeout=10)
        if response.status_code == 200:
            return True
        # Try GET if HEAD fails
        response = session.get(url, timeout=10, stream=True)
        return response.status_code == 200
    except:
        return False


def discover_abslf_files(config_path: str = "ingestion/configs/abslf.yml", 
                        disclosure_types: List[str] = None) -> List[Dict[str, Any]]:
    """Discover available ABSLF portfolio files."""
    config = load_config(config_path)
    
    if disclosure_types is None:
        disclosure_types = list(config['disclosure_types'].keys())
    
    print(f"🔍 Checking disclosure types: {disclosure_types}")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': config.get('request_settings', {}).get('user_agent', 
                               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
    })
    
    all_files = []
    
    for disclosure_type in disclosure_types:
        print(f"🔍 Building URLs for {disclosure_type}...")
        urls_to_check = build_abslf_urls(config, disclosure_type)
        print(f"🔍 Generated {len(urls_to_check)} URLs to check")
        
        for url_info in urls_to_check:
            print(f"🔍 Checking: {url_info['url']}")
            if check_url_exists(url_info['url'], session):
                print(f"✅ Found: {url_info['filename']}")
                all_files.append(url_info)
            else:
                print(f"❌ Not found: {url_info['filename']}")
    
    print(f"🔍 Total files found: {len(all_files)}")
    return sorted(all_files, key=lambda x: x['estimated_date'], reverse=True)


def create_selenium_driver(config: Dict[str, Any]) -> Optional[webdriver.Chrome]:
    """Create a Selenium Chrome driver with proper configuration."""
    if not SELENIUM_AVAILABLE:
        print("❌ Selenium not available. Install with: pip install selenium")
        return None
    
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Run in background
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        browser_settings = config.get('browser_settings', {})
        user_agent = config.get('request_settings', {}).get('user_agent')
        if user_agent:
            chrome_options.add_argument(f'--user-agent={user_agent}')
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(browser_settings.get('implicit_wait', 10))
        driver.set_page_load_timeout(browser_settings.get('page_load_timeout', 30))
        
        return driver
    except Exception as e:
        print(f"❌ Failed to create Selenium driver: {e}")
        return None


def discover_icici_files(config_path: str = "ingestion/configs/icici.yml", 
                        disclosure_types: List[str] = None) -> List[Dict[str, Any]]:
    """Discover available ICICI portfolio files using Selenium."""
    config = load_config(config_path)
    
    if disclosure_types is None:
        disclosure_types = list(config['disclosure_types'].keys())
    
    print(f"🔍 ICICI: Checking disclosure types: {disclosure_types}")
    
    driver = create_selenium_driver(config)
    if not driver:
        return []
    
    all_files = []
    
    try:
        for disclosure_type in disclosure_types:
            print(f"🔍 ICICI: Checking {disclosure_type} disclosures...")
            
            disclosure_config = config['disclosure_types'][disclosure_type]
            base_url = disclosure_config['base_url']
            
            # Load the page
            print(f"🔍 ICICI: Loading page: {base_url}")
            driver.get(base_url)
            
            # Wait for page to load
            wait = WebDriverWait(driver, config.get('browser_settings', {}).get('wait_timeout', 30))
            
            try:
                # Look for download links
                download_elements = []
                for selector in disclosure_config['link_selectors']:
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        download_elements.extend(elements) 
                    except:
                        continue
                
                print(f"🔍 ICICI: Found {len(download_elements)} potential download elements")
                
                # Check each element for target dates
                for element in download_elements:
                    try:
                        # Get the text and href
                        text = element.text.strip()
                        href = element.get_attribute('href') or element.get_attribute('onclick') or ''
                        
                        # Look for September 15, 2025 pattern
                        if re.search(r'15.*?sep.*?2025|september.*?15.*?2025', text, re.IGNORECASE):
                            filename = f"icici_{disclosure_type}_15-sep-2025.zip"
                            all_files.append({
                                'url': href,
                                'filename': filename,
                                'disclosure_type': disclosure_type,
                                'year': 2025,
                                'date_pattern': '15-sep-2025',
                                'estimated_date': '2025-09-15',
                                'amc': 'icici',
                                'element_text': text,
                                'download_element': element
                            })
                            print(f"✅ ICICI: Found target file: {text}")
                    except Exception as e:
                        continue
                        
            except TimeoutException:
                print(f"⚠️ ICICI: Timeout waiting for page to load: {base_url}")
                continue
                
    finally:
        driver.quit()
    
    print(f"🔍 ICICI: Total files found: {len(all_files)}")
    return sorted(all_files, key=lambda x: x['estimated_date'], reverse=True)


def download_file(url: str, filepath: Path, session: requests.Session) -> bool:
    """Download a file from URL to filepath."""
    try:
        response = session.get(url, timeout=30, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return True
    except Exception as e:
        print(f"❌ Failed to download {filepath.name}: {e}")
        return False


def extract_zip_file(zip_path: Path, extract_to: Path) -> List[Path]:
    """Extract ZIP file and return list of extracted files."""
    extracted_files = []
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Extract all files
            zf.extractall(extract_to)
            
            # Get list of extracted files
            for filename in zf.namelist():
                if not filename.endswith('/'):  # Skip directories
                    extracted_files.append(extract_to / filename)
        
        return extracted_files
    except Exception as e:
        print(f"❌ Failed to extract {zip_path.name}: {e}")
        return []


def determine_fund_type_from_filename(filename: str, fund_type_patterns: Dict[str, List[str]]) -> str:
    """Determine fund type from filename using patterns."""
    filename_lower = filename.lower()
    
    for fund_type, patterns in fund_type_patterns.items():
        for pattern in patterns:
            if re.search(pattern.lower(), filename_lower):
                return fund_type
    
    return 'mixed'  # Default if no pattern matches


def organize_abslf_files(extracted_files: List[Path], output_base: Path, 
                        fund_type_patterns: Dict[str, List[str]], date_str: str) -> Dict[str, List[str]]:
    """
    Organize ABSLF files: Single file with multiple sheets goes to all fund-type folders.
    
    ABSLF downloads contain one Excel file with different fund data in different sheets.
    We need to copy this single file to each fund-type folder with standardized naming.
    """
    organized_files = {}
    
    # ABSLF typically has one main Excel file
    main_file = None
    for file_path in extracted_files:
        if file_path.suffix.lower() in ['.xlsx', '.xls']:
            main_file = file_path
            break
    
    if not main_file:
        print("❌ No Excel file found in ABSLF download")
        return organized_files
    
    print(f"📄 Processing ABSLF file: {main_file.name}")
    
    # Copy the same file to each fund-type folder with standardized naming
    for fund_type in fund_type_patterns.keys():
        fund_dir = output_base / fund_type
        fund_dir.mkdir(parents=True, exist_ok=True)
        
        # Create standardized filename: abslf_fundtype_date.xlsx
        new_filename = f"abslf_{fund_type}_{date_str}.xlsx"
        dest_path = fund_dir / new_filename
        
        try:
            shutil.copy2(main_file, dest_path)
            
            if fund_type not in organized_files:
                organized_files[fund_type] = []
            organized_files[fund_type].append(new_filename)
            
            print(f"✅ Copied to {fund_type}/{new_filename}")
            
        except Exception as e:
            print(f"❌ Failed to copy to {fund_type}: {e}")
    
    return organized_files


def organize_icici_files(extracted_files: List[Path], output_base: Path, 
                        fund_type_patterns: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Organize ICICI files: Different files in ZIP go to respective fund-type folders.
    
    ICICI downloads contain multiple Excel files, each for different fund types.
    Each file should go to its appropriate fund-type folder based on filename.
    """
    organized_files = {}
    
    for file_path in extracted_files:
        if file_path.suffix.lower() not in ['.xlsx', '.xls']:
            continue
        
        # Determine fund type from filename
        fund_type = determine_fund_type_from_filename(file_path.name, fund_type_patterns)
        
        # Create fund-type directory
        fund_dir = output_base / fund_type
        fund_dir.mkdir(parents=True, exist_ok=True)
        
        # Move file to appropriate directory
        dest_path = fund_dir / file_path.name
        
        try:
            shutil.move(str(file_path), str(dest_path))
            
            if fund_type not in organized_files:
                organized_files[fund_type] = []
            organized_files[fund_type].append(file_path.name)
            
            print(f"✅ Moved {file_path.name} to {fund_type}/")
            
        except Exception as e:
            print(f"❌ Failed to move {file_path.name} to {fund_type}: {e}")
    
    return organized_files


def scrape_amc(amc_name: str, disclosure_types: List[str] = None, 
               max_files: Optional[int] = None, output_dir: Optional[str] = None,
               config_dir: str = "ingestion/configs") -> Dict[str, Any]:
    """
    Main function to scrape portfolio disclosures for any AMC.
    
    Args:
        amc_name: Name of the AMC (abslf, icici, etc.)
        disclosure_types: Types of disclosures to scrape
        max_files: Maximum number of files to download
        output_dir: Custom output directory
        config_dir: Directory containing config files
        
    Returns:
        Dictionary with scraping results
    """
    print(f"🚀 Starting {amc_name.upper()} portfolio scraping...")
    
    # Load AMC configuration
    config_file = Path(config_dir) / f"{amc_name.lower()}.yml"
    if not config_file.exists():
        config_file = Path(config_dir) / f"{amc_name.lower()}_scraper.yml"
    
    if not config_file.exists():
        return {
            'success': False,
            'message': f'Config file not found for {amc_name}',
            'amc_name': amc_name
        }
    
    config = load_config(str(config_file))
    
    # Setup output directory
    if output_dir is None:
        base_dir = config.get('output', {}).get('base_dir', 'data/raw')
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_path = Path(base_dir) / date_str / amc_name.lower()
    else:
        output_path = Path(output_dir)
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create session
    session = requests.Session()
    session.headers.update({
        'User-Agent': config.get('request_settings', {}).get('user_agent',
                               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
    })
    
    # Discover files based on AMC type
    if amc_name.lower() == 'abslf':
        available_files = discover_abslf_files(str(config_file), disclosure_types)
    elif amc_name.lower() == 'icici':
        available_files = discover_icici_files(str(config_file), disclosure_types)
    else:
        # For other AMCs, implement similar discovery functions
        print(f"⚠️ Discovery not yet implemented for {amc_name}")
        return {
            'success': False,
            'message': f'Discovery not implemented for {amc_name}',
            'amc_name': amc_name
        }
    
    if not available_files:
        return {
            'success': False,
            'message': 'No files found',
            'amc_name': amc_name,
            'files_attempted': 0,
            'files_downloaded': 0
        }
    
    # Limit files if requested
    if max_files and len(available_files) > max_files:
        available_files = available_files[:max_files]
    
    print(f"📊 Found {len(available_files)} files to download")
    
    # Download and organize files
    successful_downloads = 0
    organized_files = {}
    fund_type_patterns = config.get('fund_type_patterns', {})
    
    for i, file_info in enumerate(available_files, 1):
        print(f"[{i}/{len(available_files)}] ⬇️ {file_info['filename']}")
        
        # Download file
        download_path = output_path / file_info['filename']
        
        if download_file(file_info['url'], download_path, session):
            successful_downloads += 1
            
            # Extract if ZIP file
            if download_path.suffix.lower() == '.zip':
                extract_dir = output_path / f"extracted_{i}"
                extract_dir.mkdir(exist_ok=True)
                
                extracted_files = extract_zip_file(download_path, extract_dir)
                
                if extracted_files:
                    # Organize files based on AMC type
                    if amc_name.lower() == 'abslf':
                        file_organized = organize_abslf_files(
                            extracted_files, output_path, fund_type_patterns, date_str
                        )
                    elif amc_name.lower() == 'icici':
                        file_organized = organize_icici_files(
                            extracted_files, output_path, fund_type_patterns
                        )
                    else:
                        # Default organization
                        file_organized = organize_icici_files(
                            extracted_files, output_path, fund_type_patterns
                        )
                    
                    # Merge results
                    for fund_type, files in file_organized.items():
                        if fund_type not in organized_files:
                            organized_files[fund_type] = []
                        organized_files[fund_type].extend(files)
                    
                    # Clean up extracted directory
                    shutil.rmtree(extract_dir, ignore_errors=True)
                
                # Remove original ZIP file
                download_path.unlink(missing_ok=True)
            
            else:
                # Non-ZIP file - organize directly
                fund_type = determine_fund_type_from_filename(
                    download_path.name, fund_type_patterns
                )
                
                fund_dir = output_path / fund_type
                fund_dir.mkdir(parents=True, exist_ok=True)
                
                dest_path = fund_dir / download_path.name
                shutil.move(str(download_path), str(dest_path))
                
                if fund_type not in organized_files:
                    organized_files[fund_type] = []
                organized_files[fund_type].append(download_path.name)
        
        # Rate limiting
        delay = config.get('request_settings', {}).get('delay_between_requests', 2)
        time.sleep(delay)
    
    # Summary
    print(f"\n📊 Download Summary for {amc_name.upper()}:")
    print(f"✅ Successful: {successful_downloads}")
    print(f"❌ Failed: {len(available_files) - successful_downloads}")
    
    for fund_type, files in organized_files.items():
        print(f"📁 {fund_type}: {len(files)} files")
    
    return {
        'success': True,
        'amc_name': amc_name,
        'files_attempted': len(available_files),
        'files_downloaded': successful_downloads,
        'files_failed': len(available_files) - successful_downloads,
        'output_directory': str(output_path),
        'organized_files': organized_files
    }


def main():
    """Command line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Simple Mutual Fund Portfolio Scraper")
    parser.add_argument('amc', help='AMC name (abslf, icici, hdfc, etc.)')
    parser.add_argument('--types', nargs='+', 
                       help='Disclosure types (monthly, fortnightly, etc.)')
    parser.add_argument('--max-files', type=int, 
                       help='Maximum number of files to download')
    parser.add_argument('--output-dir', type=str, 
                       help='Custom output directory')
    parser.add_argument('--config-dir', type=str, default='ingestion/configs',
                       help='Configuration directory')
    parser.add_argument('--discover-only', action='store_true',
                       help='Only discover files, don\'t download')
    
    args = parser.parse_args()
    
    try:
        if args.discover_only:
            if args.amc.lower() == 'abslf':
                config_file = Path(args.config_dir) / f"{args.amc.lower()}.yml"
                files = discover_abslf_files(str(config_file), args.types)
                
                print(f"\n📋 Available files for {args.amc.upper()}:")
                for i, file_info in enumerate(files, 1):
                    disclosure_type = file_info.get('disclosure_type', 'unknown')
                    date = file_info.get('estimated_date', 'unknown')
                    print(f"  {i}. {file_info['filename']} ({disclosure_type}, {date})")
            else:
                print(f"⚠️ Discovery not yet implemented for {args.amc}")
        else:
            result = scrape_amc(
                amc_name=args.amc,
                disclosure_types=args.types,
                max_files=args.max_files,
                output_dir=args.output_dir,
                config_dir=args.config_dir
            )
            
            if result['success']:
                print(f"\n🎉 Scraping completed successfully!")
            else:
                print(f"\n❌ Scraping failed: {result.get('message')}")
                
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()