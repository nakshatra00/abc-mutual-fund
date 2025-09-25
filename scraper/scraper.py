#!/usr/bin/env python3
"""
General Mutual Fund Portfolio Scraper
Download portfolio files from any AMC using config files
Usage: python scraper.py <amc> --type <monthly|fortnightly> --date <YYYY-MM-DD>
"""

import os
import yaml
import requests
import zipfile
import shutil 
import re
import argparse
from pathlib import Path
from datetime import datetime

def load_config(amc_name):
    """Load configuration for specific AMC"""
    config_path = f"scraper/{amc_name.lower()}.yml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def build_urls(config, file_type, target_date=None):
    """Build URLs based on config and parameters"""
    urls = []
    
    base_url = config.get('base_url', '')
    pattern = config['url_patterns'].get(file_type, '')
    
    if not pattern:
        print(f"❌ No URL pattern for type: {file_type}")
        return urls
    
    # If target_date is provided, build URL dynamically for that date
    if target_date:
        try:
            # Parse the target date
            date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            year = date_obj.strftime('%Y')
            month = date_obj.strftime('%b')  # Short month name like "Sep"
            
            if file_type == 'fortnightly':
                # For fortnightly, determine if it's 15th or end of month
                day = date_obj.day
                if day == 15:
                    date_text = f"15th%20{date_obj.strftime('%B')}%20{year}"
                else:
                    # End of month date
                    date_text = f"{day}th%20{date_obj.strftime('%B')}%20{year}"
                
                url = pattern.format(base_url=base_url, year=year, date=date_text)
                filename = f"{config['amc_name'].lower()}_fortnightly_{target_date}.zip"
                
            elif file_type == 'monthly':
                month_short = date_obj.strftime('%b')  # "Aug"
                month_full = date_obj.strftime('%B')   # "August"
                url = pattern.format(base_url=base_url, year=year, month_short=month_short, month_full=month_full)
                filename = f"{config['amc_name'].lower()}_monthly_{target_date}.zip"
            
            urls.append({
                'url': url,
                'type': file_type,
                'filename': filename,
                'target_date': target_date
            })
            
        except ValueError as e:
            print(f"❌ Invalid date format. Use YYYY-MM-DD: {e}")
            return urls
    
    else:
        # Fallback to config targets if no date provided
        if file_type not in config.get('target_files', {}):
            print(f"❌ File type '{file_type}' not configured and no date provided")
            return urls
            
        targets = config['target_files'][file_type]
        
        for target in targets:
            try:
                url = pattern.format(base_url=base_url, **target)
                filename = f"{config['amc_name'].lower()}_{file_type}_{target.get('year', '2025')}.zip"
                
                urls.append({
                    'url': url,
                    'type': file_type,
                    'filename': filename,
                    'target': target
                })
            except KeyError as e:
                print(f"⚠️ Missing parameter in config: {e}")
                continue
    
    return urls

def download_file(url, filepath):
    """Download file from URL"""
    try:
        print(f"⬇️ Downloading: {os.path.basename(url)}")
        print(f"   URL: {url}")
        
        response = requests.get(url, timeout=30, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"✅ Downloaded: {filepath.name} ({filepath.stat().st_size} bytes)")
        return True
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False

def organize_files(zip_path, output_dir, fund_patterns, amc_name):
    """Extract ZIP and organize files by fund type"""
    print(f"📂 Extracting: {zip_path.name}")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            print(f"📄 Found {len(file_list)} files in ZIP")
            
            for file_name in file_list:
                if file_name.endswith('/'):  # Skip directories
                    continue
                    
                print(f"📄 Processing: {file_name}")
                
                # Determine fund type
                fund_type = determine_fund_type(file_name, fund_patterns)
                
                if fund_type:
                    # Create fund type directory
                    fund_dir = output_dir / fund_type
                    fund_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Extract file to fund directory
                    source = zip_ref.open(file_name)
                    target_path = fund_dir / Path(file_name).name
                    
                    with open(target_path, 'wb') as target_file:
                        shutil.copyfileobj(source, target_file)
                    
                    print(f"✅ Sorted to {fund_type}: {Path(file_name).name}")
                else:
                    print(f"⚠️ Skipped (no match): {Path(file_name).name}")
                
    except Exception as e:
        print(f"❌ Extraction failed: {e}")

def determine_fund_type(filename, fund_patterns):
    """Determine fund type based on filename - only return fund type if it matches exactly"""
    filename_lower = filename.lower()
    
    for fund_type, patterns in fund_patterns.items():
        for pattern in patterns:
            if re.search(pattern, filename_lower):
                return fund_type
    
    return None  # Don't sort if no match

def scrape_amc(amc_name, file_type, target_date=None):
    """Main scraper function for any AMC"""
    print(f"🚀 Starting {amc_name.upper()} scraper...")
    
    try:
        # Load config
        config = load_config(amc_name)
        print(f"📋 Loaded config for {config.get('amc_name', amc_name)}")
        
        # Get portfolio date from config or use provided date
        if target_date:
            date_str = target_date
        else:
            # Extract date from the first target in config
            targets = config.get('target_files', {}).get(file_type, [])
            if targets and 'date' in targets[0]:
                # Parse date from config (e.g., "15th%20September%202025" -> "2025-09-15")
                date_text = targets[0]['date'].replace('%20', ' ').replace('th', '').replace('st', '').replace('nd', '').replace('rd', '')
                try:
                    parsed_date = datetime.strptime(date_text, '%d %B %Y')
                    date_str = parsed_date.strftime('%Y-%m-%d')
                except:
                    date_str = datetime.now().strftime('%Y-%m-%d')
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')
        
        print(f"📅 Using portfolio date: {date_str}")
        output_dir = Path(f"scraper/data/raw/{date_str}/{amc_name.lower()}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Build URLs  
        urls = build_urls(config, file_type, target_date)
        if not urls:
            print("❌ No URLs to download")
            return
            
        print(f"🔍 Built {len(urls)} URLs to try")
        
        # Download and process files
        downloaded = 0
        for url_info in urls:
            zip_path = output_dir / url_info['filename']
            
            if download_file(url_info['url'], zip_path):
                # Organize the files if fund patterns exist
                if 'fund_type_patterns' in config:
                    organize_files(zip_path, output_dir, config['fund_type_patterns'], amc_name)
                downloaded += 1
        
        print(f"📊 Downloaded {downloaded} files")
        print("🎉 Scraping completed!")
        
    except Exception as e:
        print(f"❌ Scraping failed: {e}")

def main():
    parser = argparse.ArgumentParser(description='Download mutual fund portfolio files')
    parser.add_argument('amc', help='AMC name (abslf, icici, hdfc, etc.)')
    parser.add_argument('--type', '-t', choices=['monthly', 'fortnightly'], 
                       default='fortnightly', help='File type to download')
    parser.add_argument('--date', '-d', help='Target date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    scrape_amc(args.amc, args.type, args.date)

if __name__ == "__main__":
    main()