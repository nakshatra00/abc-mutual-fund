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
    
    # Check if this AMC uses manual file lists first
    if 'manual_files' in config and file_type in config['manual_files']:
        manual_files = config['manual_files'][file_type]
        for i, file_info in enumerate(manual_files):
            # Extract filename from URL
            url = file_info['url']
            original_filename = url.split('/')[-1].split('?')[0]  # Remove query params
            # Create unique filename with index to avoid overwriting
            filename = f"{config['amc_name'].lower()}_{file_type}_{file_info.get('date', '2025-09-15')}_{i+1}.xlsx"
            
            urls.append({
                'url': url,
                'type': file_type,
                'filename': filename,
                'fund_type': file_info.get('fund_type', 'other'),
                'target_date': file_info.get('date', target_date)
            })
        return urls
    
    # Traditional URL pattern approach
    base_url = config.get('base_url', '')
    if 'url_patterns' not in config:
        print(f"❌ No URL patterns or manual files configured for {config.get('amc_name', 'AMC')}")
        return urls
        
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
            
            amc_name_lower = config['amc_name'].lower()
            
            if file_type == 'fortnightly':
                if amc_name_lower == 'icici':
                    # ICICI format: "15th%20September%202025"
                    day = date_obj.day
                    if day == 15:
                        date_text = f"15th%20{date_obj.strftime('%B')}%20{year}"
                    else:
                        date_text = f"{day}th%20{date_obj.strftime('%B')}%20{year}"
                elif amc_name_lower == 'abslf':
                    # ABSLF format: "15-sep-2025"
                    date_text = date_obj.strftime('%d-%b-%Y').lower()
                else:
                    date_text = target_date
                
                url = pattern.format(base_url=base_url, year=year, date=date_text)
                filename = f"{amc_name_lower}_fortnightly_{target_date}.zip"
                
            elif file_type == 'monthly':
                if amc_name_lower == 'icici':
                    # ICICI format: needs both short and full month
                    month_short = date_obj.strftime('%b')  # "Aug"
                    month_full = date_obj.strftime('%B')   # "August"
                    url = pattern.format(base_url=base_url, year=year, month_short=month_short, month_full=month_full)
                elif amc_name_lower == 'abslf':
                    # ABSLF format: "31-aug-2025"
                    date_text = date_obj.strftime('%d-%b-%Y').lower()
                    url = pattern.format(base_url=base_url, year=year, date=date_text)
                else:
                    url = pattern.format(base_url=base_url, year=year, date=target_date)
                
                filename = f"{amc_name_lower}_monthly_{target_date}.zip"
            
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
        # Check if this AMC uses manual file lists (like HDFC)
        if 'manual_files' in config and file_type in config['manual_files']:
            manual_files = config['manual_files'][file_type]
            for file_info in manual_files:
                # Extract filename from URL
                url = file_info['url']
                original_filename = url.split('/')[-1].split('?')[0]  # Remove query params
                filename = f"{config['amc_name'].lower()}_{file_type}_{file_info.get('date', '2025')}.xlsx"
                
                urls.append({
                    'url': url,
                    'type': file_type,
                    'filename': filename,
                    'fund_type': file_info.get('fund_type', 'other'),
                    'target_date': file_info.get('date', target_date)
                })
        
        # Fallback to config targets if no manual files and no date provided
        elif file_type in config.get('target_files', {}):
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
        else:
            print(f"❌ File type '{file_type}' not configured and no date provided")
            return urls
    
    return urls

def download_file(url, filepath):
    """Download file from URL"""
    try:
        print(f"⬇️ Downloading: {os.path.basename(url)}")
        print(f"   URL: {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        response = requests.get(url, headers=headers, timeout=30, stream=True)
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
            
            # ABSLF special case: one file goes to all fund folders
            if amc_name.lower() == 'abslf':
                organize_abslf_files(zip_ref, file_list, output_dir, fund_patterns)
            else:
                # ICICI and others: sort files by fund type
                organize_other_amc_files(zip_ref, file_list, output_dir, fund_patterns)
                    
    except Exception as e:
        print(f"❌ Extraction failed: {e}")

def organize_abslf_files(zip_ref, file_list, output_dir, fund_patterns):
    """ABSLF: Copy same file to all fund-type folders"""
    for file_name in file_list:
        if file_name.endswith('/'):  # Skip directories
            continue
            
        print(f"📄 Processing ABSLF file: {file_name}")
        
        # Extract the file first
        source = zip_ref.open(file_name)
        base_filename = Path(file_name).name
        
        # Copy to each fund type folder
        for fund_type in fund_patterns.keys():
            fund_dir = output_dir / fund_type
            fund_dir.mkdir(parents=True, exist_ok=True)
            
            target_path = fund_dir / f"abslf_{base_filename}"
            
            # Reset file pointer and copy
            source.seek(0)
            with open(target_path, 'wb') as target_file:
                shutil.copyfileobj(source, target_file)
            
            print(f"✅ Copied to {fund_type}: abslf_{base_filename}")
        
        source.close()

def organize_other_amc_files(zip_ref, file_list, output_dir, fund_patterns):
    """ICICI and others: Sort files by fund type"""
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

def organize_hdfc_files(files, amc_name, config, target_date, download_info):
    """Organize HDFC files based on manual configuration."""
    organized = {
        'corporate-bond': 0,
        'money-market': 0, 
        'gilt': 0,
        'other': 0
    }
    
    # Create mapping from filename to fund_type from download info
    file_fund_type_map = {}
    for info in download_info:
        if 'fund_type' in info:
            file_fund_type_map[info['filename']] = info['fund_type']
    
    for file_path in files:
        filename = Path(file_path).name
        
        # Use fund_type from download info if available
        fund_type = file_fund_type_map.get(filename, 'other')
        
        # Create target directory
        target_dir = Path(f"scraper/data/raw/{target_date}/{amc_name}/{fund_type}")
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy file to target directory
        target_path = target_dir / filename
        shutil.copy2(file_path, target_path)
        
        organized[fund_type] += 1
        print(f"   📁 {filename} → {fund_type}/")
    
    return organized

def organize_abslf_individual_files(files, amc_name, config, target_date):
    """Organize ABSLF individual files by copying to all fund type directories."""
    organized = {
        'corporate-bond': 0,
        'money-market': 0, 
        'gilt': 0,
        'other': 0
    }
    
    fund_type_patterns = config.get('fund_type_patterns', {})
    
    for file_path in files:
        filename = Path(file_path).name
        
        # Copy to each fund type folder (ABSLF strategy)
        for fund_type in fund_type_patterns.keys():
            target_dir = Path(f"scraper/data/raw/{target_date}/{amc_name}/{fund_type}")
            target_dir.mkdir(parents=True, exist_ok=True)
            
            target_path = target_dir / filename
            shutil.copy2(file_path, target_path)
            
            organized[fund_type] += 1
            print(f"   📁 {filename} → {fund_type}/")
    
    return organized

def organize_single_file_with_patterns(files, amc_name, config, target_date):
    """Organize individual files using fund type patterns."""
    organized = {
        'corporate-bond': 0,
        'money-market': 0, 
        'gilt': 0,
        'other': 0
    }
    
    fund_type_patterns = config.get('fund_type_patterns', {})
    
    for file_path in files:
        filename = Path(file_path).name.lower()
        
        # Determine fund type based on filename patterns
        fund_type = 'other'  # default
        for ftype, patterns in fund_type_patterns.items():
            if any(pattern.lower() in filename for pattern in patterns):
                fund_type = ftype
                break
        
        # Create target directory
        target_dir = Path(f"scraper/data/raw/{target_date}/{amc_name}/{fund_type}")
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy file to target directory
        target_path = target_dir / Path(file_path).name
        shutil.copy2(file_path, target_path)
        
        organized[fund_type] += 1
        print(f"   📁 {Path(file_path).name} → {fund_type}/")
    
    return organized

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
        downloaded_files = []
        
        for url_info in urls:
            file_path = output_dir / url_info['filename']
            
            if download_file(url_info['url'], file_path):
                downloaded_files.append({
                    'path': file_path,
                    'info': url_info
                })
                downloaded += 1
        
        # Organize files based on AMC type
        if downloaded_files:
            print(f"📋 Organizing {len(downloaded_files)} files...")
            
            if amc_name.lower() == 'abslf':
                # ABSLF: Copy each file to all fund type directories
                file_paths = [f['path'] for f in downloaded_files]
                organized = organize_abslf_individual_files(file_paths, amc_name, config, date_str)
            elif amc_name.lower() == 'hdfc' and 'manual_files' in config:
                # HDFC: Use manual file configuration for organization
                file_paths = [f['path'] for f in downloaded_files]
                download_info = [f['info'] for f in downloaded_files]
                organized = organize_hdfc_files(file_paths, amc_name, config, date_str, download_info)
            elif 'fund_type_patterns' in config:
                # Other AMCs: Extract and organize ZIP files
                organized = {'corporate-bond': 0, 'money-market': 0, 'gilt': 0, 'other': 0}
                for file_info in downloaded_files:
                    if file_info['path'].suffix.lower() == '.zip':
                        organize_files(file_info['path'], output_dir, config['fund_type_patterns'], amc_name)
                    else:
                        # Handle non-ZIP files with pattern matching
                        file_organized = organize_single_file_with_patterns([file_info['path']], amc_name, config, date_str)
                        for fund_type, count in file_organized.items():
                            organized[fund_type] += count
            
            print("📊 Organization summary:")
            if 'organized' in locals():
                for fund_type, count in organized.items():
                    if count > 0:
                        print(f"   {fund_type}: {count} files")
        
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