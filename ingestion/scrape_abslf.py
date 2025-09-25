#!/usr/bin/env python3
"""
ABSLF Portfolio Scraper - Simple function-based approach
"""
import os
import yaml
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


def load_config(config_path: str = "ingestion/configs/abslf_scraper.yml") -> Dict[str, Any]:
    """Load ABSLF scraper configuration."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_years_to_check(config: Dict[str, Any]) -> List[int]:
    """Get list of years to check based on config."""
    current_year = datetime.now().year
    years_count = config.get('years_to_check', 2)
    return [current_year - i for i in range(years_count)]


def build_urls(config: Dict[str, Any], disclosure_type: str = "monthly") -> List[Dict[str, Any]]:
    """Build list of URLs to check for a specific disclosure type."""
    if disclosure_type not in config['disclosure_types']:
        raise ValueError(f"Unknown disclosure type: {disclosure_type}")
    
    disclosure_config = config['disclosure_types'][disclosure_type]
    pattern_key = disclosure_config['pattern_key']
    url_pattern = config['url_patterns'][pattern_key]
    date_patterns = disclosure_config['date_patterns']
    
    urls = []
    years = get_years_to_check(config)
    
    for year in years:
        for date_pattern in date_patterns:
            url = url_pattern.format(year=year, date=date_pattern)
            
            urls.append({
                'url': url,
                'filename': os.path.basename(url),
                'disclosure_type': disclosure_type,
                'year': year,
                'date_pattern': date_pattern,
                'estimated_date': _parse_date_pattern(date_pattern, year)
            })
    
    return urls


def _parse_date_pattern(date_pattern: str, year: int) -> str:
    """Convert date pattern to standard YYYY-MM-DD format."""
    # Map month abbreviations to numbers
    month_map = {
        'jan': '01', 'january': '01',
        'feb': '02', 'february': '02', 
        'mar': '03', 'march': '03',
        'apr': '04', 'april': '04',
        'may': '05', 'may': '05',
        'jun': '06', 'june': '06',
        'jul': '07', 'july': '07',
        'aug': '08', 'august': '08',
        'sep': '09', 'september': '09',
        'oct': '10', 'october': '10',
        'nov': '11', 'november': '11',
        'dec': '12', 'december': '12'
    }
    
    # Parse patterns like "31-january" or "15-jan"
    parts = date_pattern.split('-')
    if len(parts) == 2:
        day, month_name = parts
        month_num = month_map.get(month_name.lower(), '01')
        return f"{year}-{month_num}-{day.zfill(2)}"
    
    return f"{year}-01-01"  # fallback


def check_url_exists(url: str, session: requests.Session) -> bool:
    """Check if URL exists and returns a file."""
    try:
        response = session.get(url, timeout=10, stream=True)
        return response.status_code == 200 and 'zip' in response.headers.get('content-type', '').lower()
    except:
        return False


def discover_files(disclosure_type: str = "monthly", config_path: str = "ingestion/configs/abslf_scraper.yml") -> List[Dict[str, Any]]:
    """
    Discover available portfolio files for ABSLF.
    
    Args:
        disclosure_type: Type of disclosure ('monthly' or 'fortnightly')
        config_path: Path to configuration file
        
    Returns:
        List of available file information
    """
    print(f"🔍 Discovering {disclosure_type} portfolio files for ABSLF...")
    
    config = load_config(config_path)
    urls_to_check = build_urls(config, disclosure_type)
    
    # Create session with proper headers
    session = requests.Session()
    session.headers.update({
        'User-Agent': config['request_settings']['user_agent']
    })
    
    available_files = []
    
    print(f"📊 Checking {len(urls_to_check)} potential URLs...")
    
    for url_info in urls_to_check:
        if check_url_exists(url_info['url'], session):
            available_files.append(url_info)
            print(f"✅ Found: {url_info['filename']}")
    
    print(f"📋 Total available files: {len(available_files)}")
    return available_files


def download_file(file_info: Dict[str, Any], output_dir: Path, session: requests.Session, 
                 delay: int = 2) -> bool:
    """Download a single file."""
    url = file_info['url']
    filename = file_info['filename']
    filepath = output_dir / filename
    
    # Skip if file already exists
    if filepath.exists():
        print(f"⏭️  Skipping {filename} (already exists)")
        return True
    
    print(f"⬇️  Downloading: {filename}")
    
    try:
        response = session.get(url, timeout=30, stream=True)
        response.raise_for_status()
        
        # Save file
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        file_size = filepath.stat().st_size
        print(f"✅ Downloaded: {filename} ({file_size:,} bytes)")
        
        # Add delay between downloads
        import time
        time.sleep(delay)
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to download {filename}: {e}")
        return False


def scrape_abslf(disclosure_type: str = "monthly", max_files: Optional[int] = None, 
                output_dir: Optional[str] = None, config_path: str = "ingestion/configs/abslf_scraper.yml") -> Dict[str, Any]:
    """
    Main scraping function for ABSLF portfolio disclosures.
    
    Args:
        disclosure_type: Type of disclosure ('monthly' or 'fortnightly')
        max_files: Maximum number of files to download (None for all)
        output_dir: Custom output directory (None for default)
        config_path: Path to configuration file
        
    Returns:
        Dictionary with scraping results
    """
    print(f"🚀 Starting ABSLF {disclosure_type} portfolio scraping...")
    
    # Load configuration
    config = load_config(config_path)
    
    # Setup output directory
    if output_dir is None:
        base_dir = config['output']['base_dir']
        date_str = datetime.now().strftime('%Y-%m-%d')
        amc_name = config['amc_name'].lower()
        output_path = Path(base_dir) / date_str / amc_name
    else:
        output_path = Path(output_dir)
    
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"📁 Output directory: {output_path}")
    
    # Discover available files
    available_files = discover_files(disclosure_type, config_path)
    
    if not available_files:
        print("❌ No files found to download")
        return {
            'success': False,
            'message': 'No files found',
            'files_attempted': 0,
            'files_downloaded': 0
        }
    
    # Limit files if requested
    files_to_download = available_files
    if max_files and len(files_to_download) > max_files:
        files_to_download = files_to_download[:max_files]
        print(f"📊 Limited to {max_files} most recent files")
    
    # Create session
    session = requests.Session()
    session.headers.update({
        'User-Agent': config['request_settings']['user_agent']
    })
    
    # Download files
    print(f"\n⬇️  Starting download of {len(files_to_download)} files...")
    
    successful_downloads = 0
    failed_downloads = 0
    
    for i, file_info in enumerate(files_to_download, 1):
        print(f"[{i}/{len(files_to_download)}] ", end="")
        
        if download_file(file_info, output_path, session, 
                        config['request_settings']['delay_between_requests']):
            successful_downloads += 1
        else:
            failed_downloads += 1
    
    # Summary
    print(f"\n📊 Download Summary:")
    print(f"✅ Successful: {successful_downloads}")
    print(f"❌ Failed: {failed_downloads}")
    print(f"📁 Files saved to: {output_path}")
    
    return {
        'success': True,
        'disclosure_type': disclosure_type,
        'files_attempted': len(files_to_download),
        'files_downloaded': successful_downloads,
        'files_failed': failed_downloads,
        'output_directory': str(output_path),
        'downloaded_files': [f['filename'] for f in files_to_download[:successful_downloads]]
    }


def main():
    """Command line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(description="ABSLF Portfolio Scraper")
    parser.add_argument('--type', choices=['monthly', 'fortnightly'], default='monthly',
                       help='Type of disclosure to scrape')
    parser.add_argument('--max-files', type=int, help='Maximum number of files to download')
    parser.add_argument('--output-dir', type=str, help='Custom output directory')
    parser.add_argument('--config', type=str, default='ingestion/configs/abslf_scraper.yml',
                       help='Configuration file path')
    parser.add_argument('--discover-only', action='store_true', 
                       help='Only discover files, don\'t download')
    
    args = parser.parse_args()
    
    try:
        if args.discover_only:
            files = discover_files(args.type, args.config)
            print(f"\n📋 Available {args.type} files:")
            for i, file_info in enumerate(files, 1):
                print(f"  {i}. {file_info['filename']} ({file_info['estimated_date']})")
        else:
            result = scrape_abslf(
                disclosure_type=args.type,
                max_files=args.max_files,
                output_dir=args.output_dir,
                config_path=args.config
            )
            
            if result['success']:
                print(f"\n🎉 Scraping completed successfully!")
            else:
                print(f"\n❌ Scraping failed: {result.get('message', 'Unknown error')}")
                
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()