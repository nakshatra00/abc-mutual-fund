#!/usr/bin/env python3
"""
Central Scraper System for Mutual Fund Portfolio Disclosures
============================================================

This module provides a unified interface for scraping portfolio disclosures
from different AMC websites using various strategies:

1. Direct URL Pattern Strategy (ABSLF) - construct URLs directly
2. Dynamic HTML Parsing Strategy (ICICI) - parse HTML and follow download links
3. API-based Strategy (future) - use REST APIs where available

Each AMC has its own YAML configuration that defines:
- Scraping strategy
- URL patterns or base URLs
- File organization rules
- Fund type mappings
"""

import os
import yaml
import requests
import zipfile
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse
import re
import time


class ScrapingStrategy(ABC):
    """Abstract base class for different scraping strategies."""
    
    def __init__(self, config: Dict[str, Any], session: requests.Session):
        self.config = config
        self.session = session
        self.amc_name = config['amc_name']
    
    @abstractmethod
    def discover_files(self, disclosure_types: List[str] = None) -> List[Dict[str, Any]]:
        """Discover available files for download."""
        pass
    
    @abstractmethod
    def download_file(self, file_info: Dict[str, Any], output_path: Path) -> bool:
        """Download a single file."""
        pass


class DirectURLStrategy(ScrapingStrategy):
    """Strategy for AMCs with predictable URL patterns (like ABSLF)."""
    
    def discover_files(self, disclosure_types: List[str] = None) -> List[Dict[str, Any]]:
        """Discover files using direct URL construction."""
        if disclosure_types is None:
            disclosure_types = list(self.config['disclosure_types'].keys())
        
        all_files = []
        years = self._get_years_to_check()
        
        for disclosure_type in disclosure_types:
            if disclosure_type not in self.config['disclosure_types']:
                continue
                
            disclosure_config = self.config['disclosure_types'][disclosure_type]
            pattern_key = disclosure_config['pattern_key']
            url_pattern = self.config['url_patterns'][pattern_key]
            date_patterns = disclosure_config['date_patterns']
            
            for year in years:
                for date_pattern in date_patterns:
                    # Handle year substitution in date patterns
                    formatted_date = date_pattern.format(year=year) if '{year}' in date_pattern else date_pattern
                    url = url_pattern.format(year=year, date=formatted_date)
                    
                    if self._check_url_exists(url):
                        all_files.append({
                            'url': url,
                            'filename': os.path.basename(url),
                            'disclosure_type': disclosure_type,
                            'year': year,
                            'date_pattern': date_pattern,
                            'estimated_date': self._parse_date_pattern(date_pattern, year),
                            'strategy': 'direct_url'
                        })
        
        return sorted(all_files, key=lambda x: x['estimated_date'], reverse=True)
    
    def download_file(self, file_info: Dict[str, Any], output_path: Path) -> bool:
        """Download file using direct URL."""
        try:
            response = self.session.get(file_info['url'], timeout=30, stream=True)
            response.raise_for_status()
            
            filepath = output_path / file_info['filename']
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return True
        except Exception as e:
            print(f"❌ Failed to download {file_info['filename']}: {e}")
            return False
    
    def _get_years_to_check(self) -> List[int]:
        """Get years to check based on config."""
        current_year = datetime.now().year
        years_count = self.config.get('years_to_check', 2)
        return [current_year - i for i in range(years_count)]
    
    def _check_url_exists(self, url: str) -> bool:
        """Check if URL exists."""
        try:
            response = self.session.head(url, timeout=10)
            return response.status_code == 200
        except:
            try:
                response = self.session.get(url, timeout=10, stream=True)
                return response.status_code == 200
            except:
                return False
    
    def _parse_date_pattern(self, date_pattern: str, year: int) -> str:
        """Convert date pattern to YYYY-MM-DD format."""
        month_map = {
            'jan': '01', 'january': '01', 'feb': '02', 'february': '02',
            'mar': '03', 'march': '03', 'apr': '04', 'april': '04',
            'may': '05', 'may': '05', 'jun': '06', 'june': '06',
            'jul': '07', 'july': '07', 'aug': '08', 'august': '08',
            'sep': '09', 'september': '09', 'oct': '10', 'october': '10',
            'nov': '11', 'november': '11', 'dec': '12', 'december': '12'
        }
        
        parts = date_pattern.split('-')
        if len(parts) == 2:
            day, month_name = parts
            month_num = month_map.get(month_name.lower(), '01')
            return f"{year}-{month_num}-{day.zfill(2)}"
        
        return f"{year}-01-01"


class DynamicHTMLStrategy(ScrapingStrategy):
    """Strategy for AMCs with dynamic HTML pages and download buttons (like ICICI)."""
    
    def discover_files(self, disclosure_types: List[str] = None) -> List[Dict[str, Any]]:
        """Discover files by parsing HTML pages."""
        from bs4 import BeautifulSoup
        
        if disclosure_types is None:
            disclosure_types = list(self.config['disclosure_types'].keys())
        
        all_files = []
        
        for disclosure_type in disclosure_types:
            if disclosure_type not in self.config['disclosure_types']:
                continue
            
            disclosure_config = self.config['disclosure_types'][disclosure_type]
            base_url = disclosure_config['base_url']
            
            try:
                response = self.session.get(base_url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                files = self._extract_download_links(soup, disclosure_type, base_url)
                all_files.extend(files)
                
            except Exception as e:
                print(f"❌ Failed to fetch {disclosure_type} page: {e}")
        
        return sorted(all_files, key=lambda x: x.get('estimated_date', ''), reverse=True)
    
    def download_file(self, file_info: Dict[str, Any], output_path: Path) -> bool:
        """Download file by following download link."""
        try:
            # For dynamic pages, we might need to follow redirects or handle special headers
            headers = {
                'Referer': file_info.get('referer_url', ''),
                'Accept': 'application/octet-stream,*/*'
            }
            
            response = self.session.get(file_info['download_url'], 
                                      headers=headers, timeout=30, stream=True)
            response.raise_for_status()
            
            # Try to get filename from headers if not provided
            filename = file_info['filename']
            if 'content-disposition' in response.headers:
                cd = response.headers['content-disposition']
                filename_match = re.search(r'filename[^;=\n]*=(([\'"]).*?\2|[^;\n]*)', cd)
                if filename_match:
                    filename = filename_match.group(1).strip('\'"')
            
            filepath = output_path / filename
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Update the actual filename in file_info
            file_info['actual_filename'] = filename
            return True
            
        except Exception as e:
            print(f"❌ Failed to download {file_info['filename']}: {e}")
            return False
    
    def _extract_download_links(self, soup, disclosure_type: str, base_url: str) -> List[Dict[str, Any]]:
        """Extract download links from HTML."""
        files = []
        disclosure_config = self.config['disclosure_types'][disclosure_type]
        
        # Look for download patterns based on config
        title_patterns = disclosure_config.get('title_patterns', [])
        link_selectors = disclosure_config.get('link_selectors', [])
        
        for pattern in title_patterns:
            # Find elements with titles matching the pattern
            elements = soup.find_all(text=re.compile(pattern, re.IGNORECASE))
            
            for element in elements:
                # Find the associated download link
                parent = element.parent
                while parent and parent.name != 'body':
                    download_link = parent.find('a', href=True)
                    if download_link and ('download' in download_link.get('href', '').lower() or 
                                        'download' in download_link.text.lower()):
                        
                        download_url = urljoin(base_url, download_link['href'])
                        filename = self._extract_filename_from_title(element.strip())
                        estimated_date = self._extract_date_from_title(element.strip())
                        
                        files.append({
                            'download_url': download_url,
                            'filename': filename,
                            'title': element.strip(),
                            'disclosure_type': disclosure_type,
                            'estimated_date': estimated_date,
                            'referer_url': base_url,
                            'strategy': 'dynamic_html'
                        })
                        break
                    parent = parent.parent
        
        return files
    
    def _extract_filename_from_title(self, title: str) -> str:
        """Extract filename from title text."""
        # Clean title and create filename
        clean_title = re.sub(r'[^\w\s-]', '', title)
        clean_title = re.sub(r'\s+', '_', clean_title.strip())
        return f"{clean_title.lower()}.xlsx"  # Default extension
    
    def _extract_date_from_title(self, title: str) -> str:
        """Extract date from title text."""
        # Look for date patterns in title
        date_patterns = [
            r'(\d{1,2})[st|nd|rd|th]*\s+(\w+)\s+(\d{4})',  # 15th September 2025
            r'(\d{1,2})-(\w+)-(\d{4})',                    # 15-Sep-2025
            r'(\d{4})-(\d{2})-(\d{2})'                     # 2025-09-15
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, title)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 3:
                        day, month, year = groups
                        # Convert month name to number if needed
                        if month.isalpha():
                            month_map = {
                                'jan': '01', 'january': '01', 'feb': '02', 'february': '02',
                                'mar': '03', 'march': '03', 'apr': '04', 'april': '04',
                                'may': '05', 'jun': '06', 'june': '06',
                                'jul': '07', 'july': '07', 'aug': '08', 'august': '08',
                                'sep': '09', 'september': '09', 'oct': '10', 'october': '10',
                                'nov': '11', 'november': '11', 'dec': '12', 'december': '12'
                            }
                            month = month_map.get(month.lower(), '01')
                        
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except:
                    continue
        
        return datetime.now().strftime('%Y-%m-%d')


class FileOrganizer:
    """Handles organization of downloaded files into appropriate fund-type folders."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.fund_type_patterns = config.get('fund_type_patterns', {})
    
    def organize_file(self, filepath: Path, base_output_dir: Path) -> Tuple[str, Path]:
        """
        Organize a file into the appropriate fund-type folder.
        
        Returns:
            Tuple of (fund_type, final_path)
        """
        filename = filepath.name.lower()
        
        # Determine fund type based on filename patterns
        fund_type = self._determine_fund_type(filename, filepath)
        
        # Create fund-type directory
        fund_type_dir = base_output_dir / fund_type
        fund_type_dir.mkdir(parents=True, exist_ok=True)
        
        # Move file to appropriate directory
        final_path = fund_type_dir / filepath.name
        if filepath != final_path:
            filepath.rename(final_path)
        
        return fund_type, final_path
    
    def _determine_fund_type(self, filename: str, filepath: Path) -> str:
        """Determine fund type based on filename and content."""
        
        # Check filename patterns first
        for fund_type, patterns in self.fund_type_patterns.items():
            for pattern in patterns:
                if re.search(pattern.lower(), filename):
                    return fund_type
        
        # If it's a ZIP file, check contents
        if filename.endswith('.zip'):
            fund_type = self._analyze_zip_contents(filepath)
            if fund_type:
                return fund_type
        
        # Default to 'mixed' if can't determine
        return 'mixed'
    
    def _analyze_zip_contents(self, zip_path: Path) -> Optional[str]:
        """Analyze ZIP file contents to determine fund type."""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                filenames = [f.lower() for f in zf.namelist()]
                
                # Count occurrences of different fund types in filenames
                fund_type_counts = {}
                for fund_type, patterns in self.fund_type_patterns.items():
                    count = 0
                    for pattern in patterns:
                        for filename in filenames:
                            if re.search(pattern.lower(), filename):
                                count += 1
                    if count > 0:
                        fund_type_counts[fund_type] = count
                
                # Return the fund type with most matches
                if fund_type_counts:
                    return max(fund_type_counts, key=fund_type_counts.get)
                    
        except Exception as e:
            print(f"⚠️ Could not analyze ZIP contents for {zip_path}: {e}")
        
        return None


class CentralScraper:
    """Main scraper class that coordinates different strategies and AMCs."""
    
    def __init__(self, config_dir: str = "ingestion/configs"):
        self.config_dir = Path(config_dir)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
    def load_amc_config(self, amc_name: str) -> Dict[str, Any]:
        """Load configuration for a specific AMC."""
        # Try both naming conventions
        config_file = self.config_dir / f"{amc_name.lower()}_scraper.yml"
        
        if not config_file.exists():
            config_file = self.config_dir / f"{amc_name.lower()}.yml"
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def create_strategy(self, config: Dict[str, Any]) -> ScrapingStrategy:
        """Create appropriate scraping strategy based on config."""
        strategy_type = config.get('scraping_strategy', 'direct_url')
        
        if strategy_type == 'direct_url':
            return DirectURLStrategy(config, self.session)
        elif strategy_type == 'dynamic_html':
            return DynamicHTMLStrategy(config, self.session)
        else:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
    
    def scrape_amc(self, amc_name: str, disclosure_types: List[str] = None, 
                   max_files: Optional[int] = None, 
                   output_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Scrape portfolio disclosures for a specific AMC.
        
        Args:
            amc_name: Name of the AMC (abslf, icici, etc.)
            disclosure_types: Types of disclosures to scrape
            max_files: Maximum number of files to download
            output_dir: Custom output directory
            
        Returns:
            Dictionary with scraping results
        """
        print(f"🚀 Starting {amc_name.upper()} portfolio scraping...")
        
        # Load AMC configuration
        config = self.load_amc_config(amc_name)
        
        # Create scraping strategy
        strategy = self.create_strategy(config)
        
        # Setup output directory
        if output_dir is None:
            base_dir = config['output']['base_dir']
            date_str = datetime.now().strftime('%Y-%m-%d')
            output_path = Path(base_dir) / date_str / amc_name.lower()
        else:
            output_path = Path(output_dir)
        
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Discover files
        print(f"🔍 Discovering available files...")
        available_files = strategy.discover_files(disclosure_types)
        
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
        
        # Download files
        organizer = FileOrganizer(config)
        successful_downloads = 0
        organized_files = {}
        
        for i, file_info in enumerate(available_files, 1):
            print(f"[{i}/{len(available_files)}] ⬇️ {file_info['filename']}")
            
            if strategy.download_file(file_info, output_path):
                successful_downloads += 1
                
                # Organize file into fund-type folder
                downloaded_file = output_path / file_info['filename']
                if downloaded_file.exists():
                    fund_type, final_path = organizer.organize_file(downloaded_file, output_path)
                    
                    if fund_type not in organized_files:
                        organized_files[fund_type] = []
                    organized_files[fund_type].append(final_path.name)
                    
                    print(f"✅ Organized into {fund_type}/")
            
            # Rate limiting
            time.sleep(config.get('request_settings', {}).get('delay_between_requests', 2))
        
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
    """Command line interface for central scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Central Mutual Fund Portfolio Scraper")
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
        scraper = CentralScraper(args.config_dir)
        
        if args.discover_only:
            config = scraper.load_amc_config(args.amc)
            strategy = scraper.create_strategy(config)
            files = strategy.discover_files(args.types)
            
            print(f"\n📋 Available files for {args.amc.upper()}:")
            for i, file_info in enumerate(files, 1):
                disclosure_type = file_info.get('disclosure_type', 'unknown')
                date = file_info.get('estimated_date', 'unknown')
                print(f"  {i}. {file_info['filename']} ({disclosure_type}, {date})")
        else:
            result = scraper.scrape_amc(
                amc_name=args.amc,
                disclosure_types=args.types,
                max_files=args.max_files,
                output_dir=args.output_dir
            )
            
            if result['success']:
                print(f"\n🎉 Scraping completed successfully!")
            else:
                print(f"\n❌ Scraping failed: {result.get('message')}")
                
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()