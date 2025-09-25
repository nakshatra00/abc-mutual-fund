#!/usr/bin/env python3
"""
Simple ABSLF scraper - focused on just downloading files.
"""
import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import yaml


class SimpleABSLFScraper:
    """Simple scraper for ABSLF portfolio disclosures."""
    
    def __init__(self):
        self.base_urls = [
            "https://mutualfund.adityabirlacapital.com/forms-and-downloads/portfolio",
            "https://mutualfund.adityabirlacapital.com/forms-and-downloads/disclosures",
            "https://mutualfund.adityabirlacapital.com/forms-and-downloads/tracking-error-disclosures",
            "https://mutualfund.adityabirlacapital.com/forms-and-downloads/tracking-difference-disclosures"
        ]
        self.output_dir = Path("data/raw") / datetime.now().strftime('%Y-%m-%d') / "abslf"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def discover_monthly_portfolio_files(self):
        """Find monthly portfolio ZIP files using known URL patterns."""
        print("🔍 Looking for monthly portfolio ZIP files...")
        
        monthly_files = []
        base_portfolio_url = "https://mutualfund.adityabirlacapital.com/-/media/bsl/files/resources/monthly-portfolio/"
        
        # Try different years and months
        current_year = datetime.now().year
        years = [current_year, current_year - 1]  # Current and previous year
        
        months = [
            ("january", "31-january"), ("february", "28-february"), ("march", "31-march"),
            ("april", "30-april"), ("may", "31-may"), ("june", "30-june"),
            ("july", "31-july"), ("august", "31-august"), ("september", "30-september"),
            ("october", "31-october"), ("november", "30-november"), ("december", "31-december")
        ]
        
        for year in years:
            for month_name, month_date in months:
                # Try common filename patterns
                patterns = [
                    f"sebi_monthly_portfolio-{month_date}-{year}.zip",
                    f"monthly_portfolio-{month_date}-{year}.zip",
                    f"portfolio-{month_date}-{year}.zip",
                ]
                
                for pattern in patterns:
                    test_url = f"{base_portfolio_url}{year}/{pattern}"
                    
                    try:
                        # Test if URL exists with GET request (some servers don't like HEAD)
                        response = self.session.get(test_url, timeout=10, stream=True)
                        if response.status_code == 200:
                            monthly_files.append({
                                'url': test_url,
                                'text': f'Monthly Portfolio - {month_name.title()} {year}',
                                'filename': pattern,
                                'estimated_date': f'{year}-{months.index((month_name, month_date)) + 1:02d}-{month_date.split("-")[0]}',
                                'file_type': 'zip',
                                'source_page': 'direct_url_pattern'
                            })
                            print(f"✅ Found: {pattern}")
                            break  # Found one pattern for this month, move to next
                    except:
                        continue  # URL doesn't exist, try next pattern
        
        return monthly_files

    def discover_files(self):
        """Find all Excel/ZIP files across multiple ABSLF pages."""
        all_files = []
        
        # First, try to find monthly portfolio files using known patterns
        monthly_files = self.discover_monthly_portfolio_files()
        all_files.extend(monthly_files)
        
        for base_url in self.base_urls:
            print(f"🔍 Discovering files from: {base_url}")
            
            try:
                response = self.session.get(base_url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find all links
                links = soup.find_all('a', href=True)
                print(f"Found {len(links)} total links on this page")
                
                page_files = []
                
                for link in links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    
                    # Check if it's a downloadable file (Excel, ZIP, or PDF portfolio files)
                    if any(ext in href.lower() for ext in ['.xls', '.xlsx', '.zip', '.pdf']):
                        # Focus specifically on monthly portfolio ZIP files
                        if '.zip' in href.lower() and 'monthly-portfolio' in href.lower():
                            pass  # This is what we want - monthly portfolio ZIP
                        # For PDF, only include if it seems portfolio-related
                        elif '.pdf' in href.lower():
                            if not any(keyword in text.lower() or keyword in href.lower() 
                                     for keyword in ['portfolio', 'disclosure', 'holding', 'monthly', 'quarterly']):
                                continue
                        # For Excel files, include if portfolio-related
                        elif any(ext in href.lower() for ext in ['.xls', '.xlsx']):
                            if not any(keyword in text.lower() or keyword in href.lower() 
                                     for keyword in ['portfolio', 'disclosure', 'holding']):
                                continue
                        else:
                            continue  # Skip other file types
                        
                        full_url = urljoin(base_url, href)
                        
                        file_info = {
                            'url': full_url,
                            'text': text,
                            'filename': self._extract_filename(href, text),
                            'estimated_date': self._extract_date(text),
                            'file_type': self._get_file_type(href),
                            'source_page': base_url
                        }
                        
                        # Avoid duplicates
                        if not any(f['url'] == file_info['url'] for f in all_files):
                            page_files.append(file_info)
                            all_files.append(file_info)
                
                print(f"📊 Found {len(page_files)} downloadable files on this page")
                print()
                
            except Exception as e:
                print(f"❌ Error discovering files from {base_url}: {e}")
                continue
        
        print(f"📊 Total found: {len(all_files)} downloadable files across all pages:")
        for i, file_info in enumerate(all_files, 1):
            file_type = file_info.get('file_type', 'unknown')
            print(f"  {i}. [{file_type.upper()}] {file_info['text']}")
            print(f"     File: {file_info['filename']}")
            if file_info['estimated_date']:
                print(f"     Date: {file_info['estimated_date']}")
            print(f"     URL: {file_info['url']}")
            print()
        
        return all_files
    
    def _extract_filename(self, href, text):
        """Extract a reasonable filename from URL or link text."""
        # Try to get filename from URL
        parsed_url = urlparse(href)
        url_filename = os.path.basename(parsed_url.path)
        
        if url_filename and '.' in url_filename:
            return url_filename
        
        # Create filename from text
        clean_text = re.sub(r'[^\w\s-]', '', text)
        clean_text = re.sub(r'\s+', '_', clean_text.strip())
        
        # Add appropriate extension if missing
        if not clean_text.lower().endswith(('.xls', '.xlsx', '.zip')):
            # Default to xlsx if no clear indication
            if 'zip' in href.lower():
                clean_text += '.zip'
            else:
                clean_text += '.xlsx'
        
        return clean_text[:100]  # Limit length
    
    def _get_file_type(self, href):
        """Determine file type from URL."""
        href_lower = href.lower()
        if '.zip' in href_lower:
            return 'zip'
        elif '.pdf' in href_lower:
            return 'pdf'
        elif any(ext in href_lower for ext in ['.xls', '.xlsx']):
            return 'excel'
        else:
            return 'unknown'
    
    def _extract_date(self, text):
        """Try to extract date from link text."""
        # Common date patterns
        date_patterns = [
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # DD/MM/YYYY or DD-MM-YYYY
            r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY/MM/DD or YYYY-MM-DD
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*(\d{4})',  # Month YYYY
            r'(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*(\d{4})'  # DD Month YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        
        return None
    
    def download_file(self, file_info, delay=2):
        """Download a specific file."""
        url = file_info['url']
        filename = file_info['filename']
        filepath = self.output_dir / filename
        
        # Skip if file already exists
        if filepath.exists():
            print(f"⏭️  Skipping {filename} (already exists)")
            return True
        
        print(f"⬇️  Downloading: {filename}")
        print(f"   From: {url}")
        
        try:
            response = self.session.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Save file
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = filepath.stat().st_size
            print(f"✅ Downloaded: {filename} ({file_size:,} bytes)")
            
            # Add delay between downloads
            if delay > 0:
                time.sleep(delay)
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")
            return False
    
    def download_all(self, filter_pattern=None, max_files=None):
        """Download all discovered files."""
        files = self.discover_files()
        
        if not files:
            print("❌ No files found to download")
            return
        
        # Filter files if pattern provided
        if filter_pattern:
            pattern_lower = filter_pattern.lower()
            files = [f for f in files if pattern_lower in f['text'].lower() or pattern_lower in f['filename'].lower()]
            print(f"🔍 Filtered to {len(files)} files matching '{filter_pattern}'")
        
        # Limit number of files
        if max_files and len(files) > max_files:
            files = files[:max_files]
            print(f"📊 Limited to first {max_files} files")
        
        if not files:
            print("❌ No files match the criteria")
            return
        
        print(f"\n🚀 Starting download of {len(files)} files...")
        print(f"📁 Output directory: {self.output_dir}")
        print()
        
        successful = 0
        failed = 0
        
        for i, file_info in enumerate(files, 1):
            print(f"[{i}/{len(files)}] ", end="")
            
            if self.download_file(file_info):
                successful += 1
            else:
                failed += 1
        
        print(f"\n📊 Download Summary:")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
        print(f"📁 Files saved to: {self.output_dir}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Simple ABSLF Portfolio Scraper")
    parser.add_argument('--discover-only', action='store_true', help='Only discover files, don\'t download')
    parser.add_argument('--filter', type=str, help='Filter files by pattern (case-insensitive)')
    parser.add_argument('--max-files', type=int, help='Maximum number of files to download')
    parser.add_argument('--delay', type=float, default=2, help='Delay between downloads in seconds')
    
    args = parser.parse_args()
    
    scraper = SimpleABSLFScraper()
    
    if args.discover_only:
        scraper.discover_files()
    else:
        scraper.download_all(
            filter_pattern=args.filter,
            max_files=args.max_files
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Install with: pip install requests beautifulsoup4 pyyaml")
    except Exception as e:
        print(f"❌ Error: {e}")