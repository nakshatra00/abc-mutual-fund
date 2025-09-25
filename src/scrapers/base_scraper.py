"""
Base scraper classes for mutual fund disclosure scraping.
"""
import abc
import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse
import aiohttp
import yaml
import requests
from bs4 import BeautifulSoup
import pandas as pd


class ScraperError(Exception):
    """Base exception for scraper-related errors."""
    pass


class DownloadError(ScraperError):
    """Exception raised when download fails."""
    pass


class ValidationError(ScraperError):
    """Exception raised when data validation fails."""
    pass


class BaseScraper(abc.ABC):
    """Abstract base class for all scrapers."""
    
    def __init__(self, config: Dict[str, Any], global_config: Dict[str, Any]):
        self.config = config
        self.global_config = global_config
        self.amc_name = config.get('amc_name', 'Unknown')
        self.logger = self._setup_logger()
        self.session = None
        self._setup_output_directories()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logger for this scraper."""
        logger = logging.getLogger(f"scraper.{self.amc_name.lower()}")
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        log_level = self.global_config.get('logging', {}).get('level', 'INFO')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        return logger
    
    def _setup_output_directories(self):
        """Create necessary output directories."""
        base_dir = Path(self.global_config.get('scraper', {}).get('base_output_dir', 'data/raw'))
        today = datetime.now().strftime('%Y-%m-%d')
        
        self.output_base = base_dir / today
        self.output_base.mkdir(parents=True, exist_ok=True)
        
        # Create AMC-specific directories for each fund type
        fund_types = self.global_config.get('fund_types', [])
        for fund_type in fund_types:
            if fund_type in self.config.get('fund_types', {}):
                fund_dir = self.output_base / fund_type / self.amc_name.lower()
                fund_dir.mkdir(parents=True, exist_ok=True)
                
                # Create subdirectories
                (fund_dir / 'original').mkdir(exist_ok=True)
                (fund_dir / 'processed').mkdir(exist_ok=True)
    
    @abc.abstractmethod
    async def discover_files(self, fund_type: str) -> List[Dict[str, Any]]:
        """
        Discover available files for a given fund type.
        
        Returns:
            List of dictionaries containing file metadata:
            {
                'url': 'file_url',
                'filename': 'suggested_filename',
                'fund_type': 'fund_type',
                'estimated_date': 'YYYY-MM-DD' or None
            }
        """
        pass
    
    @abc.abstractmethod
    async def download_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download a specific file.
        
        Args:
            file_info: Dictionary containing file metadata from discover_files
            
        Returns:
            Dictionary containing download results and metadata
        """
        pass
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with proper configuration."""
        session = requests.Session()
        
        # Set headers
        headers = self.config.get('request_settings', {}).get('headers', {})
        user_agent = self.global_config.get('scraper', {}).get('user_agent', '')
        
        if user_agent and 'User-Agent' not in headers:
            headers['User-Agent'] = user_agent
            
        session.headers.update(headers)
        
        # Set verify SSL
        verify_ssl = self.config.get('request_settings', {}).get('verify_ssl', True)
        session.verify = verify_ssl
        
        return session
    
    def _get_rate_limit_delay(self) -> float:
        """Get the delay between requests based on rate limiting config."""
        rate_config = self.config.get('rate_limiting', {})
        return rate_config.get('delay_between_requests', 2)
    
    def _validate_file(self, filepath: Path, fund_type: str) -> bool:
        """Validate downloaded file meets requirements."""
        try:
            # Check file size
            file_size = filepath.stat().st_size
            min_size = self.global_config.get('global_settings', {}).get('min_file_size_kb', 10) * 1024
            max_size = self.global_config.get('global_settings', {}).get('max_file_size_mb', 50) * 1024 * 1024
            
            if file_size < min_size:
                self.logger.warning(f"File {filepath.name} is too small ({file_size} bytes)")
                return False
                
            if file_size > max_size:
                self.logger.warning(f"File {filepath.name} is too large ({file_size} bytes)")
                return False
            
            # Check file extension
            allowed_extensions = self.global_config.get('global_settings', {}).get('allowed_extensions', [])
            if allowed_extensions and filepath.suffix.lower() not in allowed_extensions:
                self.logger.warning(f"File {filepath.name} has unsupported extension")
                return False
            
            # TODO: Add content-based validation (e.g., check if Excel file can be opened)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating file {filepath}: {e}")
            return False
    
    def _create_metadata(self, file_info: Dict[str, Any], filepath: Path, 
                        download_time: datetime) -> Dict[str, Any]:
        """Create metadata for downloaded file."""
        # Calculate file hash
        with open(filepath, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        
        metadata = {
            'source_url': file_info.get('url', ''),
            'download_timestamp': download_time.isoformat(),
            'file_hash': f"sha256:{file_hash}",
            'file_size': filepath.stat().st_size,
            'original_filename': file_info.get('filename', filepath.name),
            'local_filename': filepath.name,
            'amc': self.amc_name,
            'fund_type': file_info.get('fund_type', ''),
            'disclosure_date': file_info.get('estimated_date'),
            'validation_status': 'passed' if self._validate_file(filepath, file_info.get('fund_type', '')) else 'failed',
            'processing_status': 'pending',
            'scraper_version': '1.0.0',
            'config_version': self.config.get('version', '1.0.0')
        }
        
        return metadata
    
    def _save_metadata(self, metadata: Dict[str, Any], filepath: Path):
        """Save metadata to JSON file."""
        metadata_path = filepath.parent / f"{filepath.stem}_metadata.json"
        
        try:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
                
            self.logger.debug(f"Saved metadata for {filepath.name}")
            
        except Exception as e:
            self.logger.error(f"Failed to save metadata for {filepath.name}: {e}")
    
    async def scrape_fund_type(self, fund_type: str) -> List[Dict[str, Any]]:
        """Scrape all files for a specific fund type."""
        if fund_type not in self.config.get('fund_types', {}):
            self.logger.warning(f"Fund type {fund_type} not configured for {self.amc_name}")
            return []
        
        self.logger.info(f"Starting scrape for {self.amc_name} {fund_type} funds")
        
        try:
            # Discover available files
            files = await self.discover_files(fund_type)
            self.logger.info(f"Discovered {len(files)} files for {fund_type}")
            
            results = []
            
            # Download each file
            for file_info in files:
                try:
                    # Add rate limiting delay
                    await asyncio.sleep(self._get_rate_limit_delay())
                    
                    # Download file
                    result = await self.download_file(file_info)
                    results.append(result)
                    
                    if result.get('success'):
                        self.logger.info(f"Successfully downloaded {result.get('filename', 'unknown')}")
                    else:
                        self.logger.error(f"Failed to download {file_info.get('filename', 'unknown')}: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    self.logger.error(f"Error downloading file {file_info.get('filename', 'unknown')}: {e}")
                    results.append({
                        'success': False,
                        'error': str(e),
                        'file_info': file_info
                    })
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error scraping {fund_type} for {self.amc_name}: {e}")
            return []
    
    async def scrape_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """Scrape all configured fund types."""
        self.logger.info(f"Starting full scrape for {self.amc_name}")
        
        results = {}
        configured_fund_types = list(self.config.get('fund_types', {}).keys())
        
        for fund_type in configured_fund_types:
            try:
                fund_results = await self.scrape_fund_type(fund_type)
                results[fund_type] = fund_results
                
            except Exception as e:
                self.logger.error(f"Error scraping {fund_type}: {e}")
                results[fund_type] = []
        
        self.logger.info(f"Completed scrape for {self.amc_name}")
        return results
    
    def __enter__(self):
        """Context manager entry."""
        self.session = self._create_session()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.session:
            self.session.close()


class WebsiteScraper(BaseScraper):
    """Scraper for website-based disclosures."""
    
    def __init__(self, config: Dict[str, Any], global_config: Dict[str, Any]):
        super().__init__(config, global_config)
        self.soup_cache = {}
    
    async def discover_files(self, fund_type: str) -> List[Dict[str, Any]]:
        """Discover files by scraping website pages."""
        fund_config = self.config['fund_types'][fund_type]
        source_config = self.config['disclosure_sources']['website']
        
        discovered_files = []
        
        # Get URLs to search
        urls_to_search = source_config.get('navigation_urls', [source_config.get('base_url')])
        
        for url in urls_to_search:
            try:
                files = await self._discover_files_from_url(url, fund_type)
                discovered_files.extend(files)
                
            except Exception as e:
                self.logger.error(f"Error discovering files from {url}: {e}")
        
        # Remove duplicates based on URL
        unique_files = {}
        for file_info in discovered_files:
            url = file_info.get('url', '')
            if url and url not in unique_files:
                unique_files[url] = file_info
        
        return list(unique_files.values())
    
    async def _discover_files_from_url(self, url: str, fund_type: str) -> List[Dict[str, Any]]:
        """Discover files from a specific URL."""
        fund_config = self.config['fund_types'][fund_type]
        
        try:
            # Get page content
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            self.soup_cache[url] = soup
            
            # Find potential file links
            links = []
            
            # Try CSS selectors first
            css_selectors = fund_config.get('css_selectors', [])
            for selector in css_selectors:
                try:
                    elements = soup.select(selector)
                    links.extend([elem.get('href') for elem in elements if elem.get('href')])
                except Exception as e:
                    self.logger.debug(f"CSS selector {selector} failed: {e}")
            
            # Fallback to all links
            if not links:
                all_links = soup.find_all('a', href=True)
                links = [link.get('href') for link in all_links]
            
            # Filter links based on file patterns
            file_patterns = fund_config.get('file_patterns', [])
            expected_formats = fund_config.get('expected_formats', [])
            
            matching_files = []
            
            for link in links:
                if not link:
                    continue
                    
                # Make absolute URL
                absolute_url = urljoin(url, link)
                filename = os.path.basename(urlparse(absolute_url).path)
                
                # Check if link matches patterns
                if self._matches_patterns(filename, file_patterns) or self._matches_patterns(link, file_patterns):
                    # Check file extension
                    if not expected_formats or any(filename.lower().endswith(ext.lower()) for ext in expected_formats):
                        matching_files.append({
                            'url': absolute_url,
                            'filename': filename or f"{self.amc_name}_{fund_type}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            'fund_type': fund_type,
                            'estimated_date': self._extract_date_from_filename(filename)
                        })
            
            self.logger.debug(f"Found {len(matching_files)} matching files for {fund_type} from {url}")
            return matching_files
            
        except Exception as e:
            self.logger.error(f"Error discovering files from {url}: {e}")
            return []
    
    def _matches_patterns(self, text: str, patterns: List[str]) -> bool:
        """Check if text matches any of the given patterns."""
        import fnmatch
        
        if not patterns or not text:
            return False
            
        text_lower = text.lower()
        
        for pattern in patterns:
            pattern_lower = pattern.lower()
            if fnmatch.fnmatch(text_lower, pattern_lower):
                return True
                
        return False
    
    def _extract_date_from_filename(self, filename: str) -> Optional[str]:
        """Extract date from filename if possible."""
        import re
        
        if not filename:
            return None
        
        # Common date patterns in filenames
        date_patterns = [
            r'(\d{4})[_-](\d{2})[_-](\d{2})',  # YYYY-MM-DD or YYYY_MM_DD
            r'(\d{2})[_-](\d{2})[_-](\d{4})',  # DD-MM-YYYY or DD_MM_YYYY
            r'(\d{4})(\d{2})(\d{2})',          # YYYYMMDD
            r'(\d{2})(\d{2})(\d{4})',          # DDMMYYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, filename)
            if match:
                groups = match.groups()
                try:
                    if len(groups[0]) == 4:  # Year first
                        year, month, day = groups
                    else:  # Day first
                        day, month, year = groups
                    
                    # Validate date
                    date_obj = datetime(int(year), int(month), int(day))
                    return date_obj.strftime('%Y-%m-%d')
                    
                except (ValueError, TypeError):
                    continue
        
        return None
    
    async def download_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Download a file from URL."""
        url = file_info['url']
        fund_type = file_info['fund_type']
        filename = file_info['filename']
        
        # Create output path
        output_dir = self.output_base / fund_type / self.amc_name.lower() / 'original'
        output_path = output_dir / filename
        
        # Check if file already exists and is recent
        if output_path.exists():
            file_age = datetime.now() - datetime.fromtimestamp(output_path.stat().st_mtime)
            max_age = timedelta(days=self.global_config.get('global_settings', {}).get('max_file_age_days', 45))
            
            if file_age < max_age:
                self.logger.info(f"File {filename} already exists and is recent, skipping download")
                return {
                    'success': True,
                    'filename': filename,
                    'filepath': str(output_path),
                    'skipped': True,
                    'reason': 'File already exists and is recent'
                }
        
        try:
            download_time = datetime.now()
            
            # Download file
            response = self.session.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Save file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Create and save metadata
            metadata = self._create_metadata(file_info, output_path, download_time)
            self._save_metadata(metadata, output_path)
            
            return {
                'success': True,
                'filename': filename,
                'filepath': str(output_path),
                'metadata': metadata,
                'skipped': False
            }
            
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {e}")
            return {
                'success': False,
                'filename': filename,
                'error': str(e),
                'file_info': file_info
            }