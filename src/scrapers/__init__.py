"""
Mutual Fund Disclosure Scraper Package

This package provides automated scraping capabilities for downloading
mutual fund portfolio disclosures from various Asset Management Companies (AMCs).
"""

from .base_scraper import BaseScraper, WebsiteScraper, ScraperError, DownloadError, ValidationError
from .scraper_manager import ScraperManager, ScraperFactory, load_scraper_manager, quick_scrape, bulk_scrape

__version__ = "1.0.0"
__author__ = "Mutual Fund Analysis System"

__all__ = [
    'BaseScraper',
    'WebsiteScraper', 
    'ScraperManager',
    'ScraperFactory',
    'ScraperError',
    'DownloadError', 
    'ValidationError',
    'load_scraper_manager',
    'quick_scrape',
    'bulk_scrape'
]