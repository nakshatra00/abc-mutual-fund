"""
Scraper factory and manager for orchestrating multiple AMC scrapers.
"""
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml

from .base_scraper import WebsiteScraper, BaseScraper


class ScraperFactory:
    """Factory for creating appropriate scraper instances."""
    
    SCRAPER_TYPES = {
        'website': WebsiteScraper,
        # 'api': APIScraper,      # To be implemented
        # 'ftp': FTPScraper,      # To be implemented
    }
    
    @classmethod
    def create_scraper(cls, amc_config: Dict[str, Any], global_config: Dict[str, Any]) -> BaseScraper:
        """
        Create appropriate scraper based on configuration.
        
        Args:
            amc_config: AMC-specific configuration
            global_config: Global scraper configuration
            
        Returns:
            Configured scraper instance
        """
        # Determine primary source type
        disclosure_sources = amc_config.get('disclosure_sources', {})
        
        # Find the first enabled source type
        scraper_type = None
        for source_type, source_config in disclosure_sources.items():
            if source_config.get('enabled', False):
                scraper_type = source_type
                break
        
        if not scraper_type:
            raise ValueError(f"No enabled disclosure sources found for {amc_config.get('amc_name', 'Unknown')}")
        
        if scraper_type not in cls.SCRAPER_TYPES:
            raise ValueError(f"Unsupported scraper type: {scraper_type}")
        
        scraper_class = cls.SCRAPER_TYPES[scraper_type]
        return scraper_class(amc_config, global_config)


class ScraperManager:
    """Manager for orchestrating multiple scrapers."""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.global_config = self._load_global_config()
        self.amc_configs = self._load_amc_configs()
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logger for the manager."""
        logger = logging.getLogger("scraper.manager")
        
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # File handler
            log_config = self.global_config.get('logging', {})
            log_file = log_config.get('file', 'logs/scraper.log')
            
            if log_file:
                log_path = Path(log_file)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                
                file_handler = logging.FileHandler(log_path)
                file_handler.setFormatter(console_formatter)
                logger.addHandler(file_handler)
        
        log_level = self.global_config.get('logging', {}).get('level', 'INFO')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        return logger
    
    def _load_global_config(self) -> Dict[str, Any]:
        """Load global scraper configuration."""
        config_path = self.config_dir / 'scraper_config.yml'
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            self.logger = logging.getLogger("scraper.manager")
            self.logger.debug(f"Loaded global config from {config_path}")
            return config
            
        except Exception as e:
            raise RuntimeError(f"Failed to load global config from {config_path}: {e}")
    
    def _load_amc_configs(self) -> Dict[str, Dict[str, Any]]:
        """Load all AMC-specific configurations."""
        scrapers_dir = self.config_dir / 'scrapers'
        
        if not scrapers_dir.exists():
            raise RuntimeError(f"Scrapers config directory not found: {scrapers_dir}")
        
        configs = {}
        
        for config_file in scrapers_dir.glob('*.yml'):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                
                amc_name = config.get('amc_name')
                if not amc_name:
                    self.logger.warning(f"No amc_name found in {config_file}, skipping")
                    continue
                
                # Only load active configurations
                if config.get('active', True):
                    configs[amc_name.upper()] = config
                    self.logger.debug(f"Loaded config for {amc_name}")
                else:
                    self.logger.debug(f"Skipped inactive config for {amc_name}")
                    
            except Exception as e:
                self.logger.error(f"Failed to load config from {config_file}: {e}")
        
        self.logger.info(f"Loaded {len(configs)} AMC configurations")
        return configs
    
    def get_available_amcs(self) -> List[str]:
        """Get list of available AMCs."""
        return list(self.amc_configs.keys())
    
    def get_amc_fund_types(self, amc_name: str) -> List[str]:
        """Get available fund types for a specific AMC."""
        amc_name_upper = amc_name.upper()
        if amc_name_upper not in self.amc_configs:
            return []
        
        return list(self.amc_configs[amc_name_upper].get('fund_types', {}).keys())
    
    async def scrape_amc(self, amc_name: str, fund_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Scrape a specific AMC.
        
        Args:
            amc_name: Name of the AMC to scrape
            fund_types: List of fund types to scrape (None for all)
            
        Returns:
            Dictionary containing scrape results
        """
        amc_name_upper = amc_name.upper()
        
        if amc_name_upper not in self.amc_configs:
            raise ValueError(f"Unknown AMC: {amc_name}")
        
        amc_config = self.amc_configs[amc_name_upper]
        
        self.logger.info(f"Starting scrape for {amc_name}")
        
        try:
            scraper = ScraperFactory.create_scraper(amc_config, self.global_config)
            
            with scraper:
                if fund_types:
                    # Scrape specific fund types
                    results = {}
                    for fund_type in fund_types:
                        if fund_type in amc_config.get('fund_types', {}):
                            results[fund_type] = await scraper.scrape_fund_type(fund_type)
                        else:
                            self.logger.warning(f"Fund type {fund_type} not configured for {amc_name}")
                            results[fund_type] = []
                else:
                    # Scrape all fund types
                    results = await scraper.scrape_all()
                
                # Calculate summary statistics
                total_files = sum(len(fund_results) for fund_results in results.values())
                successful_files = sum(
                    len([r for r in fund_results if r.get('success', False)])
                    for fund_results in results.values()
                )
                
                summary = {
                    'amc_name': amc_name,
                    'scrape_timestamp': datetime.now().isoformat(),
                    'total_files_attempted': total_files,
                    'successful_downloads': successful_files,
                    'success_rate': successful_files / total_files if total_files > 0 else 0,
                    'fund_type_results': results
                }
                
                self.logger.info(f"Completed scrape for {amc_name}: {successful_files}/{total_files} files successful")
                return summary
                
        except Exception as e:
            self.logger.error(f"Error scraping {amc_name}: {e}")
            return {
                'amc_name': amc_name,
                'scrape_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'success': False
            }
    
    async def scrape_all(self, fund_types: Optional[List[str]] = None, 
                        amc_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Scrape all or specified AMCs.
        
        Args:
            fund_types: List of fund types to scrape (None for all)
            amc_names: List of AMC names to scrape (None for all)
            
        Returns:
            Dictionary containing overall scrape results
        """
        start_time = datetime.now()
        self.logger.info("Starting bulk scrape operation")
        
        # Determine which AMCs to scrape
        if amc_names:
            amcs_to_scrape = [amc for amc in amc_names if amc.upper() in self.amc_configs]
            missing_amcs = [amc for amc in amc_names if amc.upper() not in self.amc_configs]
            if missing_amcs:
                self.logger.warning(f"Unknown AMCs will be skipped: {missing_amcs}")
        else:
            amcs_to_scrape = list(self.amc_configs.keys())
        
        self.logger.info(f"Scraping {len(amcs_to_scrape)} AMCs: {amcs_to_scrape}")
        
        # Execute scraping with limited concurrency
        max_concurrent = self.global_config.get('scraper', {}).get('parallel_downloads', 5)
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def scrape_with_semaphore(amc_name):
            async with semaphore:
                return await self.scrape_amc(amc_name, fund_types)
        
        # Run all scraping tasks
        tasks = [scrape_with_semaphore(amc) for amc in amcs_to_scrape]
        amc_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        results = {}
        total_files = 0
        total_successful = 0
        
        for i, result in enumerate(amc_results):
            amc_name = amcs_to_scrape[i]
            
            if isinstance(result, Exception):
                self.logger.error(f"Exception scraping {amc_name}: {result}")
                results[amc_name] = {
                    'error': str(result),
                    'success': False
                }
            else:
                results[amc_name] = result
                total_files += result.get('total_files_attempted', 0)
                total_successful += result.get('successful_downloads', 0)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        summary = {
            'operation': 'bulk_scrape',
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'amcs_scraped': len(amcs_to_scrape),
            'total_files_attempted': total_files,
            'total_successful_downloads': total_successful,
            'overall_success_rate': total_successful / total_files if total_files > 0 else 0,
            'amc_results': results
        }
        
        # Save summary to file
        self._save_scrape_summary(summary)
        
        self.logger.info(f"Bulk scrape completed in {duration:.1f}s: {total_successful}/{total_files} files successful")
        return summary
    
    def _save_scrape_summary(self, summary: Dict[str, Any]):
        """Save scrape summary to file."""
        try:
            output_dir = Path(self.global_config.get('scraper', {}).get('base_output_dir', 'data/raw'))
            today = datetime.now().strftime('%Y-%m-%d')
            summary_dir = output_dir / today
            summary_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            summary_file = summary_dir / f"scrape_summary_{timestamp}.json"
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved scrape summary to {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save scrape summary: {e}")
    
    async def run_scheduled_scrape(self):
        """Run a scheduled scrape based on configuration."""
        schedule_config = self.global_config.get('schedule', {})
        frequency = schedule_config.get('frequency', 'daily')
        
        self.logger.info(f"Running scheduled scrape (frequency: {frequency})")
        
        # For now, just run all AMCs
        # TODO: Implement more sophisticated scheduling logic
        return await self.scrape_all()


# Convenience functions for common operations

def load_scraper_manager(config_dir: str = "config") -> ScraperManager:
    """Load and return a configured scraper manager."""
    return ScraperManager(config_dir)


async def quick_scrape(amc_name: str, fund_types: Optional[List[str]] = None, 
                      config_dir: str = "config") -> Dict[str, Any]:
    """Quick scrape for a single AMC."""
    manager = load_scraper_manager(config_dir)
    return await manager.scrape_amc(amc_name, fund_types)


async def bulk_scrape(amc_names: Optional[List[str]] = None, 
                     fund_types: Optional[List[str]] = None,
                     config_dir: str = "config") -> Dict[str, Any]:
    """Bulk scrape for multiple AMCs."""
    manager = load_scraper_manager(config_dir)
    return await manager.scrape_all(fund_types, amc_names)