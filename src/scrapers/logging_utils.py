"""
Logging utilities for the scraper system.
"""
import json
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional


class ScraperLogFilter(logging.Filter):
    """Custom filter to add scraper-specific fields to log records."""
    
    def __init__(self, amc_name: str = None):
        super().__init__()
        self.amc_name = amc_name
    
    def filter(self, record):
        # Add custom fields
        record.amc_name = getattr(record, 'amc_name', self.amc_name or 'unknown')
        record.scraper_component = getattr(record, 'scraper_component', 'scraper')
        return True


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'amc_name': getattr(record, 'amc_name', None),
            'scraper_component': getattr(record, 'scraper_component', None),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'lineno', 'funcName', 'created', 
                          'msecs', 'relativeCreated', 'thread', 'threadName', 
                          'processName', 'process', 'exc_info', 'exc_text', 'stack_info',
                          'amc_name', 'scraper_component', 'message']:
                log_entry[key] = value
        
        return json.dumps(log_entry, ensure_ascii=False)


class DownloadTracker:
    """Track download statistics and events."""
    
    def __init__(self, output_dir: str = "data/raw"):
        self.output_dir = Path(output_dir)
        self.logger = logging.getLogger("scraper.tracker")
        self.stats = {
            'downloads_attempted': 0,
            'downloads_successful': 0,
            'downloads_failed': 0,
            'downloads_skipped': 0,
            'total_bytes_downloaded': 0,
            'start_time': None,
            'end_time': None
        }
        
    def start_session(self):
        """Start a download session."""
        self.stats['start_time'] = datetime.now()
        self.logger.info("Download session started")
        
    def end_session(self):
        """End a download session and save stats."""
        self.stats['end_time'] = datetime.now()
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        self.logger.info(f"Download session completed in {duration:.1f}s")
        self.logger.info(f"Session stats: {self.get_summary()}")
        
        # Save session stats
        self._save_session_stats()
        
    def record_download_attempt(self, file_info: Dict[str, Any]):
        """Record a download attempt."""
        self.stats['downloads_attempted'] += 1
        self.logger.debug(f"Download attempt: {file_info.get('filename', 'unknown')}")
        
    def record_download_success(self, file_info: Dict[str, Any], file_size: int):
        """Record a successful download."""
        self.stats['downloads_successful'] += 1
        self.stats['total_bytes_downloaded'] += file_size
        
        self.logger.info(
            f"Download successful: {file_info.get('filename', 'unknown')} ({file_size:,} bytes)",
            extra={
                'file_url': file_info.get('url'),
                'file_size': file_size,
                'fund_type': file_info.get('fund_type'),
                'event_type': 'download_success'
            }
        )
        
    def record_download_failure(self, file_info: Dict[str, Any], error: str):
        """Record a failed download."""
        self.stats['downloads_failed'] += 1
        
        self.logger.error(
            f"Download failed: {file_info.get('filename', 'unknown')} - {error}",
            extra={
                'file_url': file_info.get('url'),
                'error': error,
                'fund_type': file_info.get('fund_type'),
                'event_type': 'download_failure'
            }
        )
        
    def record_download_skipped(self, file_info: Dict[str, Any], reason: str):
        """Record a skipped download."""
        self.stats['downloads_skipped'] += 1
        
        self.logger.info(
            f"Download skipped: {file_info.get('filename', 'unknown')} - {reason}",
            extra={
                'file_url': file_info.get('url'),
                'reason': reason,
                'fund_type': file_info.get('fund_type'),
                'event_type': 'download_skipped'
            }
        )
        
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        summary = self.stats.copy()
        
        if summary['start_time'] and summary['end_time']:
            duration = (summary['end_time'] - summary['start_time']).total_seconds()
            summary['duration_seconds'] = duration
            
            if duration > 0:
                summary['download_rate_mbps'] = (summary['total_bytes_downloaded'] / (1024 * 1024)) / duration
        
        if summary['downloads_attempted'] > 0:
            summary['success_rate'] = summary['downloads_successful'] / summary['downloads_attempted']
        
        return summary
        
    def _save_session_stats(self):
        """Save session statistics to file."""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            stats_dir = self.output_dir / today / 'session_stats'
            stats_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            stats_file = stats_dir / f"download_stats_{timestamp}.json"
            
            # Convert datetime objects to strings for JSON serialization
            stats_to_save = {}
            for key, value in self.stats.items():
                if isinstance(value, datetime):
                    stats_to_save[key] = value.isoformat()
                else:
                    stats_to_save[key] = value
            
            # Add summary fields
            summary = self.get_summary()
            stats_to_save.update({
                'duration_seconds': summary.get('duration_seconds'),
                'success_rate': summary.get('success_rate'),
                'download_rate_mbps': summary.get('download_rate_mbps')
            })
            
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats_to_save, f, indent=2, ensure_ascii=False)
                
            self.logger.debug(f"Session stats saved to {stats_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save session stats: {e}")


def setup_scraper_logging(config: Dict[str, Any], amc_name: Optional[str] = None) -> logging.Logger:
    """
    Set up comprehensive logging for the scraper system.
    
    Args:
        config: Logging configuration from global config
        amc_name: Optional AMC name for filtering
        
    Returns:
        Configured logger instance
    """
    # Create logs directory
    log_file = config.get('file', 'logs/scraper.log')
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create root logger
    logger_name = f"scraper.{amc_name.lower()}" if amc_name else "scraper"
    logger = logging.getLogger(logger_name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Set level
    log_level = config.get('level', 'INFO')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler with simple format
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation
    max_size_mb = config.get('max_size_mb', 10)
    backup_count = config.get('backup_count', 5)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=max_size_mb * 1024 * 1024,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setFormatter(console_formatter)
    logger.addHandler(file_handler)
    
    # JSON file handler for structured logs
    json_log_path = log_path.parent / f"{log_path.stem}_structured.jsonl"
    json_handler = logging.handlers.RotatingFileHandler(
        json_log_path,
        maxBytes=max_size_mb * 1024 * 1024,
        backupCount=backup_count,
        encoding='utf-8'
    )
    json_handler.setFormatter(JSONFormatter())
    logger.addHandler(json_handler)
    
    # Add custom filter
    if amc_name:
        custom_filter = ScraperLogFilter(amc_name)
        for handler in logger.handlers:
            handler.addFilter(custom_filter)
    
    return logger


class MetadataManager:
    """Manages metadata for downloaded files."""
    
    def __init__(self, output_dir: str = "data/raw"):
        self.output_dir = Path(output_dir)
        self.logger = logging.getLogger("scraper.metadata")
        
    def create_file_metadata(self, file_info: Dict[str, Any], filepath: Path,
                           download_time: datetime, amc_name: str) -> Dict[str, Any]:
        """Create comprehensive metadata for a downloaded file."""
        import hashlib
        
        # Calculate file hash
        with open(filepath, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        
        file_stat = filepath.stat()
        
        metadata = {
            # File information
            'original_filename': file_info.get('filename', filepath.name),
            'local_filename': filepath.name,
            'local_filepath': str(filepath),
            'file_size_bytes': file_stat.st_size,
            'file_hash_sha256': file_hash,
            
            # Download information
            'source_url': file_info.get('url', ''),
            'download_timestamp': download_time.isoformat(),
            'download_user_agent': file_info.get('user_agent', ''),
            
            # Fund information
            'amc_name': amc_name,
            'fund_type': file_info.get('fund_type', ''),
            'estimated_disclosure_date': file_info.get('estimated_date'),
            
            # Processing status
            'validation_status': 'pending',
            'processing_status': 'pending',
            'extraction_status': 'pending',
            
            # System information
            'scraper_version': '1.0.0',
            'metadata_version': '1.0.0',
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat()
        }
        
        return metadata
    
    def save_metadata(self, metadata: Dict[str, Any], filepath: Path):
        """Save metadata to JSON file."""
        metadata_path = filepath.parent / f"{filepath.stem}_metadata.json"
        
        try:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
                
            self.logger.debug(f"Saved metadata for {filepath.name}")
            
        except Exception as e:
            self.logger.error(f"Failed to save metadata for {filepath.name}: {e}")
            
    def update_metadata(self, filepath: Path, updates: Dict[str, Any]):
        """Update existing metadata file."""
        metadata_path = filepath.parent / f"{filepath.stem}_metadata.json"
        
        try:
            # Load existing metadata
            if metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
            else:
                metadata = {}
            
            # Apply updates
            metadata.update(updates)
            metadata['last_updated'] = datetime.now().isoformat()
            
            # Save updated metadata
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
                
            self.logger.debug(f"Updated metadata for {filepath.name}")
            
        except Exception as e:
            self.logger.error(f"Failed to update metadata for {filepath.name}: {e}")
            
    def get_metadata(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """Load metadata for a file."""
        metadata_path = filepath.parent / f"{filepath.stem}_metadata.json"
        
        try:
            if metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to load metadata for {filepath.name}: {e}")
            return None