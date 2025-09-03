# logger_setup.py
import os
import json
import logging
import logging.handlers
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum
import gzip
import shutil

class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

@dataclass
class LogConfig:
    """Configuration for logging setup"""
    log_directory: str = "logs"
    max_log_files: int = 30  # Keep logs for 30 days
    max_file_size: int = 10 * 1024 * 1024  # 10MB per file
    backup_count: int = 5  # Keep 5 backup files per log type
    compress_old_logs: bool = True
    log_format: str = "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    console_output: bool = True
    console_level: LogLevel = LogLevel.INFO

@dataclass
class ForwardingSession:
    """Data class to track a forwarding session"""
    session_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_targets: int = 0
    successful_forwards: int = 0
    failed_forwards: int = 0
    errors: List[Dict] = None
    message_preview: str = ""
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        return (self.successful_forwards / self.total_targets * 100) if self.total_targets > 0 else 0
    
    @property
    def duration(self) -> timedelta:
        """Calculate session duration"""
        end = self.end_time or datetime.now()
        return end - self.start_time

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime and other objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        return super().default(obj)

class LoggerSetup:
    """Enhanced logging configuration and operations manager"""
    
    def __init__(self, config: LogConfig = None):
        self.config = config or LogConfig()
        self.logger = None
        self.current_session: Optional[ForwardingSession] = None
        self.log_files = {
            'main': None,
            'success': None,
            'error': None,
            'debug': None,
            'stats': None
        }
        
    def setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging configuration"""
        self._create_log_directory()
        self._cleanup_old_logs()
        
        # Setup main logger
        logger = logging.getLogger('TelegramForwarder')
        logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Setup different log handlers
        self._setup_main_handler(logger)
        self._setup_success_handler(logger)
        self._setup_error_handler(logger)
        self._setup_debug_handler(logger)
        self._setup_stats_handler(logger)
        
        if self.config.console_output:
            self._setup_console_handler(logger)
        
        self.logger = logger
        logger.info("Enhanced logging system initialized")
        logger.info(f"Log configuration: {asdict(self.config)}")
        
        return logger

    def _create_log_directory(self):
        """Create log directory structure"""
        log_path = Path(self.config.log_directory)
        log_path.mkdir(exist_ok=True)
        
        # Create subdirectories for organization
        for subdir in ['daily', 'sessions', 'archives']:
            (log_path / subdir).mkdir(exist_ok=True)

    def _cleanup_old_logs(self):
        """Clean up old log files based on retention policy"""
        try:
            log_path = Path(self.config.log_directory)
            cutoff_date = datetime.now() - timedelta(days=self.config.max_log_files)
            
            for log_file in log_path.glob('*.log'):
                if log_file.stat().st_mtime < cutoff_date.timestamp():
                    if self.config.compress_old_logs:
                        self._compress_and_archive(log_file)
                    else:
                        log_file.unlink()
                        
        except Exception as e:
            print(f"Warning: Failed to cleanup old logs: {e}")

    def _compress_and_archive(self, log_file: Path):
        """Compress old log files and move to archives"""
        try:
            archive_path = Path(self.config.log_directory) / 'archives'
            compressed_name = f"{log_file.stem}_{datetime.now().strftime('%Y%m%d')}.gz"
            compressed_path = archive_path / compressed_name
            
            with open(log_file, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            log_file.unlink()  # Remove original file
            
        except Exception as e:
            print(f"Warning: Failed to compress log file {log_file}: {e}")

    def _get_rotating_handler(self, filename: str, level: int) -> logging.handlers.RotatingFileHandler:
        """Create a rotating file handler"""
        handler = logging.handlers.RotatingFileHandler(
            filename=filename,
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        handler.setLevel(level)
        
        formatter = logging.Formatter(
            self.config.log_format,
            datefmt=self.config.date_format
        )
        handler.setFormatter(formatter)
        
        return handler

    def _setup_main_handler(self, logger: logging.Logger):
        """Setup main activity log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"bot_activity_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.INFO)
        logger.addHandler(handler)
        self.log_files['main'] = filename

    def _setup_success_handler(self, logger: logging.Logger):
        """Setup success-only log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"success_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.INFO)
        handler.addFilter(lambda record: 'SUCCESS' in record.getMessage() or record.levelname == 'SUCCESS')
        logger.addHandler(handler)
        self.log_files['success'] = filename

    def _setup_error_handler(self, logger: logging.Logger):
        """Setup error-only log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"errors_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.ERROR)
        logger.addHandler(handler)
        self.log_files['error'] = filename

    def _setup_debug_handler(self, logger: logging.Logger):
        """Setup debug log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"debug_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.DEBUG)
        handler.addFilter(lambda record: record.levelname == 'DEBUG')
        logger.addHandler(handler)
        self.log_files['debug'] = filename

    def _setup_stats_handler(self, logger: logging.Logger):
        """Setup statistics log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"stats_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.INFO)
        handler.addFilter(lambda record: 'STATS' in record.getMessage() or 'FORWARDING' in record.getMessage())
        logger.addHandler(handler)
        self.log_files['stats'] = filename

    def _setup_console_handler(self, logger: logging.Logger):
        """Setup console output handler"""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.config.console_level.value)
        
        # Simplified format for console
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    def start_forwarding_session(self, session_id: str = None, total_targets: int = 0, message_preview: str = "") -> ForwardingSession:
        """Start a new forwarding session"""
        if not session_id:
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.current_session = ForwardingSession(
            session_id=session_id,
            start_time=datetime.now(),
            total_targets=total_targets,
            message_preview=message_preview[:100]  # Limit preview length
        )
        
        self.log_info(f"SESSION_START | {session_id} | Targets: {total_targets}")
        return self.current_session

    def end_forwarding_session(self) -> Optional[ForwardingSession]:
        """End the current forwarding session"""
        if not self.current_session:
            return None
        
        self.current_session.end_time = datetime.now()
        
        # Log session summary
        self.log_session_summary(self.current_session)
        
        # Save session data
        self._save_session_data(self.current_session)
        
        session = self.current_session
        self.current_session = None
        return session

    def log_forwarding_result(self, target: str, success: bool, error_message: str = None):
        """Log individual forwarding result"""
        if not self.current_session:
            self.log_warning("No active session for forwarding result")
            return
        
        if success:
            self.current_session.successful_forwards += 1
            self.log_success(f"Forward to {target}")
        else:
            self.current_session.failed_forwards += 1
            error_data = {
                'target': target,
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            }
            self.current_session.errors.append(error_data)
            self.log_error(f"Forward failed to {target}: {error_message}")

    def log_session_summary(self, session: ForwardingSession):
        """Log comprehensive session summary"""
        summary = {
            'session_id': session.session_id,
            'duration': session.duration.total_seconds(),
            'total_targets': session.total_targets,
            'successful_forwards': session.successful_forwards,
            'failed_forwards': session.failed_forwards,
            'success_rate': session.success_rate,
            'errors_count': len(session.errors)
        }
        
        self.log_info(f"STATS | SESSION_SUMMARY | {json.dumps(summary)}")

    def _save_session_data(self, session: ForwardingSession):
        """Save detailed session data to JSON file"""
        try:
            sessions_dir = Path(self.config.log_directory) / 'sessions'
            session_file = sessions_dir / f"{session.session_id}.json"
            
            session_data = asdict(session)
            
            with open(session_file, 'w', encoding='utf-8') as f:
                json.dump(session_data, f, indent=2, cls=CustomJSONEncoder)
                
        except Exception as e:
            self.log_error(f"Failed to save session data: {e}")

    def write_daily_summary(self, additional_stats: Dict = None):
        """Write comprehensive daily summary"""
        try:
            summary_file = Path(self.config.log_directory) / 'daily' / f"summary_{datetime.now().strftime('%Y%m%d')}.txt"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Collect daily statistics
            daily_stats = self._collect_daily_statistics()
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"{'='*80}\n")
                f.write(f"DAILY SUMMARY - {timestamp}\n")
                f.write(f"{'='*80}\n\n")
                
                # Session statistics
                f.write("📊 SESSION STATISTICS:\n")
                f.write(f"   Total Sessions: {daily_stats.get('total_sessions', 0)}\n")
                f.write(f"   Total Forwards: {daily_stats.get('total_forwards', 0)}\n")
                f.write(f"   Successful: {daily_stats.get('successful_forwards', 0)}\n")
                f.write(f"   Failed: {daily_stats.get('failed_forwards', 0)}\n")
                f.write(f"   Success Rate: {daily_stats.get('success_rate', 0):.1f}%\n\n")
                
                # Error analysis
                if daily_stats.get('common_errors'):
                    f.write("❌ COMMON ERRORS:\n")
                    for error, count in daily_stats['common_errors'].items():
                        f.write(f"   {error}: {count} occurrences\n")
                    f.write("\n")
                
                # Additional stats if provided
                if additional_stats:
                    f.write("📈 ADDITIONAL STATISTICS:\n")
                    for key, value in additional_stats.items():
                        f.write(f"   {key}: {value}\n")
                    f.write("\n")
                
                f.write(f"{'='*80}\n")
                f.write("Enhanced Telegram Forwarder v2.5\n")
                f.write(f"Generated: {timestamp}\n")
                f.write(f"{'='*80}\n")
                
        except Exception as e:
            self.log_error(f"Failed to write daily summary: {e}")

    def _collect_daily_statistics(self) -> Dict:
        """Collect statistics from daily session files"""
        try:
            sessions_dir = Path(self.config.log_directory) / 'sessions'
            today = datetime.now().strftime('%Y%m%d')
            
            stats = {
                'total_sessions': 0,
                'total_forwards': 0,
                'successful_forwards': 0,
                'failed_forwards': 0,
                'common_errors': {}
            }
            
            for session_file in sessions_dir.glob(f"session_{today}_*.json"):
                try:
                    with open(session_file, 'r', encoding='utf-8') as f:
                        session_data = json.load(f)
                    
                    stats['total_sessions'] += 1
                    stats['successful_forwards'] += session_data.get('successful_forwards', 0)
                    stats['failed_forwards'] += session_data.get('failed_forwards', 0)
                    
                    # Analyze errors
                    for error in session_data.get('errors', []):
                        error_msg = error.get('error', 'Unknown error')
                        stats['common_errors'][error_msg] = stats['common_errors'].get(error_msg, 0) + 1
                        
                except Exception:
                    continue
            
            stats['total_forwards'] = stats['successful_forwards'] + stats['failed_forwards']
            if stats['total_forwards'] > 0:
                stats['success_rate'] = (stats['successful_forwards'] / stats['total_forwards']) * 100
            else:
                stats['success_rate'] = 0
                
            return stats
            
        except Exception as e:
            self.log_error(f"Failed to collect daily statistics: {e}")
            return {}

    def get_log_file_info(self) -> Dict[str, Dict]:
        """Get information about current log files"""
        info = {}
        
        for log_type, filename in self.log_files.items():
            if filename and os.path.exists(filename):
                stat = os.stat(filename)
                info[log_type] = {
                    'filename': filename,
                    'size': stat.st_size,
                    'size_human': self._format_file_size(stat.st_size),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                }
            else:
                info[log_type] = {'filename': filename, 'exists': False}
                
        return info

    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024.0 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f} {size_names[i]}"

    # Enhanced logging methods with better formatting
    def log_success(self, message: str, extra_data: Dict = None):
        """Log success message with optional extra data"""
        if self.logger:
            log_message = f"SUCCESS | {message}"
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.info(log_message)

    def log_error(self, message: str, exception: Exception = None, extra_data: Dict = None):
        """Log error message with optional exception and extra data"""
        if self.logger:
            log_message = f"ERROR | {message}"
            if exception:
                log_message += f" | Exception: {str(exception)}"
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.error(log_message, exc_info=exception is not None)

    def log_warning(self, message: str, extra_data: Dict = None):
        """Log warning message with optional extra data"""
        if self.logger:
            log_message = f"WARNING | {message}"
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.warning(log_message)

    def log_info(self, message: str, extra_data: Dict = None):
        """Log info message with optional extra data"""
        if self.logger:
            log_message = message
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.info(log_message)

    def log_debug(self, message: str, extra_data: Dict = None):
        """Log debug message with optional extra data"""
        if self.logger:
            log_message = f"DEBUG | {message}"
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.debug(log_message)

    def export_logs(self, start_date: datetime = None, end_date: datetime = None, log_types: List[str] = None) -> str:
        """Export logs for a specific date range and types"""
        try:
            if not start_date:
                start_date = datetime.now() - timedelta(days=7)  # Last week by default
            if not end_date:
                end_date = datetime.now()
            if not log_types:
                log_types = ['main', 'success', 'error']
            
            export_filename = f"logs/export_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.txt"
            
            with open(export_filename, 'w', encoding='utf-8') as export_file:
                export_file.write(f"Log Export - {start_date} to {end_date}\n")
                export_file.write("="*80 + "\n\n")
                
                for log_type in log_types:
                    if log_type in self.log_files:
                        export_file.write(f"[{log_type.upper()} LOGS]\n")
                        export_file.write("-"*40 + "\n")
                        
                        # Here you would implement date-range filtering
                        # This is a simplified version
                        filename = self.log_files[log_type]
                        if filename and os.path.exists(filename):
                            with open(filename, 'r', encoding='utf-8') as log_file:
                                export_file.write(log_file.read())
                        
                        export_file.write("\n\n")
            
            self.log_info(f"Logs exported to: {export_filename}")
            return export_filename
            
        except Exception as e:
            self.log_error(f"Failed to export logs: {e}")
            return ""