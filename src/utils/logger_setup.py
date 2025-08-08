"""
Logging Setup for ETL Pipeline

Configures structured logging with structlog for the entire
ETL pipeline system with proper formatting and output handling.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import structlog
from structlog.stdlib import LoggerFactory

def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    enable_console: bool = True,
    enable_file: bool = False
) -> None:
    """
    Setup structured logging for the ETL pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        enable_console: Enable console logging
        enable_file: Enable file logging
    """
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Setup standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper())
    )
    
    # Create log directory if file logging is enabled
    if enable_file and log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Add file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Get the root logger and add file handler
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
    
    # Create console handler if enabled
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Get the root logger and add console handler
        root_logger = logging.getLogger()
        root_logger.addHandler(console_handler)
    
    # Set loggers for external libraries
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Structured logger instance
    """
    return structlog.get_logger(name)

class PipelineLogger:
    """Enhanced logger for ETL pipeline operations."""
    
    def __init__(self, name: str):
        """Initialize pipeline logger."""
        self.logger = get_logger(name)
        self.name = name
    
    def log_ingestion_start(self, source: str, record_count: int = None):
        """Log the start of data ingestion."""
        self.logger.info(
            "Data ingestion started",
            source=source,
            record_count=record_count,
            event="ingestion_start"
        )
    
    def log_ingestion_complete(self, source: str, record_count: int, duration: float):
        """Log the completion of data ingestion."""
        self.logger.info(
            "Data ingestion completed",
            source=source,
            record_count=record_count,
            duration_seconds=duration,
            event="ingestion_complete"
        )
    
    def log_processing_start(self, job_id: str, data_size: int):
        """Log the start of data processing."""
        self.logger.info(
            "Data processing started",
            job_id=job_id,
            data_size=data_size,
            event="processing_start"
        )
    
    def log_processing_complete(self, job_id: str, processed_count: int, duration: float):
        """Log the completion of data processing."""
        self.logger.info(
            "Data processing completed",
            job_id=job_id,
            processed_count=processed_count,
            duration_seconds=duration,
            event="processing_complete"
        )
    
    def log_warehouse_load(self, table: str, record_count: int, duration: float):
        """Log data warehouse loading."""
        self.logger.info(
            "Data warehouse load completed",
            table=table,
            record_count=record_count,
            duration_seconds=duration,
            event="warehouse_load"
        )
    
    def log_error(self, error: Exception, context: dict = None):
        """Log an error with context."""
        self.logger.error(
            "Pipeline error occurred",
            error_type=type(error).__name__,
            error_message=str(error),
            context=context or {},
            event="pipeline_error"
        )
    
    def log_metric(self, metric_name: str, value: float, tags: dict = None):
        """Log a custom metric."""
        self.logger.info(
            "Custom metric",
            metric_name=metric_name,
            value=value,
            tags=tags or {},
            event="metric"
        )
    
    def log_pipeline_status(self, status: str, component: str, details: dict = None):
        """Log pipeline component status."""
        self.logger.info(
            "Pipeline component status",
            status=status,
            component=component,
            details=details or {},
            event="pipeline_status"
        )
