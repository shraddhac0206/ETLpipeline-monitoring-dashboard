"""
CSV Data Ingestor

Handles ingestion of data from CSV files with support for:
- Batch processing
- Schema validation
- Data quality checks
- Incremental loading
"""

import asyncio
import pandas as pd
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

import structlog
from kafka import KafkaProducer

from src.utils.logger_setup import PipelineLogger

class CSVIngestor:
    """Handles CSV file ingestion with data validation and quality checks."""
    
    def __init__(self, kafka_producer: KafkaProducer, monitor):
        """Initialize the CSV ingestor."""
        self.kafka_producer = kafka_producer
        self.monitor = monitor
        self.logger = PipelineLogger(__name__)
        
        # Processing state
        self.is_running = False
        self.current_file = None
        
        # Statistics
        self.stats = {
            "files_processed": 0,
            "records_ingested": 0,
            "processing_errors": 0,
            "last_processed_file": None,
            "processing_time": 0.0
        }
    
    async def initialize(self):
        """Initialize the CSV ingestor."""
        self.logger.log_pipeline_status("initializing", "csv_ingestor")
        self.logger.log_pipeline_status("initialized", "csv_ingestor")
    
    async def start(self):
        """Start the CSV ingestor."""
        self.logger.log_pipeline_status("started", "csv_ingestor")
        self.is_running = True
    
    async def shutdown(self):
        """Shutdown the CSV ingestor."""
        self.logger.log_pipeline_status("shutdown", "csv_ingestor")
        self.is_running = False
    
    async def ingest_data(self, config: Dict[str, Any]):
        """
        Ingest data from CSV files based on configuration.
        
        Args:
            config: Configuration dictionary containing:
                - file_path: Path to CSV file or directory
                - schema: Expected column schema
                - batch_size: Number of records to process per batch
                - validate_data: Whether to perform data validation
                - incremental: Whether to use incremental loading
        """
        try:
            file_path = config.get("file_path")
            schema = config.get("schema", {})
            batch_size = config.get("batch_size", 1000)
            validate_data = config.get("validate_data", True)
            incremental = config.get("incremental", False)
            
            self.logger.log_ingestion_start("csv", None)
            start_time = time.time()
            
            # Process CSV file(s)
            if Path(file_path).is_dir():
                await self._process_directory(file_path, schema, batch_size, validate_data, incremental)
            else:
                await self._process_single_file(file_path, schema, batch_size, validate_data, incremental)
            
            processing_time = time.time() - start_time
            self.stats["processing_time"] = processing_time
            self.stats["last_processed_file"] = file_path
            
            self.logger.log_ingestion_complete("csv", self.stats["records_ingested"], processing_time)
            
        except Exception as e:
            self.stats["processing_errors"] += 1
            self.logger.log_error(e, {
                "component": "csv_ingestor",
                "config": config
            })
            raise
    
    async def _process_single_file(self, file_path: str, schema: Dict, batch_size: int, 
                                  validate_data: bool, incremental: bool):
        """Process a single CSV file."""
        try:
            self.current_file = file_path
            self.logger.logger.info(f"Processing CSV file: {file_path}")
            
            # Read CSV file
            df = pd.read_csv(file_path)
            total_records = len(df)
            
            # Validate schema if provided
            if schema and validate_data:
                df = self._validate_schema(df, schema)
            
            # Process in batches
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                await self._process_batch(batch, file_path)
                
                # Small delay to prevent overwhelming the system
                await asyncio.sleep(0.01)
            
            self.stats["files_processed"] += 1
            self.stats["records_ingested"] += total_records
            
            self.logger.logger.info(f"Successfully processed {total_records} records from {file_path}")
            
        except Exception as e:
            self.logger.log_error(e, {"file_path": file_path})
            raise
    
    async def _process_directory(self, directory_path: str, schema: Dict, batch_size: int,
                               validate_data: bool, incremental: bool):
        """Process all CSV files in a directory."""
        directory = Path(directory_path)
        csv_files = list(directory.glob("*.csv"))
        
        self.logger.logger.info(f"Found {len(csv_files)} CSV files in {directory_path}")
        
        for csv_file in csv_files:
            try:
                await self._process_single_file(
                    str(csv_file), schema, batch_size, validate_data, incremental
                )
            except Exception as e:
                self.logger.log_error(e, {"file_path": str(csv_file)})
                continue  # Continue with next file even if one fails
    
    async def _process_batch(self, batch_df: pd.DataFrame, source_file: str):
        """Process a batch of records and send to Kafka."""
        try:
            # Convert batch to records
            records = batch_df.to_dict('records')
            
            # Add metadata to each record
            for record in records:
                record['_metadata'] = {
                    'source_file': source_file,
                    'ingestion_timestamp': datetime.utcnow().isoformat(),
                    'ingestor_type': 'csv'
                }
            
            # Send to Kafka
            for record in records:
                self.kafka_producer.send(
                    topic='raw-data',
                    key=source_file.encode('utf-8'),
                    value=record
                )
            
            # Record metrics
            self.monitor.record_metric("csv_records_processed", len(records))
            
        except Exception as e:
            self.logger.log_error(e, {
                "component": "csv_batch_processing",
                "batch_size": len(batch_df),
                "source_file": source_file
            })
            raise
    
    def _validate_schema(self, df: pd.DataFrame, schema: Dict) -> pd.DataFrame:
        """
        Validate DataFrame against expected schema.
        
        Args:
            df: DataFrame to validate
            schema: Expected schema with column names and types
            
        Returns:
            Validated DataFrame
        """
        try:
            # Check required columns
            required_columns = [col for col, config in schema.items() 
                              if config.get('required', True)]
            
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Validate data types
            for column, config in schema.items():
                if column in df.columns:
                    expected_type = config.get('type')
                    if expected_type:
                        # Convert column to expected type
                        if expected_type == 'datetime':
                            df[column] = pd.to_datetime(df[column], errors='coerce')
                        elif expected_type == 'numeric':
                            df[column] = pd.to_numeric(df[column], errors='coerce')
                        elif expected_type == 'string':
                            df[column] = df[column].astype(str)
            
            # Handle missing values
            for column, config in schema.items():
                if column in df.columns:
                    default_value = config.get('default')
                    if default_value is not None:
                        df[column] = df[column].fillna(default_value)
            
            return df
            
        except Exception as e:
            self.logger.log_error(e, {"component": "schema_validation"})
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the CSV ingestor."""
        return {
            "is_running": self.is_running,
            "current_file": self.current_file,
            "stats": self.stats
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get detailed statistics."""
        return self.stats
