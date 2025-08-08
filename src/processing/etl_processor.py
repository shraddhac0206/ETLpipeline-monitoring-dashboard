"""
ETL Processor

Main processor for Extract, Transform, Load operations.
Handles data transformation, validation, and loading into the data warehouse.
"""

import asyncio
import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime

import structlog
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd

from src.utils.logger_setup import PipelineLogger
from src.processing.data_transformer import DataTransformer
from src.processing.data_validator import DataValidator
from src.processing.data_enricher import DataEnricher

class ETLProcessor:
    """Main ETL processor for data transformation and loading."""
    
    def __init__(self, warehouse_manager, monitor):
        """Initialize the ETL processor."""
        self.warehouse_manager = warehouse_manager
        self.monitor = monitor
        self.logger = PipelineLogger(__name__)
        
        # Kafka components
        self.kafka_consumer = None
        self.kafka_producer = None
        
        # Processing components
        self.transformer = None
        self.validator = None
        self.enricher = None
        
        # Processing state
        self.is_running = False
        self.current_job = None
        
        # Statistics
        self.stats = {
            "records_processed": 0,
            "records_transformed": 0,
            "records_validated": 0,
            "records_enriched": 0,
            "processing_errors": 0,
            "processing_time": 0.0,
            "last_processed_batch": None
        }
    
    async def initialize(self):
        """Initialize the ETL processor."""
        try:
            self.logger.log_pipeline_status("initializing", "etl_processor")
            
            # Initialize processing components
            self.transformer = DataTransformer()
            self.validator = DataValidator()
            self.enricher = DataEnricher()
            
            # Initialize Kafka consumer for raw data
            self.kafka_consumer = KafkaConsumer(
                'raw-data',
                bootstrap_servers='localhost:9092',
                group_id='etl-processor-group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            # Initialize Kafka producer for processed data
            self.kafka_producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            self.logger.log_pipeline_status("initialized", "etl_processor")
            
        except Exception as e:
            self.logger.log_error(e, {"component": "etl_processor"})
            raise
    
    async def start(self):
        """Start the ETL processor."""
        try:
            self.logger.log_pipeline_status("starting", "etl_processor")
            self.is_running = True
            
            # Start processing loop
            asyncio.create_task(self._process_data_stream())
            
            self.logger.log_pipeline_status("started", "etl_processor")
            
        except Exception as e:
            self.logger.log_error(e, {"component": "etl_processor"})
            raise
    
    async def shutdown(self):
        """Shutdown the ETL processor."""
        try:
            self.logger.log_pipeline_status("shutting_down", "etl_processor")
            self.is_running = False
            
            if self.kafka_consumer:
                self.kafka_consumer.close()
            
            if self.kafka_producer:
                self.kafka_producer.close()
            
            self.logger.log_pipeline_status("shutdown", "etl_processor")
            
        except Exception as e:
            self.logger.log_error(e, {"component": "etl_processor"})
    
    async def _process_data_stream(self):
        """Process incoming data stream from Kafka."""
        try:
            for message in self.kafka_consumer:
                if not self.is_running:
                    break
                
                try:
                    await self._process_record(message.value)
                    self.stats["records_processed"] += 1
                    
                except Exception as e:
                    self.stats["processing_errors"] += 1
                    self.logger.log_error(e, {
                        "component": "record_processing",
                        "message": message.value
                    })
                    continue
                
        except Exception as e:
            self.logger.log_error(e, {"component": "data_stream_processing"})
            raise
    
    async def _process_record(self, record: Dict[str, Any]):
        """Process a single record through the ETL pipeline."""
        try:
            start_time = time.time()
            
            # Extract metadata
            metadata = record.get('_metadata', {})
            source_type = metadata.get('ingestor_type', 'unknown')
            
            # Step 1: Validate data
            validated_record = await self.validator.validate_record(record)
            self.stats["records_validated"] += 1
            
            # Step 2: Transform data
            transformed_record = await self.transformer.transform_record(validated_record)
            self.stats["records_transformed"] += 1
            
            # Step 3: Enrich data
            enriched_record = await self.enricher.enrich_record(transformed_record)
            self.stats["records_enriched"] += 1
            
            # Step 4: Send to processed data topic
            await self._send_to_processed_topic(enriched_record)
            
            # Step 5: Load to data warehouse
            await self.warehouse_manager.load_record(enriched_record)
            
            processing_time = time.time() - start_time
            self.stats["processing_time"] += processing_time
            
            # Record metrics
            self.monitor.record_metric("etl_processing_time", processing_time)
            self.monitor.record_metric("etl_records_processed", 1)
            
        except Exception as e:
            self.logger.log_error(e, {
                "component": "record_processing",
                "record": record
            })
            raise
    
    async def _send_to_processed_topic(self, record: Dict[str, Any]):
        """Send processed record to Kafka processed data topic."""
        try:
            self.kafka_producer.send(
                topic='processed-data',
                key=record.get('id', 'unknown').encode('utf-8'),
                value=record
            )
            
        except Exception as e:
            self.logger.log_error(e, {
                "component": "kafka_send",
                "topic": "processed-data"
            })
            raise
    
    async def process_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of records.
        
        Args:
            records: List of records to process
            
        Returns:
            List of processed records
        """
        try:
            self.logger.log_processing_start("batch", len(records))
            start_time = time.time()
            
            processed_records = []
            
            for record in records:
                try:
                    # Validate
                    validated_record = await self.validator.validate_record(record)
                    
                    # Transform
                    transformed_record = await self.transformer.transform_record(validated_record)
                    
                    # Enrich
                    enriched_record = await self.enricher.enrich_record(transformed_record)
                    
                    processed_records.append(enriched_record)
                    
                except Exception as e:
                    self.logger.log_error(e, {
                        "component": "batch_processing",
                        "record": record
                    })
                    continue
            
            processing_time = time.time() - start_time
            self.logger.log_processing_complete("batch", len(processed_records), processing_time)
            
            return processed_records
            
        except Exception as e:
            self.logger.log_error(e, {"component": "batch_processing"})
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the ETL processor."""
        return {
            "is_running": self.is_running,
            "current_job": self.current_job,
            "stats": self.stats,
            "kafka_consumer_connected": self.kafka_consumer is not None,
            "kafka_producer_connected": self.kafka_producer is not None
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get detailed processing statistics."""
        return self.stats
