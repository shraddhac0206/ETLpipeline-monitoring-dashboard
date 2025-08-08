"""
Data Ingestion Manager

Orchestrates data ingestion from multiple sources and manages
the flow of data into the ETL pipeline.
"""

import asyncio
import time
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod

import structlog
from kafka import KafkaProducer, KafkaConsumer
import json

from src.utils.config_manager import ConfigManager
from src.utils.logger_setup import PipelineLogger
from src.ingestion.csv_ingestor import CSVIngestor
from src.ingestion.api_ingestor import APIIngestor
from src.ingestion.web_scraper_ingestor import WebScraperIngestor
from src.ingestion.streaming_ingestor import StreamingIngestor

class DataIngestionManager:
    """Main manager for data ingestion operations."""
    
    def __init__(self, etl_processor, monitor):
        """Initialize the data ingestion manager."""
        self.config = ConfigManager()
        self.logger = PipelineLogger(__name__)
        self.etl_processor = etl_processor
        self.monitor = monitor
        
        # Kafka producer for sending data to processing pipeline
        self.kafka_producer = None
        self.kafka_consumer = None
        
        # Ingestion components
        self.ingestors = {}
        self.is_running = False
        
        # Statistics
        self.stats = {
            "total_records_ingested": 0,
            "ingestion_errors": 0,
            "last_ingestion_time": None,
            "active_sources": []
        }
    
    async def initialize(self):
        """Initialize the data ingestion manager."""
        try:
            self.logger.log_pipeline_status("initializing", "data_ingestion_manager")
            
            # Initialize Kafka producer
            kafka_config = self.config.get_kafka_config()
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            # Initialize ingestion components
            await self._initialize_ingestors()
            
            self.logger.log_pipeline_status("initialized", "data_ingestion_manager")
            
        except Exception as e:
            self.logger.log_error(e, {"component": "data_ingestion_manager"})
            raise
    
    async def _initialize_ingestors(self):
        """Initialize all data ingestion components."""
        # CSV Ingestor
        self.ingestors['csv'] = CSVIngestor(
            kafka_producer=self.kafka_producer,
            monitor=self.monitor
        )
        
        # API Ingestor
        self.ingestors['api'] = APIIngestor(
            kafka_producer=self.kafka_producer,
            monitor=self.monitor
        )
        
        # Web Scraper Ingestor
        self.ingestors['web_scraper'] = WebScraperIngestor(
            kafka_producer=self.kafka_producer,
            monitor=self.monitor
        )
        
        # Streaming Ingestor
        self.ingestors['streaming'] = StreamingIngestor(
            kafka_producer=self.kafka_producer,
            monitor=self.monitor
        )
        
        # Initialize all ingestors
        for name, ingestor in self.ingestors.items():
            await ingestor.initialize()
            self.logger.logger.info(f"Initialized {name} ingestor")
    
    async def start(self):
        """Start the data ingestion manager."""
        try:
            self.logger.log_pipeline_status("starting", "data_ingestion_manager")
            self.is_running = True
            
            # Start all ingestors
            for name, ingestor in self.ingestors.items():
                await ingestor.start()
                self.stats["active_sources"].append(name)
            
            # Start monitoring ingestion
            asyncio.create_task(self._monitor_ingestion())
            
            self.logger.log_pipeline_status("started", "data_ingestion_manager")
            
        except Exception as e:
            self.logger.log_error(e, {"component": "data_ingestion_manager"})
            raise
    
    async def shutdown(self):
        """Shutdown the data ingestion manager."""
        try:
            self.logger.log_pipeline_status("shutting_down", "data_ingestion_manager")
            self.is_running = False
            
            # Shutdown all ingestors
            for name, ingestor in self.ingestors.items():
                await ingestor.shutdown()
            
            # Close Kafka producer
            if self.kafka_producer:
                self.kafka_producer.close()
            
            self.logger.log_pipeline_status("shutdown", "data_ingestion_manager")
            
        except Exception as e:
            self.logger.log_error(e, {"component": "data_ingestion_manager"})
    
    async def _monitor_ingestion(self):
        """Monitor ingestion performance and health."""
        while self.is_running:
            try:
                # Update statistics
                total_records = sum(
                    ingestor.get_stats().get("records_ingested", 0)
                    for ingestor in self.ingestors.values()
                )
                
                self.stats["total_records_ingested"] = total_records
                self.stats["last_ingestion_time"] = time.time()
                
                # Send metrics to monitoring system
                self.monitor.record_metric("ingestion_total_records", total_records)
                self.monitor.record_metric("ingestion_active_sources", len(self.stats["active_sources"]))
                
                await asyncio.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                self.logger.log_error(e, {"component": "ingestion_monitoring"})
                await asyncio.sleep(60)  # Wait longer on error
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the ingestion manager."""
        ingestor_statuses = {}
        for name, ingestor in self.ingestors.items():
            ingestor_statuses[name] = ingestor.get_status()
        
        return {
            "is_running": self.is_running,
            "stats": self.stats,
            "ingestors": ingestor_statuses,
            "kafka_producer_connected": self.kafka_producer is not None
        }
    
    async def ingest_from_source(self, source_type: str, source_config: Dict[str, Any]):
        """Manually trigger ingestion from a specific source."""
        try:
            if source_type not in self.ingestors:
                raise ValueError(f"Unknown source type: {source_type}")
            
            ingestor = self.ingestors[source_type]
            await ingestor.ingest_data(source_config)
            
            self.logger.log_ingestion_start(source_type)
            
        except Exception as e:
            self.logger.log_error(e, {
                "component": "data_ingestion_manager",
                "source_type": source_type,
                "source_config": source_config
            })
            raise
    
    def get_ingestion_stats(self) -> Dict[str, Any]:
        """Get detailed ingestion statistics."""
        stats = {
            "total_records": self.stats["total_records_ingested"],
            "errors": self.stats["ingestion_errors"],
            "active_sources": self.stats["active_sources"],
            "last_ingestion": self.stats["last_ingestion_time"]
        }
        
        # Add per-ingestor stats
        for name, ingestor in self.ingestors.items():
            stats[f"{name}_stats"] = ingestor.get_stats()
        
        return stats
