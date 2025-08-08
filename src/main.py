#!/usr/bin/env python3
"""
ETL Pipeline Main Entry Point

Orchestrates the entire data pipeline system including:
- Data ingestion from multiple sources
- Real-time processing with Kafka
- Batch processing with Airflow
- Data warehouse loading
- Monitoring and alerting
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Dict, Any

import structlog
from dotenv import load_dotenv

from src.ingestion.data_ingestion_manager import DataIngestionManager
from src.processing.etl_processor import ETLProcessor
from src.storage.data_warehouse_manager import DataWarehouseManager
from src.monitoring.pipeline_monitor import PipelineMonitor
from src.utils.config_manager import ConfigManager
from src.utils.logger_setup import setup_logging

# Load environment variables
load_dotenv()

class ETLPipelineOrchestrator:
    """Main orchestrator for the ETL pipeline system."""
    
    def __init__(self):
        """Initialize the ETL pipeline orchestrator."""
        self.config = ConfigManager()
        self.logger = structlog.get_logger(__name__)
        
        # Initialize components
        self.ingestion_manager = None
        self.etl_processor = None
        self.warehouse_manager = None
        self.monitor = None
        
        # Pipeline state
        self.is_running = False
        self.components = {}
        
    async def initialize(self):
        """Initialize all pipeline components."""
        try:
            self.logger.info("Initializing ETL Pipeline System")
            
            # Initialize monitoring first
            self.monitor = PipelineMonitor()
            await self.monitor.start()
            
            # Initialize data warehouse
            self.warehouse_manager = DataWarehouseManager()
            await self.warehouse_manager.initialize()
            
            # Initialize ETL processor
            self.etl_processor = ETLProcessor(
                warehouse_manager=self.warehouse_manager,
                monitor=self.monitor
            )
            await self.etl_processor.initialize()
            
            # Initialize data ingestion
            self.ingestion_manager = DataIngestionManager(
                etl_processor=self.etl_processor,
                monitor=self.monitor
            )
            await self.ingestion_manager.initialize()
            
            self.logger.info("ETL Pipeline System initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize ETL pipeline: {e}")
            raise
    
    async def start(self):
        """Start the ETL pipeline system."""
        try:
            self.logger.info("Starting ETL Pipeline System")
            self.is_running = True
            
            # Start all components
            await self.ingestion_manager.start()
            await self.etl_processor.start()
            await self.monitor.start_monitoring()
            
            self.logger.info("ETL Pipeline System started successfully")
            
            # Keep the pipeline running
            while self.is_running:
                await asyncio.sleep(1)
                
        except Exception as e:
            self.logger.error(f"Error in ETL pipeline: {e}")
            await self.shutdown()
    
    async def shutdown(self):
        """Gracefully shutdown the ETL pipeline system."""
        try:
            self.logger.info("Shutting down ETL Pipeline System")
            self.is_running = False
            
            # Shutdown components in reverse order
            if self.ingestion_manager:
                await self.ingestion_manager.shutdown()
            
            if self.etl_processor:
                await self.etl_processor.shutdown()
            
            if self.warehouse_manager:
                await self.warehouse_manager.shutdown()
            
            if self.monitor:
                await self.monitor.shutdown()
            
            self.logger.info("ETL Pipeline System shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get the current status of all pipeline components."""
        return {
            "is_running": self.is_running,
            "ingestion": self.ingestion_manager.get_status() if self.ingestion_manager else None,
            "processing": self.etl_processor.get_status() if self.etl_processor else None,
            "warehouse": self.warehouse_manager.get_status() if self.warehouse_manager else None,
            "monitoring": self.monitor.get_status() if self.monitor else None,
        }

async def main():
    """Main entry point for the ETL pipeline."""
    # Setup logging
    setup_logging()
    logger = structlog.get_logger(__name__)
    
    # Create orchestrator
    orchestrator = ETLPipelineOrchestrator()
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown")
        asyncio.create_task(orchestrator.shutdown())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize and start the pipeline
        await orchestrator.initialize()
        await orchestrator.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        sys.exit(1)
    finally:
        await orchestrator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
