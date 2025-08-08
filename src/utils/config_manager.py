"""
Configuration Manager for ETL Pipeline

Handles all configuration settings, environment variables,
and provides centralized access to pipeline configuration.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

import structlog

@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    url: str
    host: str
    port: int
    database: str
    username: str
    password: str
    pool_size: int = 10
    max_overflow: int = 20

@dataclass
class KafkaConfig:
    """Kafka configuration settings."""
    bootstrap_servers: str
    topic_raw_data: str
    topic_processed_data: str
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True

@dataclass
class CloudConfig:
    """Cloud storage configuration settings."""
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "us-east-1"
    azure_connection_string: Optional[str] = None
    gcp_credentials_path: Optional[str] = None

@dataclass
class MonitoringConfig:
    """Monitoring configuration settings."""
    prometheus_port: int = 9090
    grafana_port: int = 3000
    streamlit_port: int = 8501
    log_level: str = "INFO"

class ConfigManager:
    """Centralized configuration manager for the ETL pipeline."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the configuration manager."""
        self.logger = structlog.get_logger(__name__)
        self.config_path = config_path or "config/pipeline_config.yaml"
        
        # Load configurations
        self.database_config = self._load_database_config()
        self.kafka_config = self._load_kafka_config()
        self.cloud_config = self._load_cloud_config()
        self.monitoring_config = self._load_monitoring_config()
        
        self.logger.info("Configuration manager initialized")
    
    def _load_database_config(self) -> DatabaseConfig:
        """Load database configuration from environment variables."""
        return DatabaseConfig(
            url=os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/etl_db"),
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "etl_db"),
            username=os.getenv("DB_USER", "user"),
            password=os.getenv("DB_PASSWORD", "password"),
            pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20"))
        )
    
    def _load_kafka_config(self) -> KafkaConfig:
        """Load Kafka configuration from environment variables."""
        return KafkaConfig(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_raw_data=os.getenv("KAFKA_TOPIC_RAW_DATA", "raw-data"),
            topic_processed_data=os.getenv("KAFKA_TOPIC_PROCESSED_DATA", "processed-data"),
            group_id=os.getenv("KAFKA_GROUP_ID", "etl-pipeline-group"),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            enable_auto_commit=os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true"
        )
    
    def _load_cloud_config(self) -> CloudConfig:
        """Load cloud configuration from environment variables."""
        return CloudConfig(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            azure_connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            gcp_credentials_path=os.getenv("GCP_CREDENTIALS_PATH")
        )
    
    def _load_monitoring_config(self) -> MonitoringConfig:
        """Load monitoring configuration from environment variables."""
        return MonitoringConfig(
            prometheus_port=int(os.getenv("PROMETHEUS_PORT", "9090")),
            grafana_port=int(os.getenv("GRAFANA_PORT", "3000")),
            streamlit_port=int(os.getenv("STREAMLIT_PORT", "8501")),
            log_level=os.getenv("LOG_LEVEL", "INFO")
        )
    
    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration."""
        return self.database_config
    
    def get_kafka_config(self) -> KafkaConfig:
        """Get Kafka configuration."""
        return self.kafka_config
    
    def get_cloud_config(self) -> CloudConfig:
        """Get cloud configuration."""
        return self.cloud_config
    
    def get_monitoring_config(self) -> MonitoringConfig:
        """Get monitoring configuration."""
        return self.monitoring_config
    
    def get_all_config(self) -> Dict[str, Any]:
        """Get all configuration as a dictionary."""
        return {
            "database": self.database_config,
            "kafka": self.kafka_config,
            "cloud": self.cloud_config,
            "monitoring": self.monitoring_config
        }
    
    def validate_config(self) -> bool:
        """Validate all configuration settings."""
        try:
            # Validate database config
            if not self.database_config.url:
                raise ValueError("Database URL is required")
            
            # Validate Kafka config
            if not self.kafka_config.bootstrap_servers:
                raise ValueError("Kafka bootstrap servers are required")
            
            # Validate cloud config (at least one provider should be configured)
            cloud_providers = [
                self.cloud_config.aws_access_key_id,
                self.cloud_config.azure_connection_string,
                self.cloud_config.gcp_credentials_path
            ]
            
            if not any(cloud_providers):
                self.logger.warning("No cloud storage provider configured")
            
            self.logger.info("Configuration validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    
    def get_environment_info(self) -> Dict[str, str]:
        """Get environment information for debugging."""
        return {
            "python_version": os.getenv("PYTHON_VERSION", "Unknown"),
            "environment": os.getenv("ENVIRONMENT", "development"),
            "config_path": self.config_path,
            "log_level": self.monitoring_config.log_level
        }
