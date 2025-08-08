#!/usr/bin/env python3
"""
ETL Pipeline Setup Script

This script sets up the complete ETL pipeline system including:
- Installing dependencies
- Creating necessary directories
- Setting up configuration files
- Initializing the database
"""

import os
import sys
import subprocess
from pathlib import Path
import shutil

def print_header(title):
    """Print a formatted header."""
    print("\n" + "=" * 60)
    print(f"🚀 {title}")
    print("=" * 60)

def print_step(step):
    """Print a step message."""
    print(f"\n📋 {step}")

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def create_directory(path):
    """Create a directory if it doesn't exist."""
    Path(path).mkdir(parents=True, exist_ok=True)
    print(f"✅ Created directory: {path}")

def main():
    """Main setup function."""
    print_header("ETL Pipeline System Setup")
    
    # Check Python version
    print_step("Checking Python version")
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        sys.exit(1)
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    
    # Create necessary directories
    print_step("Creating project directories")
    directories = [
        "data/input",
        "data/output", 
        "data/processed",
        "logs",
        "config",
        "tests/unit",
        "tests/integration",
        "docs",
        "scripts"
    ]
    
    for directory in directories:
        create_directory(directory)
    
    # Install dependencies
    print_step("Installing Python dependencies")
    if not run_command(f"{sys.executable} -m pip install --upgrade pip", "Upgrading pip"):
        sys.exit(1)
    
    if not run_command(f"{sys.executable} -m pip install -r requirements.txt", "Installing requirements"):
        print("⚠️  Some dependencies may not be installed. Continuing...")
    
    # Create configuration files
    print_step("Creating configuration files")
    
    # Create .env file
    env_content = """# ETL Pipeline Configuration

# Database Configuration
DATABASE_URL=postgresql://etl_user:etl_password@localhost:5432/etl_db
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=etl_user
DB_PASSWORD=etl_password

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RAW_DATA=raw-data
KAFKA_TOPIC_PROCESSED_DATA=processed-data
KAFKA_GROUP_ID=etl-pipeline-group

# Redis Configuration
REDIS_URL=redis://localhost:6379

# Monitoring Configuration
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
STREAMLIT_PORT=8501
LOG_LEVEL=INFO

# Cloud Storage (Optional)
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=us-east-1

# Environment
ENVIRONMENT=development
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    print("✅ Created .env configuration file")
    
    # Create sample configuration
    config_content = """# ETL Pipeline Configuration

# Data Sources Configuration
data_sources:
  csv:
    enabled: true
    input_directory: "data/input"
    batch_size: 1000
    validate_schema: true
  
  api:
    enabled: true
    endpoints:
      - name: "customer_api"
        url: "https://api.example.com/customers"
        auth_type: "bearer"
  
  web_scraper:
    enabled: true
    sources:
      - name: "news_site"
        url: "https://example.com/news"
        selectors:
          title: "h1"
          content: ".article-content"

# Processing Configuration
processing:
  batch_size: 1000
  max_workers: 4
  retry_attempts: 3
  timeout_seconds: 30
  
# Data Quality Configuration
data_quality:
  enabled: true
  rules:
    - name: "completeness_check"
      field: "customer_id"
      rule: "not_null"
    - name: "email_format"
      field: "email"
      rule: "email_format"

# Monitoring Configuration
monitoring:
  metrics_collection: true
  alerting: true
  dashboard_refresh_rate: 30
"""
    
    with open("config/pipeline_config.yaml", "w") as f:
        f.write(config_content)
    print("✅ Created pipeline configuration file")
    
    # Create database initialization script
    db_init_content = """-- Database initialization script for ETL pipeline

-- Create tables for data warehouse
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(10),
    zip_code VARCHAR(10),
    registration_date DATE,
    last_purchase_date DATE,
    total_purchases INTEGER DEFAULT 0,
    avg_order_value DECIMAL(10,2),
    customer_segment VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id SERIAL PRIMARY KEY,
    component VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    message TEXT,
    severity VARCHAR(20) DEFAULT 'INFO',
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(10,4),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(customer_segment);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_timestamp ON pipeline_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_component ON pipeline_logs(component);

-- Insert sample data
INSERT INTO customers (customer_id, customer_name, email, phone, address, city, state, zip_code, registration_date, last_purchase_date, total_purchases, avg_order_value, customer_segment)
VALUES 
    ('CUST001', 'John Smith', 'john.smith@email.com', '555-0101', '123 Main St', 'New York', 'NY', '10001', '2023-01-15', '2024-01-20', 25, 150.50, 'Premium'),
    ('CUST002', 'Sarah Johnson', 'sarah.j@email.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90210', '2023-02-10', '2024-01-18', 18, 120.75, 'Standard')
ON CONFLICT (customer_id) DO NOTHING;
"""
    
    with open("config/init.sql", "w") as f:
        f.write(db_init_content)
    print("✅ Created database initialization script")
    
    # Create sample data files
    print_step("Creating sample data files")
    
    # Copy sample CSV if it exists
    sample_csv = Path("data/sample_data.csv")
    if sample_csv.exists():
        shutil.copy(sample_csv, "data/input/sample_data.csv")
        print("✅ Copied sample data to input directory")
    
    # Create a simple test script
    test_script_content = """#!/usr/bin/env python3
\"\"\"
Simple test script for the ETL pipeline
\"\"\"

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    \"\"\"Test that all modules can be imported.\"\"\"
    try:
        from src.utils.config_manager import ConfigManager
        from src.utils.logger_setup import setup_logging
        print("✅ All modules imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_config():
    \"\"\"Test configuration loading.\"\"\"
    try:
        config = ConfigManager()
        print("✅ Configuration loaded successfully")
        return True
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Running ETL Pipeline Tests...")
    
    success = True
    success &= test_imports()
    success &= test_config()
    
    if success:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")
        sys.exit(1)
"""
    
    with open("test_pipeline.py", "w") as f:
        f.write(test_script_content)
    print("✅ Created test script")
    
    # Make scripts executable
    os.chmod("run_dashboard.py", 0o755)
    os.chmod("test_pipeline.py", 0o755)
    
    print_header("Setup Complete!")
    
    print("🎉 ETL Pipeline System has been set up successfully!")
    print("\n📋 Next Steps:")
    print("1. Configure your environment variables in .env file")
    print("2. Start the monitoring dashboard: python run_dashboard.py")
    print("3. Run tests: python test_pipeline.py")
    print("4. Start the full pipeline: docker-compose up")
    print("\n📚 Documentation:")
    print("- README.md - Complete system documentation")
    print("- config/ - Configuration files")
    print("- src/ - Source code")
    
    print("\n🔗 Quick Access:")
    print("- Dashboard: http://localhost:8501")
    print("- Airflow: http://localhost:8080")
    print("- Grafana: http://localhost:3000")
    print("- Prometheus: http://localhost:9090")

if __name__ == "__main__":
    main()
