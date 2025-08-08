#!/usr/bin/env python3
"""
Quick Start Script for ETL Pipeline Dashboard

This script provides an easy way to run the monitoring dashboard
without setting up the full pipeline infrastructure.
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Run the ETL pipeline monitoring dashboard."""
    
    print("🚀 Starting ETL Pipeline Monitoring Dashboard...")
    print("=" * 50)
    
    # Check if streamlit is installed
    try:
        import streamlit
        print("✅ Streamlit is installed")
    except ImportError:
        print("❌ Streamlit not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit", "plotly"])
        print("✅ Streamlit installed successfully")
    
    # Check if dashboard file exists
    dashboard_path = Path("src/monitoring/dashboard.py")
    if not dashboard_path.exists():
        print(f"❌ Dashboard file not found at {dashboard_path}")
        print("Please ensure the dashboard file exists.")
        return
    
    print("📊 Launching monitoring dashboard...")
    print("🌐 Dashboard will be available at: http://localhost:8501")
    print("⏹️  Press Ctrl+C to stop the dashboard")
    print("=" * 50)
    
    try:
        # Run the dashboard
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(dashboard_path),
            "--server.port", "8501",
            "--server.address", "0.0.0.0"
        ])
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Error running dashboard: {e}")

if __name__ == "__main__":
    main()
