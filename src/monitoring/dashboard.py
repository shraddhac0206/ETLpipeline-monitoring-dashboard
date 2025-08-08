"""
ETL Pipeline Monitoring Dashboard

Real-time monitoring dashboard for the ETL pipeline system
using Streamlit for visualization and monitoring.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import time
from datetime import datetime, timedelta
import json
from typing import Dict, Any, List
import numpy as np

# Mock data for demonstration - in real implementation, this would come from the monitoring system
class MockMonitoringData:
    """Mock monitoring data for demonstration purposes."""
    
    @staticmethod
    def get_pipeline_status():
        return {
            "is_running": True,
            "ingestion": {
                "is_running": True,
                "stats": {
                    "total_records_ingested": 15420,
                    "ingestion_errors": 3,
                    "active_sources": ["csv", "api", "web_scraper"]
                }
            },
            "processing": {
                "is_running": True,
                "stats": {
                    "records_processed": 15420,
                    "records_transformed": 15420,
                    "records_validated": 15420,
                    "processing_errors": 1
                }
            },
            "warehouse": {
                "is_running": True,
                "stats": {
                    "tables_loaded": 5,
                    "records_loaded": 15420,
                    "load_errors": 0
                }
            }
        }
    
    @staticmethod
    def get_metrics_history():
        """Get historical metrics data."""
        now = datetime.now()
        hours = []
        records_processed = []
        processing_time = []
        errors = []
        
        for i in range(24):
            hour = now - timedelta(hours=i)
            hours.append(hour)
            records_processed.append(1500 + (i * 50) + (i % 3) * 100)
            processing_time.append(0.5 + (i % 5) * 0.1)
            errors.append(i % 3)
        
        return {
            "timestamps": hours[::-1],
            "records_processed": records_processed[::-1],
            "processing_time": processing_time[::-1],
            "errors": errors[::-1]
        }
    
    @staticmethod
    def get_data_quality_metrics():
        return {
            "completeness": 98.5,
            "accuracy": 97.2,
            "consistency": 99.1,
            "timeliness": 95.8
        }
    
    @staticmethod
    def get_recent_errors():
        return [
            {
                "timestamp": datetime.now() - timedelta(minutes=5),
                "error_type": "Data Validation Error",
                "message": "Missing required field 'customer_id'",
                "severity": "WARNING"
            },
            {
                "timestamp": datetime.now() - timedelta(minutes=15),
                "error_type": "Processing Error",
                "message": "Failed to transform record due to invalid date format",
                "severity": "ERROR"
            },
            {
                "timestamp": datetime.now() - timedelta(minutes=30),
                "error_type": "Connection Error",
                "message": "Temporary connection issue with Kafka",
                "severity": "INFO"
            }
        ]

def create_gradient_gauge(value, title, color_scheme="viridis"):
    """Create a beautiful gradient gauge chart."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 20, 'color': '#2c3e50'}},
        delta={'reference': 90, 'increasing': {'color': "#3498db"}},
        gauge={
            'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "#2c3e50"},
            'bar': {'color': "#3498db"},
            'bgcolor': "rgba(52, 152, 219, 0.1)",
            'borderwidth': 2,
            'bordercolor': "#3498db",
            'steps': [
                {'range': [0, 50], 'color': '#e74c3c'},
                {'range': [50, 80], 'color': '#f39c12'},
                {'range': [80, 100], 'color': '#27ae60'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 90
            }
        }
    ))
    
    fig.update_layout(
        height=300,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': '#2c3e50'},
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

def create_animated_line_chart(df, x_col, y_col, title, color="#3498db"):
    """Create an animated line chart with gradient fill."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df[x_col],
        y=df[y_col],
        mode='lines+markers',
        line=dict(color=color, width=3),
        marker=dict(size=6, color=color),
        fill='tonexty',
        fillcolor=f'rgba(52, 152, 219, 0.1)',
        name=y_col
    ))
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color='#2c3e50')),
        xaxis=dict(
            title="Time",
            gridcolor='rgba(44, 62, 80, 0.1)',
            zerolinecolor='rgba(44, 62, 80, 0.1)',
            tickfont=dict(color='#2c3e50')
        ),
        yaxis=dict(
            title=y_col,
            gridcolor='rgba(44, 62, 80, 0.1)',
            zerolinecolor='rgba(44, 62, 80, 0.1)',
            tickfont=dict(color='#2c3e50')
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=400,
        margin=dict(l=40, r=40, t=60, b=40)
    )
    
    return fig

def create_radar_chart(quality_metrics):
    """Create a radar chart for data quality metrics."""
    categories = list(quality_metrics.keys())
    values = list(quality_metrics.values())
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        fillcolor='rgba(52, 152, 219, 0.3)',
        line_color='#3498db',
        line_width=3,
        name='Data Quality'
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                tickfont=dict(color='#2c3e50'),
                gridcolor='rgba(44, 62, 80, 0.1)'
            ),
            angularaxis=dict(
                tickfont=dict(color='#2c3e50')
            )
        ),
        showlegend=False,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=400,
        title=dict(text="Data Quality Overview", font=dict(size=20, color='#2c3e50'))
    )
    
    return fig

def main():
    """Main dashboard function."""
    st.set_page_config(
        page_title="🚀 ETL Pipeline Monitor",
        page_icon="🚀",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for stunning light theme with soft gradients and animations
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap');
    
    .main {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 50%, #dee2e6 100%);
        color: #2c3e50;
    }
    
    .stApp {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 50%, #dee2e6 100%);
    }
    
    .main-header {
        font-family: 'Poppins', sans-serif;
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(45deg, #3498db, #9b59b6, #e74c3c);
        background-size: 300% 300%;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        text-align: center;
        margin-bottom: 2rem;
        animation: gradient-shift 3s ease-in-out infinite;
    }
    
    @keyframes gradient-shift {
        0%, 100% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
    }
    
    .metric-card {
        background: linear-gradient(135deg, rgba(52, 152, 219, 0.1) 0%, rgba(155, 89, 182, 0.1) 100%);
        border: 2px solid rgba(52, 152, 219, 0.3);
        border-radius: 20px;
        padding: 1.5rem;
        margin: 1rem 0;
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
        box-shadow: 0 8px 32px rgba(52, 152, 219, 0.15);
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 40px rgba(52, 152, 219, 0.25);
        border-color: rgba(52, 152, 219, 0.6);
    }
    
    .status-indicator {
        display: inline-block;
        width: 15px;
        height: 15px;
        border-radius: 50%;
        margin-right: 10px;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(52, 152, 219, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(52, 152, 219, 0); }
        100% { box-shadow: 0 0 0 0 rgba(52, 152, 219, 0); }
    }
    
    .status-running { 
        background: linear-gradient(45deg, #27ae60, #2ecc71);
        box-shadow: 0 0 20px rgba(39, 174, 96, 0.5);
    }
    
    .status-error { 
        background: linear-gradient(45deg, #e74c3c, #c0392b);
        box-shadow: 0 0 20px rgba(231, 76, 60, 0.5);
    }
    
    .status-warning { 
        background: linear-gradient(45deg, #f39c12, #e67e22);
        box-shadow: 0 0 20px rgba(243, 156, 18, 0.5);
    }
    
    .section-header {
        font-family: 'Poppins', sans-serif;
        font-size: 1.8rem;
        font-weight: 600;
        color: #2c3e50;
        text-align: center;
        margin: 2rem 0 1rem 0;
        text-shadow: 0 2px 4px rgba(44, 62, 80, 0.1);
    }
    
    .error-card {
        background: linear-gradient(135deg, rgba(231, 76, 60, 0.1) 0%, rgba(192, 57, 43, 0.1) 100%);
        border: 2px solid rgba(231, 76, 60, 0.3);
        border-radius: 15px;
        padding: 1rem;
        margin: 0.5rem 0;
        backdrop-filter: blur(10px);
    }
    
    .warning-card {
        background: linear-gradient(135deg, rgba(243, 156, 18, 0.1) 0%, rgba(230, 126, 34, 0.1) 100%);
        border: 2px solid rgba(243, 156, 18, 0.3);
        border-radius: 15px;
        padding: 1rem;
        margin: 0.5rem 0;
        backdrop-filter: blur(10px);
    }
    
    .info-card {
        background: linear-gradient(135deg, rgba(52, 152, 219, 0.1) 0%, rgba(41, 128, 185, 0.1) 100%);
        border: 2px solid rgba(52, 152, 219, 0.3);
        border-radius: 15px;
        padding: 1rem;
        margin: 0.5rem 0;
        backdrop-filter: blur(10px);
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #ffffff 0%, #f1f3f4 100%);
        border-right: 2px solid rgba(52, 152, 219, 0.3);
        box-shadow: 2px 0 10px rgba(0, 0, 0, 0.1);
    }
    
    .stButton > button {
        background: linear-gradient(45deg, #3498db, #9b59b6);
        border: none;
        border-radius: 25px;
        color: #ffffff;
        font-weight: 600;
        padding: 0.5rem 1.5rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(52, 152, 219, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(52, 152, 219, 0.4);
    }
    
    .stSelectbox > div > div {
        background: rgba(52, 152, 219, 0.1);
        border: 2px solid rgba(52, 152, 219, 0.3);
        border-radius: 10px;
        color: #2c3e50;
    }
    
    .stCheckbox > div > div {
        background: rgba(52, 152, 219, 0.1);
        border: 2px solid rgba(52, 152, 219, 0.3);
        border-radius: 5px;
    }
    
    .stMetric > div {
        background: linear-gradient(135deg, rgba(52, 152, 219, 0.1) 0%, rgba(155, 89, 182, 0.1) 100%);
        border: 2px solid rgba(52, 152, 219, 0.3);
        border-radius: 15px;
        padding: 1rem;
        backdrop-filter: blur(10px);
    }
    
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #3498db, #9b59b6);
    }
    
    .footer {
        text-align: center;
        color: #7f8c8d;
        margin-top: 3rem;
        padding: 2rem;
        border-top: 2px solid rgba(52, 152, 219, 0.2);
        background: linear-gradient(135deg, rgba(52, 152, 219, 0.05) 0%, rgba(155, 89, 182, 0.05) 100%);
        border-radius: 20px;
    }
    
    .quality-score-card {
        margin: 1rem 0;
        padding: 1rem;
        background: linear-gradient(135deg, rgba(52, 152, 219, 0.1) 0%, rgba(155, 89, 182, 0.1) 100%);
        border-radius: 15px;
        border: 2px solid rgba(52, 152, 219, 0.3);
        transition: all 0.3s ease;
    }
    
    .quality-score-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(52, 152, 219, 0.2);
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header with animated gradient
    st.markdown('<h1 class="main-header">🚀 ETL Pipeline Monitoring Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar with enhanced styling
    with st.sidebar:
        st.markdown('<h2 style="color: #2c3e50; font-family: \'Poppins\', sans-serif; font-weight: 600;">🎛️ Control Panel</h2>', unsafe_allow_html=True)
        
        # Refresh rate with custom styling
        st.markdown('<h3 style="color: #2c3e50; font-size: 1.2rem; font-weight: 500;">⚡ Refresh Settings</h3>', unsafe_allow_html=True)
        refresh_rate = st.selectbox(
            "🔄 Refresh Rate",
            ["5 seconds", "10 seconds", "30 seconds", "1 minute"],
            index=1
        )
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("🔄 Auto-refresh", value=True)
        
        st.markdown('<h3 style="color: #2c3e50; font-size: 1.2rem; font-weight: 500;">🎮 Pipeline Controls</h3>', unsafe_allow_html=True)
        
        # Pipeline control buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🟢 Start Pipeline", type="primary"):
                st.success("🚀 Pipeline started successfully!")
        
        with col2:
            if st.button("🔴 Stop Pipeline"):
                st.error("⏹️ Pipeline stopped!")
        
        st.markdown('<h3 style="color: #2c3e50; font-size: 1.2rem; font-weight: 500;">📊 Quick Stats</h3>', unsafe_allow_html=True)
        
        # Quick stats in sidebar
        mock_data = MockMonitoringData()
        status = mock_data.get_pipeline_status()
        
        st.metric("📈 Total Records", f"{status['ingestion']['stats']['total_records_ingested']:,}")
        st.metric("⚡ Processing Rate", "1,500 records/min")
        st.metric("⚠️ Error Rate", f"{status['processing']['stats']['processing_errors']} errors")
        
        # Add a beautiful gauge for overall health
        st.markdown('<h3 style="color: #2c3e50; font-size: 1.2rem; font-weight: 500;">🏥 System Health</h3>', unsafe_allow_html=True)
        health_score = 95.5
        fig_gauge = create_gradient_gauge(health_score, "System Health Score")
        st.plotly_chart(fig_gauge, use_container_width=True)
    
    # Main content area with enhanced cards
    st.markdown('<h2 class="section-header">📊 Pipeline Status Overview</h2>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3 style="color: #2c3e50; font-size: 1.3rem; font-weight: 600;">📥 Data Ingestion</h3>
            <p><span class="status-indicator status-running"></span><strong style="color: #27ae60;">Active</strong></p>
            <p style="color: #3498db; font-weight: 500;">Sources: 3</p>
            <p style="color: #2c3e50;">Records: 15,420</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3 style="color: #2c3e50; font-size: 1.3rem; font-weight: 600;">⚙️ Data Processing</h3>
            <p><span class="status-indicator status-running"></span><strong style="color: #27ae60;">Active</strong></p>
            <p style="color: #3498db; font-weight: 500;">Transformed: 15,420</p>
            <p style="color: #2c3e50;">Validated: 15,420</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <h3 style="color: #2c3e50; font-size: 1.3rem; font-weight: 600;">💾 Data Warehouse</h3>
            <p><span class="status-indicator status-running"></span><strong style="color: #27ae60;">Active</strong></p>
            <p style="color: #3498db; font-weight: 500;">Tables: 5</p>
            <p style="color: #2c3e50;">Loaded: 15,420</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="metric-card">
            <h3 style="color: #2c3e50; font-size: 1.3rem; font-weight: 600;">📊 Data Quality</h3>
            <p><span class="status-indicator status-running"></span><strong style="color: #27ae60;">Excellent</strong></p>
            <p style="color: #3498db; font-weight: 500;">Score: 98.5%</p>
            <p style="color: #2c3e50;">Errors: 1</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Enhanced charts section
    st.markdown('<h2 class="section-header">📈 Real-time Performance Metrics</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Records processed over time with enhanced styling
        mock_data = MockMonitoringData()
        metrics = mock_data.get_metrics_history()
        
        df = pd.DataFrame({
            'Time': metrics['timestamps'],
            'Records Processed': metrics['records_processed']
        })
        
        fig = create_animated_line_chart(df, 'Time', 'Records Processed', '📈 Records Processed Over Time')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Processing time over time
        df_time = pd.DataFrame({
            'Time': metrics['timestamps'],
            'Processing Time (s)': metrics['processing_time']
        })
        
        fig = create_animated_line_chart(df_time, 'Time', 'Processing Time (s)', '⚡ Processing Time Over Time', "#9b59b6")
        st.plotly_chart(fig, use_container_width=True)
    
    # Data Quality Metrics with radar chart
    st.markdown('<h2 class="section-header">🔍 Data Quality Analysis</h2>', unsafe_allow_html=True)
    
    quality_metrics = mock_data.get_data_quality_metrics()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Radar chart for data quality
        fig_radar = create_radar_chart(quality_metrics)
        st.plotly_chart(fig_radar, use_container_width=True)
    
    with col2:
        # Individual quality metrics with enhanced styling
        st.markdown('<h3 style="color: #2c3e50; text-align: center; font-weight: 600;">Quality Scores</h3>', unsafe_allow_html=True)
        
        for metric, value in quality_metrics.items():
            st.markdown(f"""
            <div class="quality-score-card">
                <h4 style="color: #2c3e50; margin: 0; font-weight: 600;">{metric.title()}</h4>
                <p style="color: #3498db; font-size: 1.5rem; font-weight: bold; margin: 0;">{value}%</p>
            </div>
            """, unsafe_allow_html=True)
    
    # Recent Errors with enhanced styling
    st.markdown('<h2 class="section-header">⚠️ Recent Errors & Alerts</h2>', unsafe_allow_html=True)
    
    errors = mock_data.get_recent_errors()
    
    for error in errors:
        severity_color = {
            "ERROR": "🔴",
            "WARNING": "🟡", 
            "INFO": "🔵"
        }.get(error['severity'], "⚪")
        
        card_class = {
            "ERROR": "error-card",
            "WARNING": "warning-card",
            "INFO": "info-card"
        }.get(error['severity'], "info-card")
        
        st.markdown(f"""
        <div class="{card_class}">
            <h4 style="color: #2c3e50; margin: 0 0 0.5rem 0; font-weight: 600;">
                {severity_color} {error['error_type']}
            </h4>
            <p style="color: #7f8c8d; margin: 0 0 0.5rem 0;">
                <strong>Time:</strong> {error['timestamp'].strftime('%H:%M:%S')}
            </p>
            <p style="color: #2c3e50; margin: 0;">
                {error['message']}
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    # Pipeline Configuration with enhanced styling
    st.markdown('<h2 class="section-header">⚙️ Pipeline Configuration</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3 style="color: #2c3e50; margin-bottom: 1rem; font-weight: 600;">📡 Data Sources</h3>
            <p style="color: #3498db; font-weight: 500;">📁 CSV Files: <code>/data/input/</code></p>
            <p style="color: #3498db; font-weight: 500;">🌐 API Endpoints: 3 active</p>
            <p style="color: #3498db; font-weight: 500;">🕷️ Web Scraping: 2 sources</p>
            <p style="color: #3498db; font-weight: 500;">📱 IoT Devices: 5 connected</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3 style="color: #2c3e50; margin-bottom: 1rem; font-weight: 600;">⚙️ Processing Settings</h3>
            <p style="color: #3498db; font-weight: 500;">📦 Batch Size: 1,000 records</p>
            <p style="color: #3498db; font-weight: 500;">👥 Processing Workers: 4</p>
            <p style="color: #3498db; font-weight: 500;">🔄 Retry Attempts: 3</p>
            <p style="color: #3498db; font-weight: 500;">⏱️ Timeout: 30 seconds</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Footer with enhanced styling
    st.markdown("""
    <div class="footer">
        <h3 style="color: #2c3e50; font-family: 'Poppins', sans-serif; font-weight: 600;">🚀 ETL Pipeline Monitoring Dashboard</h3>
        <p style="color: #7f8c8d;">Built with ❤️ using Streamlit & Plotly</p>
        <p style="color: #7f8c8d;">Last updated: {}</p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)
    
    # Auto-refresh functionality
    if auto_refresh:
        time.sleep(int(refresh_rate.split()[0]) * (1 if "second" in refresh_rate else 60))
        st.rerun()

if __name__ == "__main__":
    main()
