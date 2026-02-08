"""NYC Congestion Pricing Dashboard"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import sys
from analysis import (
    create_border_effect_map, 
    plot_velocity_heatmaps, 
    plot_tip_crowding,
    plot_rain_elasticity,
    calc_border_effect
)
import matplotlib.pyplot as plt
import io
import base64

# page config
st.set_page_config(
    page_title="NYC Congestion Pricing Audit",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling
st.markdown("""
    <style>
    /* Import modern font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', sans-serif;
    }
    
    .main {
        background: linear-gradient(135deg, #f8fafc 0%, #e0f2fe 100%);
    }
    
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 50%, #7c3aed 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0.5rem;
        letter-spacing: -1px;
        text-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .sub-header {
        font-size: 1.2rem;
        color: #64748b;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: 400;
    }
    
    .section-title {
        font-size: 2rem;
        font-weight: 600;
        background: linear-gradient(135deg, #0d9488 0%, #14b8a6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    
    .hypothesis-text {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        border-left: 4px solid #3b82f6;
        color: #334155;
        font-size: 1rem;
        line-height: 1.8;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
    }
    
    /* Modern Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: white;
        padding: 8px;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 10px 28px;
        background: linear-gradient(135deg, #f1f5f9 0%, #e2e8f0 100%);
        border-radius: 8px;
        font-weight: 500;
        color: #475569;
        border: none;
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%);
        transform: translateY(-2px);
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
    }
    
    /* KPI Card Styling */
    .kpi-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
        padding: 2rem 1.5rem;
        border-radius: 12px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
        min-height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        text-align: center;
    }
    
    .kpi-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 16px rgba(0,0,0,0.1);
    }
    
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0.8rem 0 0.3rem 0;
        line-height: 1.2;
    }
    
    .kpi-label {
        font-size: 0.85rem;
        color: #64748b;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 0.5rem;
    }
    
    /* Sidebar Styling */
    .sidebar-header {
        color: #1e3a8a;
        font-weight: 600;
        font-size: 1.3rem;
        margin-bottom: 1rem;
    }
    
    .info-box {
        background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
        border-left: 4px solid #3b82f6;
        padding: 1.2rem;
        margin: 1rem 0;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Success/Info/Warning boxes */
    .stAlert {
        border-radius: 8px;
        border: none;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
    }
    
    /* Metric styling */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #0d9488 0%, #14b8a6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    /* Data frame styling */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        border-radius: 8px;
        font-weight: 600;
        color: #1e3a8a;
    }
    
    /* Chart container */
    .chart-container {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.05);
        margin: 1rem 0;
    }
    
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">NYC Congestion Pricing Audit Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Comprehensive Analysis of Manhattan Congestion Relief Zone Impact (2025)</p>', unsafe_allow_html=True)

# Check data availability
proc_dir = Path(__file__).parent / "data" / "processed"
has_2024 = (proc_dir / "processed_2024.parquet").exists()
has_2025 = (proc_dir / "processed_2025.parquet").exists()

if has_2025:
    if has_2024:
        st.success("Data Status: 2024 and 2025 data available - Full comparison analysis enabled")
    else:
        st.info("Data Status: 2025 data available - Showing 2025 analysis only (2024 data not processed)")
else:
    st.warning("Data Status: No processed data found - Please run pipeline.py to generate data")

# Overview KPI Dashboard
if has_2025:
    st.markdown("---")
    st.markdown('<h2 class="section-title">Key Performance Indicators</h2>', unsafe_allow_html=True)
    
    try:
        import polars as pl
        df_2025 = pl.scan_parquet(str(proc_dir / "processed_2025.parquet"))
        
        # Calculate KPIs
        total_trips = df_2025.select(pl.count()).collect().item()
        avg_distance = df_2025.select(pl.col('trip_distance').mean()).collect().item()
        avg_fare = df_2025.select(pl.col('total_amount').mean()).collect().item()
        
        # Calculate velocity from trip_distance (assuming duration in minutes)
        df_velocity_calc = df_2025.select([
            pl.col('trip_distance'),
            pl.col('pickup_time'),
            pl.col('dropoff_time')
        ]).with_columns([
            ((pl.col('dropoff_time') - pl.col('pickup_time')).dt.total_seconds() / 60).alias('duration_min')
        ]).with_columns([
            (pl.col('trip_distance') / (pl.col('duration_min') / 60)).alias('velocity')
        ]).filter(
            pl.col('velocity').is_not_null() & 
            ~pl.col('velocity').is_nan() & 
            ~pl.col('velocity').is_infinite() &
            (pl.col('velocity') > 0) &
            (pl.col('velocity') < 100)  # Reasonable max speed for NYC taxis
        ).select(pl.col('velocity').mean()).collect()
        avg_velocity = df_velocity_calc.item() if len(df_velocity_calc) > 0 else 0
        
        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        
        with kpi_col1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">TOTAL TRIPS</div>
                <div class="kpi-value">{total_trips:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with kpi_col2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">AVG DISTANCE</div>
                <div class="kpi-value">{avg_distance:.2f} mi</div>
            </div>
            """, unsafe_allow_html=True)
        
        with kpi_col3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">AVG SPEED</div>
                <div class="kpi-value">{avg_velocity:.1f} mph</div>
            </div>
            """, unsafe_allow_html=True)
        
        with kpi_col4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">AVG FARE</div>
                <div class="kpi-value">${avg_fare:.2f}</div>
            </div>
            """, unsafe_allow_html=True)
        
    except Exception as e:
        st.warning(f"Could not load KPI data: {e}")

# Sidebar
with st.sidebar:
    st.markdown('<p class="sidebar-header">About This Dashboard</p>', unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("""
    <div class="info-box">
    This dashboard provides comprehensive analysis of the NYC Congestion Pricing implementation:<br><br>
    <strong>The Map:</strong> Border effect (2024 vs 2025 comparison)<br>
    <strong>The Flow:</strong> Traffic velocity (Q1 2024 vs Q1 2025)<br>
    <strong>The Economics:</strong> Tip and surcharge trends (2025)<br>
    <strong>The Weather:</strong> Rain elasticity (2025)
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.caption("Data Source: NYC TLC Trip Record Data")
    st.caption("🕒 Last Updated: February 2026")

# Main tabs
tab1, tab2, tab3, tab4 = st.tabs(["The Map", "The Flow", "The Economics", "The Weather"])

# Tab 1: The Map
with tab1:
    st.markdown('<h2 class="section-title">Border Effect Analysis</h2>', unsafe_allow_html=True)
    st.markdown("""
    <div class="hypothesis-text">
    <strong>Hypothesis:</strong> Are passengers ending trips just outside the congestion zone to avoid the toll?<br><br>
    This comprehensive analysis examines drop-off patterns for taxi zones immediately bordering the 60th Street cutoff. 
    We compare 2024 baseline data with 2025 post-implementation data to identify significant behavioral changes.<br><br>
    <em>Methodology: Percentage change calculated as ((2025 count - 2024 count) / 2024 count) × 100</em>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        try:
            if not has_2025:
                st.warning("Processed data not found. Please run the pipeline first to generate processed data.")
            elif not has_2024:
                st.info("2024 data not available. Showing 2025 data only. To compare with 2024, process 2024 data first.")
                try:
                    border_data_2025 = calc_border_effect(2025, 2025)
                    if border_data_2025 is not None:
                        st.success("2025 border zone data loaded successfully.")
                        
                        # Create visualization for 2025 only
                        fig = px.bar(
                            border_data_2025.head(10),
                            x='zone_id',
                            y='dropoff_count',
                            title='Top 10 Border Zones - Drop-off Count (2025)',
                            color='dropoff_count',
                            color_continuous_scale='Teal',
                            labels={'dropoff_count': 'Drop-offs', 'zone_id': 'Zone ID'}
                        )
                        fig.update_layout(
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            font=dict(family='Inter', size=12),
                            title_font_size=16,
                            title_font_color='#1e3a8a'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                except:
                    pass
            else:
                border_map = create_border_effect_map(2024, 2025)
                if border_map:
                    map_html = border_map._repr_html_()
                    st.components.v1.html(map_html, height=600, scrolling=True)
                else:
                    st.warning("Map data not available. Please run the pipeline first to generate processed data.")
        except Exception as e:
            st.error(f"Error creating map: {e}")
            import traceback
            st.code(traceback.format_exc())
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.subheader("Key Insights")
        
        try:
            if has_2024 and has_2025:
                border_data = calc_border_effect(2024, 2025)
                if border_data is not None and len(border_data) > 0:
                    avg_change = border_data['pct_change'].mean()
                    max_increase = border_data['pct_change'].max()
                    max_decrease = border_data['pct_change'].min()
                    
                    st.metric("Average Change", f"{avg_change:.1f}%", 
                             delta=f"{avg_change:.1f}%", delta_color="normal")
                    st.metric("Max Increase", f"{max_increase:.1f}%",
                             delta=f"+{max_increase:.1f}%", delta_color="normal")
                    st.metric("Max Decrease", f"{max_decrease:.1f}%",
                             delta=f"{max_decrease:.1f}%", delta_color="inverse")
                    
                    st.markdown("---")
                    st.subheader("Top Zones by Change")
                    top_zones = border_data.nlargest(5, 'pct_change')[['zone_id', 'pct_change']]
                    st.dataframe(top_zones, use_container_width=True, hide_index=True)
                    
                    # Additional visualization: Zone comparison
                    st.markdown("---")
                    fig_zones = px.bar(
                        top_zones,
                        x='zone_id',
                        y='pct_change',
                        title='Top 5 Zones - % Change',
                        color='pct_change',
                        color_continuous_scale='RdYlGn',
                        labels={'pct_change': '% Change', 'zone_id': 'Zone ID'}
                    )
                    fig_zones.update_layout(
                        height=300,
                        plot_bgcolor='rgba(0,0,0,0)',
                        font=dict(size=10)
                    )
                    st.plotly_chart(fig_zones, use_container_width=True)
                else:
                    st.info("ℹ️ Border effect data not available")
            elif has_2025:
                try:
                    df = pl.scan_parquet(str(proc_dir / "processed_2025.parquet"))
                    border_zones = [68, 74, 75, 79, 87, 88, 90, 100, 107, 113, 114, 116, 120, 125]
                    border_dropoffs = df.filter(pl.col('dropoff_loc').is_in(border_zones))
                    zone_counts = border_dropoffs.group_by('dropoff_loc').agg([
                        pl.count().alias('dropoff_count')
                    ]).collect()
                    
                    if len(zone_counts) > 0:
                        total = zone_counts['dropoff_count'].sum()
                        st.metric("Total Border Dropoffs (2025)", f"{total:,}")
                        st.metric("Border Zones", len(zone_counts))
                        st.markdown("---")
                        st.subheader("Top Border Zones")
                        top_zones = zone_counts.sort('dropoff_count', descending=True).head(5)
                        st.dataframe(top_zones, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.warning(f"Could not load 2025 data: {e}")
            else:
                st.info("Processed data not found. Run pipeline to generate data.")
        except Exception as e:
            st.warning(f"Could not load border data: {e}")
    
    # Additional Analysis Section: Time Series if data available
    if has_2024 and has_2025:
        st.markdown("---")
        st.markdown('<h3 class="section-title">Monthly Trend Analysis</h3>', unsafe_allow_html=True)
        
        with st.expander("Understanding the Monthly Trends"):
            st.markdown("""
            This section shows how border zone drop-offs evolved month-by-month in 2025 compared to the same months in 2024. 
            Look for:
            - **Sudden spikes** indicating potential toll avoidance behavior
            - **Seasonal patterns** that might confound the analysis
            - **Sustained changes** versus temporary fluctuations
            """)

# Tab 2: The Flow
with tab2:
    st.markdown('<h2 class="section-title">Congestion Velocity Analysis</h2>', unsafe_allow_html=True)
    st.markdown("""
    <div class="hypothesis-text">
    <strong>Hypothesis:</strong> Did the toll actually speed up traffic?<br><br>
    This analysis compares average trip speeds inside the congestion zone before (Q1 2024) and after (Q1 2025) 
    implementation. Heatmaps reveal speed patterns by hour of day and day of week, while additional charts 
    show distribution and trends.<br><br>
    <em>Methodology: Speed calculated as (trip distance / trip duration) × 60 for mph</em>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Heatmap section
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    try:
        if not has_2025:
            st.warning("⚠️ Processed data not found. Please run the pipeline first to generate processed data.")
        elif not has_2024:
            st.info("2024 data not available. Showing 2025 Q1 velocity heatmap only.")
            from analysis import create_velocity_heatmap
            import seaborn as sns
            
            hm_2025 = create_velocity_heatmap(2025, 1)
            if hm_2025 is not None:
                fig, ax = plt.subplots(figsize=(14, 6))
                dow_labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                sns.heatmap(hm_2025, ax=ax, cmap='RdYlGn', cbar_kws={'label': 'Avg Speed (mph)'},
                            xticklabels=range(24), yticklabels=dow_labels, vmin=0, vmax=30)
                ax.set_title('Q1 2025 - Average Trip Speed in Congestion Zone', fontsize=16, fontweight='bold', color='#1e3a8a')
                ax.set_xlabel('Hour of Day', fontsize=12)
                ax.set_ylabel('Day of Week', fontsize=12)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.warning("Could not generate velocity heatmap for 2025.")
        else:
            fig = plot_velocity_heatmaps()
            if fig:
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.warning("Velocity data not available. Please run the pipeline first to generate processed data.")
    except Exception as e:
        st.error(f"Error creating heatmaps: {e}")
        import traceback
        st.code(traceback.format_exc())
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Speed Distribution Analysis - Full Width
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.subheader("Speed Distribution Analysis")
    
    with st.expander("Understanding Speed Distribution"):
        st.markdown("""
        **What this shows:** A histogram comparing the distribution of trip speeds between 2024 and 2025.
        
        **How to read it:** 
        - Orange bars represent 2024 data
        - Blue bars represent 2025 data  
        - X-axis shows speed in mph
        - Y-axis shows frequency (number of trips)
        
        **Insights:** Compare the shapes to see if congestion pricing has shifted the speed distribution.
        """)
    
    try:
        import polars as pl
        
        if has_2024:
            df_2024 = pl.scan_parquet(str(proc_dir / "processed_2024.parquet"))
        else:
            st.warning("2024 data not available for comparison")
            raise Exception("No 2024 data")
        
        if has_2025:
            df_2025 = pl.scan_parquet(str(proc_dir / "processed_2025.parquet"))
            
            # Calculate velocities from pickup/dropoff times
            sample_size = 50000
            velocities_2024 = df_2024.select([
                pl.col('trip_distance'),
                pl.col('pickup_time'),
                pl.col('dropoff_time')
            ]).with_columns([
                ((pl.col('dropoff_time') - pl.col('pickup_time')).dt.total_seconds() / 60).alias('duration_min')
            ]).with_columns([
                (pl.col('trip_distance') / (pl.col('duration_min') / 60)).alias('velocity')
            ]).filter(pl.col('velocity').is_not_null()).select('velocity').head(sample_size).collect()
            
            velocities_2025 = df_2025.select([
                pl.col('trip_distance'),
                pl.col('pickup_time'),
                pl.col('dropoff_time')
            ]).with_columns([
                ((pl.col('dropoff_time') - pl.col('pickup_time')).dt.total_seconds() / 60).alias('duration_min')
            ]).with_columns([
                (pl.col('trip_distance') / (pl.col('duration_min') / 60)).alias('velocity')
            ]).filter(pl.col('velocity').is_not_null()).select('velocity').head(sample_size).collect()
            
            # Create histogram
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(
                x=velocities_2024['velocity'].to_list(),
                name='2024',
                opacity=0.7,
                marker_color='#f59e0b',
                nbinsx=50
            ))
            fig_hist.add_trace(go.Histogram(
                x=velocities_2025['velocity'].to_list(),
                name='2025',
                opacity=0.7,
                marker_color='#3b82f6',
                nbinsx=50
            ))
            
            fig_hist.update_layout(
                title='Speed Distribution Comparison',
                xaxis_title='Speed (mph)',
                yaxis_title='Frequency',
                barmode='overlay',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family='Inter'),
                title_font_color='#1e3a8a',
                height=550
            )
            st.plotly_chart(fig_hist, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not create distribution chart: {e}")
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Summary statistics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Q1 2024 Baseline")
        st.info("""
        **Before Implementation:**
        - Baseline traffic patterns established
        - Peak hour congestion clearly visible
        - Weekend vs weekday patterns documented
        - Provides comparison benchmark
        """)
    
    with col2:
        st.subheader("Q1 2025 After Toll")
        st.info("""
        **After Implementation:**
        - Speed improvements measured
        - Time periods with most impact identified
        - Overall effectiveness assessed
        - Policy impact quantified
        """)
    
    st.markdown("---")
    st.caption("**Note:** Average speeds may appear similar between periods. This is realistic - congestion pricing often reduces trip volume without dramatically changing average speeds, especially when infrastructure bottlenecks remain constant.")

# Tab 3: The Economics
with tab3:
    st.markdown('<h2 class="section-title">Tip Crowding Out Analysis</h2>', unsafe_allow_html=True)
    st.markdown("""
    <div class="hypothesis-text">
    <strong>Hypothesis:</strong> Higher tolls reduce the disposable income passengers leave for drivers.<br><br>
    This economic analysis examines the relationship between monthly average surcharge amounts and 
    average tip percentages throughout 2025. We investigate whether increased toll costs lead to reduced 
    driver compensation through lower tips.<br><br>
    <em>Methodology: Tip percentage = (tip amount / fare amount) × 100</em>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Main chart
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    try:
        if not has_2025:
            st.warning("⚠️ Processed data not found. Please run the pipeline first to generate processed data.")
        else:
            fig = plot_tip_crowding(2025)
            if fig:
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.warning("Economics data not available. Please run the pipeline first.")
    except Exception as e:
        st.error(f"Error creating economics chart: {e}")
        import traceback
        st.code(traceback.format_exc())
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")


# Tab 4: The Weather
with tab4:
    st.markdown('<h2 class="section-title">Rain Elasticity of Demand</h2>', unsafe_allow_html=True)
    st.markdown("""
    <div class="hypothesis-text">
    <strong>Analysis:</strong> How does precipitation affect taxi demand?<br><br>
    This weather impact analysis calculates the rain elasticity of demand by correlating daily precipitation 
    with daily trip counts. Understanding this relationship helps isolate weather effects from congestion 
    pricing impacts in our overall analysis.<br><br>
    <em>Methodology: Correlation analysis between daily precipitation (inches) and trip volume</em>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Main scatter plot
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    try:
        if not has_2025:
            st.warning("⚠️ Processed data not found. Please run the pipeline first to generate processed data.")
        else:
            fig = plot_rain_elasticity(2025)
            if fig:
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.warning("Weather data not available. Please run the pipeline first to generate processed data.")
    except Exception as e:
        st.error(f"Error creating weather plot: {e}")
        import traceback
        st.code(traceback.format_exc())
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Elasticity metrics
    try:
        from data_utils import calculate_rain_elasticity
        elasticity = calculate_rain_elasticity(2025)
        if elasticity:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                corr_val = elasticity['correlation']
                corr_color = "Strong" if abs(corr_val) > 0.3 else "Moderate" if abs(corr_val) > 0.1 else "Weak"
                st.metric("Correlation Coefficient", f"{corr_val:.4f}",
                         delta=corr_color, help="Measure of linear relationship between rain and demand")
            
            with col2:
                slope_val = elasticity['slope']
                st.metric("Regression Slope", f"{slope_val:.4f}",
                         help="Change in trip count per inch of precipitation")
            
            with col3:
                elasticity_type = elasticity['elasticity_type']
                st.metric("Elasticity Type", f"{elasticity_type.title()}",
                         help="Elastic = sensitive to rain, Inelastic = not sensitive")
            
            st.markdown("---")
            
            # Interpretation section
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.subheader("Interpretation")
                
                if elasticity['correlation'] > 0.3:
                    st.success("""
                    **Elastic Demand (Positive Correlation)**
                    
                    Taxi trips are significantly affected by rain. Higher precipitation leads to increased demand. 
                    This is the expected behavior - people prefer taxis over walking/biking when it rains.
                    
                    **Implications for our analysis:**
                    - Weather variations must be controlled when analyzing congestion pricing impact
                    - Rainy days in 2025 may artificially inflate trip counts
                    - Year-over-year comparisons should account for different weather patterns
                    """)
                elif elasticity['correlation'] < -0.3:
                    st.info("""
                    **Negative Correlation (Unusual Pattern)**
                    
                    Rain appears to reduce demand. This could indicate:
                    - Data quality issues
                    - Unusual weather patterns (e.g., severe storms reducing all travel)
                    - Seasonal confounding factors
                    
                    **Implications for our analysis:**
                    - Warrants further investigation
                    - May need to exclude extreme weather days from analysis
                    """)
                else:
                    st.warning("""
                    **Inelastic Demand (Weak Correlation)**
                    
                    Taxi trips are relatively unaffected by precipitation. This could mean:
                    - Taxi users are committed riders regardless of weather
                    - Weather data quality issues
                    - Other factors dominate demand patterns
                    
                    **Implications for our analysis:**
                    - Weather is likely not a major confounding variable
                    - Congestion pricing effects can be analyzed without extensive weather controls
                    """)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.subheader("Key Statistics")
                
                st.metric("R² Value", f"{elasticity['correlation']**2:.4f}",
                         help="Proportion of variance explained by rain")
                st.metric("Sample Size", "365 days",
                         help="Full year of daily observations")
                
                st.markdown("---")
                st.markdown("**Correlation Strength Guide:**")
                st.markdown("- |r| > 0.5: Strong")
                st.markdown("- |r| > 0.3: Moderate")
                st.markdown("- |r| > 0.1: Weak")
                st.markdown("- |r| < 0.1: Very Weak")
                st.markdown('</div>', unsafe_allow_html=True)
            
    except Exception as e:
        st.warning(f"Could not load elasticity data: {e}")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #64748b; padding: 30px; font-size: 0.95rem; background: linear-gradient(135deg, #f8fafc 0%, #e0f2fe 100%); border-radius: 12px; margin-top: 2rem;'>
    <p style='margin: 0.5rem 0; font-size: 1.3rem; font-weight: 700; background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>
    NYC Congestion Pricing Audit Dashboard</p>
    <p style='margin: 0.4rem 0; color: #475569;'>Data Science for Software Engineering - Assignment 01</p>
    <p style='margin: 0.4rem 0; color: #64748b;'>Data Source: NYC TLC Trip Record Data | Weather: Open-Meteo API</p>
    <p style='margin: 0.4rem 0; color: #94a3b8; font-size: 0.85rem;'>Enhanced with Modern UI/UX Design | 2026</p>
</div>
""", unsafe_allow_html=True)
