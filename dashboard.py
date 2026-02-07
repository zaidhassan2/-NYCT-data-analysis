"""
Streamlit Dashboard for NYC Congestion Pricing Analysis
4 tabs: Map, Flow, Economics, Weather
"""

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

# custom CSS for professional styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 600;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 0.5rem;
        letter-spacing: -0.5px;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #5a6c7d;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: 300;
    }
    .section-title {
        font-size: 1.8rem;
        font-weight: 600;
        color: #2c3e50;
        margin-top: 1rem;
        margin-bottom: 1rem;
    }
    .hypothesis-text {
        color: #34495e;
        font-size: 1rem;
        line-height: 1.6;
        margin-bottom: 1.5rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        border-bottom: 2px solid #e0e0e0;
    }
    .stTabs [data-baseweb="tab"] {
        height: 45px;
        padding: 8px 24px;
        background-color: #f8f9fa;
        border-radius: 4px 4px 0px 0px;
        font-weight: 500;
        color: #495057;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ffffff;
        color: #2c3e50;
        border-bottom: 3px solid #2c3e50;
    }
    .sidebar-header {
        color: #2c3e50;
        font-weight: 600;
        font-size: 1.2rem;
    }
    .info-box {
        background-color: #f8f9fa;
        border-left: 4px solid #2c3e50;
        padding: 1rem;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# header
st.markdown('<h1 class="main-header">NYC Congestion Pricing Audit Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Comprehensive Analysis of Manhattan Congestion Relief Zone Impact (2025)</p>', unsafe_allow_html=True)

# check data availability
from pathlib import Path
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

# sidebar
with st.sidebar:
    st.markdown('<p class="sidebar-header">About This Dashboard</p>', unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("""
    <div class="info-box">
    This dashboard provides comprehensive analysis of the NYC Congestion Pricing implementation:<br><br>
    • <strong>The Map</strong>: Border effect (2024 vs 2025 comparison)<br>
    • <strong>The Flow</strong>: Traffic velocity (Q1 2024 vs Q1 2025)<br>
    • <strong>The Economics</strong>: Tip and surcharge trends (2025)<br>
    • <strong>The Weather</strong>: Rain elasticity (2025)
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.caption("Data Source: NYC TLC Trip Record Data")

# main tabs
tab1, tab2, tab3, tab4 = st.tabs(["The Map", "The Flow", "The Economics", "The Weather"])

# Tab 1: The Map
with tab1:
    st.markdown('<h2 class="section-title">Border Effect Analysis</h2>', unsafe_allow_html=True)
    st.markdown("""
    <div class="hypothesis-text">
    <strong>Hypothesis:</strong> Are passengers ending trips just outside the congestion zone to avoid the toll?<br><br>
    This map shows the percentage change in drop-offs (2024 vs 2025) for taxi zones immediately 
    bordering the 60th Street cutoff.<br><br>
    <em>Note: Comparison uses available data. For full year comparison, ensure both years have complete data.</em>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        try:
            # check if 2024 data exists, if not show 2025 only
            from pathlib import Path
            proc_dir = Path(__file__).parent / "data" / "processed"
            has_2024 = (proc_dir / "processed_2024.parquet").exists()
            has_2025 = (proc_dir / "processed_2025.parquet").exists()
            
            if not has_2025:
                st.warning("Processed data not found. Please run the pipeline first to generate processed data.")
            elif not has_2024:
                st.info("2024 data not available. Showing 2025 data only. To compare with 2024, process 2024 data first.")
                # show 2025 data visualization instead
                try:
                    border_data_2025 = calc_border_effect(2025, 2025)
                    if border_data_2025 is not None:
                        st.success("2025 border zone data loaded successfully.")
                        st.dataframe(border_data_2025[['zone_id', 'dropoff_count']].head(10), use_container_width=True, hide_index=True)
                except:
                    pass
            else:
                border_map = create_border_effect_map(2024, 2025)
                if border_map:
                    # save map to HTML and display
                    map_html = border_map._repr_html_()
                    st.components.v1.html(map_html, height=600, scrolling=True)
                else:
                    st.warning("Map data not available. Please run the pipeline first to generate processed data.")
        except Exception as e:
            st.error(f"Error creating map: {e}")
            import traceback
            st.code(traceback.format_exc())
    
    with col2:
        st.subheader("Key Insights")
        
        try:
            from pathlib import Path
            proc_dir = Path(__file__).parent / "data" / "processed"
            has_2024 = (proc_dir / "processed_2024.parquet").exists()
            has_2025 = (proc_dir / "processed_2025.parquet").exists()
            
            if has_2024 and has_2025:
                border_data = calc_border_effect(2024, 2025)
                if border_data is not None and len(border_data) > 0:
                    avg_change = border_data['pct_change'].mean()
                    max_increase = border_data['pct_change'].max()
                    max_decrease = border_data['pct_change'].min()
                    
                    st.metric("Average Change", f"{avg_change:.1f}%")
                    st.metric("Max Increase", f"{max_increase:.1f}%")
                    st.metric("Max Decrease", f"{max_decrease:.1f}%")
                    
                    st.markdown("---")
                    st.subheader("Top Zones by Change")
                    top_zones = border_data.nlargest(5, 'pct_change')[['zone_id', 'pct_change']]
                    st.dataframe(top_zones, use_container_width=True, hide_index=True)
                else:
                    st.info("Border effect data not available")
            elif has_2025:
                # show 2025 only stats
                try:
                    from data_utils import PROC_DIR
                    import polars as pl
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

# Tab 2: The Flow
with tab2:
    st.markdown('<h2 class="section-title">Congestion Velocity Analysis</h2>', unsafe_allow_html=True)
    st.markdown("""
    <div class="hypothesis-text">
    <strong>Hypothesis:</strong> Did the toll actually speed up traffic?<br><br>
    Compare average trip speeds inside the congestion zone before (Q1 2024) and after (Q1 2025) 
    implementation. Heatmaps show speed patterns by hour of day and day of week.
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    try:
        # check if data exists
        from pathlib import Path
        proc_dir = Path(__file__).parent / "data" / "processed"
        has_2024 = (proc_dir / "processed_2024.parquet").exists()
        has_2025 = (proc_dir / "processed_2025.parquet").exists()
        
        if not has_2025:
            st.warning("Processed data not found. Please run the pipeline first to generate processed data.")
        elif not has_2024:
            st.info("2024 data not available. Showing 2025 Q1 velocity heatmap only.")
            # show 2025 only
            from analysis import create_velocity_heatmap
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            hm_2025 = create_velocity_heatmap(2025, 1)
            if hm_2025 is not None:
                fig, ax = plt.subplots(figsize=(14, 6))
                dow_labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                sns.heatmap(hm_2025, ax=ax, cmap='RdYlGn', cbar_kws={'label': 'Avg Speed (mph)'},
                            xticklabels=range(24), yticklabels=dow_labels, vmin=0, vmax=30)
                ax.set_title('Q1 2025 - Average Trip Speed in Congestion Zone', fontsize=14, fontweight='bold')
                ax.set_xlabel('Hour of Day', fontsize=12)
                ax.set_ylabel('Day of Week', fontsize=12)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close(fig)
            else:
                st.warning("Could not generate velocity heatmap for 2025.")
        else:
            # create comparison heatmaps
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
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Q1 2024 Baseline")
        st.info("""
        **Before Implementation:**
        - Baseline traffic patterns
        - Peak hour congestion visible
        - Weekend vs weekday patterns
        """)
    
    with col2:
        st.subheader("Q1 2025 After Toll")
        st.info("""
        **After Implementation:**
        - Compare speed improvements
        - Identify time periods with most impact
        - Assess overall effectiveness
        """)
    
    st.markdown("---")
    st.caption("**Note:** Average speeds may appear similar between periods. This is realistic - congestion pricing often reduces trip volume without dramatically changing average speeds, especially when infrastructure bottlenecks remain constant.")

# Tab 3: The Economics
with tab3:
    st.markdown('<h2 class="section-title">Tip Crowding Out Analysis</h2>', unsafe_allow_html=True)
    st.markdown("""
    <div class="hypothesis-text">
    <strong>Hypothesis:</strong> Higher tolls reduce the disposable income passengers leave for drivers.<br><br>
    This analysis examines the relationship between monthly average surcharge amounts and 
    average tip percentages throughout 2025.
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    try:
        # check if data exists
        from pathlib import Path
        proc_dir = Path(__file__).parent / "data" / "processed"
        has_2025 = (proc_dir / "processed_2025.parquet").exists()
        
        if not has_2025:
            st.warning("Processed data not found. Please run the pipeline first to generate processed data.")
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
    
    st.markdown("---")
    
    # metrics row
    try:
        from analysis import analyze_tip_crowding
        tip_data = analyze_tip_crowding(2025)
        if tip_data is not None:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                avg_surcharge = tip_data['avg_surcharge'].mean()
                st.metric("Avg Monthly Surcharge", f"${avg_surcharge:.2f}")
            
            with col2:
                avg_tip_pct = tip_data['avg_tip_pct'].mean()
                st.metric("Avg Tip Percentage", f"{avg_tip_pct:.2f}%")
            
            with col3:
                max_surcharge = tip_data['avg_surcharge'].max()
                st.metric("Peak Surcharge", f"${max_surcharge:.2f}")
            
            with col4:
                min_tip_pct = tip_data['avg_tip_pct'].min()
                st.metric("Lowest Tip %", f"{min_tip_pct:.2f}%")
            
            st.markdown("---")
            st.subheader("Monthly Breakdown")
            st.dataframe(tip_data[['month_name', 'avg_surcharge', 'avg_tip_pct', 'trip_count']], 
                        use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Could not load tip data: {e}")

# Tab 4: The Weather
with tab4:
    st.markdown('<h2 class="section-title">Rain Elasticity of Demand</h2>', unsafe_allow_html=True)
    st.markdown("""
    <div class="hypothesis-text">
    <strong>Analysis:</strong> How does precipitation affect taxi demand?<br><br>
    This analysis calculates the rain elasticity of demand by correlating daily precipitation 
    with daily trip counts. The scatter plot shows the relationship for the wettest month of 2025.
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    try:
        # check if data exists
        from pathlib import Path
        proc_dir = Path(__file__).parent / "data" / "processed"
        has_2025 = (proc_dir / "processed_2025.parquet").exists()
        
        if not has_2025:
            st.warning("Processed data not found. Please run the pipeline first to generate processed data.")
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
    
    st.markdown("---")
    
    # elasticity metrics
    try:
        from data_utils import calculate_rain_elasticity
        elasticity = calculate_rain_elasticity(2025)
        if elasticity:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Correlation Coefficient", f"{elasticity['correlation']:.4f}")
            
            with col2:
                st.metric("Regression Slope", f"{elasticity['slope']:.4f}")
            
            with col3:
                elasticity_type = elasticity['elasticity_type']
                color = "🟢" if elasticity_type == "inelastic" else "🔴"
                st.metric("Elasticity Type", f"{color} {elasticity_type.title()}")
            
            st.markdown("---")
            st.subheader("Interpretation")
            if elasticity['correlation'] > 0.3:
                st.success("**Elastic Demand:** Taxi trips are significantly affected by rain. Higher precipitation leads to increased demand.")
            elif elasticity['correlation'] < -0.3:
                st.info("**Negative Correlation:** Rain may reduce demand (unusual but possible).")
            else:
                st.warning("**Inelastic Demand:** Taxi trips are relatively unaffected by precipitation.")
    except Exception as e:
        st.warning(f"Could not load elasticity data: {e}")

# footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #5a6c7d; padding: 20px; font-size: 0.9rem;'>
    <p style='margin: 0.3rem 0;'><strong style='color: #2c3e50;'>NYC Congestion Pricing Audit Dashboard</strong></p>
    <p style='margin: 0.3rem 0;'>Data Science for Software Engineering - Assignment 01</p>
    <p style='margin: 0.3rem 0;'>Data Source: NYC TLC Trip Record Data | Weather: Open-Meteo API</p>
</div>
""", unsafe_allow_html=True)

