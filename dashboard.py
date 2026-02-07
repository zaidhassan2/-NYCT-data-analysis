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
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 10px 20px;
        background-color: #f0f2f6;
        border-radius: 5px 5px 0px 0px;
    }
    </style>
""", unsafe_allow_html=True)

# header
st.markdown('<h1 class="main-header">🚕 NYC Congestion Pricing Audit Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Comprehensive Analysis of Manhattan Congestion Relief Zone Impact (2025)</p>', unsafe_allow_html=True)

# sidebar
with st.sidebar:
    st.header("📊 Dashboard Controls")
    st.markdown("---")
    
    year_select = st.selectbox("Select Year", [2025, 2024, 2023], index=0)
    
    st.markdown("---")
    st.info("""
    **About This Dashboard:**
    
    This dashboard provides comprehensive analysis of the NYC Congestion Pricing implementation:
    
    - **The Map**: Border effect visualization
    - **The Flow**: Traffic velocity analysis
    - **The Economics**: Tip and surcharge trends
    - **The Weather**: Rain elasticity analysis
    """)
    
    st.markdown("---")
    st.caption("Data Source: NYC TLC Trip Record Data")

# main tabs
tab1, tab2, tab3, tab4 = st.tabs(["🗺️ The Map", "🌊 The Flow", "💰 The Economics", "🌧️ The Weather"])

# Tab 1: The Map
with tab1:
    st.header("Border Effect Analysis")
    st.markdown("""
    **Hypothesis:** Are passengers ending trips just outside the congestion zone to avoid the toll?
    
    This map shows the percentage change in drop-offs (2024 vs 2025) for taxi zones immediately 
    bordering the 60th Street cutoff.
    """)
    
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        try:
            border_map = create_border_effect_map(2024, 2025)
            if border_map:
                # save map to HTML and display
                map_html = border_map._repr_html_()
                st.components.v1.html(map_html, height=600, scrolling=True)
            else:
                st.warning("Map data not available. Please run the pipeline first.")
        except Exception as e:
            st.error(f"Error creating map: {e}")
            st.info("Note: Map requires processed data. Run pipeline.py first.")
    
    with col2:
        st.subheader("Key Insights")
        
        try:
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
                st.info("Run pipeline to generate border effect data")
        except Exception as e:
            st.warning(f"Could not load border data: {e}")

# Tab 2: The Flow
with tab2:
    st.header("Congestion Velocity Analysis")
    st.markdown("""
    **Hypothesis:** Did the toll actually speed up traffic?
    
    Compare average trip speeds inside the congestion zone before (Q1 2024) and after (Q1 2025) 
    implementation. Heatmaps show speed patterns by hour of day and day of week.
    """)
    
    st.markdown("---")
    
    try:
        # create heatmaps
        fig = plot_velocity_heatmaps()
        if fig:
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.warning("Velocity data not available. Please run the pipeline first.")
    except Exception as e:
        st.error(f"Error creating heatmaps: {e}")
        st.info("Note: Heatmaps require processed data. Run pipeline.py first.")
    
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

# Tab 3: The Economics
with tab3:
    st.header("Tip Crowding Out Analysis")
    st.markdown("""
    **Hypothesis:** Higher tolls reduce the disposable income passengers leave for drivers.
    
    This analysis examines the relationship between monthly average surcharge amounts and 
    average tip percentages throughout 2025.
    """)
    
    st.markdown("---")
    
    try:
        fig = plot_tip_crowding(2025)
        if fig:
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.warning("Economics data not available. Please run the pipeline first.")
    except Exception as e:
        st.error(f"Error creating economics chart: {e}")
        st.info("Note: Economics analysis requires processed data. Run pipeline.py first.")
    
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
    st.header("Rain Elasticity of Demand")
    st.markdown("""
    **Analysis:** How does precipitation affect taxi demand?
    
    This analysis calculates the rain elasticity of demand by correlating daily precipitation 
    with daily trip counts. The scatter plot shows the relationship for the wettest month of 2025.
    """)
    
    st.markdown("---")
    
    try:
        fig = plot_rain_elasticity(2025)
        if fig:
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.warning("Weather data not available. Please run the pipeline first.")
    except Exception as e:
        st.error(f"Error creating weather plot: {e}")
        st.info("Note: Weather analysis requires API access and processed data. Run pipeline.py first.")
    
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
<div style='text-align: center; color: #666; padding: 20px;'>
    <p><strong>NYC Congestion Pricing Audit Dashboard</strong></p>
    <p>Data Science for Software Engineering - Assignment 01</p>
    <p>Data Source: NYC TLC Trip Record Data | Weather: Open-Meteo API</p>
</div>
""", unsafe_allow_html=True)

