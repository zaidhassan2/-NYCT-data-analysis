"""
Analysis and visualization functions for NYC Congestion Pricing
"""

import polars as pl
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import logging
from data_utils import CONGESTION_ZONE_IDS, PROC_DIR

logger = logging.getLogger(__name__)

# set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


def calc_border_effect(year_2024=2024, year_2025=2025):
    """
    Calculate % change in drop-offs for zones bordering 60th St
    Compares full year 2024 vs full year 2025
    Returns data ready for choropleth mapping
    """
    logger.info("Calculating border effect...")
    
    # approximate border zones (zones near 60th St cutoff)
    # this is simplified - would need actual zone boundaries for precise mapping
    border_zones = [68, 74, 75, 79, 87, 88, 90, 100, 107, 113, 114, 116, 120, 125]
    
    results = {}
    
    for year in [year_2024, year_2025]:
        data_path = PROC_DIR / f"processed_{year}.parquet"
        if not data_path.exists():
            logger.warning(f"Data not found for {year}")
            continue
        
        df = pl.scan_parquet(str(data_path))
        
        # filter dropoffs in border zones - use full year data
        border_dropoffs = df.filter(pl.col('dropoff_loc').is_in(border_zones))
        
        # count by zone - aggregate first in Polars
        zone_counts = border_dropoffs.group_by('dropoff_loc').agg([
            pl.count().alias('dropoff_count')
        ]).collect()
        
        results[year] = zone_counts
    
    # calculate % change
    if year_2024 in results and year_2025 in results:
        df_2024 = results[year_2024]
        df_2025 = results[year_2025]
        
        # merge
        merged = df_2024.join(
            df_2025,
            on='dropoff_loc',
            how='outer',
            suffix='_2025'
        ).fill_null(0)
        
        # calculate % change - handle division by zero and cap extreme values
        merged = merged.with_columns([
            pl.when(pl.col('dropoff_count') > 10)  # only calculate if base is meaningful
            .then(
                ((pl.col('dropoff_count_2025') - pl.col('dropoff_count')) / 
                 pl.col('dropoff_count') * 100)
                .clip(-100, 500)  # cap at -100% to 500% to avoid extreme outliers
            )
            .otherwise(
                pl.when(pl.col('dropoff_count_2025') > 10)
                .then(pl.lit(100.0))  # new zone with meaningful data
                .otherwise(pl.lit(0.0))  # both too small, no meaningful change
            )
            .alias('pct_change')
        ])
        
        # prepare for mapping
        mapping_data = merged.select([
            pl.col('dropoff_loc').alias('zone_id'),
            pl.col('dropoff_count').alias('count_2024'),
            pl.col('dropoff_count_2025').alias('count_2025'),
            pl.col('pct_change')
        ]).to_pandas()
        
        logger.info(f"  Calculated changes for {len(mapping_data)} border zones")
        return mapping_data
    
    return None


def create_velocity_heatmap(year=2024, quarter=1):
    """
    Create heatmap of average trip speed inside congestion zone
    Axes: Hour of Day (0-23) x Day of Week (Mon-Sun)
    Returns aggregated data ready for plotting
    """
    logger.info(f"Creating velocity heatmap for Q{quarter} {year}...")
    
    data_path = PROC_DIR / f"processed_{year}.parquet"
    if not data_path.exists():
        logger.warning(f"Data not found for {year}")
        return None
    
    df = pl.scan_parquet(str(data_path))
    
    # filter quarter
    month_start = (quarter - 1) * 3 + 1
    month_end = quarter * 3
    
    q_df = df.filter(
        (pl.col('pickup_time').dt.month() >= month_start) &
        (pl.col('pickup_time').dt.month() <= month_end) &
        (pl.col('pickup_time').dt.year() == year)
    )
    
    # filter trips inside zone
    zone_trips = q_df.filter(
        (pl.col('pickup_loc').is_in(CONGESTION_ZONE_IDS)) |
        (pl.col('dropoff_loc').is_in(CONGESTION_ZONE_IDS))
    )
    
    # calculate speed
    zone_trips = zone_trips.with_columns([
        ((pl.col('dropoff_time') - pl.col('pickup_time')).dt.total_seconds() / 3600.0).alias('trip_hours'),
        (pl.col('trip_distance') / ((pl.col('dropoff_time') - pl.col('pickup_time')).dt.total_seconds() / 3600.0)).alias('avg_speed_mph')
    ])
    
    # filter out invalid speeds
    zone_trips = zone_trips.filter(
        (pl.col('avg_speed_mph') > 0) &
        (pl.col('avg_speed_mph') < 65) &  # filter ghost trips
        (pl.col('trip_hours') > 0)
    )
    
    # extract hour and day of week
    zone_trips = zone_trips.with_columns([
        pl.col('pickup_time').dt.hour().alias('hour'),
        pl.col('pickup_time').dt.weekday().alias('dow')  # 1=Mon, 7=Sun
    ])
    
    # aggregate by hour and day of week
    heatmap_data = zone_trips.group_by(['hour', 'dow']).agg([
        pl.mean('avg_speed_mph').alias('avg_speed'),
        pl.count().alias('trip_count')
    ]).collect()
    
    # convert to pivot table for heatmap
    heatmap_df = heatmap_data.to_pandas()
    pivot = heatmap_df.pivot(index='dow', columns='hour', values='avg_speed')
    
    # fill missing values with 0 or interpolate
    pivot = pivot.fillna(0)
    
    logger.info(f"  Created heatmap data: {pivot.shape}")
    return pivot


def plot_velocity_heatmaps():
    """
    Create side-by-side velocity heatmaps for Q1 2024 vs Q1 2025
    Returns figure object
    """
    logger.info("Creating velocity heatmap comparison...")
    
    hm_2024 = create_velocity_heatmap(2024, 1)
    hm_2025 = create_velocity_heatmap(2025, 1)
    
    if hm_2024 is None or hm_2025 is None:
        logger.error("Could not create heatmaps")
        return None
    
    # create figure with subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # day of week labels
    dow_labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    # plot 2024
    sns.heatmap(hm_2024, ax=ax1, cmap='RdYlGn', cbar_kws={'label': 'Avg Speed (mph)'},
                xticklabels=range(24), yticklabels=dow_labels, vmin=0, vmax=30)
    ax1.set_title('Q1 2024 - Average Trip Speed in Congestion Zone', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Hour of Day', fontsize=12)
    ax1.set_ylabel('Day of Week', fontsize=12)
    
    # plot 2025
    sns.heatmap(hm_2025, ax=ax2, cmap='RdYlGn', cbar_kws={'label': 'Avg Speed (mph)'},
                xticklabels=range(24), yticklabels=dow_labels, vmin=0, vmax=30)
    ax2.set_title('Q1 2025 - Average Trip Speed in Congestion Zone', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Hour of Day', fontsize=12)
    ax2.set_ylabel('Day of Week', fontsize=12)
    
    plt.tight_layout()
    
    return fig


def analyze_tip_crowding(year=2025):
    """
    Analyze tip percentage vs surcharge amount monthly
    Returns data for dual-axis chart
    """
    logger.info("Analyzing tip crowding effect...")
    
    data_path = PROC_DIR / f"processed_{year}.parquet"
    if not data_path.exists():
        logger.warning(f"Data not found for {year}")
        return None
    
    df = pl.scan_parquet(str(data_path))
    
    # need tip amount - might be in different column
    # check if we have tip column
    tip_col = None
    for col in df.columns:
        if 'tip' in col.lower():
            tip_col = col
            break
    
    if tip_col is None:
        logger.warning("Tip column not found, using total_amount - fare as proxy")
        # estimate tip from total - fare - surcharge
        df = df.with_columns([
            (pl.col('total_amount') - pl.col('fare') - 
             pl.col('congestion_surcharge').fill_null(0)).alias('estimated_tip')
        ])
        tip_col = 'estimated_tip'
    
    # calculate tip percentage
    df = df.with_columns([
        ((pl.col(tip_col) / pl.col('fare') * 100).fill_null(0)).alias('tip_pct')
    ])
    
    # monthly aggregation
    monthly = df.group_by(pl.col('pickup_time').dt.month().alias('month')).agg([
        pl.mean('congestion_surcharge').alias('avg_surcharge'),
        pl.mean('tip_pct').alias('avg_tip_pct'),
        pl.count().alias('trip_count')
    ]).collect()
    
    monthly_df = monthly.to_pandas()
    monthly_df['month_name'] = pd.to_datetime(monthly_df['month'], format='%m').dt.strftime('%b')
    
    logger.info(f"  Calculated monthly averages for {len(monthly_df)} months")
    return monthly_df


def plot_tip_crowding(year=2025):
    """
    Create dual-axis chart: bars for surcharge, line for tip percentage
    Returns figure object
    """
    logger.info("Plotting tip crowding analysis...")
    
    data = analyze_tip_crowding(year)
    if data is None:
        return None
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # bars for surcharge
    ax1.bar(data['month_name'], data['avg_surcharge'], alpha=0.7, color='steelblue', label='Avg Surcharge ($)')
    ax1.set_xlabel('Month', fontsize=12)
    ax1.set_ylabel('Average Surcharge ($)', fontsize=12, color='steelblue')
    ax1.tick_params(axis='y', labelcolor='steelblue')
    
    # line for tip percentage
    ax2 = ax1.twinx()
    ax2.plot(data['month_name'], data['avg_tip_pct'], color='coral', marker='o', 
             linewidth=2, markersize=8, label='Avg Tip %')
    ax2.set_ylabel('Average Tip Percentage (%)', fontsize=12, color='coral')
    ax2.tick_params(axis='y', labelcolor='coral')
    
    plt.title(f'Tip Crowding Analysis - {year}\nSurcharge vs Tip Percentage', 
              fontsize=14, fontweight='bold', pad=20)
    
    # combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.tight_layout()
    
    return fig


def plot_rain_elasticity(year=2025):
    """
    Plot daily trip count vs precipitation for wettest month
    Returns figure object
    """
    logger.info("Plotting rain elasticity...")
    
    from data_utils import calculate_rain_elasticity
    
    elasticity_data = calculate_rain_elasticity(year)
    if elasticity_data is None:
        return None
    
    # convert to dataframe
    df = pd.DataFrame(elasticity_data['data'])
    
    # find wettest month
    df['month'] = pd.to_datetime(df['date']).dt.month
    monthly_precip = df.groupby('month')['precipitation_mm'].sum()
    wettest_month = monthly_precip.idxmax()
    
    # filter to wettest month
    df_month = df[df['month'] == wettest_month].copy()
    
    # create scatter plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.scatter(df_month['precipitation_mm'], df_month['trip_count'], 
               alpha=0.6, s=50, color='navy')
    
    # add trend line
    z = np.polyfit(df_month['precipitation_mm'].fillna(0), 
                   df_month['trip_count'], 1)
    p = np.poly1d(z)
    ax.plot(df_month['precipitation_mm'].sort_values(), 
            p(df_month['precipitation_mm'].sort_values()), 
            "r--", alpha=0.8, linewidth=2, label=f'Trend (slope={z[0]:.2f})')
    
    ax.set_xlabel('Daily Precipitation (mm)', fontsize=12)
    ax.set_ylabel('Daily Trip Count', fontsize=12)
    ax.set_title(f'Rain Elasticity - Wettest Month ({pd.to_datetime(f"{year}-{wettest_month}-01").strftime("%B")} {year})\n'
                 f'Correlation: {elasticity_data["correlation"]:.4f} ({elasticity_data["elasticity_type"]})',
                 fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend()
    
    plt.tight_layout()
    
    return fig


def create_border_effect_map(year_2024=2024, year_2025=2025):
    """
    Create interactive Folium map showing border effect
    Returns Folium map object
    """
    logger.info("Creating border effect map...")
    
    try:
        import folium
        from folium import plugins
    except ImportError:
        logger.error("Folium not installed, skipping map creation")
        return None
    
    border_data = calc_border_effect(year_2024, year_2025)
    if border_data is None:
        return None
    
    # create base map centered on NYC
    m = folium.Map(location=[40.7580, -73.9855], zoom_start=12, tiles='OpenStreetMap')
    
    # approximate zone centers (simplified - would need actual GeoJSON)
    # using rough coordinates for border zones
    zone_coords = {
        68: [40.7614, -73.9776],   # Central Park South
        74: [40.7589, -73.9851],   # Times Sq
        75: [40.7505, -73.9934],   # Hell's Kitchen
        79: [40.7489, -73.9680],   # Midtown East
        87: [40.7282, -73.9942],   # Chelsea
        88: [40.7282, -73.9792],   # Flatiron
        90: [40.7489, -73.9851],   # Midtown
        100: [40.7614, -73.9776],  # Central Park
        107: [40.7282, -73.9792],  # Gramercy
        113: [40.7505, -73.9934],  # West Village
        114: [40.7282, -73.9942],  # East Village
        116: [40.7282, -73.9792],  # Lower East Side
        120: [40.7282, -73.9942],  # SoHo
        125: [40.7282, -73.9792]   # Tribeca
    }
    
    # add markers for each zone
    for _, row in border_data.iterrows():
        zone_id = int(row['zone_id'])
        pct_change = row['pct_change']
        
        if zone_id in zone_coords:
            lat, lon = zone_coords[zone_id]
            
            # color based on change
            if pct_change > 10:
                color = 'red'
            elif pct_change > 0:
                color = 'orange'
            elif pct_change > -10:
                color = 'yellow'
            else:
                color = 'green'
            
            popup_text = f"""
            Zone {zone_id}<br>
            2024 Dropoffs: {row['count_2024']:,.0f}<br>
            2025 Dropoffs: {row['count_2025']:,.0f}<br>
            Change: {pct_change:.1f}%
            """
            
            folium.CircleMarker(
                location=[lat, lon],
                radius=10,
                popup=folium.Popup(popup_text, max_width=200),
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.7
            ).add_to(m)
    
    # add legend
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 200px; height: 120px; 
                background-color: white; z-index:9999; font-size:14px;
                border:2px solid grey; padding: 10px">
    <p><b>Border Effect</b></p>
    <p><span style="color:red">●</span> >10% increase</p>
    <p><span style="color:orange">●</span> 0-10% increase</p>
    <p><span style="color:yellow">●</span> 0-10% decrease</p>
    <p><span style="color:green">●</span> >10% decrease</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    logger.info("  Map created successfully")
    return m

