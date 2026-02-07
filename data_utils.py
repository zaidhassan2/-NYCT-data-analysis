"""
Data utilities for NYC Congestion Pricing analysis
Handles scraping, ETL, geo mapping, leakage audit, traffic analysis, and weather
"""

import os
import time
import logging
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import polars as pl
from datetime import datetime, timedelta
import json

# setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROC_DIR = DATA_DIR / "processed"
AUDIT_DIR = DATA_DIR / "audit_logs"

# create dirs if needed
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

# TLC base URL
TLC_BASE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# congestion zone location IDs (Manhattan south of 60th St)
# these are approximate - would need actual shapefile for precise mapping
CONGESTION_ZONE_IDS = [
    4, 12, 13, 24, 41, 42, 43, 45, 48, 50, 68, 74, 75, 79, 87, 88, 90, 100,
    107, 113, 114, 116, 120, 125, 127, 128, 137, 140, 141, 142, 143, 144, 148,
    151, 158, 161, 162, 163, 164, 166, 170, 186, 194, 202, 209, 211, 224, 229,
    230, 231, 232, 233, 234, 236, 237, 238, 239, 240, 241, 242, 243, 244, 246,
    249, 261, 262, 263
]


def scrape_tlc_data(year=2025, taxi_types=['yellow', 'green']):
    """
    Scrape TLC website and download parquet files for specified year
    Handles missing December data with imputation
    """
    logger.info(f"Starting TLC data scrape for year {year}")
    
    # correct base URL from TLC website
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # headers to avoid 403 errors
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    }
    
    downloaded_files = []
    
    for taxi_type in taxi_types:
        logger.info(f"Processing {taxi_type} taxi data...")
        
        months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        
        for month in months:
            file_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"
            local_path = RAW_DIR / taxi_type / file_name
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # skip if already downloaded
            if local_path.exists():
                logger.info(f"  {file_name} already exists, skipping")
                downloaded_files.append(str(local_path))
                continue
            
            # try both URL formats - underscores and URL-encoded
            url_variants = [
                f"{base_url}{file_name}",  # try with underscores first
                f"{base_url}{file_name.replace('_', '%5F')}"  # then URL-encoded
            ]
            
            downloaded = False
            for url in url_variants:
                try:
                    logger.info(f"  Downloading {file_name}...")
                    response = requests.get(url, headers=headers, timeout=60, stream=True)
                    
                    if response.status_code == 200:
                        total_size = 0
                        with open(local_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    total_size += len(chunk)
                        
                        file_size_mb = total_size / (1024 * 1024)
                        logger.info(f"  Successfully downloaded {file_name} ({file_size_mb:.1f} MB)")
                        downloaded_files.append(str(local_path))
                        downloaded = True
                        time.sleep(0.5)  # be nice to server
                        break
                    elif response.status_code == 404:
                        # try next variant
                        continue
                    elif response.status_code == 403:
                        logger.warning(f"  Access denied (403) for {file_name}")
                        # try next variant
                        continue
                    else:
                        logger.warning(f"  Got status {response.status_code} for {file_name}")
                        continue
                        
                except Exception as e:
                    logger.warning(f"  Error with URL {url}: {e}")
                    continue
            
            if not downloaded:
                if month == '12' and year == 2025:
                    logger.info(f"  December 2025 not available, will impute later")
                else:
                    logger.warning(f"  Could not download {file_name} - file may not exist yet")
    
    # check if we need to impute December
    dec_2025_yellow = RAW_DIR / "yellow" / f"yellow_tripdata_2025-12.parquet"
    dec_2025_green = RAW_DIR / "green" / f"green_tripdata_2025-12.parquet"
    
    if not dec_2025_yellow.exists() or not dec_2025_green.exists():
        logger.info("December 2025 missing, will create imputed version during processing")
    
    return downloaded_files


def impute_december_2025(taxi_type='yellow'):
    """
    Create imputed December 2025 data using weighted average
    Dec 2023 (30%) + Dec 2024 (70%)
    """
    logger.info(f"Imputing December 2025 for {taxi_type} taxis")
    
    dec_2023_path = RAW_DIR / taxi_type / f"{taxi_type}_tripdata_2023-12.parquet"
    dec_2024_path = RAW_DIR / taxi_type / f"{taxi_type}_tripdata_2024-12.parquet"
    output_path = RAW_DIR / taxi_type / f"{taxi_type}_tripdata_2025-12.parquet"
    
    # try to download missing source files
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    }
    
    if not dec_2023_path.exists():
        logger.info(f"  Downloading Dec 2023 data for imputation...")
        url = f"{base_url}{taxi_type}_tripdata_2023-12.parquet"
        try:
            response = requests.get(url, headers=headers, timeout=60, stream=True)
            if response.status_code == 200:
                dec_2023_path.parent.mkdir(parents=True, exist_ok=True)
                with open(dec_2023_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                logger.info(f"  Downloaded Dec 2023 data")
            else:
                logger.warning(f"  Could not download Dec 2023 data (status {response.status_code})")
        except Exception as e:
            logger.warning(f"  Error downloading Dec 2023 data: {e}")
    
    if not dec_2024_path.exists():
        logger.info(f"  Downloading Dec 2024 data for imputation...")
        url = f"{base_url}{taxi_type}_tripdata_2024-12.parquet"
        try:
            response = requests.get(url, headers=headers, timeout=60, stream=True)
            if response.status_code == 200:
                dec_2024_path.parent.mkdir(parents=True, exist_ok=True)
                with open(dec_2024_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                logger.info(f"  Downloaded Dec 2024 data")
            else:
                logger.warning(f"  Could not download Dec 2024 data (status {response.status_code})")
        except Exception as e:
            logger.warning(f"  Error downloading Dec 2024 data: {e}")
    
    if not dec_2023_path.exists() or not dec_2024_path.exists():
        logger.warning(f"Missing source files for imputation - skipping December 2025 imputation")
        logger.warning(f"  Dec 2023 exists: {dec_2023_path.exists()}, Dec 2024 exists: {dec_2024_path.exists()}")
        return None
    
    # read in chunks to avoid memory issues
    logger.info("  Reading Dec 2023 data...")
    df_2023 = pl.scan_parquet(str(dec_2023_path))
    
    logger.info("  Reading Dec 2024 data...")
    df_2024 = pl.scan_parquet(str(dec_2024_path))
    
    # sample 30% from 2023, 70% from 2024
    # adjust dates to 2025
    logger.info("  Sampling and adjusting dates...")
    
    df_2023_sample = df_2023.sample(fraction=0.3, seed=42).collect()
    df_2024_sample = df_2024.sample(fraction=0.7, seed=42).collect()
    
    # update date columns to 2025
    # add days: 2 years = ~730 days, 1 year = ~365 days
    date_cols_2023 = [col for col in df_2023_sample.columns if 'datetime' in col.lower() or 'date' in col.lower() or 'time' in col.lower()]
    date_cols_2024 = [col for col in df_2024_sample.columns if 'datetime' in col.lower() or 'date' in col.lower() or 'time' in col.lower()]
    
    for col in date_cols_2023:
        if col in df_2023_sample.columns and df_2023_sample[col].dtype in [pl.Datetime]:
            df_2023_sample = df_2023_sample.with_columns(
                (pl.col(col) + pl.duration(days=730)).alias(col)
            )
    
    for col in date_cols_2024:
        if col in df_2024_sample.columns and df_2024_sample[col].dtype in [pl.Datetime]:
            df_2024_sample = df_2024_sample.with_columns(
                (pl.col(col) + pl.duration(days=365)).alias(col)
            )
    
    # combine
    combined = pl.concat([df_2023_sample, df_2024_sample])
    
    # save
    combined.write_parquet(str(output_path))
    logger.info(f"  Saved imputed data to {output_path}")
    
    return str(output_path)


def unify_schema(df):
    """
    Unify schema across different taxi types and years
    Returns standardized columns: pickup_time, dropoff_time, pickup_loc, dropoff_loc, 
    trip_distance, fare, total_amount, congestion_surcharge
    """
    # map common column name variations - only map if target doesn't already exist
    col_mapping = {}
    target_cols = set()  # track which target columns we've already mapped
    
    # required columns
    required_cols = ['pickup_time', 'dropoff_time', 'pickup_loc', 'dropoff_loc', 
                     'trip_distance', 'fare', 'total_amount', 'congestion_surcharge']
    
    # check what already exists
    existing_cols = {col.lower(): col for col in df.columns}
    
    # pickup time
    if 'pickup_time' not in [c.lower() for c in df.columns]:
        for col in df.columns:
            col_lower = col.lower()
            if ('pickup' in col_lower and 'datetime' in col_lower) or \
               ('pickup' in col_lower and 'time' in col_lower and 'datetime' not in col_lower) or \
               col_lower in ['tpep_pickup_datetime', 'lpep_pickup_datetime']:
                if 'pickup_time' not in target_cols:
                    col_mapping[col] = 'pickup_time'
                    target_cols.add('pickup_time')
                    break
    
    # dropoff time
    if 'dropoff_time' not in [c.lower() for c in df.columns]:
        for col in df.columns:
            col_lower = col.lower()
            if ('dropoff' in col_lower and 'datetime' in col_lower) or \
               ('dropoff' in col_lower and 'time' in col_lower and 'datetime' not in col_lower) or \
               col_lower in ['tpep_dropoff_datetime', 'lpep_dropoff_datetime']:
                if 'dropoff_time' not in target_cols:
                    col_mapping[col] = 'dropoff_time'
                    target_cols.add('dropoff_time')
                    break
    
    # pickup location
    if 'pickup_loc' not in [c.lower() for c in df.columns]:
        for col in df.columns:
            col_lower = col.lower()
            if 'pulocationid' in col_lower or 'pickup_location' in col_lower:
                if 'pickup_loc' not in target_cols:
                    col_mapping[col] = 'pickup_loc'
                    target_cols.add('pickup_loc')
                    break
    
    # dropoff location
    if 'dropoff_loc' not in [c.lower() for c in df.columns]:
        for col in df.columns:
            col_lower = col.lower()
            if 'dolocationid' in col_lower or 'dropoff_location' in col_lower:
                if 'dropoff_loc' not in target_cols:
                    col_mapping[col] = 'dropoff_loc'
                    target_cols.add('dropoff_loc')
                    break
    
    # trip distance
    if 'trip_distance' not in [c.lower() for c in df.columns]:
        for col in df.columns:
            if 'trip_distance' in col.lower():
                if 'trip_distance' not in target_cols:
                    col_mapping[col] = 'trip_distance'
                    target_cols.add('trip_distance')
                    break
    
    # fare
    if 'fare' not in [c.lower() for c in df.columns]:
        for col in df.columns:
            if col.lower() == 'fare_amount':
                if 'fare' not in target_cols:
                    col_mapping[col] = 'fare'
                    target_cols.add('fare')
                    break
    
    # total amount
    if 'total_amount' not in [c.lower() for c in df.columns]:
        for col in df.columns:
            if 'total_amount' in col.lower():
                if 'total_amount' not in target_cols:
                    col_mapping[col] = 'total_amount'
                    target_cols.add('total_amount')
                    break
    
    # congestion surcharge - check for cbd_congestion_fee (2025 data) or other variations
    if 'congestion_surcharge' not in [c.lower() for c in df.columns]:
        for col in df.columns:
            col_lower = col.lower()
            # 2025 data uses cbd_congestion_fee
            if col_lower == 'cbd_congestion_fee':
                if 'congestion_surcharge' not in target_cols:
                    col_mapping[col] = 'congestion_surcharge'
                    target_cols.add('congestion_surcharge')
                    break
            elif 'congestion' in col_lower and 'surcharge' in col_lower:
                if 'congestion_surcharge' not in target_cols:
                    col_mapping[col] = 'congestion_surcharge'
                    target_cols.add('congestion_surcharge')
                    break
    
    # rename columns if we have mappings
    if col_mapping:
        df_renamed = df.rename(col_mapping)
    else:
        df_renamed = df
    
    # ensure we have all required columns, fill missing with null
    for col in required_cols:
        if col not in df_renamed.columns:
            df_renamed = df_renamed.with_columns(pl.lit(None).alias(col))
    
    # select only required columns
    df_final = df_renamed.select(required_cols)
    
    return df_final


def filter_ghost_trips(df, audit_log_path=None):
    """
    Filter out suspicious/ghost trips
    Returns filtered dataframe and audit log
    """
    logger.info("Filtering ghost trips...")
    
    audit_log = {
        'impossible_physics': [],
        'teleporter': [],
        'stationary': [],
        'total_filtered': 0
    }
    
    # calculate trip time and speed
    df = df.with_columns([
        ((pl.col('dropoff_time') - pl.col('pickup_time')).dt.total_seconds() / 60.0).alias('trip_time_min'),
        (pl.col('trip_distance') / ((pl.col('dropoff_time') - pl.col('pickup_time')).dt.total_seconds() / 3600.0)).alias('avg_speed_mph')
    ])
    
    # Impossible Physics: avg_speed > 65 MPH
    impossible = df.filter(pl.col('avg_speed_mph') > 65)
    audit_log['impossible_physics'] = impossible.select(['pickup_time', 'dropoff_time', 'trip_distance', 'avg_speed_mph']).to_dicts()
    
    # Teleporter: trip_time < 1 min AND fare > $20
    teleporter = df.filter((pl.col('trip_time_min') < 1.0) & (pl.col('fare') > 20.0))
    audit_log['teleporter'] = teleporter.select(['pickup_time', 'dropoff_time', 'trip_time_min', 'fare']).to_dicts()
    
    # Stationary: distance = 0 AND fare > 0
    stationary = df.filter((pl.col('trip_distance') == 0) & (pl.col('fare') > 0))
    audit_log['stationary'] = stationary.select(['pickup_time', 'dropoff_time', 'trip_distance', 'fare']).to_dicts()
    
    # combine all suspicious trips
    all_suspicious = pl.concat([impossible, teleporter, stationary]).unique()
    
    # filter them out
    df_clean = df.join(all_suspicious, on=df.columns, how='anti')
    
    audit_log['total_filtered'] = len(all_suspicious)
    
    # save audit log
    if audit_log_path:
        with open(audit_log_path, 'w') as f:
            json.dump(audit_log, f, indent=2, default=str)
        logger.info(f"  Saved audit log to {audit_log_path}")
    
    logger.info(f"  Filtered {audit_log['total_filtered']} suspicious trips")
    
    return df_clean.drop(['trip_time_min', 'avg_speed_mph']), audit_log


def process_year_data(year=2025, taxi_types=['yellow', 'green'], chunk_size=100000):
    """
    Process all data for a year using Polars in chunks
    Returns path to processed data
    """
    logger.info(f"Processing data for year {year}")
    
    all_data = []
    total_audit_log = {
        'impossible_physics': [],
        'teleporter': [],
        'stationary': [],
        'total_filtered': 0
    }
    
    for taxi_type in taxi_types:
        logger.info(f"Processing {taxi_type} taxi data...")
        
        # check if December needs imputation
        dec_path = RAW_DIR / taxi_type / f"{taxi_type}_tripdata_{year}-12.parquet"
        if not dec_path.exists() and year == 2025:
            logger.info("  Imputing December 2025...")
            impute_december_2025(taxi_type)
        
        # process each month
        for month in range(1, 13):
            month_str = f"{month:02d}"
            file_path = RAW_DIR / taxi_type / f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
            
            if not file_path.exists():
                logger.warning(f"  File not found: {file_path}")
                continue
            
            logger.info(f"  Processing {file_path.name}...")
            
            # read in chunks using Polars scan
            df_scan = pl.scan_parquet(str(file_path))
            
            # get total rows for progress
            total_rows = df_scan.select(pl.count()).collect().item()
            logger.info(f"    Total rows: {total_rows:,}")
            
            # process in chunks
            processed_chunks = []
            for i in range(0, total_rows, chunk_size):
                chunk = df_scan.slice(i, chunk_size).collect()
                
                # unify schema
                chunk = unify_schema(chunk)
                
                # filter ghost trips
                audit_path = AUDIT_DIR / f"{taxi_type}_{year}_{month_str}_audit.json"
                chunk_clean, audit = filter_ghost_trips(chunk, str(audit_path))
                
                # accumulate audit log
                total_audit_log['impossible_physics'].extend(audit['impossible_physics'])
                total_audit_log['teleporter'].extend(audit['teleporter'])
                total_audit_log['stationary'].extend(audit['stationary'])
                total_audit_log['total_filtered'] += audit['total_filtered']
                
                processed_chunks.append(chunk_clean)
                
                if (i // chunk_size) % 10 == 0:
                    logger.info(f"    Processed {i:,} rows...")
            
            # combine chunks for this month
            if processed_chunks:
                month_data = pl.concat(processed_chunks)
                all_data.append(month_data)
                logger.info(f"    Completed {file_path.name}")
    
    # combine all data
    if all_data:
        logger.info("Combining all processed data...")
        final_df = pl.concat(all_data)
        
        # save processed data
        output_path = PROC_DIR / f"processed_{year}.parquet"
        final_df.write_parquet(str(output_path))
        logger.info(f"Saved processed data to {output_path}")
        
        # save combined audit log
        audit_output = AUDIT_DIR / f"combined_audit_{year}.json"
        with open(audit_output, 'w') as f:
            json.dump(total_audit_log, f, indent=2, default=str)
        
        return str(output_path)
    
    return None


def get_congestion_zone_locations():
    """
    Return list of LocationIDs in congestion zone
    For now using hardcoded list, ideally would load from shapefile
    """
    return CONGESTION_ZONE_IDS


def is_in_congestion_zone(location_id):
    """Check if location ID is in congestion zone"""
    return location_id in CONGESTION_ZONE_IDS


def audit_leakage(year=2025, toll_start_date='2025-01-05'):
    """
    Calculate surcharge compliance rate for trips outside->inside zone after toll start
    Returns compliance stats and top 3 pickup locations with missing surcharges
    """
    logger.info("Auditing surcharge leakage...")
    
    # load processed data
    data_path = PROC_DIR / f"processed_{year}.parquet"
    if not data_path.exists():
        logger.error(f"Processed data not found: {data_path}")
        return None
    
    df = pl.scan_parquet(str(data_path))
    
    # filter trips after toll start
    toll_start = datetime.strptime(toll_start_date, '%Y-%m-%d')
    df = df.filter(pl.col('pickup_time') >= pl.lit(toll_start))
    
    # trips that started outside and ended inside zone
    outside_inside = df.filter(
        (~pl.col('pickup_loc').is_in(CONGESTION_ZONE_IDS)) &
        (pl.col('dropoff_loc').is_in(CONGESTION_ZONE_IDS))
    )
    
    # collect for analysis
    outside_inside_df = outside_inside.collect()
    
    if len(outside_inside_df) == 0:
        logger.warning("No outside->inside trips found")
        return {
            'total_trips': 0,
            'with_surcharge': 0,
            'without_surcharge': 0,
            'compliance_rate': 0.0,
            'top_missing_locations': []
        }
    
    # check surcharge compliance
    with_surcharge = outside_inside_df.filter(pl.col('congestion_surcharge').is_not_null() & (pl.col('congestion_surcharge') > 0))
    without_surcharge = outside_inside_df.filter(pl.col('congestion_surcharge').is_null() | (pl.col('congestion_surcharge') == 0))
    
    total = len(outside_inside_df)
    with_s = len(with_surcharge)
    without_s = len(without_surcharge)
    compliance_rate = (with_s / total * 100) if total > 0 else 0.0
    
    # top 3 pickup locations with missing surcharges
    top_missing = without_surcharge.group_by('pickup_loc').agg([
        pl.count().alias('count')
    ]).sort('count', descending=True).head(3)
    
    result = {
        'total_trips': total,
        'with_surcharge': with_s,
        'without_surcharge': without_s,
        'compliance_rate': compliance_rate,
        'top_missing_locations': top_missing.to_dicts()
    }
    
    logger.info(f"  Compliance rate: {compliance_rate:.2f}%")
    logger.info(f"  Top missing locations: {top_missing['pickup_loc'].to_list()}")
    
    return result


def compare_q1_volumes():
    """
    Compare Q1 2024 vs Q1 2025 trip volumes entering congestion zone
    Returns comparison stats
    """
    logger.info("Comparing Q1 volumes...")
    
    results = {}
    
    for year in [2024, 2025]:
        data_path = PROC_DIR / f"processed_{year}.parquet"
        if not data_path.exists():
            logger.warning(f"Data not found for {year}")
            continue
        
        df = pl.scan_parquet(str(data_path))
        
        # filter Q1 (Jan-Mar)
        q1_df = df.filter(
            (pl.col('pickup_time').dt.month() >= 1) &
            (pl.col('pickup_time').dt.month() <= 3) &
            (pl.col('pickup_time').dt.year() == year)
        )
        
        # trips entering zone (pickup outside, dropoff inside OR pickup inside)
        entering = q1_df.filter(
            (pl.col('dropoff_loc').is_in(CONGESTION_ZONE_IDS)) |
            (pl.col('pickup_loc').is_in(CONGESTION_ZONE_IDS))
        )
        
        # split by taxi type if available
        # for now just total
        total = entering.select(pl.count()).collect().item()
        
        results[year] = {
            'total_entering': total
        }
    
    if 2024 in results and 2025 in results:
        change = ((results[2025]['total_entering'] - results[2024]['total_entering']) / 
                 results[2024]['total_entering'] * 100)
        results['percent_change'] = change
        logger.info(f"  Q1 2024: {results[2024]['total_entering']:,} trips")
        logger.info(f"  Q1 2025: {results[2025]['total_entering']:,} trips")
        logger.info(f"  Change: {change:.2f}%")
    
    return results


def get_weather_data(year=2025, location='Central Park, NY'):
    """
    Fetch daily precipitation data from Open-Meteo API
    Returns dataframe with date and precipitation
    """
    logger.info(f"Fetching weather data for {year}...")
    
    # Open-Meteo API (free, no key needed)
    # Central Park coordinates: ~40.7829, -73.9654
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        'latitude': 40.7829,
        'longitude': -73.9654,
        'start_date': f'{year}-01-01',
        'end_date': f'{year}-12-31',
        'daily': 'precipitation_sum',
        'timezone': 'America/New_York'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # convert to dataframe
        dates = data['daily']['time']
        precip = data['daily']['precipitation_sum']
        
        df = pl.DataFrame({
            'date': pl.Series(dates).str.strptime(pl.Date, '%Y-%m-%d'),
            'precipitation_mm': pl.Series(precip)
        })
        
        logger.info(f"  Retrieved {len(df)} days of weather data")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching weather data: {e}")
        return None


def calculate_rain_elasticity(year=2025):
    """
    Calculate rain elasticity of demand
    Joins weather with daily trip counts and calculates correlation
    """
    logger.info("Calculating rain elasticity...")
    
    # get weather data
    weather_df = get_weather_data(year)
    if weather_df is None:
        return None
    
    # get daily trip counts
    data_path = PROC_DIR / f"processed_{year}.parquet"
    if not data_path.exists():
        logger.error("Processed data not found")
        return None
    
    df = pl.scan_parquet(str(data_path))
    
    # daily trip counts
    daily_trips = df.group_by(pl.col('pickup_time').dt.date().alias('date')).agg([
        pl.count().alias('trip_count')
    ]).collect()
    
    # join with weather
    combined = daily_trips.join(
        weather_df,
        left_on='date',
        right_on='date',
        how='inner'
    )
    
    # calculate correlation
    import numpy as np
    precip = combined['precipitation_mm'].to_numpy()
    trips = combined['trip_count'].to_numpy()
    
    # remove nulls
    mask = ~(np.isnan(precip) | np.isnan(trips))
    precip_clean = precip[mask]
    trips_clean = trips[mask]
    
    if len(precip_clean) < 2:
        logger.warning("Not enough data for correlation")
        return None
    
    correlation = np.corrcoef(precip_clean, trips_clean)[0, 1]
    
    # simple regression slope
    if np.std(precip_clean) > 0:
        slope = np.cov(precip_clean, trips_clean)[0, 1] / np.var(precip_clean)
    else:
        slope = 0
    
    result = {
        'correlation': correlation,
        'slope': slope,
        'elasticity_type': 'elastic' if abs(correlation) > 0.3 else 'inelastic',
        'data': combined.to_dicts()
    }
    
    logger.info(f"  Correlation: {correlation:.4f}")
    logger.info(f"  Slope: {slope:.4f}")
    logger.info(f"  Type: {result['elasticity_type']}")
    
    return result

