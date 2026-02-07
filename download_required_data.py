#!/usr/bin/env python3
"""
Download only the required data according to assignment:
- Dec 2023 (for December 2025 imputation)
- Jan-Mar 2024 (for Q1 2024 vs Q1 2025 comparison)
- Dec 2024 (for December 2025 imputation and border effect)
- All of 2025 (for main analysis)
"""

import data_utils
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_required_data():
    """Download only the data required by the assignment"""
    
    logger.info("="*60)
    logger.info("Downloading Required Data According to Assignment")
    logger.info("="*60)
    
    # 1. Download Dec 2023 (for imputation)
    logger.info("\n1. Downloading December 2023 (for December 2025 imputation)...")
    data_utils.scrape_tlc_data(year=2023, taxi_types=['yellow', 'green'], specific_months=['12'])
    
    # 2. Download Jan-Mar 2024 (for Q1 comparison)
    logger.info("\n2. Downloading Q1 2024 (Jan-Mar) for Q1 2024 vs Q1 2025 comparison...")
    data_utils.scrape_tlc_data(year=2024, taxi_types=['yellow', 'green'], specific_months=['01', '02', '03'])
    
    # 3. Download Dec 2024 (for imputation and border effect)
    logger.info("\n3. Downloading December 2024 (for imputation and border effect)...")
    data_utils.scrape_tlc_data(year=2024, taxi_types=['yellow', 'green'], specific_months=['12'])
    
    # 4. Download all of 2025 (main analysis year)
    logger.info("\n4. Downloading all of 2025 (main analysis year)...")
    data_utils.scrape_tlc_data(year=2025, taxi_types=['yellow', 'green'], specific_months=None)
    
    logger.info("\n" + "="*60)
    logger.info("Required data download complete!")
    logger.info("="*60)
    logger.info("\nNext steps:")
    logger.info("  1. Process 2024 data: python pipeline.py --process --year 2024 --skip-scrape")
    logger.info("  2. Process 2025 data: python pipeline.py --process --year 2025 --skip-scrape")
    logger.info("  3. Run analysis: python pipeline.py --analyze --report")

if __name__ == "__main__":
    download_required_data()

