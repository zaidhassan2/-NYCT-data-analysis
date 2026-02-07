import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# import modules
try:
    import data_utils
    import analysis
    import report_gen
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error("Make sure all modules are in the same directory")
    sys.exit(1)


def run_scraping(year=2025, taxi_types=['yellow', 'green']):
    """Step 1: Scrape TLC data"""
    logger.info("="*60)
    logger.info("STEP 1: Web Scraping")
    logger.info("="*60)
    
    try:
        files = data_utils.scrape_tlc_data(year, taxi_types)
        logger.info(f"Downloaded {len(files)} files")
        return True
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        return False


def run_processing(year=2025, taxi_types=['yellow', 'green']):
    """Step 2: Process and clean data"""
    logger.info("="*60)
    logger.info("STEP 2: Data Processing")
    logger.info("="*60)
    
    try:
        output_path = data_utils.process_year_data(year, taxi_types)
        if output_path:
            logger.info(f"Processing complete: {output_path}")
            return True
        else:
            logger.warning("Processing returned no output")
            return False
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def run_analysis():
    """Step 3: Run all analyses"""
    logger.info("="*60)
    logger.info("STEP 3: Analysis")
    logger.info("="*60)
    
    try:
        # leakage audit
        logger.info("Running leakage audit...")
        leakage = data_utils.audit_leakage(2025)
        if leakage:
            logger.info(f"Compliance rate: {leakage['compliance_rate']:.2f}%")
        
        # Q1 comparison
        logger.info("Comparing Q1 volumes...")
        q1_comp = data_utils.compare_q1_volumes()
        if q1_comp:
            logger.info(f"Q1 change: {q1_comp.get('percent_change', 0):.2f}%")
        
        # rain elasticity
        logger.info("Calculating rain elasticity...")
        elasticity = data_utils.calculate_rain_elasticity(2025)
        if elasticity:
            logger.info(f"Elasticity: {elasticity['correlation']:.4f} ({elasticity['elasticity_type']})")
        
        # border effect
        logger.info("Calculating border effect...")
        border = analysis.calc_border_effect(2024, 2025)
        if border is not None:
            logger.info(f"Border zones analyzed: {len(border)}")
        
        logger.info("Analysis complete")
        return True
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def run_report_generation():
    """Step 4: Generate reports and content"""
    logger.info("="*60)
    logger.info("STEP 4: Report Generation")
    logger.info("="*60)
    
    try:
        # PDF report
        logger.info("Generating PDF report...")
        pdf_path = report_gen.generate_pdf_report()
        logger.info(f"PDF report: {pdf_path}")
        
        # blog post
        logger.info("Generating blog post...")
        blog_path = report_gen.generate_blog_post()
        logger.info(f"Blog post: {blog_path}")
        
        # LinkedIn post
        logger.info("Generating LinkedIn post...")
        linkedin_path = report_gen.generate_linkedin_post()
        logger.info(f"LinkedIn post: {linkedin_path}")
        
        return True
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Main pipeline execution"""
    parser = argparse.ArgumentParser(
        description='NYC Congestion Pricing Analysis Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python pipeline.py --all

  # Run only scraping
  python pipeline.py --scrape

  # Run processing and analysis
  python pipeline.py --process --analyze

  # Skip scraping (use existing data)
  python pipeline.py --process --analyze --report
        """
    )
    
    parser.add_argument('--all', action='store_true',
                       help='Run all pipeline steps')
    parser.add_argument('--scrape', action='store_true',
                       help='Run web scraping step')
    parser.add_argument('--process', action='store_true',
                       help='Run data processing step')
    parser.add_argument('--analyze', action='store_true',
                       help='Run analysis step')
    parser.add_argument('--report', action='store_true',
                       help='Generate reports and content')
    parser.add_argument('--year', type=int, default=2025,
                       help='Year to process (default: 2025)')
    parser.add_argument('--taxi-types', nargs='+', 
                       default=['yellow', 'green'],
                       choices=['yellow', 'green'],
                       help='Taxi types to process (default: yellow green)')
    parser.add_argument('--skip-scrape', action='store_true',
                       help='Skip scraping, use existing data')
    
    args = parser.parse_args()
    
    # if --all, set all flags and process both 2024 and 2025
    if args.all:
        args.scrape = True
        args.process = True
        args.analyze = True
        args.report = True
        # process both years when using --all
        process_both_years = True
    else:
        process_both_years = False
    
    # if no flags set, show help
    if not any([args.scrape, args.process, args.process, args.analyze, args.report]):
        parser.print_help()
        return
    
    logger.info("="*60)
    logger.info("NYC Congestion Pricing Analysis Pipeline")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    success = True
    
    # Step 1: Scraping
    # Note: --all only scrapes 2025 (main year). For 2024, use download_required_data.py
    if args.scrape and not args.skip_scrape:
        success = run_scraping(args.year, args.taxi_types)
        if not success:
            logger.warning("Scraping had issues, but continuing...")
    
    # Step 2: Processing
    if args.process:
        if not success and not args.skip_scrape:
            logger.warning("Previous step had issues, but continuing with processing...")
        
        # if --all, process both 2024 and 2025
        if process_both_years:
            logger.info("Processing both 2024 and 2025 data...")
            success_2024 = run_processing(2024, args.taxi_types)
            success_2025 = run_processing(2025, args.taxi_types)
            success = success_2024 and success_2025
        else:
            success = run_processing(args.year, args.taxi_types)
        
        if not success:
            logger.error("Processing failed, cannot continue")
            return
    
    # Step 3: Analysis
    if args.analyze:
        if not success:
            logger.warning("Previous step had issues, but continuing with analysis...")
        success = run_analysis()
        if not success:
            logger.warning("Analysis had issues, but continuing...")
    
    # Step 4: Reports
    if args.report:
        if not success:
            logger.warning("Previous steps had issues, but generating reports anyway...")
        success = run_report_generation()
    
    logger.info("="*60)
    logger.info(f"Pipeline completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    if success:
        logger.info("✓ All steps completed successfully")
        logger.info("\nNext steps:")
        logger.info("  1. Review outputs in outputs/ directory")
        logger.info("  2. Run dashboard: streamlit run dashboard.py")
        logger.info("  3. Check pipeline.log for detailed logs")
    else:
        logger.warning("⚠ Pipeline completed with warnings/errors")
        logger.warning("  Check pipeline.log for details")


if __name__ == "__main__":
    main()

