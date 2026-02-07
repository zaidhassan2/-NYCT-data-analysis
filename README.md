# NYC Congestion Pricing Analysis Pipeline

This project analyzes the impact of NYC's Manhattan Congestion Relief Zone Toll implemented in January 2025.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Run Full Pipeline
```bash
python pipeline.py --all
```

### Run Individual Steps
```bash
# Scrape data
python pipeline.py --scrape

# Process data (requires scraped data)
python pipeline.py --process

# Run analysis (requires processed data)
python pipeline.py --analyze

# Generate reports (requires analysis)
python pipeline.py --report
```

### Run Dashboard
```bash
streamlit run dashboard.py
```

## Project Structure

- `pipeline.py` - Main orchestration script
- `data_utils.py` - Data scraping, ETL, geo mapping, leakage audit, traffic analysis, weather
- `analysis.py` - Visualization and analysis functions
- `dashboard.py` - Streamlit dashboard (4 tabs)
- `report_gen.py` - PDF report and content generation

## Outputs

- `outputs/audit_report.pdf` - Executive summary PDF
- `outputs/blog_post.md` - Medium blog post
- `outputs/linkedin_post.md` - LinkedIn post content
- `data/processed/` - Processed parquet files
- `data/audit_logs/` - Ghost trip audit logs

## Notes

- Data is downloaded to `data/raw/` during scraping
- December 2025 data is automatically imputed if missing
- All processing uses Polars for Big Data handling
- Dashboard requires processed data to be generated first

