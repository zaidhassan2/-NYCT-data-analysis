# NYC Congestion Pricing Audit

Analysis of the Manhattan Congestion Relief Zone Toll impact on NYC taxi industry throughout 2025.

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Download Data
```bash
python download_required_data.py
```

This downloads:
- Q1 2024 (Jan-Mar) for comparison
- December 2024 for baseline
- All of 2025 for main analysis

### 3. Process Data
```bash
python pipeline.py --process --year 2024 --skip-scrape
python pipeline.py --process --year 2025 --skip-scrape
```

### 4. View Dashboard
```bash
streamlit run dashboard.py
```

Open http://localhost:8501 in your browser.

## Dashboard Tabs

- **The Map**: Border effect showing % change in drop-offs (2024 vs 2025)
- **The Flow**: Traffic velocity heatmaps (Q1 2024 vs Q1 2025)
- **The Economics**: Tip percentage vs surcharge trends (2025)
- **The Weather**: Rain elasticity analysis (2025)

## Generate Reports

```bash
python pipeline.py --analyze --report
```

Outputs:
- `outputs/audit_report.pdf`
- `outputs/blog_post.md`
- `outputs/linkedin_post.md`

## Project Files

- `pipeline.py` - Main processing pipeline
- `data_utils.py` - Data download, cleaning, and filtering
- `analysis.py` - Visualization functions
- `dashboard.py` - Streamlit dashboard
- `report_gen.py` - Report generation
- `download_required_data.py` - Data download script

## Data

Processed data saved to:
- `data/processed/processed_2024.parquet`
- `data/processed/processed_2025.parquet`

Ghost trip audit logs: `data/audit_logs/`
