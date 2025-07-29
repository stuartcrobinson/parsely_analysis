# Parsely Analysis

Analyze journalist performance metrics from Parsely export data.

## Overview

This project provides tools to analyze journalist performance metrics from Parsely data exports, with features for:
- Equal credit distribution for collaborative articles
- Monthly performance rankings
- Configurable time filters and output formats
- CSV and Parquet file support

## Installation

### Option 1: Use with pipx (no installation needed)
```bash
# Run journalist metrics analysis
pipx run --spec git+https://github.com/stuartcrobinson/parsely_analysis.git combined data.csv --top-n 20

# Run monthly rankings

pipx run --spec git+https://github.com/stuartcrobinson/parsely_analysis.git monthly data.csv --format compact

pipx run --spec git+https://github.com/stuartcrobinson/parsely_analysis.git monthly data.csv
```

### Option 2: Direct Python usage (for development)
```bash
# Clone the repository
git clone <your-repo-url>
cd parsely_analysis

# Install dependencies
pip install -r requirements.txt
```

### Option 3: Install as package
```bash
pip install -e .
```

## Usage Examples

### Quick start with pipx
```bash
# Analyze overall journalist metrics
pipx run --spec git+https://github.com/stuartcrobinson/parsely_analysis.git combined \
    inputs/data.csv \
    --top-n 20 \
    --after-date 2024-01-01 \
    --output-dir ./results

# Generate monthly rankings
pipx run --spec git+https://github.com/stuartcrobinson/parsely_analysis.git monthly \
    inputs/data.csv \
    --top-n 5 \
    --format compact \
    --ignore-authors "INDY staff" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --ignore-authors "INDY Sales" \
    --after-date 2024-01-01 \
    --output-dir ./results
```

### Local development
```bash
# Overall metrics
python journalist_metrics.py inputs/data.csv --top-n 10

# Monthly rankings
python monthly_auth_rank.py inputs/data.csv --format compact
```

## Scripts

### journalist_metrics.py
Analyzes overall journalist performance with equal credit distribution for collaborative articles.

### monthly_auth_rank.py
Generates month-by-month performance rankings with multiple output format options.

## Data Format

Expects CSV or Parquet files with these required columns:
- `Authors` - comma-separated list of author names
- `Publish date` - article publication date
- `Views` - page view count
- `Visitors` - unique visitor count
- `Social refs` - social media referrals
- `New vis.` - new visitor count
- `Engaged minutes` - total engaged time

## Output Structure

When using `--output-dir`, outputs are organized as:
```
output_dir/
├── journalist_metrics/
│   └── journalist_analysis_[filter]_run_[timestamp].csv
└── monthly_auth_rank/
    └── [timestamp]_[filter]_top-[n]/
        ├── YYYY-MM.csv              # All articles for each month
        └── authors/
            └── YYYY-MM/
                └── author_name.csv   # Per-author articles
```