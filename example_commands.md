# Example Commands

## journalist_metrics.py Examples

### Basic usage with top 20 journalists
```bash
python journalist_metrics.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv
```

### Filter by date and save outputs
```bash
python journalist_metrics.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
    --after-date 2024-07-01 \
    --top-n 10 \
    --output-dir outputs
```

### Process Parquet file with custom top N
```bash
python journalist_metrics.py \
    data/posts-export.parquet \
    --top-n 5 \
    --after-date 2025-01-01 \
    --output-dir results/q1_2025
```

### Save CSV as Parquet for faster processing
```bash
python journalist_metrics.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
    --save-parquet \
    --top-n 15
```

### everything 
```bash
python journalist_metrics.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
    --output-dir results \
    --save-parquet \
    --top-n 5 \
    --after-date 2025-05-01
```

## monthly_auth_rank.py Examples

### Basic monthly rankings (verbose format)
```bash
python monthly_auth_rank.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv
```

### Compact format with ignored authors
```bash
python monthly_auth_rank.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
    --format compact \
    --output-dir results \
    --save-parquet
    --top-n 3 \
    --ignore-authors "INDY staff" \
    --ignore-authors "Staff" \
    --ignore-authors "adminnewspack" \
    --ignore-authors "INDY Sales" \
    --after-date 2025-05-01
```
### Save monthly outputs with date filter
```bash
python monthly_auth_rank.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
    --after-date 2024-10-01 \
    --top-n 5 \
    --output-dir monthly_analysis \
    --format compact
```

### Process full year with comprehensive outputs
```bash
python monthly_auth_rank.py \
    data/2024_full_year.parquet \
    --top-n 10 \
    --output-dir yearly_reports/2024 \
    --ignore-authors "Editorial Board" \
    --ignore-authors "Guest Author"
```

## Combined Examples

### Full analysis pipeline
```bash
# First, convert to Parquet and analyze overall metrics
python journalist_metrics.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
    --save-parquet \
    --after-date 2024-07-01 \
    --top-n 20 \
    --output-dir analysis_results

# Then run monthly rankings on the Parquet file
python monthly_auth_rank.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.parquet \
    --after-date 2024-07-01 \
    --top-n 5 \
    --format compact \
    --output-dir analysis_results
```

### Quick test with small date range
```bash
# Last 3 months only, top 3 journalists
python journalist_metrics.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
    --after-date 2025-04-01 \
    --top-n 3

# Same for monthly
python monthly_auth_rank.py \
    inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
    --after-date 2025-04-01 \
    --top-n 3 \
    --format compact
```

## pipx

```sh
pipx run --spec git+https://github.com/stuartcrobinson/parsely_analysis.git \
monthly \
inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
--format compact \
--output-dir results \
--save-parquet \
--top-n 3 \
--ignore-authors "INDY staff" \
--ignore-authors "Staff" \
--ignore-authors "adminnewspack" \
--ignore-authors "INDY Sales" \
--after-date 2025-05-01



pipx run --spec git+https://github.com/stuartcrobinson/parsely_analysis.git \
combined \
inputs/posts-export-by-page-views-Jul-01-2024-Jul-29-2025-indyweek-com.csv \
--output-dir results \
--save-parquet \
--top-n 5 \
--after-date 2025-05-01

```