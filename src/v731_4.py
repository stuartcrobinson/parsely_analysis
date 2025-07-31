#!/usr/bin/env python
"""
Parse.ly Journalist Performance Report Generator

Processes Parse.ly export CSVs to generate journalist performance reports
with rankings across multiple metrics.
"""

import pandas as pd
import numpy as np
import click
from pathlib import Path
from datetime import datetime
import warnings
from tqdm import tqdm

warnings.filterwarnings('ignore')

# Define metrics to analyze
METRICS = {
    'views': 'Views',
    # 'visitors': 'Visitors', 
    'social_refs': 'Social refs',
    # 'new_vis': 'New vis.',
    'engaged_minutes': 'Engaged minutes',
    'num_articles': 'Articles'
}

METRIC_DISPLAY_NAMES = {
    'views': 'VIEWS',
    'visitors': 'VISITORS',
    'social_refs': 'SOCIAL REFS',
    'new_vis': 'NEW VISITORS',
    'engaged_minutes': 'ENGAGED MINUTES',
    'num_articles': 'ARTICLES'
}


def parse_authors(authors_str):
    """Parse authors string and return list of individual authors."""
    if pd.isna(authors_str):
        return []
    # Split by comma and strip whitespace
    return [author.strip() for author in authors_str.split(',')]


def load_csv_files(directory):
    """Load all CSV files from a directory."""
    path = Path(directory)
    csv_files = list(path.glob('*.csv'))
    
    dataframes = []
    for csv_file in tqdm(csv_files, desc=f"Loading files from {directory}"):
        # Read only the columns we need to save memory
        needed_cols = ['URL', 'Title', 'Publish date', 'Authors', 'Section', 'Tags',
                    #   'Visitors', 
                      'Views', 'Engaged minutes', 
                    #   'New vis.', 
                      'Social refs']
        
        try:
            df = pd.read_csv(csv_file, usecols=lambda x: x in needed_cols)
        except:
            # If column selection fails, read all columns
            df = pd.read_csv(csv_file)
        
        df['source_file'] = csv_file.name
        
        # Extract date from filename
        # Format: posts-export-by-page-views-MMM-DD-YYYY-MMM-DD-YYYY-domain.csv
        parts = csv_file.stem.split('-')
        if len(parts) >= 10:
            month_start = parts[5]
            year_start = parts[7]
            try:
                date_str = f"{month_start} {year_start}"
                df['report_date'] = pd.to_datetime(date_str, format='%b %Y')
            except:
                df['report_date'] = pd.NaT
        
        dataframes.append(df)
    
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    return pd.DataFrame()


def process_data(df, ignore_authors):
    """Process data and attribute metrics to individual authors."""
    # First, filter out rows with no authors
    df_with_authors = df[df['Authors'].notna()].copy()
    
    if df_with_authors.empty:
        return pd.DataFrame()
    
    # Split authors and explode to create one row per author
    df_with_authors['Author'] = df_with_authors['Authors'].str.split(',')
    df_expanded = df_with_authors.explode('Author')
    
    # Clean up author names (strip whitespace)
    df_expanded['Author'] = df_expanded['Author'].str.strip()
    
    # Filter out ignored authors
    df_expanded = df_expanded[~df_expanded['Author'].isin(ignore_authors)]
    
    return df_expanded


def aggregate_by_author(df, metrics):
    """Aggregate metrics by author."""
    # Build aggregation dictionary more efficiently
    agg_funcs = {
        'URL': 'count',  # For num_articles
        'Views': 'sum',
        'Visitors': 'sum',
        'Social refs': 'sum',
        'New vis.': 'sum',
        'Engaged minutes': 'sum'
    }
    
    # Filter to only the aggregations we need
    agg_funcs = {k: v for k, v in agg_funcs.items() if k in df.columns}
    
    # Group by author and aggregate
    author_stats = df.groupby('Author', as_index=False).agg(agg_funcs)
    
    # Rename columns to match our metric names
    author_stats = author_stats.rename(columns={
        'URL': 'num_articles',
        'Views': 'views',
        'Visitors': 'visitors',
        'Social refs': 'social_refs',
        'New vis.': 'new_vis',
        'Engaged minutes': 'engaged_minutes'
    })
    
    # Ensure all metrics exist, fill with 0 if missing
    for metric in metrics.keys():
        if metric not in author_stats.columns:
            author_stats[metric] = 0
    
    # Convert to integers
    numeric_cols = ['views', 'visitors', 'social_refs', 'new_vis', 'engaged_minutes', 'num_articles']
    for col in numeric_cols:
        if col in author_stats.columns:
            author_stats[col] = author_stats[col].fillna(0).astype(int)
    
    return author_stats


def format_number(num):
    """Format number with thousands separator."""
    return f"{int(num):,}" if pd.notna(num) else "0"


def print_date_range_report(df, top_n, metrics, ignore_authors):
    """Print report for date range data."""
    # Process data
    processed_df = process_data(df, ignore_authors)
    
    if processed_df.empty:
        print("No data to report")
        return
    
    # Aggregate by author
    author_stats = aggregate_by_author(processed_df, metrics)
    
    # Determine date range from filename
    if 'source_file' in df.columns and len(df['source_file'].unique()) > 0:
        filename = df['source_file'].iloc[0]
        # Extract dates from filename format: posts-export-by-page-views-Jul-01-2024-Jul-31-2025-indyweek-com.csv
        parts = filename.split('-')
        if len(parts) >= 10:
            try:
                # Parse start date
                start_month = parts[5]
                start_day = parts[6]
                start_year = parts[7]
                start_date = datetime.strptime(f"{start_month} {start_day} {start_year}", "%b %d %Y")
                
                # Parse end date
                end_month = parts[8]
                end_day = parts[9]
                end_year = parts[10]
                end_date = datetime.strptime(f"{end_month} {end_day} {end_year}", "%b %d %Y")
                
                print(f"{start_date.strftime('%B %-d, %Y')} - {end_date.strftime('%B %-d, %Y')}\n")
            except:
                # Fallback to publish dates if filename parsing fails
                if 'Publish date' in df.columns:
                    dates = pd.to_datetime(df['Publish date'], errors='coerce')
                    dates = dates.dropna()
                    if not dates.empty:
                        start_date = dates.min().strftime('%B %-d, %Y')
                        end_date = dates.max().strftime('%B %-d, %Y')
                        print(f"{start_date} - {end_date}\n")
    
    # Print top journalists for each metric
    for metric, col_name in metrics.items():
        if metric == 'num_articles':
            continue  # Skip articles for individual metric sections
            
        # Sort by metric
        sorted_df = author_stats.sort_values(metric, ascending=False).head(top_n)
        
        if not sorted_df.empty:
            # Calculate percentage difference between 1st and 2nd
            pct_diff = ""
            if len(sorted_df) >= 2:
                first_val = sorted_df.iloc[0][metric]
                second_val = sorted_df.iloc[1][metric]
                if second_val > 0:
                    pct = int((first_val - second_val) / second_val * 100)
                    pct_diff = f"1st vs 2nd: +{pct}%"
            
            display_name = METRIC_DISPLAY_NAMES[metric]
            print("=" * 62)
            print(f"TOP {top_n} JOURNALISTS BY {display_name:<30}{pct_diff:>20}")
            print("=" * 62)
            print(f"{'Rank':<6}{'Journalist':<30}{display_name:<18}{'Articles'}")
            print("-" * 62)
            
            for i, row in sorted_df.iterrows():
                rank = sorted_df.index.get_loc(i) + 1
                print(f"{rank:<6}{row['Author']:<30}{format_number(row[metric]):<18}{row['num_articles']}")
            
            print()


def print_monthly_report(df, top_n, metrics, ignore_authors):
    """Print report for monthly data."""
    # Process data
    processed_df = process_data(df, ignore_authors)
    
    if processed_df.empty:
        print("No data to report")
        return
    
    # Group by month
    processed_df['Year-Month'] = processed_df['report_date'].dt.strftime('%Y-%m')
    
    monthly_results = {}
    
    # Process each metric
    for metric, col_name in metrics.items():
        if metric == 'num_articles':
            continue
            
        display_name = METRIC_DISPLAY_NAMES[metric]
        print("=" * 62)
        print(f"MONTHLY RANKINGS: {display_name}")
        print("=" * 62)
        print()
        
        # Track wins per journalist
        wins = {}
        monthly_data = []
        
        # Process each month
        for year_month in sorted(processed_df['Year-Month'].unique()):
            month_df = processed_df[processed_df['Year-Month'] == year_month]
            
            # Aggregate by author for this month
            month_stats = aggregate_by_author(month_df, metrics)
            
            # Get top N for this metric
            top_df = month_stats.sort_values(metric, ascending=False).head(top_n)
            
            if not top_df.empty:
                # Track winner (using first name only)
                winner_full = top_df.iloc[0]['Author']
                winner_first = winner_full.split()[0]
                wins[winner_first] = wins.get(winner_first, 0) + 1
                
                # Format month data - use first names only
                month_label = datetime.strptime(year_month, '%Y-%m').strftime('%Y-%m')
                authors = ', '.join([name.split()[0] for name in top_df['Author'].tolist()])
                values = ', '.join([format_number(val) for val in top_df[metric]])
                articles = ', '.join([str(val) for val in top_df['num_articles']])
                
                monthly_data.append(f"{month_label}  {authors} - {values} - {articles}")
        
        # Print wins summary
        print("Months as #1 ranked:")
        for author, count in sorted(wins.items(), key=lambda x: x[1], reverse=True):
            print(f"{author:<8}{count}")
        
        print()
        
        # Print monthly details
        for line in monthly_data:
            print(line)
        
        print()


@click.command()
@click.option('--monthly-datadir', type=click.Path(exists=True), help='Directory containing monthly CSV files')
@click.option('--range-datadir', type=click.Path(exists=True), help='Directory containing date range CSV files')
@click.option('--top-n-range', default=6, type=int, help='Number of top journalists to show for date range')
@click.option('--top-n-monthly', default=3, type=int, help='Number of top journalists to show per month')
@click.option('--ignore-authors', multiple=True, help='Authors to ignore in analysis')
def main(monthly_datadir, range_datadir, top_n_range, top_n_monthly, ignore_authors):
    """Generate journalist performance reports from Parse.ly data."""
    
    ignore_authors_set = set(ignore_authors)
    
    # Process date range data
    if range_datadir:
        print("Loading date range data...")
        range_df = load_csv_files(range_datadir)
        if not range_df.empty:
            print_date_range_report(range_df, top_n_range, METRICS, ignore_authors_set)
    
    # Process monthly data
    if monthly_datadir:
        print("\nLoading monthly data...")
        monthly_df = load_csv_files(monthly_datadir)
        if not monthly_df.empty:
            print_monthly_report(monthly_df, top_n_monthly, METRICS, ignore_authors_set)


if __name__ == '__main__':
    main()