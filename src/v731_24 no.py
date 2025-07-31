# https://claude.ai/chat/8b95e0af-6ab4-4022-a4d2-d4cf09313404


def process_data(df, ignore_authors):
    """Process data and attribute metrics to individual authors."""
    # First, filter out rows with no authors
    df_with_authors = df[df['Authors'].notna()].copy()
    
    if df_with_authors.empty:
        return pd.DataFrame()
    
    # Deduplicate articles by URL before processing
    # Keep the first occurrence of each article (#!/usr/bin/env python
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
    'social_refs': 'Social refs',
    'engaged_minutes': 'Engaged minutes',
    'avg_engaged_minutes': 'Avg. minutes',
    'publications': 'Publish date'  # Special handling needed
}

METRIC_DISPLAY_NAMES = {
    'views': 'VIEWS',
    'social_refs': 'SOCIAL REFS',
    'engaged_minutes': 'ENGAGED MINUTES',
    'avg_engaged_minutes': 'AVG ENGAGED MINUTES',
    'publications': 'PUBLICATIONS'
}


def parse_authors(authors_str):
    """Parse authors string and return list of individual authors."""
    if pd.isna(authors_str):
        return []
    # Split by comma and strip whitespace
    return [author.strip() for author in authors_str.split(',')]


def parse_date_range_from_filename(filename):
    """Extract date range from filename."""
    # Remove .csv and any suffix after the domain
    base_name = filename.replace('.csv', '')
    
    # Look for date pattern: MMM-DD-YYYY-MMM-DD-YYYY
    import re
    date_pattern = r'([A-Za-z]{3})-(\d{2})-(\d{4})-([A-Za-z]{3})-(\d{2})-(\d{4})'
    match = re.search(date_pattern, base_name)
    
    if match:
        try:
            start_month, start_day, start_year = match.group(1), match.group(2), match.group(3)
            end_month, end_day, end_year = match.group(4), match.group(5), match.group(6)
            
            start_date = datetime.strptime(f"{start_month} {start_day} {start_year}", "%b %d %Y")
            end_date = datetime.strptime(f"{end_month} {end_day} {end_year}", "%b %d %Y")
            
            return start_date, end_date
        except:
            pass
    
    return None, None


def load_csv_files(directory):
    """Load all CSV files from a directory."""
    path = Path(directory)
    csv_files = list(path.glob('*.csv'))
    
    dataframes = []
    file_metadata = []
    
    for csv_file in tqdm(csv_files, desc=f"Loading files from {directory}"):
        # Get file creation time
        file_stat = csv_file.stat()
        creation_time = datetime.fromtimestamp(file_stat.st_mtime)
        file_metadata.append({
            'filename': csv_file.name,
            'created': creation_time.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Read only the columns we need to save memory
        needed_cols = ['URL', 'Title', 'Publish date', 'Authors', 'Section', 'Tags',
                      'Views', 'Engaged minutes', 'Social refs']
        
        try:
            df = pd.read_csv(csv_file, usecols=lambda x: x in needed_cols)
        except:
            # If column selection fails, read all columns
            df = pd.read_csv(csv_file)
        
        df['source_file'] = csv_file.name
        
        # Parse date range from filename
        start_date, end_date = parse_date_range_from_filename(csv_file.name)
        if start_date and end_date:
            df['file_start_date'] = start_date
            df['file_end_date'] = end_date
        
        # Extract month/year for monthly reports
        if start_date:
            df['report_date'] = pd.Timestamp(start_date.year, start_date.month, 1)
        else:
            df['report_date'] = pd.NaT
        
        dataframes.append(df)
    
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df, file_metadata
    return pd.DataFrame(), file_metadata


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


def aggregate_by_author(df, metrics, start_date=None, end_date=None):
    """Aggregate metrics by author."""
    # Build aggregation dictionary more efficiently
    agg_funcs = {
        'URL': 'count',  # For counting articles (used for other metrics)
        'Views': 'sum',
        'Social refs': 'sum',
        'Engaged minutes': 'sum'
    }
    
    # Filter to only the aggregations we need
    agg_funcs = {k: v for k, v in agg_funcs.items() if k in df.columns}
    
    # Group by author and aggregate - NO deduplication for other metrics
    author_stats = df.groupby('Author', as_index=False).agg(agg_funcs)
    
    # Calculate average engaged minutes
    if 'Engaged minutes' in author_stats.columns and 'URL' in author_stats.columns:
        author_stats['avg_engaged_minutes'] = (
            author_stats['Engaged minutes'] / author_stats['URL']
        ).round(1)
    else:
        author_stats['avg_engaged_minutes'] = 0
    
    # For publications, count unique articles (Title + Date) published within the date range
    if start_date and end_date and 'Publish date' in df.columns and 'Title' in df.columns:
        # Convert publish dates
        df_copy = df.copy()
        df_copy['publish_dt'] = pd.to_datetime(df_copy['Publish date'], errors='coerce')
        
        # Filter to articles published within the date range
        published_in_range = df_copy[
            (df_copy['publish_dt'] >= start_date) & 
            (df_copy['publish_dt'] <= end_date) &
            (df_copy['publish_dt'].notna())
        ].copy()
        
        if not published_in_range.empty:
            # Create unique article identifier: Title + Date (without time)
            published_in_range['article_date'] = published_in_range['publish_dt'].dt.date
            published_in_range['unique_article'] = (
                published_in_range['Title'].astype(str) + '_' + 
                published_in_range['article_date'].astype(str)
            )
            
            # Count unique publications per author
            pub_counts = (published_in_range.groupby('Author')['unique_article']
                         .nunique()
                         .reset_index()
                         .rename(columns={'unique_article': 'publications'}))
            
            # Merge with main stats
            author_stats = author_stats.merge(pub_counts, on='Author', how='left')
            author_stats['publications'] = author_stats['publications'].fillna(0).astype(int)
        else:
            author_stats['publications'] = 0
    else:
        author_stats['publications'] = 0
    
    # Rename columns to match our metric names
    author_stats = author_stats.rename(columns={
        'URL': 'num_articles',
        'Views': 'views',
        'Social refs': 'social_refs',
        'Engaged minutes': 'engaged_minutes'
    })
    
    # Ensure all metrics exist, fill with 0 if missing
    for metric in metrics.keys():
        if metric not in author_stats.columns:
            author_stats[metric] = 0
    
    # Convert to integers (except avg_engaged_minutes which should be float)
    numeric_cols = ['views', 'social_refs', 'engaged_minutes', 'num_articles', 'publications']
    for col in numeric_cols:
        if col in author_stats.columns:
            author_stats[col] = author_stats[col].fillna(0).astype(int)
    
    return author_stats


def format_number(num):
    """Format number with thousands separator."""
    if isinstance(num, float):
        # For averages, show one decimal place
        return f"{num:,.1f}"
    return f"{int(num):,}" if pd.notna(num) else "0"


def print_date_range_report(df, top_n, metrics, ignore_authors, file_metadata):
    """Print report for date range data."""
    # Print file metadata
    print("=" * 62)
    print("INPUT FILES")
    print("=" * 62)
    for file_info in file_metadata:
        print(f"File: {file_info['filename']}")
        print(f"Created: {file_info['created']}")
    print()
    
    # Process data
    processed_df = process_data(df, ignore_authors)
    
    if processed_df.empty:
        print("No data to report")
        return
    
    # Get date range from the file data
    start_date = None
    end_date = None
    if 'file_start_date' in df.columns and 'file_end_date' in df.columns:
        start_date = df['file_start_date'].iloc[0]
        end_date = df['file_end_date'].iloc[0]
        
        if pd.notna(start_date) and pd.notna(end_date):
            print(f"{start_date.strftime('%B %-d, %Y')} - {end_date.strftime('%B %-d, %Y')}\n")
    
    # Aggregate by author with date range for publications
    author_stats = aggregate_by_author(processed_df, metrics, start_date, end_date)
    
    # Print top journalists for each metric
    for metric, col_name in metrics.items():
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
            print(f"{'Rank':<6}{'Journalist':<30}{display_name:<18}")
            print("-" * 62)
            
            for i, row in sorted_df.iterrows():
                rank = sorted_df.index.get_loc(i) + 1
                print(f"{rank:<6}{row['Author']:<30}{format_number(row[metric]):<18}")
            
            print()


def print_monthly_report(df, top_n, metrics, ignore_authors, file_metadata):
    """Print report for monthly data."""
    # Print file metadata
    print("=" * 62)
    print("INPUT FILES")
    print("=" * 62)
    for file_info in file_metadata:
        print(f"File: {file_info['filename']}")
        print(f"Created: {file_info['created']}")
    print()
    
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
            
            # Get date range from file data for this month
            month_start = None
            month_end = None
            if 'file_start_date' in month_df.columns and 'file_end_date' in month_df.columns:
                month_start = month_df['file_start_date'].iloc[0]
                month_end = month_df['file_end_date'].iloc[0]
            
            # Aggregate by author for this month with proper date range
            month_stats = aggregate_by_author(month_df, metrics, month_start, month_end)
            
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
                
                # For publications metric, show publications count; for others show num_articles
                if metric == 'publications':
                    articles = ', '.join([str(val) for val in top_df['publications']])
                else:
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
        range_df, range_metadata = load_csv_files(range_datadir)
        if not range_df.empty:
            print_date_range_report(range_df, top_n_range, METRICS, ignore_authors_set, range_metadata)
    
    # Process monthly data
    if monthly_datadir:
        print("\nLoading monthly data...")
        monthly_df, monthly_metadata = load_csv_files(monthly_datadir)
        if not monthly_df.empty:
            print_monthly_report(monthly_df, top_n_monthly, METRICS, ignore_authors_set, monthly_metadata)


if __name__ == '__main__':
    main()