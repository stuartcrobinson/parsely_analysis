#!/usr/bin/env python3
"""Generate monthly rankings of journalist performance metrics."""

import pandas as pd
from pathlib import Path
from collections import defaultdict
import click
from datetime import datetime
import os
import re
import time


def parse_authors(author_str):
    """Parse author string into list of cleaned names."""
    if pd.isna(author_str) or author_str.strip() == '':
        return []
    
    # Split by comma and clean whitespace
    authors = [a.strip() for a in author_str.split(',')]
    # Filter out empty strings
    return [a for a in authors if a]


def save_parquet_if_needed(csv_path, df):
    """Save DataFrame as Parquet if source was CSV and Parquet doesn't exist or is older."""
    csv_path = Path(csv_path)
    parquet_path = csv_path.with_suffix('.parquet')
    
    # Check if parquet exists and is newer
    if parquet_path.exists():
        csv_mtime = csv_path.stat().st_mtime
        parquet_mtime = parquet_path.stat().st_mtime
        
        if parquet_mtime > csv_mtime:
            print(f"Skipping Parquet save - {parquet_path.name} is newer than source CSV")
            return
    
    # Save as Parquet
    try:
        df.to_parquet(parquet_path, engine='pyarrow')
        print(f"Saved Parquet file: {parquet_path}")
    except Exception as e:
        print(f"ERROR: Failed to save Parquet file: {e}")
        raise


def analyze_monthly_metrics(df, ignored_authors):
    """Analyze journalist metrics by month with equal credit distribution."""
    
    # Group by year-month
    df['year_month'] = df['Publish date'].dt.to_period('M')
    
    # Initialize nested defaultdict for metrics by month
    monthly_metrics = defaultdict(lambda: {
        'views': defaultdict(float),
        'visitors': defaultdict(float),
        'social_refs': defaultdict(float),
        'new_visitors': defaultdict(float),
        'engaged_minutes': defaultdict(float),
        'article_count': defaultdict(int)
    })
    
    # Track articles by month and author for output
    articles_by_month = defaultdict(list)
    articles_by_month_author = defaultdict(lambda: defaultdict(list))
    
    # Process each article
    for _, row in df.iterrows():
        authors = parse_authors(row['Authors'])
        # Filter out ignored authors
        authors = [a for a in authors if a not in ignored_authors]
        
        if not authors:
            continue
            
        month = row['year_month']
        num_authors = len(authors)
        
        # Equal split for all metrics
        views_per_author = row['Views'] / num_authors
        visitors_per_author = row['Visitors'] / num_authors
        social_refs_per_author = row['Social refs'] / num_authors if pd.notna(row['Social refs']) else 0
        new_vis_per_author = row['New vis.'] / num_authors
        minutes_per_author = row['Engaged minutes'] / num_authors
        
        for author in authors:
            monthly_metrics[month]['views'][author] += views_per_author
            monthly_metrics[month]['visitors'][author] += visitors_per_author
            monthly_metrics[month]['social_refs'][author] += social_refs_per_author
            monthly_metrics[month]['new_visitors'][author] += new_vis_per_author
            monthly_metrics[month]['engaged_minutes'][author] += minutes_per_author
            monthly_metrics[month]['article_count'][author] += 1
            
            # Track articles for output
            articles_by_month_author[month][author].append(row)
        
        # Track articles by month (only once per article)
        articles_by_month[month].append(row)
    
    return monthly_metrics, articles_by_month, articles_by_month_author


def print_monthly_rankings_compact(monthly_metrics, metric_name, metric_key, top_n):
    """Print monthly rankings in compact format."""
    
    print(f"\n{'='*60}")
    print(f"MONTHLY RANKINGS: {metric_name.upper()}\n")
    
    # Sort months chronologically
    sorted_months = sorted(monthly_metrics.keys())
    
    for month in sorted_months:
        month_data = monthly_metrics[month]
        
        # Sort journalists by metric for this month
        sorted_authors = sorted(
            month_data[metric_key].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:top_n]
        
        if not sorted_authors:
            continue
            
        # Extract first names and values
        first_names = []
        values = []
        article_counts = []
        
        for author, value in sorted_authors:
            # Get first name (handle edge cases)
            first_name = author.split()[0] if author else "Unknown"
            first_names.append(first_name)
            values.append(f"{int(value)}")
            
            # Only add article counts if this isn't the article count metric itself
            if metric_key != 'article_count':
                article_counts.append(str(month_data['article_count'][author]))
        
        # Format output line
        names_str = ", ".join(first_names)
        values_str = ", ".join(values)
        
        if metric_key != 'article_count':
            articles_str = ", ".join(article_counts)
            print(f"{month}  {names_str} - {values_str} - {articles_str}")
        else:
            print(f"{month}  {names_str} - {values_str}")


def sanitize_filename(name):
    """Convert author name to safe filename."""
    # Replace problematic characters
    safe_name = re.sub(r'[^\w\s-]', '', name)
    safe_name = re.sub(r'[-\s]+', '_', safe_name)
    return safe_name.lower()


def get_all_top_authors(monthly_metrics, top_n):
    """Collect all authors who appeared in any top-N ranking."""
    top_authors = set()
    
    for month, month_data in monthly_metrics.items():
        for metric_key in ['views', 'visitors', 'social_refs', 'new_visitors', 'engaged_minutes', 'article_count']:
            sorted_authors = sorted(
                month_data[metric_key].items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:top_n]
            
            for author, _ in sorted_authors:
                top_authors.add(author)
    
    return top_authors


def save_month_csv(articles, output_path):
    """Save articles for a specific month to CSV."""
    if not articles:
        return
        
    # Convert to DataFrame if needed
    df = pd.DataFrame(articles)
    
    # Ensure year_month is string for CSV
    if 'year_month' in df.columns:
        df = df.drop('year_month', axis=1)
    
    df.to_csv(output_path, index=False)


def save_outputs(output_dir, after_date, top_n, monthly_metrics, articles_by_month, articles_by_month_author):
    """Save all analysis outputs to structured directory."""
    # Create timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    # Format after_date for dirname
    after_date_str = f"after-{after_date}" if after_date else "all-dates"
    
    # Create run directory
    run_dirname = f"{timestamp}_{after_date_str}_top-{top_n}"
    inneroutdir = os.path.join(output_dir, 'monthly_auth_rank')
    runoutdir = os.path.join(inneroutdir, run_dirname)
    authors_dir = os.path.join(runoutdir, 'authors')
    
    # Create directories
    os.makedirs(runoutdir, exist_ok=True)
    os.makedirs(authors_dir, exist_ok=True)
    
    print(f"\nSaving outputs to: {runoutdir}")
    
    # Save month-level CSVs
    for month, articles in articles_by_month.items():
        month_str = str(month)
        month_file = os.path.join(runoutdir, f"{month_str}.csv")
        save_month_csv(articles, month_file)
        print(f"  Saved {len(articles)} articles to {month_str}.csv")
    
    # Get top authors
    top_authors = get_all_top_authors(monthly_metrics, top_n)
    print(f"\nFound {len(top_authors)} unique top-{top_n} authors across all metrics")
    
    # Save author-specific CSVs by month
    for month, month_authors in articles_by_month_author.items():
        month_str = str(month)
        month_authors_dir = os.path.join(authors_dir, month_str)
        os.makedirs(month_authors_dir, exist_ok=True)
        
        # Only save CSVs for top authors
        saved_count = 0
        for author, articles in month_authors.items():
            if author in top_authors:
                author_filename = f"{sanitize_filename(author)}.csv"
                author_path = os.path.join(month_authors_dir, author_filename)
                save_month_csv(articles, author_path)
                saved_count += 1
        
        if saved_count > 0:
            print(f"  Saved {saved_count} author CSVs for {month_str}")


def print_monthly_rankings(monthly_metrics, metric_name, metric_key, top_n):
    """Print monthly rankings for a specific metric."""
    
    print(f"\n{'='*60}")
    print(f"MONTHLY RANKINGS: {metric_name.upper()}")
    print(f"{'='*60}")
    
    # Sort months chronologically
    sorted_months = sorted(monthly_metrics.keys())
    
    for month in sorted_months:
        month_data = monthly_metrics[month]
        
        # Count total articles and unique journalists for this month
        total_articles = sum(month_data['article_count'].values())
        active_journalists = len(month_data['article_count'])
        
        print(f"\n{month} ({total_articles} articles, {active_journalists} journalists)")
        print(f"{'-'*60}")
        
        # Sort journalists by metric for this month
        sorted_authors = sorted(
            month_data[metric_key].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:top_n]
        
        # Print rankings
        for rank, (author, value) in enumerate(sorted_authors, 1):
            articles = month_data['article_count'][author]
            
            # Format value based on metric type
            if metric_key in ['views', 'visitors', 'social_refs', 'new_visitors']:
                value_str = f"{value:>12,.0f}"
            elif metric_key == 'engaged_minutes':
                value_str = f"{value:>12,.0f}"
            elif metric_key == 'article_count':
                value_str = f"{value:>12}"
                
            # Abbreviated unit labels
            unit = {
                'views': 'views',
                'visitors': 'visitors',
                'social_refs': 'social',
                'new_visitors': 'new vis',
                'engaged_minutes': 'min',
                'article_count': 'articles'
            }[metric_key]
            
            # Only show article count in parentheses if this isn't the article count metric
            if metric_key != 'article_count':
                print(f"{rank:>2}. {author:<30} {value_str} {unit:<8} ({articles} articles)")
            else:
                print(f"{rank:>2}. {author:<30} {value_str} {unit:<8}")


@click.command()
@click.argument('parquet_file', type=click.Path(exists=True))
@click.option('--top-n', default=5, help='Number of top journalists to show per month (default: 5)')
@click.option('--after-date', default=None, help='Only include articles published after this date (YYYY-MM-DD)')
@click.option('--ignore-authors', '-i', multiple=True, help='Authors to ignore (can be specified multiple times)')
@click.option('--format', type=click.Choice(['verbose', 'compact']), default='verbose', help='Output format style')
@click.option('--output-dir', default=None, help='Directory to save analysis outputs (optional)')
@click.option('--save-parquet', is_flag=True, help='Save CSV input as Parquet file for faster future processing')
def main(parquet_file, top_n, after_date, ignore_authors, format, output_dir, save_parquet):
    """Generate monthly rankings of journalist performance metrics."""
    
    print(f"Loading data from: {parquet_file}")
    is_csv = parquet_file.endswith('.csv')
    df = pd.read_csv(parquet_file) if is_csv else pd.read_parquet(parquet_file)
    
    print(f"Total articles loaded: {len(df)}")
    
    # Save as Parquet if requested and input was CSV
    if save_parquet and is_csv:
        save_parquet_if_needed(parquet_file, df)
    
    # Handle date parsing
    try:
        df['Publish date'] = pd.to_datetime(df['Publish date'], errors='coerce')
        df = df.dropna(subset=['Publish date'])
        print(f"Date range: {df['Publish date'].min().date()} to {df['Publish date'].max().date()}")
    except Exception as e:
        print(f"Error parsing dates: {e}")
        return
    
    # Apply date filter if specified
    if after_date:
        try:
            filter_date = pd.to_datetime(after_date)
            print(f"Filtering articles after: {filter_date.date()}")
            df = df[df['Publish date'] > filter_date]
            print(f"Articles after filtering: {len(df)}")
            
            if len(df) == 0:
                print("No articles found after the specified date!")
                return
        except Exception as e:
            print(f"Error parsing date filter '{after_date}': {e}")
            return
    
    # Convert ignore_authors tuple to set for faster lookup
    ignored_authors = set(ignore_authors) if ignore_authors else set()
    if ignored_authors:
        print(f"Ignoring authors: {', '.join(sorted(ignored_authors))}")
    
    print(f"\nAnalyzing {len(df)} articles...")
    
    # Analyze metrics by month
    monthly_metrics, articles_by_month, articles_by_month_author = analyze_monthly_metrics(df, ignored_authors)
    
    # Print rankings for each metric
    metrics_to_display = [
        ('Page Views', 'views'),
        ('Visitors', 'visitors'),
        ('Social Referrals', 'social_refs'),
        ('New Visitors', 'new_visitors'),
        ('Engaged Minutes', 'engaged_minutes'),
        ('Articles Published', 'article_count')
    ]
    
    for metric_name, metric_key in metrics_to_display:
        if format == 'compact':
            print_monthly_rankings_compact(monthly_metrics, metric_name, metric_key, top_n)
        else:
            print_monthly_rankings(monthly_metrics, metric_name, metric_key, top_n)
    
    # Save outputs if directory specified
    if output_dir:
        save_outputs(output_dir, after_date, top_n, monthly_metrics, articles_by_month, articles_by_month_author)


if __name__ == '__main__':
    main()