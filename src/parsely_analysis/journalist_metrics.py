#!/usr/bin/env python3
"""Analyze journalist performance metrics from parquet file."""

import pandas as pd
from pathlib import Path
from collections import defaultdict
import click
from datetime import datetime
import os
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


def save_analysis_data(df, after_date=None, output_dir=None):
    """Save the data used for analysis to a CSV file with descriptive naming."""
    # Skip saving if no output directory specified
    if output_dir is None:
        return None
    
    # Create output directory and journalist_metrics subdirectory if they don't exist
    base_output_dir = Path(output_dir)
    base_output_dir.mkdir(parents=True, exist_ok=True)
    
    analysis_dir = base_output_dir / "journalist_metrics"
    analysis_dir.mkdir(exist_ok=True)
    
    # Generate filename with timestamp and filter info
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if after_date:
        # Include the filter date in filename
        filter_date_str = pd.to_datetime(after_date).strftime("%Y%m%d")
        filename = f"journalist_analysis_after_{filter_date_str}_run_{timestamp}.csv"
    else:
        filename = f"journalist_analysis_all_dates_run_{timestamp}.csv"
    
    filepath = analysis_dir / filename
    
    # Save the dataframe
    df.to_csv(filepath, index=False)
    
    print(f"\nSaved analysis data to: {filepath}")
    print(f"  Total articles saved: {len(df)}")
    
    return filepath


def analyze_journalists(df):
    """Analyze journalist metrics with equal credit distribution."""
    
    # Initialize metric dictionaries
    metrics = {
        'views': defaultdict(float),
        'visitors': defaultdict(float),
        'social_refs': defaultdict(float),
        'new_visitors': defaultdict(float),
        'engaged_minutes': defaultdict(float),
        'article_count': defaultdict(int),
        'solo_articles': defaultdict(int),
        'collab_articles': defaultdict(int)
    }
    
    # Process each article
    for _, row in df.iterrows():
        authors = parse_authors(row['Authors'])
        if not authors:
            continue
            
        num_authors = len(authors)
        
        # Equal split for all metrics
        views_per_author = row['Views'] / num_authors
        visitors_per_author = row['Visitors'] / num_authors
        social_refs_per_author = row['Social refs'] / num_authors if pd.notna(row['Social refs']) else 0
        new_vis_per_author = row['New vis.'] / num_authors
        minutes_per_author = row['Engaged minutes'] / num_authors
        
        for author in authors:
            metrics['views'][author] += views_per_author
            metrics['visitors'][author] += visitors_per_author
            metrics['social_refs'][author] += social_refs_per_author
            metrics['new_visitors'][author] += new_vis_per_author
            metrics['engaged_minutes'][author] += minutes_per_author
            metrics['article_count'][author] += 1
            
            if num_authors == 1:
                metrics['solo_articles'][author] += 1
            else:
                metrics['collab_articles'][author] += 1
    
    return metrics


def print_top_journalists(metrics, metric_name, metric_key, top_n=20):
    """Print top journalists for a specific metric."""
    # Sort by metric
    sorted_authors = sorted(metrics[metric_key].items(), key=lambda x: x[1], reverse=True)
    
    print(f"\n{'='*60}")
    print(f"TOP {top_n} JOURNALISTS BY {metric_name.upper()}")
    print(f"{'='*60}")
    print(f"{'Rank':<5} {'Journalist':<30} {metric_name:<15} {'Articles':<10} {'Solo/Collab'}")
    print(f"{'-'*60}")
    
    for i, (author, value) in enumerate(sorted_authors[:top_n], 1):
        articles = metrics['article_count'][author]
        solo = metrics['solo_articles'][author]
        collab = metrics['collab_articles'][author]
        
        print(f"{i:<5} {author:<30} {value:>15.0f} {articles:>10} {solo:>5}/{collab:<5}")


@click.command()
@click.argument('parquet_file', type=click.Path(exists=True))
@click.option('--top-n', default=20, help='Number of top journalists to show (default: 20)')
@click.option('--after-date', default=None, help='Only include articles published after this date (YYYY-MM-DD)')
@click.option('--output-dir', default=None, help='Output directory for journalist_metrics folder (if specified, saves CSV output)')
@click.option('--save-parquet', is_flag=True, help='Save CSV input as Parquet file for faster future processing')
def main(parquet_file, top_n, after_date, output_dir, save_parquet):
    """Analyze journalist metrics from parquet file."""
    
    print(f"Loading data from: {parquet_file}")
    is_csv = parquet_file.endswith('.csv')
    df = pd.read_csv(parquet_file) if is_csv else pd.read_parquet(parquet_file)
    
    print(f"Total articles loaded: {len(df)}")
    
    # Save as Parquet if requested and input was CSV
    if save_parquet and is_csv:
        save_parquet_if_needed(parquet_file, df)
    
    # Handle date parsing more robustly
    try:
        df['Publish date'] = pd.to_datetime(df['Publish date'], errors='coerce')
        valid_dates = df['Publish date'].dropna()
        if len(valid_dates) > 0:
            print(f"Date range: {valid_dates.min().date()} to {valid_dates.max().date()}")
            print(f"Articles with invalid dates: {len(df) - len(valid_dates)}")
        else:
            print("Warning: No valid dates found")
    except Exception as e:
        print(f"Warning: Could not parse dates - {e}")
    
    # Apply date filter if specified
    if after_date:
        try:
            filter_date = pd.to_datetime(after_date)
            print(f"\nFiltering articles published after: {filter_date.date()}")
            
            # Filter dataframe
            original_count = len(df)
            df = df[df['Publish date'] > filter_date]
            filtered_count = len(df)
            
            print(f"Articles after filtering: {filtered_count} (removed {original_count - filtered_count})")
            
            if filtered_count == 0:
                print("Warning: No articles found after the specified date!")
                return
                
        except Exception as e:
            print(f"Error parsing date filter '{after_date}': {e}")
            print("Please use YYYY-MM-DD format (e.g., 2024-01-01)")
            return
    
    print(f"\nAnalyzing {len(df)} articles...")
    
    # Save the data being analyzed
    saved_file = save_analysis_data(df, after_date, output_dir)
    
    # Analyze metrics
    metrics = analyze_journalists(df)
    
    # Print summary statistics
    total_authors = len(metrics['article_count'])
    authors_with_collabs = sum(1 for a in metrics['collab_articles'] if metrics['collab_articles'][a] > 0)
    
    print(f"\nTotal unique journalists: {total_authors}")
    print(f"Journalists with collaborations: {authors_with_collabs} ({authors_with_collabs/total_authors*100:.1f}%)")
    
    # Print top lists
    print_top_journalists(metrics, "Page Views", "views", top_n)
    print_top_journalists(metrics, "Visitors", "visitors", top_n)
    print_top_journalists(metrics, "Social Referrals", "social_refs", top_n)
    print_top_journalists(metrics, "New Visitors", "new_visitors", top_n)
    print_top_journalists(metrics, "Engaged Minutes", "engaged_minutes", top_n)
    
    # # Additional analysis: collaboration patterns
    # print(f"\n{'='*60}")
    # print("COLLABORATION ANALYSIS")
    # print(f"{'='*60}")
    
    # collab_ratios = []
    # for author in metrics['article_count']:
    #     total = metrics['article_count'][author]
    #     collab = metrics['collab_articles'][author]
    #     if total > 5:  # Only include authors with >5 articles
    #         collab_ratios.append((author, collab/total, total, collab))
    
    # collab_ratios.sort(key=lambda x: x[1], reverse=True)
    
    # print(f"{'Journalist':<30} {'Collab %':<10} {'Total':<8} {'Collabs'}")
    # print(f"{'-'*60}")
    # for author, ratio, total, collabs in collab_ratios[:10]:
    #     print(f"{author:<30} {ratio*100:>8.1f}% {total:>8} {collabs:>8}")
    
    # Reminder about saved data
    if output_dir:
        print(f"\n{'='*60}")
        print(f"Analysis data has been saved to: {output_dir}/journalist_metrics/")
        print(f"{'='*60}")


if __name__ == '__main__':
    main()