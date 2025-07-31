#!/usr/bin/env python3
"""
Analyze journalist performance metrics from article data.
"""

import pandas as pd
import numpy as np
import click
from datetime import datetime
from collections import defaultdict
import sys


def parse_authors(authors_str):
    """Split comma-separated authors string into list."""
    if pd.isna(authors_str):
        return []
    return [author.strip() for author in authors_str.split(',')]


def get_first_name(full_name):
    """Extract first name from full name."""
    return full_name.split()[0] if full_name else ""


def calculate_staff_average(df, metric, staff_names):
    """Calculate average performance for specified staff writers."""
    staff_totals = {}
    for name in staff_names:
        author_rows = df[df['Authors'].str.contains(name, na=False)]
        staff_totals[name] = author_rows[metric].sum()
    
    values = [v for v in staff_totals.values() if v > 0]
    return np.mean(values) if values else 0


@click.command()
@click.option('--input-csv', required=True, help='Path to input CSV file')
@click.option('--start-date', required=True, help='Filter articles published on or after this date (YYYY-MM-DD)')
@click.option('--top-n', type=int, required=True, help='Number of top journalists to show')
@click.option('--ignore-authors', multiple=True, help='Authors to exclude from analysis')
def analyze_journalists(input_csv, start_date, top_n, ignore_authors):
    """Analyze journalist performance across multiple metrics."""
    
    # Display run metadata
    click.echo("="*60)
    click.echo("JOURNALIST PERFORMANCE ANALYSIS")
    click.echo("="*60)
    click.echo(f"Input file: {input_csv}")
    click.echo(f"Start date: {start_date}")
    click.echo(f"Top N: {top_n}")
    if ignore_authors:
        click.echo(f"Ignored authors: {', '.join(ignore_authors)}")
    else:
        click.echo("Ignored authors: None")
    click.echo("="*60)
    
    # Parse start date
    try:
        start_dt = pd.to_datetime(start_date)
    except:
        click.echo(f"Error: Invalid date format '{start_date}'. Use YYYY-MM-DD")
        sys.exit(1)
    
    # Read CSV
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        click.echo(f"Error: {input_csv} not found")
        sys.exit(1)
    
    # Convert publish date to datetime
    df['Publish date'] = pd.to_datetime(df['Publish date'])
    
    # Filter by date
    df = df[df['Publish date'] >= start_dt]
    
    if df.empty:
        click.echo(f"No articles found after {start_date}")
        sys.exit(0)
    
    # Add month column
    df['Month'] = df['Publish date'].dt.to_period('M')
    
    # Define metrics
    metrics = {
        'views': 'Views',
        'visitors': 'Visitors', 
        'social_refs': 'Social refs',
        'new_visitors': 'New vis.',
        'engaged_minutes': 'Engaged minutes',
        'article_count': None  # Special case - count articles
    }
    
    # Staff writers for comparison
    staff_writers = ['Chloe Courtney Bohl', 'Chase Pellegrini de Paur', 'Justin Laidlaw']
    
    # Process each metric
    for metric_name, column_name in metrics.items():
        click.echo(f"\n{'='*60}")
        click.echo(f"{metric_name.upper()} ANALYSIS")
        click.echo('='*60)
        
        # Create author-article mapping with full credit to each author
        author_data = []
        for _, row in df.iterrows():
            authors = parse_authors(row['Authors'])
            for author in authors:
                if author and author not in ignore_authors:
                    author_row = row.copy()
                    author_row['Author'] = author
                    author_data.append(author_row)
        
        if not author_data:
            click.echo("No valid authors found after filtering")
            continue
            
        author_df = pd.DataFrame(author_data)
        
        # 1. Monthly winners
        click.echo(f"\n--- Monthly Top {top_n} ---")
        
        months = sorted(df['Month'].unique())
        month_winners = defaultdict(int)
        month_ties = defaultdict(int)  # Track tied wins
        
        for month in months:
            month_data = author_df[author_df['Month'] == month]
            
            if month_data.empty:
                click.echo(f"{month}: No articles")
                continue
            
            if metric_name == 'article_count':
                month_totals = month_data.groupby('Author').size()
            else:
                month_totals = month_data.groupby('Author')[column_name].sum()
            
            top_authors = month_totals.nlargest(top_n)
            
            # Track winners and ties
            if not top_authors.empty:
                max_value = top_authors.iloc[0]
                winners = top_authors[top_authors == max_value].index.tolist()
                for winner in winners:
                    month_winners[winner] += 1
                    if len(winners) > 1:
                        month_ties[winner] += 1
            
            # Format output with tie handling
            formatted = []
            if len(winners) > 1:
                # Handle tied winners
                winner_names = [get_first_name(w) for w in winners]
                formatted.append(f"[{', '.join(winner_names)}] TIE ({int(max_value)})")
                # Add remaining authors
                remaining = top_authors[top_authors < max_value].head(top_n - len(winners))
                for author, value in remaining.items():
                    formatted.append(f"{get_first_name(author)} ({int(value)})")
            else:
                # No ties, format normally
                for author, value in top_authors.items():
                    formatted.append(f"{get_first_name(author)} ({int(value)})")
            
            click.echo(f"{month}: {', '.join(formatted)}")
        
        # Show months won summary
        if month_winners:
            click.echo(f"\nMonths Won:")
            sorted_winners = sorted(month_winners.items(), key=lambda x: x[1], reverse=True)
            max_name_len = max(len(get_first_name(author)) for author, _ in sorted_winners)
            for author, count in sorted_winners:
                first_name = get_first_name(author)
                ties = month_ties.get(author, 0)
                tie_str = f" ({ties} tied)" if ties > 0 else ""
                click.echo(f"{first_name:<{max_name_len}}: {count}{tie_str}")
        
        # 2. Overall top performers
        click.echo(f"\n--- Overall Top {top_n} ---")
        
        if metric_name == 'article_count':
            overall_totals = author_df.groupby('Author').size()
        else:
            overall_totals = author_df.groupby('Author')[column_name].sum()
        
        top_overall = overall_totals.nlargest(top_n)
        
        # Get max length for alignment
        max_name_len = max(len(get_first_name(author)) for author in top_overall.index)
        max_value_len = max(len(str(int(value))) for value in top_overall.values)
        
        for i, (author, value) in enumerate(top_overall.items()):
            first_name = get_first_name(author)
            click.echo(f"{i+1}. {first_name:<{max_name_len}}: {int(value):>{max_value_len}}")
        
        # Calculate percentage differences
        if len(top_overall) >= 2:
            top_value = top_overall.iloc[0]
            second_value = top_overall.iloc[1]
            pct_diff_second = ((top_value - second_value) / second_value * 100) if second_value > 0 else float('inf')
            
            click.echo(f"\nTop performer vs 2nd: +{pct_diff_second:.1f}%")
        
        # Staff writer average
        if metric_name == 'article_count':
            staff_counts = []
            for name in staff_writers:
                count = len(author_df[author_df['Author'] == name])
                staff_counts.append(count)
            staff_avg = np.mean(staff_counts) if staff_counts else 0
        else:
            staff_avg = calculate_staff_average(author_df, column_name, staff_writers)
        
        if staff_avg > 0 and len(top_overall) > 0:
            top_value = top_overall.iloc[0]
            pct_diff_staff = ((top_value - staff_avg) / staff_avg * 100)
            click.echo(f"Top performer vs staff avg: +{pct_diff_staff:.1f}%")
            click.echo(f"(Staff avg among {', '.join([get_first_name(n) for n in staff_writers])}: {int(staff_avg)})")


if __name__ == '__main__':
    analyze_journalists()