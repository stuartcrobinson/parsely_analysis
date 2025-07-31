#!/usr/bin/env python3
"""Analyze journalist performance metrics from Parsely data."""

import pandas as pd
import numpy as np
import click
from datetime import datetime
from collections import defaultdict
import sys


# Column mapping from CSV headers to metric names
METRIC_COLUMNS = {
    'views': 'Views',
    'visitors': 'Visitors', 
    'social_refs': 'Social refs',
    'new_visitors': 'New vis.',
    'engaged_minutes': 'Engaged minutes'
}


def parse_authors(author_string):
    """Parse comma-separated authors, return full names."""
    if pd.isna(author_string):
        return []
    
    authors = []
    for author in author_string.split(','):
        author = author.strip()
        if author:
            authors.append(author)
    return authors


def aggregate_journalist_metrics(df, ignored_authors):
    """Aggregate metrics by journalist."""
    journalist_data = defaultdict(lambda: defaultdict(float))
    journalist_articles = defaultdict(int)
    
    for _, row in df.iterrows():
        authors = parse_authors(row['Authors'])
        
        for author in authors:
            # Skip ignored authors
            if author in ignored_authors:
                continue
                
            # Extract first name for display
            first_name = author.split()[0] if author.split() else author
            
            journalist_articles[first_name] += 1
            
            # Aggregate each metric
            for metric_key, csv_column in METRIC_COLUMNS.items():
                value = row.get(csv_column, 0)
                if pd.isna(value):
                    value = 0
                journalist_data[first_name][metric_key] += value
    
    return journalist_data, journalist_articles


def aggregate_monthly_metrics(df, ignored_authors):
    """Aggregate metrics by journalist and month."""
    monthly_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    monthly_articles = defaultdict(lambda: defaultdict(int))
    
    for _, row in df.iterrows():
        authors = parse_authors(row['Authors'])
        
        # Extract year-month from publish date
        publish_date = pd.to_datetime(row['Publish date'])
        year_month = publish_date.strftime('%Y-%m')
        
        for author in authors:
            # Skip ignored authors
            if author in ignored_authors:
                continue
                
            # Extract first name for display
            first_name = author.split()[0] if author.split() else author
            
            monthly_articles[year_month][first_name] += 1
            
            # Aggregate each metric
            for metric_key, csv_column in METRIC_COLUMNS.items():
                value = row.get(csv_column, 0)
                if pd.isna(value):
                    value = 0
                monthly_data[year_month][first_name][metric_key] += value
    
    return monthly_data, monthly_articles


def get_top_n_overall(journalist_data, journalist_articles, metric, n):
    """Get top N journalists for a specific metric overall."""
    # Sort journalists by metric value
    sorted_journalists = sorted(
        journalist_data.items(),
        key=lambda x: x[1][metric],
        reverse=True
    )
    
    # Get top N
    top_n = sorted_journalists[:n]
    
    return [(name, data[metric], journalist_articles[name]) for name, data in top_n]


def get_monthly_winners(monthly_data, monthly_articles, metric, n):
    """Get top N journalists for each month for a specific metric."""
    monthly_winners = {}
    
    for year_month in sorted(monthly_data.keys()):
        month_data = monthly_data[year_month]
        
        # Sort journalists by metric value for this month
        sorted_journalists = sorted(
            month_data.items(),
            key=lambda x: x[1][metric],
            reverse=True
        )
        
        # Get top N with their values and article counts
        top_n = []
        for name, data in sorted_journalists[:n]:
            value = data[metric]
            articles = monthly_articles[year_month][name]
            top_n.append((name, value, articles))
        
        monthly_winners[year_month] = top_n
    
    return monthly_winners


def count_monthly_wins(monthly_winners):
    """Count how many months each journalist won."""
    win_counts = defaultdict(int)
    
    for year_month, winners in monthly_winners.items():
        if not winners:
            continue
            
        # Get the highest value (first place)
        top_value = winners[0][1]
        
        # Count all journalists with this value as winners (handles ties)
        for name, value, _ in winners:
            if value == top_value:
                win_counts[name] += 1
    
    return win_counts


def format_monthly_ranking_line(year_month, winners):
    """Format a single monthly ranking line with proper tie notation."""
    if not winners:
        return f"{year_month}  [No data]"
    
    # Group by value to detect ties
    value_groups = defaultdict(list)
    for name, value, articles in winners:
        value_groups[value].append((name, articles))
    
    # Get sorted values
    sorted_values = sorted(value_groups.keys(), reverse=True)
    
    # Build the output - process in order of rank
    names_in_order = []
    values_in_order = []
    articles_in_order = []
    
    rank = 0
    for value in sorted_values:
        journalists = value_groups[value]
        rank += 1
        
        # Only show top N ranks
        if rank > len(winners):
            break
            
        if len(journalists) > 1 and rank == 1:  # Only mark ties in first place
            # This is a tie for first place
            tied_names = [j[0] for j in journalists]
            names_in_order.append(f"[{', '.join(tied_names)}] TIE")
        else:
            # Not a tie for first, or not first place
            for name, _ in journalists:
                names_in_order.append(name)
        
        # Add values and article counts
        for _, articles in journalists:
            values_in_order.append(str(int(value)))
            articles_in_order.append(str(articles))
    
    # Only show top N entries
    names_to_show = names_in_order[:len(winners)]
    values_to_show = values_in_order[:len(winners)]
    articles_to_show = articles_in_order[:len(winners)]
    
    # Format: "2024-07  Lena, Chase, Justin - 85814, 13545, 12314 - 3, 7, 9"
    return f"{year_month}  {', '.join(names_to_show)} - {', '.join(values_to_show)} - {', '.join(articles_to_show)}"


def print_results(journalist_data, journalist_articles, monthly_data, monthly_articles, 
                  top_n, input_file, filter_date, ignored_authors):
    """Print analysis results in the specified format."""
    
    print("=" * 62)
    print("JOURNALIST PERFORMANCE ANALYSIS")
    print("=" * 62)
    print(f"Input file: {input_file}")
    print(f"Parsely data filtered by publication date: {filter_date}")
    print(f"Top N: {top_n}")
    print(f"Ignored authors: {', '.join(ignored_authors)}")
    print("Parsely metrics:")
    for metric in METRIC_COLUMNS.keys():
        print(f"    {metric}")
    print("\n")
    
    # Overall top performers for each metric
    for metric in METRIC_COLUMNS.keys():
        print("=" * 62)
        
        # Get top performers
        top_performers = get_top_n_overall(journalist_data, journalist_articles, metric, top_n)
        
        base_header = f"TOP {top_n} JOURNALISTS BY {metric.upper().replace('_', ' ')}"
        
        if len(top_performers) >= 2:
            first_value = top_performers[0][1]
            second_value = top_performers[1][1]
            percent_diff = ((first_value - second_value) / second_value * 100) if second_value > 0 else 0
            percent_str = f"1st vs 2nd: +{percent_diff:.0f}%"
            # Right-justify the percentage
            padding = 62 - len(base_header) - len(percent_str)
            header = base_header + " " * padding + percent_str
        else:
            header = base_header
        
        print(header)
        print("=" * 62)
        
        # Column header with actual metric name
        metric_display = metric.upper().replace('_', ' ')
        print(f"{'Rank':<6}{'Journalist':<30}{metric_display:<18}Articles")
        print("-" * 62)
        
        for i, (name, value, articles) in enumerate(top_performers, 1):
            # Format metric value based on type
            if metric in ['views', 'visitors', 'social_refs', 'new_visitors', 'engaged_minutes']:
                formatted_value = f"{int(value):<18}"
            else:
                formatted_value = f"{value:<18.0f}"
            
            print(f"{i:<6}{name:<30}{formatted_value}{articles}")
        
        print()
    
    print("\n")
    
    # Monthly rankings for each metric
    for metric in METRIC_COLUMNS.keys():
        print("=" * 62)
        print(f"MONTHLY RANKINGS: {metric.upper().replace('_', ' ')}")
        print()
        
        # Get monthly winners
        monthly_winners = get_monthly_winners(monthly_data, monthly_articles, metric, top_n)
        
        # Count wins
        win_counts = count_monthly_wins(monthly_winners)
        
        # Display win counts (sorted by wins descending, then by name)
        sorted_winners = sorted(win_counts.items(), key=lambda x: (-x[1], x[0]))
        for name, wins in sorted_winners:
            print(f"{name:<8}{wins}")
        
        print()
        
        # Display monthly details
        for year_month in sorted(monthly_winners.keys()):
            print(format_monthly_ranking_line(year_month, monthly_winners[year_month]))
        
        print()


@click.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--top-n', type=int, default=5, help='Number of top journalists to show')
@click.option('--ignore-authors', multiple=True, help='Authors to ignore in analysis')
@click.option('--after-date', type=str, required=True, help='Only include articles published on or after this date (YYYY-MM-DD)')
def main(input_file, top_n, ignore_authors, after_date):
    """Analyze journalist performance metrics from Parsely data."""
    
    # Parse filter date
    try:
        filter_date = datetime.strptime(after_date, '%Y-%m-%d')
    except ValueError:
        click.echo(f"Error: Invalid date format '{after_date}'. Use YYYY-MM-DD format.", err=True)
        sys.exit(1)
    
    # Read CSV
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        click.echo(f"Error reading CSV file: {e}", err=True)
        sys.exit(1)
    
    # Convert publish date to datetime
    df['Publish date'] = pd.to_datetime(df['Publish date'])
    
    # Filter by date
    df = df[df['Publish date'] >= filter_date]
    
    if df.empty:
        click.echo(f"No articles found after {after_date}", err=True)
        sys.exit(1)
    
    # Convert ignored authors to set
    ignored_authors_set = set(ignore_authors)
    
    # Aggregate data
    journalist_data, journalist_articles = aggregate_journalist_metrics(df, ignored_authors_set)
    monthly_data, monthly_articles = aggregate_monthly_metrics(df, ignored_authors_set)
    
    # Print results
    print_results(
        journalist_data, 
        journalist_articles,
        monthly_data,
        monthly_articles,
        top_n, 
        input_file, 
        after_date,
        ignored_authors_set
    )


if __name__ == '__main__':
    main()