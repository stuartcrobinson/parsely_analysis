#!/usr/bin/env python3
"""
Split CSV data by author and by author/month.
"""

import click
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys


def parse_authors(authors_str):
    """Parse comma-separated authors string into list."""
    if pd.isna(authors_str):
        return []
    return [author.strip() for author in authors_str.split(',') if author.strip()]


def sanitize_filename(name):
    """Convert author name to safe filename."""
    return name.replace(' ', '_').replace('/', '_').replace('\\', '_')


@click.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('start_date', type=click.DateTime(formats=['%Y-%m-%d']))
@click.option('--output-dir', '-o', default='output', help='Output directory')
def main(input_file, start_date, output_dir):
    """
    Split CSV by author and author/month.
    
    INPUT_FILE: Path to input CSV
    START_DATE: Earliest publication date to include (YYYY-MM-DD)
    """
    # Read CSV
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        click.echo(f"Error reading CSV: {e}", err=True)
        sys.exit(1)
    
    # Validate required columns
    required_cols = ['Authors', 'Publish date']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        click.echo(f"Missing required columns: {missing}", err=True)
        sys.exit(1)
    
    # Parse publish dates
    df['Publish date'] = pd.to_datetime(df['Publish date'], errors='coerce')
    
    # Filter by start date
    initial_count = len(df)
    df = df[df['Publish date'] >= start_date]
    filtered_count = len(df)
    
    click.echo(f"Loaded {initial_count} articles, kept {filtered_count} from {start_date.date()} onwards")
    
    if filtered_count == 0:
        click.echo("No articles match the date criteria", err=True)
        sys.exit(1)
    
    # Create output directories
    output_path = Path(output_dir)
    by_author_path = output_path / 'by_author'
    by_author_month_path = output_path / 'by_author_month'
    
    by_author_path.mkdir(parents=True, exist_ok=True)
    by_author_month_path.mkdir(parents=True, exist_ok=True)
    
    # Process each row
    author_data = {}  # author -> list of row indices
    author_month_data = {}  # (author, year-month) -> list of row indices
    
    for idx, row in df.iterrows():
        authors = parse_authors(row['Authors'])
        if not authors:
            continue
            
        year_month = row['Publish date'].strftime('%Y-%m')
        
        for author in authors:
            # By author
            if author not in author_data:
                author_data[author] = []
            author_data[author].append(idx)
            
            # By author/month
            key = (author, year_month)
            if key not in author_month_data:
                author_month_data[key] = []
            author_month_data[key].append(idx)
    
    # Write by-author files
    click.echo(f"\nWriting {len(author_data)} author files...")
    for author, indices in author_data.items():
        author_df = df.loc[indices]
        filename = by_author_path / f"{sanitize_filename(author)}.csv"
        author_df.to_csv(filename, index=False)
        click.echo(f"  {author}: {len(author_df)} articles -> {filename}")
    
    # Write by-author-month files
    click.echo(f"\nWriting {len(author_month_data)} author-month files...")
    for (author, year_month), indices in author_month_data.items():
        # Create author subdirectory
        author_dir = by_author_month_path / sanitize_filename(author)
        author_dir.mkdir(exist_ok=True)
        
        month_df = df.loc[indices]
        filename = author_dir / f"{year_month}.csv"
        month_df.to_csv(filename, index=False)
        click.echo(f"  {author} / {year_month}: {len(month_df)} articles -> {filename}")
    
    click.echo("\nDone!")


if __name__ == '__main__':
    main()