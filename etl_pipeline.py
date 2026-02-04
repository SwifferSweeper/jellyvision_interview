"""
ETL Pipeline for Benefits Engagement Data

This script processes raw events data and user data, cleaning and transforming
the data, and outputting Parquet files with Snappy compression.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
INPUT_DIR = Path('.')
OUTPUT_DIR = Path('output')
EVENTS_FILE = INPUT_DIR / 'raw_events.json'
USERS_FILE = INPUT_DIR / 'users.csv'
CLEAN_EVENTS_OUTPUT = OUTPUT_DIR / 'clean_events.parquet'
DAILY_SUMMARY_OUTPUT = OUTPUT_DIR / 'daily_summary.parquet'


def load_events(filepath: Path) -> pd.DataFrame:
    """Load events from JSON file."""
    logger.info(f"Loading events from {filepath}")
    with open(filepath, 'r') as f:
        events_data = json.load(f)
    df = pd.DataFrame(events_data)
    logger.info(f"Loaded {len(df)} events")
    return df


def load_users(filepath: Path) -> pd.DataFrame:
    """Load users from CSV file."""
    logger.info(f"Loading users from {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df)} users")
    return df


def clean_events(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Clean events data by:
    - Dropping rows with missing user_id
    - Dropping rows with invalid timestamps
    
    Returns cleaned DataFrame and dict of dropped row counts.
    """
    dropped = {}
    initial_count = len(df)
    
    # Drop rows with missing user_id
    missing_user_id = df['user_id'].isna()
    dropped['missing_user_id'] = missing_user_id.sum()
    df = df[~missing_user_id]
    
    # Convert timestamp to datetime and drop invalid ones
    df['timestamp_dt'] = pd.to_datetime(df['timestamp'], errors='coerce')
    invalid_timestamp = df['timestamp_dt'].isna()
    dropped['invalid_timestamp'] = invalid_timestamp.sum()
    df = df[~invalid_timestamp]
    
    # Extract date from timestamp
    df['event_date'] = df['timestamp_dt'].dt.date
    
    final_count = len(df)
    logger.info(f"Cleaned events: {initial_count} -> {final_count} "
                f"(dropped {initial_count - final_count} rows)")
    
    return df, dropped


def deduplicate_events(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Deduplicate events based on all columns except metadata.
    Metadata can vary even for the same event, so we exclude it.
    """
    # Columns to use for deduplication (excluding metadata)
    dedup_cols = ['user_id', 'event_type', 'timestamp', 'value']
    
    initial_count = len(df)
    
    # Drop duplicates based on dedup columns, keeping first occurrence
    df_deduped = df.drop_duplicates(subset=dedup_cols, keep='first')
    
    dropped_count = initial_count - len(df_deduped)
    logger.info(f"Deduplication: {initial_count} -> {len(df_deduped)} "
                f"(removed {dropped_count} duplicates)")
    
    return df_deduped, dropped_count


def flatten_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten the metadata dictionary into separate columns."""
    if 'metadata' in df.columns:
        df['device'] = df['metadata'].apply(lambda x: x.get('device') if x else None)
        df['page'] = df['metadata'].apply(lambda x: x.get('page') if x else None)
        df = df.drop(columns=['metadata'])
    return df


def join_with_users(events_df: pd.DataFrame, users_df: pd.DataFrame) -> pd.DataFrame:
    """Join events with user data."""
    logger.info(f"Joining {len(events_df)} events with {len(users_df)} users")
    merged_df = events_df.merge(users_df, on='user_id', how='left')
    
    # Check for events without matching users
    unmatched = merged_df['country'].isna().sum()
    if unmatched > 0:
        logger.warning(f"Found {unmatched} events without matching users")
    
    logger.info(f"After join: {len(merged_df)} rows")
    return merged_df


def filter_us_users(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Filter to only US users."""
    initial_count = len(df)
    us_df = df[df['country'] == 'US'].copy()
    dropped_count = initial_count - len(us_df)
    
    logger.info(f"US filter: {initial_count} -> {len(us_df)} "
                f"(removed {dropped_count} non-US users)")
    
    return us_df, dropped_count


def create_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create daily aggregate: number of events per user per date.
    """
    logger.info("Creating daily event summary")
    
    # Group by user_id and date, count events
    summary = df.groupby(['user_id', 'event_date']).size().reset_index(name='event_count')
    
    logger.info(f"Daily summary: {len(summary)} user-date combinations")
    return summary


def write_parquet(df: pd.DataFrame, filepath: Path, compression: str = 'snappy'):
    """Write DataFrame to Parquet file with compression."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath, compression=compression, index=False)
    logger.info(f"Wrote {len(df)} rows to {filepath}")


def log_dropped_counts(dropped: dict, dedup_dropped: int, us_filter_dropped: int):
    """Log detailed counts of dropped rows."""
    logger.info("=" * 50)
    logger.info("DROPPED ROW SUMMARY:")
    logger.info("=" * 50)
    
    total_clean_dropped = sum(dropped.values())
    logger.info(f"  During cleaning:")
    for reason, count in dropped.items():
        logger.info(f"    - {reason}: {count}")
    
    logger.info(f"  During deduplication: {dedup_dropped}")
    logger.info(f"  During US filter: {us_filter_dropped}")
    
    total_dropped = total_clean_dropped + dedup_dropped + us_filter_dropped
    logger.info(f"  TOTAL DROPPED: {total_dropped}")
    logger.info("=" * 50)


def main():
    """Main ETL pipeline."""
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 50)
    
    # EXTRACT
    events_df = load_events(EVENTS_FILE)
    users_df = load_users(USERS_FILE)
    
    # TRANSFORM - CLEAN
    events_df, dropped = clean_events(events_df)
    
    # TRANSFORM - DEDUPLICATE
    events_df, dedup_dropped = deduplicate_events(events_df)
    
    # Flatten metadata for cleaner output
    events_df = flatten_metadata(events_df)
    
    # TRANSFORM - JOIN
    events_df = join_with_users(events_df, users_df)
    
    # TRANSFORM - FILTER TO US USERS
    events_df, us_filter_dropped = filter_us_users(events_df)
    
    # Select final columns for clean_events output
    final_columns = [
        'user_id', 'event_type', 'timestamp', 'timestamp_dt', 
        'event_date', 'value', 'device', 'page', 'signup_date', 'country'
    ]
    clean_events_df = events_df[final_columns].copy()
    
    # Create daily summary (aggregate)
    daily_summary_df = create_daily_summary(events_df)
    
    # LOAD
    write_parquet(clean_events_df, CLEAN_EVENTS_OUTPUT)
    write_parquet(daily_summary_df, DAILY_SUMMARY_OUTPUT)
    
    # Log summary
    log_dropped_counts(dropped, dedup_dropped, us_filter_dropped)
    
    logger.info("=" * 50)
    logger.info("ETL Pipeline Complete!")
    logger.info(f"  - Clean events: {len(clean_events_df)} rows")
    logger.info(f"  - Daily summary: {len(daily_summary_df)} rows")
    logger.info("=" * 50)
    
    # Print sample output
    print("\n--- Sample Clean Events ---")
    print(clean_events_df.head(3).to_string())
    print("\n--- Sample Daily Summary ---")
    print(daily_summary_df.head(5).to_string())


if __name__ == '__main__':
    main()
