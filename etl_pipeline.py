"""
ETL Pipeline for Benefits Engagement Data

This script processes raw events data and user data, cleaning and transforming
the data, and outputting Parquet files with Snappy compression using pyarrow.

Classes:
    - DataLoader: Handles data extraction from JSON and CSV files using pyarrow
    - DataCleaner: Handles data cleaning operations
    - DataTransformer: Handles data transformations (join, filter, aggregate)
    - DataWriter: Handles writing data to output files using pyarrow
    - ETLPipeline: Orchestrates the entire ETL process
"""

import json
import logging
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Handles extraction of data from source files using pyarrow."""

    def __init__(self, events_path: Path, users_path: Path):
        """
        Initialize loader with file paths.

        Args:
            events_path: Path to raw_events.json
            users_path: Path to users.csv
        """
        self.events_path = events_path
        self.users_path = users_path

    def load_events(self) -> pd.DataFrame:
        """Load events from JSON file using pyarrow for parsing.

        Returns:
            pd.DataFrame: Events data with columns (user_id, event_type, timestamp, value, metadata)
        """
        """
        Load events from JSON file using pyarrow for parsing.
        
        Returns:
            DataFrame with events data
        """
        logger.info(f"Loading events from {self.events_path}")

        # Read JSON file
        with open(self.events_path, "r") as f:
            events_data = json.load(f)

        # Create pyarrow Table
        table = pa.Table.from_pylist(events_data)
        logger.info(f"Loaded {len(table)} events using pyarrow")

        return table.to_pandas()

    def load_users(self) -> pd.DataFrame:
        """Load users from CSV file.

        Returns:
            pd.DataFrame: Users data with columns (user_id, signup_date, country)
        """
        """
        Load users from CSV file.
        
        Returns:
            DataFrame with users data
        """
        logger.info(f"Loading users from {self.users_path}")

        # Read CSV using pandas (pyarrow.csv requires separate install)
        df = pd.read_csv(self.users_path)
        logger.info(f"Loaded {len(df)} users")

        return df

    def load_all(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load both events and users data."""
        events_df = self.load_events()
        users_df = self.load_users()
        return events_df, users_df


class DataCleaner:
    """Handles cleaning operations on the data."""

    def __init__(self):
        """Initialize cleaner with tracking dicts for dropped counts."""
        self.dropped_counts: dict[str, int] = {}
        self.dedup_dropped = 0
        self.us_filter_dropped = 0

    def clean_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean events data by dropping invalid/missing rows.

        Args:
            df (pd.DataFrame): Raw events with columns (user_id, event_type, timestamp, value, metadata)

        Returns:
            pd.DataFrame: Cleaned events with added columns (timestamp_dt, event_date)
        """
        initial_count = len(df)

        # Drop rows with missing user_id
        missing_user_id = df["user_id"].isna()
        self.dropped_counts["missing_user_id"] = missing_user_id.sum()
        df = df[~missing_user_id]

        # Convert timestamp to datetime and drop invalid ones
        df["timestamp_dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
        invalid_timestamp = df["timestamp_dt"].isna()
        self.dropped_counts["invalid_timestamp"] = invalid_timestamp.sum()
        df = df[~invalid_timestamp]

        # Extract date from timestamp
        df["event_date"] = df["timestamp_dt"].dt.date

        final_count = len(df)
        logger.info(
            f"Cleaned events: {initial_count} -> {final_count} "
            f"(dropped {initial_count - final_count} rows)"
        )

        return df

    def deduplicate_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate events based on key columns.

        Args:
            df (pd.DataFrame): Events DataFrame with columns (user_id, event_type, timestamp, value)

        Returns:
            pd.DataFrame: Deduplicated events
        """
        # Columns to use for deduplication (excluding metadata)
        dedup_cols = ["user_id", "event_type", "timestamp", "value"]

        initial_count = len(df)

        # Drop duplicates based on dedup columns, keeping first occurrence
        df_deduped = df.drop_duplicates(subset=dedup_cols, keep="first")

        self.dedup_dropped = initial_count - len(df_deduped)
        logger.info(
            f"Deduplication: {initial_count} -> {len(df_deduped)} "
            f"(removed {self.dedup_dropped} duplicates)"
        )

        return df_deduped

    def flatten_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flatten metadata dict into separate columns.

        Args:
            df (pd.DataFrame): DataFrame with 'metadata' column containing dict

        Returns:
            pd.DataFrame: DataFrame with 'device' and 'page' columns instead of metadata
        """
        if "metadata" in df.columns:
            df["device"] = df["metadata"].apply(
                lambda x: x.get("device") if x else None
            )
            df["page"] = df["metadata"].apply(lambda x: x.get("page") if x else None)
            df = df.drop(columns=["metadata"])
        return df


class DataTransformer:
    """Handles data transformations (join, filter, aggregate)."""

    def __init__(self):
        """Initialize transformer."""
        self.stats: dict[str, Any] = {}

    def join_with_users(
        self, events_df: pd.DataFrame, users_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Left join events with users on user_id.

        Args:
            events_df (pd.DataFrame): Events with columns (user_id, event_type, timestamp, ...)
            users_df (pd.DataFrame): Users with columns (user_id, signup_date, country)

        Returns:
            pd.DataFrame: Merged events with user info (signup_date, country)
        """
        logger.info(f"Joining {len(events_df)} events with {len(users_df)} users")
        merged_df = events_df.merge(users_df, on="user_id", how="left")

        # Check for events without matching users
        unmatched = merged_df["country"].isna().sum()
        if unmatched > 0:
            logger.warning(f"Found {unmatched} events without matching users")

        logger.info(f"After join: {len(merged_df)} rows")
        self.stats["join_unmatched"] = unmatched

        return merged_df

    def filter_us_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter DataFrame to only US users.

        Args:
            df (pd.DataFrame): DataFrame with 'country' column

        Returns:
            pd.DataFrame: Filtered DataFrame where country == 'US'
        """
        initial_count = len(df)
        us_df = df[df["country"] == "US"].copy()
        self.us_filter_dropped = initial_count - len(us_df)

        logger.info(
            f"US filter: {initial_count} -> {len(us_df)} "
            f"(removed {self.us_filter_dropped} non-US users)"
        )

        return us_df

    def create_daily_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate events: count per user per date.

        Args:
            df (pd.DataFrame): Events with 'user_id' and 'event_date' columns

        Returns:
            pd.DataFrame: Summary with columns (user_id, event_date, event_count)
        """
        logger.info("Creating daily event summary")

        # Group by user_id and date, count events
        summary = (
            df.groupby(["user_id", "event_date"]).size().reset_index(name="event_count")
        )

        logger.info(f"Daily summary: {len(summary)} user-date combinations")
        return summary


class DataWriter:
    """Handles writing data to output files using pyarrow."""

    def __init__(self, output_dir: Path):
        """
        Initialize writer with output directory.

        Args:
            output_dir: Directory for output files
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_parquet(
        self, df: pd.DataFrame, filename: str, compression: str = "snappy"
    ) -> Path:
        """Write DataFrame to Parquet file using pyarrow.

        Args:
            df (pd.DataFrame): DataFrame to write
            filename (str): Output filename (e.g., 'clean_events.parquet')
            compression (str): Compression codec - 'snappy' (default), 'gzip', 'lz4'

        Returns:
            Path: Path to written parquet file
        """
        filepath = self.output_dir / filename

        # Convert pandas DataFrame to pyarrow Table
        table = pa.Table.from_pandas(df)

        # Write to Parquet using pyarrow
        pq.write_table(
            table,
            str(filepath),
            compression=compression,
            use_dictionary=True,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

        logger.info(f"Wrote {len(df)} rows to {filepath} using pyarrow")
        return filepath

    def write_clean_events(self, df: pd.DataFrame) -> Path:
        """Write cleaned events to Parquet."""
        return self.write_parquet(df, "clean_events.parquet")

    def write_daily_summary(self, df: pd.DataFrame) -> Path:
        """Write daily summary to Parquet."""
        return self.write_parquet(df, "daily_summary.parquet")


class ETLPipeline:
    """Orchestrates the entire ETL process."""

    def __init__(self, events_path: Path, users_path: Path, output_dir: Path):
        """
        Initialize pipeline with paths.

        Args:
            events_path: Path to raw_events.json
            users_path: Path to users.csv
            output_dir: Directory for output files
        """
        self.events_path = events_path
        self.users_path = users_path
        self.output_dir = output_dir

        # Initialize components
        self.loader = DataLoader(events_path, users_path)
        self.cleaner = DataCleaner()
        self.transformer = DataTransformer()
        self.writer = DataWriter(output_dir)

    def run(self) -> dict[str, Any]:
        """
        Execute the full ETL pipeline.

        Returns:
            Dictionary with pipeline statistics
        """
        logger.info("Starting ETL Pipeline (using pyarrow)")
        logger.info("=" * 50)

        # EXTRACT
        events_df, users_df = self.loader.load_all()

        # TRANSFORM - CLEAN
        events_df = self.cleaner.clean_events(events_df)
        events_df = self.cleaner.deduplicate_events(events_df)
        events_df = self.cleaner.flatten_metadata(events_df)

        # TRANSFORM - JOIN & FILTER
        events_df = self.transformer.join_with_users(events_df, users_df)
        events_df = self.transformer.filter_us_users(events_df)

        # Store for later use
        self._events_df = events_df

        # Select final columns for clean_events output
        final_columns = [
            "user_id",
            "event_type",
            "timestamp",
            "timestamp_dt",
            "event_date",
            "value",
            "device",
            "page",
            "signup_date",
            "country",
        ]
        self.clean_events_df = events_df[final_columns].copy()

        # Create daily summary
        self.daily_summary_df = self.transformer.create_daily_summary(events_df)

        # LOAD - Write using pyarrow
        self.writer.write_clean_events(self.clean_events_df)
        self.writer.write_daily_summary(self.daily_summary_df)

        # Log summary
        self._log_summary()

        return self._get_stats()

    def _log_summary(self):
        """Log detailed counts of dropped rows."""
        logger.info("=" * 50)
        logger.info("DROPPED ROW SUMMARY:")
        logger.info("=" * 50)

        total_clean_dropped = sum(self.cleaner.dropped_counts.values())
        logger.info("  During cleaning:")
        for reason, count in self.cleaner.dropped_counts.items():
            logger.info(f"    - {reason}: {count}")

        logger.info(f"  During deduplication: {self.cleaner.dedup_dropped}")
        logger.info(f"  During US filter: {self.transformer.us_filter_dropped}")

        total_dropped = (
            total_clean_dropped
            + self.cleaner.dedup_dropped
            + self.transformer.us_filter_dropped
        )
        logger.info(f"  TOTAL DROPPED: {total_dropped}")
        logger.info("=" * 50)

        logger.info("=" * 50)
        logger.info("ETL Pipeline Complete!")
        logger.info(f"  - Clean events: {len(self.clean_events_df)} rows")
        logger.info(f"  - Daily summary: {len(self.daily_summary_df)} rows")
        logger.info("=" * 50)

    def _get_stats(self) -> dict[str, Any]:
        """Get pipeline statistics."""
        return {
            "events_loaded": len(self.loader.load_events()),
            "users_loaded": len(self.loader.load_users()),
            "dropped_cleaning": self.cleaner.dropped_counts,
            "dropped_dedup": self.cleaner.dedup_dropped,
            "dropped_us_filter": self.transformer.us_filter_dropped,
            "clean_events_count": len(self.clean_events_df),
            "daily_summary_count": len(self.daily_summary_df),
        }


def main():
    """Main entry point for running the ETL pipeline."""
    # Define paths
    INPUT_DIR = Path(".")
    OUTPUT_DIR = Path("output")
    EVENTS_FILE = INPUT_DIR / "raw_events.json"
    USERS_FILE = INPUT_DIR / "users.csv"

    # Create and run pipeline
    pipeline = ETLPipeline(EVENTS_FILE, USERS_FILE, OUTPUT_DIR)
    pipeline.run()

    # Print sample output
    print("\n--- Sample Clean Events ---")
    print(pipeline.clean_events_df.head(3).to_string())
    print("\n--- Sample Daily Summary ---")
    print(pipeline.daily_summary_df.head(5).to_string())


if __name__ == "__main__":
    main()
