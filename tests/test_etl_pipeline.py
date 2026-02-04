"""Unit tests for ETL pipeline classes."""

import json

import pandas as pd
import pytest

from etl_pipeline import DataLoader, DataCleaner, DataTransformer, DataWriter


class TestDataLoader:
    """Tests for DataLoader class."""

    def test_load_events(self, loader):
        """Test loading events from JSON file."""
        events_df = loader.load_events()
        assert isinstance(events_df, pd.DataFrame)
        # Should have 4 valid events (2 invalid ones filtered out during load)
        # Actually, load_events just loads - cleaning happens separately
        assert len(events_df) == 6  # All events loaded, no cleaning yet
        assert "user_id" in events_df.columns
        assert "event_type" in events_df.columns
        assert "timestamp" in events_df.columns

    def test_load_users(self, loader):
        """Test loading users from CSV file."""
        users_df = loader.load_users()
        assert isinstance(users_df, pd.DataFrame)
        assert len(users_df) == 4
        assert "user_id" in users_df.columns
        assert "signup_date" in users_df.columns
        assert "country" in users_df.columns

    def test_load_all(self, loader):
        """Test loading both events and users."""
        events_df, users_df = loader.load_all()
        assert isinstance(events_df, pd.DataFrame)
        assert isinstance(users_df, pd.DataFrame)
        assert len(events_df) == 6
        assert len(users_df) == 4

    def test_load_events_with_missing_user_id(self, events_json_path, users_csv_path):
        """Test that events with missing user_id are handled correctly."""
        loader = DataLoader(events_json_path, users_csv_path)
        events_df = loader.load_events()
        # Find row with missing user_id
        assert events_df["user_id"].isna().any()


class TestDataCleaner:
    """Tests for DataCleaner class."""

    def test_clean_events_removes_missing_user_id(self, cleaner):
        """Test that events with missing user_id are removed."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", None, "user_2"],
                "event_type": ["login", "login", "view"],
                "timestamp": ["2024-01-15T08:00:00", "2024-01-15T09:00:00", "2024-01-15T10:00:00"],
                "value": [1, 2, 3],
                "metadata": [{}, {}, {}],
            }
        )
        cleaned_df = cleaner.clean_events(events_df)
        assert len(cleaned_df) == 2
        assert "missing_user_id" in cleaner.dropped_counts
        assert cleaner.dropped_counts["missing_user_id"] == 1

    def test_clean_events_removes_invalid_timestamps(self, cleaner):
        """Test that events with invalid timestamps are removed."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2", "user_3"],
                "event_type": ["login", "view", "purchase"],
                "timestamp": ["2024-01-15T08:00:00", "invalid", "2024-01-15T10:00:00"],
                "value": [1, 2, 3],
                "metadata": [{}, {}, {}],
            }
        )
        cleaned_df = cleaner.clean_events(events_df)
        assert len(cleaned_df) == 2
        assert "invalid_timestamp" in cleaner.dropped_counts
        assert cleaner.dropped_counts["invalid_timestamp"] == 1

    def test_clean_events_adds_timestamp_dt_column(self, cleaner):
        """Test that cleaned events have timestamp_dt column."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2"],
                "event_type": ["login", "view"],
                "timestamp": ["2024-01-15T08:00:00", "2024-01-15T09:00:00"],
                "value": [1, 2],
                "metadata": [{}, {}],
            }
        )
        cleaned_df = cleaner.clean_events(events_df)
        assert "timestamp_dt" in cleaned_df.columns
        assert pd.api.types.is_datetime64_any_dtype(cleaned_df["timestamp_dt"])

    def test_clean_events_adds_event_date_column(self, cleaner):
        """Test that cleaned events have event_date column."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2"],
                "event_type": ["login", "view"],
                "timestamp": ["2024-01-15T08:00:00", "2024-01-15T09:00:00"],
                "value": [1, 2],
                "metadata": [{}, {}],
            }
        )
        cleaned_df = cleaner.clean_events(events_df)
        assert "event_date" in cleaned_df.columns

    def test_deduplicate_events(self, cleaner):
        """Test that duplicate events are removed."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_1", "user_2"],
                "event_type": ["login", "login", "view"],
                "timestamp": ["2024-01-15T08:00:00", "2024-01-15T08:00:00", "2024-01-15T09:00:00"],
                "value": [1, 1, 2],
                "metadata": [{}, {}, {}],
            }
        )
        deduped_df = cleaner.deduplicate_events(events_df)
        assert len(deduped_df) == 2
        assert cleaner.dedup_dropped == 1

    def test_flatten_metadata(self, cleaner):
        """Test that metadata dict is flattened into device and page columns."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2"],
                "event_type": ["login", "view"],
                "timestamp": ["2024-01-15T08:00:00", "2024-01-15T09:00:00"],
                "value": [1, 2],
                "metadata": [{"device": "mobile", "page": "home"}, {"device": "desktop", "page": "product"}],
            }
        )
        flattened_df = cleaner.flatten_metadata(events_df)
        assert "device" in flattened_df.columns
        assert "page" in flattened_df.columns
        assert "metadata" not in flattened_df.columns
        assert flattened_df["device"].iloc[0] == "mobile"
        assert flattened_df["page"].iloc[0] == "home"

    def test_flatten_metadata_handles_missing_metadata(self, cleaner):
        """Test that flatten_metadata handles DataFrame without metadata column."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2"],
                "event_type": ["login", "view"],
            }
        )
        flattened_df = cleaner.flatten_metadata(events_df)
        assert flattened_df.equals(events_df)

    def test_flatten_metadata_handles_none_values(self, cleaner):
        """Test that flatten_metadata handles None metadata values."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2"],
                "event_type": ["login", "view"],
                "timestamp": ["2024-01-15T08:00:00", "2024-01-15T09:00:00"],
                "value": [1, 2],
                "metadata": [{"device": "mobile"}, None],
            }
        )
        flattened_df = cleaner.flatten_metadata(events_df)
        assert "device" in flattened_df.columns
        assert pd.isna(flattened_df["device"].iloc[1])


class TestDataTransformer:
    """Tests for DataTransformer class."""

    def test_join_with_users(self, transformer):
        """Test left join of events with users."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2", "user_3"],
                "event_type": ["login", "view", "purchase"],
            }
        )
        users_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2"],
                "country": ["US", "CA"],
            }
        )
        merged_df = transformer.join_with_users(events_df, users_df)
        assert len(merged_df) == 3
        assert "country" in merged_df.columns
        # user_3 has no match, so country should be NaN
        assert pd.isna(merged_df[merged_df["user_id"] == "user_3"]["country"].iloc[0])

    def test_join_with_users_logs_unmatched(self, transformer):
        """Test that join logs unmatched users."""
        events_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_3"],
                "event_type": ["login", "view"],
            }
        )
        users_df = pd.DataFrame(
            {
                "user_id": ["user_1"],
                "country": ["US"],
            }
        )
        merged_df = transformer.join_with_users(events_df, users_df)
        assert transformer.stats["join_unmatched"] == 1

    def test_filter_us_users(self, transformer):
        """Test filtering to only US users."""
        df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2", "user_3"],
                "country": ["US", "CA", "US"],
            }
        )
        us_df = transformer.filter_us_users(df)
        assert len(us_df) == 2
        assert all(us_df["country"] == "US")
        assert transformer.us_filter_dropped == 1

    def test_create_daily_summary(self, transformer, sample_events_for_aggregation):
        """Test creating daily event summary."""
        summary = transformer.create_daily_summary(sample_events_for_aggregation)
        assert isinstance(summary, pd.DataFrame)
        assert "user_id" in summary.columns
        assert "event_date" in summary.columns
        assert "event_count" in summary.columns
        # user_1 has 2 events on 2024-01-15 and 1 on 2024-01-16
        user_1_summary = summary[summary["user_id"] == "user_1"]
        assert len(user_1_summary) == 2


class TestDataWriter:
    """Tests for DataWriter class."""

    def test_write_parquet(self, writer, sample_clean_events_df, output_dir):
        """Test writing DataFrame to Parquet file."""
        filepath = writer.write_parquet(sample_clean_events_df, "test_events.parquet")
        assert filepath.exists()
        assert filepath.suffix == ".parquet"

    def test_write_parquet_with_gzip_compression(self, writer, sample_clean_events_df, output_dir):
        """Test writing Parquet with gzip compression."""
        filepath = writer.write_parquet(sample_clean_events_df, "test_events_gzip.parquet", compression="gzip")
        assert filepath.exists()

    def test_write_clean_events(self, writer, sample_clean_events_df, output_dir):
        """Test writing cleaned events."""
        filepath = writer.write_clean_events(sample_clean_events_df)
        assert filepath.exists()
        assert filepath.name == "clean_events.parquet"

    def test_write_daily_summary(self, writer, output_dir):
        """Test writing daily summary."""
        summary_df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2"],
                "event_date": [pd.Timestamp("2024-01-15").date(), pd.Timestamp("2024-01-15").date()],
                "event_count": [5, 3],
            }
        )
        filepath = writer.write_daily_summary(summary_df)
        assert filepath.exists()
        assert filepath.name == "daily_summary.parquet"

    def test_parquet_file_readable(self, writer, sample_clean_events_df, output_dir):
        """Test that written Parquet file is readable."""
        import pyarrow.parquet as pq

        filepath = writer.write_parquet(sample_clean_events_df, "test_readable.parquet")
        table = pq.read_table(filepath)
        assert len(table) == len(sample_clean_events_df)


class TestETLPipeline:
    """Integration tests for ETLPipeline class."""

    def test_pipeline_run(self, events_json_path, users_csv_path, tmp_path):
        """Test running the full ETL pipeline."""
        from etl_pipeline import ETLPipeline

        output_dir = tmp_path / "output"
        pipeline = ETLPipeline(events_json_path, users_csv_path, output_dir)
        stats = pipeline.run()

        assert "events_loaded" in stats
        assert "users_loaded" in stats
        assert "clean_events_count" in stats
        assert "daily_summary_count" in stats

    def test_pipeline_creates_output_files(self, events_json_path, users_csv_path, tmp_path):
        """Test that pipeline creates expected output files."""
        from etl_pipeline import ETLPipeline

        output_dir = tmp_path / "output"
        pipeline = ETLPipeline(events_json_path, users_csv_path, output_dir)
        pipeline.run()

        assert (output_dir / "clean_events.parquet").exists()
        assert (output_dir / "daily_summary.parquet").exists()

    def test_pipeline_cleans_events(self, events_json_path, users_csv_path, tmp_path):
        """Test that pipeline cleans events correctly."""
        from etl_pipeline import ETLPipeline

        output_dir = tmp_path / "output"
        pipeline = ETLPipeline(events_json_path, users_csv_path, output_dir)
        pipeline.run()

        # Check no missing user_id in clean events
        assert pipeline.clean_events_df["user_id"].notna().all()
        # Check no invalid timestamps
        assert pipeline.clean_events_df["timestamp_dt"].notna().all()

    def test_pipeline_filters_us_users(self, events_json_path, users_csv_path, tmp_path):
        """Test that pipeline filters to US users only."""
        from etl_pipeline import ETLPipeline

        output_dir = tmp_path / "output"
        pipeline = ETLPipeline(events_json_path, users_csv_path, output_dir)
        pipeline.run()

        # All clean events should be from US users
        assert (pipeline.clean_events_df["country"] == "US").all()

    def test_pipeline_metadata_flattened(self, events_json_path, users_csv_path, tmp_path):
        """Test that pipeline flattens metadata into device and page."""
        from etl_pipeline import ETLPipeline

        output_dir = tmp_path / "output"
        pipeline = ETLPipeline(events_json_path, users_csv_path, output_dir)
        pipeline.run()

        assert "device" in pipeline.clean_events_df.columns
        assert "page" in pipeline.clean_events_df.columns
        assert "metadata" not in pipeline.clean_events_df.columns
