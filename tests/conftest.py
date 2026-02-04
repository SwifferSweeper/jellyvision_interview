"""Pytest fixtures for ETL pipeline tests."""

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from etl_pipeline import DataLoader, DataCleaner, DataTransformer, DataWriter


@pytest.fixture
def sample_events_data():
    """Sample events data for testing."""
    return [
        {
            "user_id": "user_1",
            "event_type": "login",
            "timestamp": "2024-01-15T08:00:00",
            "value": 1,
            "metadata": {"device": "mobile", "page": "home"},
        },
        {
            "user_id": "user_2",
            "event_type": "view",
            "timestamp": "2024-01-15T09:30:00",
            "value": 5,
            "metadata": {"device": "desktop", "page": "product"},
        },
        {
            "user_id": "user_1",
            "event_type": "purchase",
            "timestamp": "2024-01-15T10:00:00",
            "value": 100,
            "metadata": {"device": "mobile", "page": "checkout"},
        },
        {
            "user_id": None,  # Invalid: missing user_id
            "event_type": "login",
            "timestamp": "2024-01-15T11:00:00",
            "value": 0,
            "metadata": {"device": "tablet", "page": "home"},
        },
        {
            "user_id": "user_3",
            "event_type": "login",
            "timestamp": "invalid_timestamp",  # Invalid: bad timestamp
            "value": 1,
            "metadata": {"device": "desktop", "page": "home"},
        },
        {
            "user_id": "user_1",  # Duplicate of first event
            "event_type": "login",
            "timestamp": "2024-01-15T08:00:00",
            "value": 1,
            "metadata": {"device": "mobile", "page": "home"},
        },
    ]


@pytest.fixture
def sample_users_data():
    """Sample users data for testing."""
    return """user_id,signup_date,country
user_1,2023-06-15,US
user_2,2023-08-20,US
user_3,2023-12-01,CA
user_4,2024-01-01,US
"""


@pytest.fixture
def events_json_path(sample_events_data, tmp_path):
    """Create a temporary JSON file with sample events."""
    filepath = tmp_path / "events.json"
    with open(filepath, "w") as f:
        json.dump(sample_events_data, f)
    return filepath


@pytest.fixture
def users_csv_path(sample_users_data, tmp_path):
    """Create a temporary CSV file with sample users."""
    filepath = tmp_path / "users.csv"
    with open(filepath, "w") as f:
        f.write(sample_users_data)
    return filepath


@pytest.fixture
def loader(events_json_path, users_csv_path):
    """Create a DataLoader instance with temporary files."""
    return DataLoader(events_json_path, users_csv_path)


@pytest.fixture
def cleaner():
    """Create a DataCleaner instance."""
    return DataCleaner()


@pytest.fixture
def transformer():
    """Create a DataTransformer instance."""
    return DataTransformer()


@pytest.fixture
def output_dir(tmp_path):
    """Create a temporary output directory."""
    return tmp_path / "output"


@pytest.fixture
def writer(output_dir):
    """Create a DataWriter instance with temporary output directory."""
    return DataWriter(output_dir)


@pytest.fixture
def sample_clean_events_df():
    """Sample cleaned events DataFrame."""
    return pd.DataFrame(
        {
            "user_id": ["user_1", "user_2", "user_1"],
            "event_type": ["login", "view", "purchase"],
            "timestamp": [
                "2024-01-15T08:00:00",
                "2024-01-15T09:30:00",
                "2024-01-15T10:00:00",
            ],
            "value": [1, 5, 100],
            "device": ["mobile", "desktop", "mobile"],
            "page": ["home", "product", "checkout"],
            "country": ["US", "US", "US"],
        }
    )


@pytest.fixture
def sample_events_for_aggregation():
    """Sample events DataFrame for aggregation tests."""
    return pd.DataFrame(
        {
            "user_id": ["user_1", "user_1", "user_1", "user_2"],
            "event_date": [
                pd.Timestamp("2024-01-15").date(),
                pd.Timestamp("2024-01-15").date(),
                pd.Timestamp("2024-01-16").date(),
                pd.Timestamp("2024-01-15").date(),
            ],
        }
    )
