# Benefits Engagement ETL Pipeline

A simple local ETL pipeline that processes mock benefits engagement data, outputting clean results as Parquet files using pyarrow.

## Architecture

The pipeline uses pyarrow for efficient Parquet I/O and is organized into classes:

| Class | Responsibility | Pyarrow Usage |
|-------|---------------|---------------|
| [`DataLoader`](etl_pipeline.py:31) | Extraction from JSON and CSV | Reads JSON via `pa.Table.from_pylist()` |
| [`DataCleaner`](etl_pipeline.py:68) | Cleaning operations | Pandas for data manipulation |
| [`DataTransformer`](etl_pipeline.py:156) | Join, filter, aggregate | Pandas for transformations |
| [`DataWriter`](etl_pipeline.py:231) | Write to Parquet | `pq.write_table()` with Snappy |
| [`ETLPipeline`](etl_pipeline.py:275) | Orchestrates ETL | Coordinates all components |

## Input Files

| File | Description |
|------|-------------|
| `raw_events.json` | Event data with user_id, event_type, timestamp, value, and metadata |
| `users.csv` | User data with user_id, signup_date, and country |

## Output Files

| File | Description |
|------|-------------|
| `output/clean_events.parquet` | Cleaned, joined, and filtered events (US users only) |
| `output/daily_summary.parquet` | Daily event counts per user |

Both use **Snappy compression** via pyarrow.

## Data Cleaning Steps

1. **Drop rows with missing `user_id`** - Events must have a valid user
2. **Drop rows with invalid timestamps** - Only valid ISO 8601 timestamps are kept
3. **Deduplicate events** - Exact matches on user_id, event_type, timestamp, and value
4. **Join with users table** - Enrich events with user signup_date and country
5. **Filter to US users only** - Keep only events from United States users

## Requirements

- Python 3.8+
- pandas
- pyarrow

Install dependencies:
```bash
pip install pandas pyarrow
```

## Running the Pipeline

```bash
python etl_pipeline.py
```

## Output Schema

### clean_events.parquet

| Column | Type | Description |
|--------|------|-------------|
| user_id | string | Unique user identifier |
| event_type | string | Type of event (login, logout, view_benefits, etc.) |
| timestamp | string | Original ISO 8601 timestamp |
| timestamp_dt | timestamp | Parsed datetime |
| event_date | date | Date portion of timestamp |
| value | int/float | Event value (may be null) |
| device | string | Device type (desktop, mobile, tablet) |
| page | string | Page path |
| signup_date | date | User signup date |
| country | string | User country (US only) |

### daily_summary.parquet

| Column | Type | Description |
|--------|------|-------------|
| user_id | string | Unique user identifier |
| event_date | date | Date of events |
| event_count | int | Number of events for that user on that date |

## Logging

The pipeline logs detailed information about row counts, dropped rows, and processing steps.

## PyArrow Features Used

- **Reading JSON**: `pa.Table.from_pylist()` to efficiently parse JSON arrays
- **Writing Parquet**: `pq.write_table()` with:
  - Snappy compression
  - Dictionary encoding (`use_dictionary=True`)
  - Timestamp coercion to microseconds

## Usage as a Library

```python
from pathlib import Path
from etl_pipeline import ETLPipeline

# Define paths
events_path = Path('raw_events.json')
users_path = Path('users.csv')
output_dir = Path('output')

# Create and run pipeline
pipeline = ETLPipeline(events_path, users_path, output_dir)
stats = pipeline.run()

# Access results
print(f"Clean events: {len(pipeline.clean_events_df)}")
print(f"Daily summary: {len(pipeline.daily_summary_df)}")
```
