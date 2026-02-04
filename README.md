# Benefits Engagement ETL Pipeline

A simple local ETL pipeline that processes mock benefits engagement data, outputting clean results as Parquet files with Snappy compression.

## Overview

This pipeline:
1. **Extracts** data from `raw_events.json` and `users.csv`
2. **Transforms** the data through cleaning, deduplication, joining, and filtering
3. **Loads** cleaned data to Parquet files

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

The pipeline logs:
- Row counts at each stage
- Dropped row counts and reasons
- Deduplication statistics
- Final output summary

Example output:
```
2025-02-04 22:03:12,123 - INFO - Loaded 8934 events
2025-02-04 22:03:12,456 - INFO - Loaded 1000 users
2025-02-04 22:03:12,789 - INFO - Cleaned events: 8934 -> 8920 (dropped 14 rows)
2025-02-04 22:03:12,012 - INFO - Deduplication: 8920 -> 8750 (removed 170 duplicates)
...
2025-02-04 22:03:13,345 - INFO - DROPPED ROW SUMMARY:
...
```

## Compression

Both output files use **Snappy compression**, which provides:
- Fast compression/decompression speeds
- Good compression ratios
- Native support in pandas/pyarrow
