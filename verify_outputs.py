#!/usr/bin/env python
"""Verify ETL outputs and print statistics."""
import pyarrow.parquet as pq

clean = pq.read_table('output/clean_events.parquet')
summary = pq.read_table('output/daily_summary.parquet')
print(f'Clean events: {len(clean)} rows')
print(f'Daily summary: {len(summary)} rows')
