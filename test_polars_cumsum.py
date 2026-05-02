#!/usr/bin/env python3
import polars as pl

df = pl.DataFrame({
    'session_id': [1, 1, 2, 2],
    'revenue': [10, 20, 5, 15]
})

# Test different ways to do cumulative sum in polars
try:
    result = df.sort('session_id').with_columns([
        pl.col('revenue').cumsum().over('session_id').alias('cumulative_revenue')
    ])
    print('Method 1 works:')
    print(result)
except Exception as e:
    print('Method 1 failed:', e)

try:
    result = df.sort('session_id').with_columns([
        pl.col('revenue').cumsum().alias('cumulative_revenue')
    ])
    print('Method 2 works:')
    print(result)
except Exception as e:
    print('Method 2 failed:', e)

try:
    result = df.sort('session_id').with_columns([
        pl.col('revenue').rolling_sum(window_size=1000000).over('session_id').alias('cumulative_revenue')
    ])
    print('Method 3 works:')
    print(result)
except Exception as e:
    print('Method 3 failed:', e)

try:
    result = df.sort('session_id').with_columns([
        pl.col('revenue').sum().over('session_id').alias('cumulative_revenue')
    ])
    print('Method 4 works:')
    print(result)
except Exception as e:
    print('Method 4 failed:', e)
