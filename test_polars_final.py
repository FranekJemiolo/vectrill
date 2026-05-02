#!/usr/bin/env python3

# Test final polars syntax for cumsum
import polars as pl

# Test data
data = pl.DataFrame({
    'session_id': [1, 1, 2, 2],
    'revenue': [10, 20, 5, 15]
})

# Try using cumsum on the column directly
try:
    result = data.sort('session_id').with_columns([
        pl.col('revenue').cumsum().alias('cumulative_revenue')
    ])
    print("direct cumsum works!")
    print(result)
except Exception as e:
    print(f"direct cumsum fails: {e}")

# Try using shift + sum
try:
    result = data.sort('session_id').with_columns([
        (pl.col('revenue').shift(1).fill_null(0) + pl.col('revenue')).alias('cumulative_revenue')
    ])
    print("shift + sum works!")
    print(result)
except Exception as e:
    print(f"shift + sum fails: {e}")

# Try using window functions with different approach
try:
    result = data.sort('session_id').with_columns([
        pl.col('revenue').rolling_sum(window_size=1000000).over('session_id').alias('cumulative_revenue')
    ])
    print("rolling_sum works!")
    print(result)
except Exception as e:
    print(f"rolling_sum fails: {e}")

# Try using cumsum on the column without over
try:
    result = data.sort('session_id').with_columns([
        pl.col('revenue').cumsum().alias('cumulative_revenue')
    ])
    print("cumsum without over works!")
    print(result)
except Exception as e:
    print(f"cumsum without over fails: {e}")
