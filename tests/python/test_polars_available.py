#!/usr/bin/env python3

# Test available polars functions
import polars as pl

# Test the correct polars syntax
try:
    # Test data
    data = pl.DataFrame({
        'session_id': [1, 1, 2, 2],
        'revenue': [10, 20, 5, 15]
    })
    
    # Try using shift + sum
    try:
        result = data.sort('session_id').with_columns([
            (pl.col('revenue').shift_and_fill(fill_value=0, periods=1).cumsum().over('session_id')).alias('cumulative_revenue')
        ])
        print("shift + sum works!")
        print(result)
    except Exception as e:
        print(f"shift + sum fails: {e}")
    
    # Try using expanding_sum
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').expanding_sum().over('session_id').alias('cumulative_revenue')
        ])
        print("expanding_sum works!")
        print(result)
    except Exception as e:
        print(f"expanding_sum fails: {e}")
        
    # Try using window functions with different approach
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').rolling_sum(window_size=1000000).over('session_id').alias('cumulative_revenue')
        ])
        print("rolling_sum with large window works!")
        print(result)
    except Exception as e:
        print(f"rolling_sum with large window fails: {e}")
        
    # Try using a different approach - sort and then use cumsum on the column
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').cumsum().alias('cumulative_revenue')
        ])
        print("simple cumsum works!")
        print(result)
    except Exception as e:
        print(f"simple cumsum fails: {e}")
        
except Exception as e:
    print(f"Test failed: {e}")
