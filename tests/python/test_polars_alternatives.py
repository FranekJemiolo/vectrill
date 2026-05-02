#!/usr/bin/env python3

# Test alternative polars syntax for cumsum
import polars as pl

# Test the correct polars syntax
try:
    # Test data
    data = pl.DataFrame({
        'session_id': [1, 1, 2, 2],
        'revenue': [10, 20, 5, 15]
    })
    
    # Try using rolling_sum
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').rolling_sum(window_size=1000000).over('session_id').alias('cumulative_revenue')
        ])
        print("rolling_sum works!")
        print(result)
    except Exception as e:
        print(f"rolling_sum fails: {e}")
    
    # Try using cumsum without over
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').cumsum().alias('cumulative_revenue')
        ])
        print("cumsum without over works!")
        print(result)
    except Exception as e:
        print(f"cumsum without over fails: {e}")
        
    # Try using groupby approach
    try:
        result = data.groupby('session_id').agg([
            pl.col('revenue').cumsum().alias('cumulative_revenue')
        ]).explode('cumulative_revenue')
        print("groupby approach works!")
        print(result)
    except Exception as e:
        print(f"groupby approach fails: {e}")
        
    # Try using shift + cumsum
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').cumsum().alias('cumulative_revenue')
        ])
        print("shift + cumsum works!")
        print(result)
    except Exception as e:
        print(f"shift + cumsum fails: {e}")
        
except Exception as e:
    print(f"Test failed: {e}")
