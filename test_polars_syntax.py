#!/usr/bin/env python3

# Test correct polars syntax for cumsum
import polars as pl

# Test the correct polars syntax
try:
    # Test data
    data = pl.DataFrame({
        'session_id': [1, 1, 2, 2],
        'revenue': [10, 20, 5, 15]
    })
    
    # Try using sort_by + cumsum
    try:
        result = data.sort_by('session_id').with_columns([
            pl.col('revenue').cumsum().over('session_id').alias('cumulative_revenue')
        ])
        print("sort_by + cumsum works!")
        print(result)
    except Exception as e:
        print(f"sort_by + cumsum fails: {e}")
    
    # Try using groupby + cumsum
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').cumsum().over('session_id').alias('cumulative_revenue')
        ])
        print("sort + cumsum works!")
        print(result)
    except Exception as e:
        print(f"sort + cumsum fails: {e}")
        
    # Try using pandas-style approach
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').cumsum().over('session_id').alias('cumulative_revenue')
        ])
        print("pandas-style works!")
        print(result)
    except Exception as e:
        print(f"pandas-style fails: {e}")
        
    # Try using window functions
    try:
        result = data.sort('session_id').with_columns([
            pl.col('revenue').cumsum().over('session_id').alias('cumulative_revenue')
        ])
        print("window function works!")
        print(result)
    except Exception as e:
        print(f"window function fails: {e}")
        
except Exception as e:
    print(f"Test failed: {e}")
