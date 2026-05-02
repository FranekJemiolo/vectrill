#!/usr/bin/env python3

# Fix polars cumsum syntax
import polars as pl

# Test the current polars syntax
try:
    # Test data
    data = pl.DataFrame({
        'session_id': [1, 1, 2, 2],
        'revenue': [10, 20, 5, 15]
    })
    
    # Try the current syntax
    result = data.with_columns([
        pl.col('revenue').cumsum().over('session_id').alias('cumulative_revenue')
    ])
    print("Current syntax works!")
    print(result)
    
except Exception as e:
    print(f"Current syntax fails: {e}")
    
    # Try alternative syntax
    try:
        result = data.with_columns([
            pl.col('revenue').cumsum().over('session_id').alias('cumulative_revenue')
        ])
        print("Alternative syntax works!")
        print(result)
    except Exception as e2:
        print(f"Alternative syntax also fails: {e2}")
        
        # Try using sort + cumsum
        try:
            result = data.sort('session_id').with_columns([
                pl.col('revenue').cumsum().over('session_id').alias('cumulative_revenue')
            ])
            print("Sort + cumsum works!")
            print(result)
        except Exception as e3:
            print(f"Sort + cumsum also fails: {e3}")
