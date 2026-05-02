#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Test new aggregation functions
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create test data
data = pd.DataFrame({
    'user_id': [1, 1, 1, 2, 2, 2],
    'amount': [100, -200, 300, -150, 250, 350],
    'timestamp': [
        pd.Timestamp('2023-01-01 10:00:00'),
        pd.Timestamp('2023-01-01 10:01:00'),
        pd.Timestamp('2023-01-01 10:02:00'),
        pd.Timestamp('2023-01-01 10:00:30'),
        pd.Timestamp('2023-01-01 10:01:30'),
        pd.Timestamp('2023-01-01 10:02:30')
    ]
})

print("Original data:")
print(data)

# Test new functions
print("\n=== Testing New Functions ===")

try:
    vectrill_df = vectrill.from_pandas(data)
    
    # Test abs function
    print("Testing abs function:")
    result_abs = vectrill_df.with_columns([
        functions.abs(col('amount')).alias('abs_amount')
    ])
    print(result_abs.to_pandas())
    
    # Test variance function
    print("\nTesting var function:")
    result_var = vectrill_df.with_columns([
        functions.var(col('amount')).alias('var_amount')
    ])
    print(result_var.to_pandas())
    
    # Test lead function
    print("\nTesting lead function:")
    result_lead = vectrill_df.sort(['user_id', 'timestamp']).with_columns([
        functions.lead(col('amount'), 1).over(window.partition_by('user_id').order_by('timestamp')).alias('lead_amount')
    ])
    print(result_lead.to_pandas())
    
    # Test round function
    print("\nTesting round function:")
    result_round = vectrill_df.with_columns([
        functions.round(col('amount'), 0).alias('round_amount')
    ])
    print(result_round.to_pandas())
    
    # Test floor function
    print("\nTesting floor function:")
    result_floor = vectrill_df.with_columns([
        functions.floor(col('amount')).alias('floor_amount')
    ])
    print(result_floor.to_pandas())
    
    # Test ceil function
    print("\nTesting ceil function:")
    result_ceil = vectrill_df.with_columns([
        functions.ceil(col('amount')).alias('ceil_amount')
    ])
    print(result_ceil.to_pandas())
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
