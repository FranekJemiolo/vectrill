#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug lag window function index mapping
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'group': ['X', 'X', 'Y', 'Y'],
    'value': [100, 200, 300, 400],
    'date': pd.date_range('2023-01-01', periods=4)
})

print("Testing lag window function index mapping:")

try:
    vectrill_df = vectrill.from_pandas(data)
    print("Original Vectrill DataFrame:")
    print(vectrill_df.to_pandas())
    
    # Create lag expression
    lag_expr = functions.lag(col('value')).over(window.partition_by('group').order_by('date'))
    print(f"Lag expression: {lag_expr}")
    
    # Check window spec
    window_spec = lag_expr.window_spec
    print(f"Window spec: {window_spec}")
    
    # Check if window spec has partition columns
    if hasattr(window_spec, 'partition_columns'):
        print(f"Partition columns: {window_spec.partition_columns}")
    if hasattr(window_spec, 'order_columns'):
        print(f"Order columns: {window_spec.order_columns}")
    
    # Test the actual window function
    result_df = vectrill_df.with_columns([lag_expr.alias('lag_value')])
    print("Result:")
    print(result_df.to_pandas())
    
    # Let's manually trace what should happen
    print("\n=== Manual trace ===")
    df = data.copy()
    print("Original df:")
    print(df)
    
    # Sort by partition and order columns
    partition_cols = ['group']
    order_cols = ['date']
    sort_cols = partition_cols + order_cols
    df_sorted = df.sort_values(sort_cols)
    print("Sorted df:")
    print(df_sorted)
    
    # Apply lag function
    df_sorted['lag_value'] = df_sorted.groupby(partition_cols)['value'].shift(1)
    print("After lag:")
    print(df_sorted)
    
    # Restore original order
    df_result = df_sorted.sort_index()
    print("Restored original order:")
    print(df_result)
    
    # Compare with Vectrill result
    vectrill_result = result_df.to_pandas()
    print("Vectrill result:")
    print(vectrill_result)
    
    # Check if they match
    match = np.array_equal(df_result['lag_value'].values, vectrill_result['lag_value'].values, equal_nan=True)
    print(f"Results match: {match}")
    
    if not match:
        print("Differences:")
        for i in range(len(df_result)):
            manual_val = df_result.iloc[i]['lag_value']
            vectrill_val = vectrill_result.iloc[i]['lag_value']
            if (pd.isna(manual_val) and not pd.isna(vectrill_val)) or (not pd.isna(manual_val) and pd.isna(vectrill_val)):
                print(f"  Row {i}: manual={manual_val}, vectrill={vectrill_val} (NaN mismatch)")
            elif not pd.isna(manual_val) and not pd.isna(vectrill_val):
                if abs(manual_val - vectrill_val) > 1e-10:
                    print(f"  Row {i}: manual={manual_val}, vectrill={vectrill_val}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
