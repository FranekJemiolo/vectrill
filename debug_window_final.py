#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Final attempt to debug window function with exact pandas logic
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'user_id': [1, 1, 1, 2, 2, 2],
    'amount': [100, 200, 300, 150, 250, 350],
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

# Pandas implementation step by step
print("\n=== Pandas Implementation Step by Step ===")
pandas_step1 = data.copy()
print("Step 1 - Original:")
print(pandas_step1)

pandas_step2 = pandas_step1.sort_values(['user_id', 'timestamp'])
print("Step 2 - Sorted:")
print(pandas_step2)

pandas_step3 = pandas_step2.copy()
pandas_step3['prev_amount'] = pandas_step3.groupby('user_id')['amount'].shift(1)
print("Step 3 - After lag:")
print(pandas_step3)

# Now let me try to implement the exact same logic in Vectrill
print("\n=== Vectrill Custom Implementation ===")
try:
    # Create a custom implementation that exactly mimics pandas
    def custom_lag_function(df, col_name, partition_cols, order_cols):
        # Step 1: Sort the DataFrame
        df_sorted = df.sort_values(order_cols)
        print(f"After sorting by {order_cols}:")
        print(df_sorted)
        
        # Step 2: Apply lag function
        df_sorted['result'] = df_sorted.groupby(partition_cols)[col_name].shift(1)
        print("After applying lag:")
        print(df_sorted)
        
        # Step 3: Restore original order
        df_result = df_sorted.sort_index()
        print("After restoring original order:")
        print(df_result)
        
        return df_result['result'].values
    
    # Test the custom function
    vectrill_df = vectrill.from_pandas(data)
    df_internal = vectrill_df._arrow_table.to_pandas()
    
    result = custom_lag_function(df_internal, 'amount', ['user_id'], ['user_id', 'timestamp'])
    
    print("Custom implementation result:")
    print(result)
    
    # Compare with pandas
    pandas_result = data.sort_values(['user_id', 'timestamp'])
    pandas_result['prev_amount'] = pandas_result.groupby('user_id')['amount'].shift(1)
    pandas_restored = pandas_result.sort_index()
    
    print("Pandas result:")
    print(pandas_restored['prev_amount'].values)
    
    # Check if they match
    match = np.array_equal(pandas_restored['prev_amount'].values, result, equal_nan=True)
    print(f"Results match: {match}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
