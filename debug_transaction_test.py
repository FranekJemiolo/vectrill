#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug transaction frequency test
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create the same test data as the test
np.random.seed(42)
num_users = 1000
transactions_per_user = np.random.poisson(12, num_users) + 1

data = []
for user_id in range(1, num_users + 1):
    num_tx = transactions_per_user[user_id - 1]
    base_time = pd.Timestamp('2023-01-01') + pd.Timedelta(days=np.random.randint(0, 30))
    
    for tx_idx in range(num_tx):
        # Add some rapid transactions for testing
        if tx_idx > 0 and np.random.random() < 0.2:  # 20% chance of rapid transaction
            time_diff = np.random.randint(1, 300)  # 1-300 seconds
        else:
            time_diff = np.random.randint(300, 86400)  # 5 minutes to 1 day
        
        data.append({
            'user_id': user_id,
            'timestamp': base_time + pd.Timedelta(seconds=time_diff * tx_idx),
            'amount': np.random.uniform(10, 1000)
        })

transaction_data = pd.DataFrame(data).sort_values('timestamp')

print(f"Test data shape: {transaction_data.shape}")
print(f"Sample data:")
print(transaction_data.head(10))

# Pandas implementation
print("\n=== Pandas Implementation ===")
pandas_result = transaction_data.copy()
pandas_result = pandas_result.sort_values(['user_id', 'timestamp'])
pandas_result['prev_timestamp'] = pandas_result.groupby('user_id')['timestamp'].shift(1)
pandas_result['time_diff_seconds'] = (pandas_result['timestamp'] - pandas_result['prev_timestamp']).dt.total_seconds()
pandas_result['time_diff_seconds'] = pandas_result['time_diff_seconds'].fillna(0)
pandas_result['rapid_transaction'] = (pandas_result['time_diff_seconds'] < 60).astype(int)

print("Pandas result sample:")
print(pandas_result.head(10))
print(f"Pandas rapid transactions: {pandas_result['rapid_transaction'].sum()}")

# Vectrill implementation
print("\n=== Vectrill Implementation ===")
try:
    vectrill_df = vectrill.from_pandas(transaction_data)
    print("Created vectrill DataFrame")
    
    # Step 1: Sort
    vectrill_sorted = vectrill_df.sort(['user_id', 'timestamp'])
    print("Step 1: Sort completed")
    
    # Step 2: Add lag column
    vectrill_with_lag = vectrill_sorted.with_columns([
        functions.lag(col('timestamp'), 1).over(window.partition_by('user_id').order_by('timestamp')).alias('prev_timestamp')
    ])
    print("Step 2: Lag column added")
    
    # Step 3: Calculate time difference
    vectrill_with_diff = vectrill_with_lag.with_columns([
        (col('timestamp') - col('prev_timestamp')).alias('time_diff_seconds')
    ])
    print("Step 3: Time difference calculated")
    
    # Step 4: Fill NaN values
    vectrill_filled = vectrill_with_diff.with_columns([
        functions.when(col('time_diff_seconds').is_null()).then(0).otherwise(col('time_diff_seconds')).alias('time_diff_seconds')
    ])
    print("Step 4: NaN values filled")
    
    # Step 5: Flag rapid transactions
    vectrill_final = vectrill_filled.with_columns([
        functions.when(col('time_diff_seconds') < 60).then(1).otherwise(0).alias('rapid_transaction')
    ])
    print("Step 5: Rapid transactions flagged")
    
    vectrill_result = vectrill_final.to_pandas()
    print("Vectrill result sample:")
    print(vectrill_result.head(10))
    print(f"Vectrill rapid transactions: {vectrill_result['rapid_transaction'].sum()}")
    
    # Compare results
    print("\n=== Comparison ===")
    pandas_sorted = pandas_result.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
    vectrill_sorted = vectrill_result.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
    
    print(f"Shapes match: {pandas_sorted.shape == vectrill_sorted.shape}")
    
    # Check time_diff_seconds
    time_diff_match = np.allclose(pandas_sorted['time_diff_seconds'].values, 
                                 vectrill_sorted['time_diff_seconds'].values, 
                                 rtol=1e-10, equal_nan=True)
    print(f"Time diff seconds match: {time_diff_match}")
    
    if not time_diff_match:
        diff = np.abs(pandas_sorted['time_diff_seconds'].values - vectrill_sorted['time_diff_seconds'].values)
        max_diff = np.nanmax(diff)
        print(f"Max difference in time_diff_seconds: {max_diff}")
        
        # Find mismatches
        mismatches = ~np.isclose(pandas_sorted['time_diff_seconds'].values, 
                                vectrill_sorted['time_diff_seconds'].values, 
                                rtol=1e-10, equal_nan=True)
        print(f"Number of mismatches: {mismatches.sum()}")
        
        if mismatches.sum() > 0:
            print("First few mismatches:")
            for i in range(min(5, mismatches.sum())):
                idx = np.where(mismatches)[0][i]
                print(f"  Row {idx}: pandas={pandas_sorted.iloc[idx]['time_diff_seconds']}, "
                      f"vectrill={vectrill_sorted.iloc[idx]['time_diff_seconds']}")
    
    # Check rapid_transaction
    rapid_match = np.array_equal(pandas_sorted['rapid_transaction'].values, 
                                vectrill_sorted['rapid_transaction'].values)
    print(f"Rapid transaction match: {rapid_match}")
    
    if not rapid_match:
        rapid_diff = pandas_sorted['rapid_transaction'].values - vectrill_sorted['rapid_transaction'].values
        print(f"Rapid transaction differences: {np.unique(rapid_diff)}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
