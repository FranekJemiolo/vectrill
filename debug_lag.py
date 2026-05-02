#!/usr/bin/env python3
import sys
sys.path.insert(0, 'python')
import vectrill
import pandas as pd
import numpy as np

# Create test data similar to transaction frequency test
np.random.seed(42)
transaction_data = []

for i in range(100):
    user_id = np.random.randint(1, 10)
    timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(seconds=i * np.random.randint(1, 300))
    amount = np.random.uniform(10, 1000)
    
    transaction_data.append({
        'user_id': user_id,
        'timestamp': timestamp,
        'amount': amount
    })

transaction_data = pd.DataFrame(transaction_data)

print("Test data:")
print(transaction_data.head(10))

# Test vectrill implementation
vectrill_df = vectrill.from_pandas(transaction_data)
vectrill_result = vectrill_df.sort(['user_id', 'timestamp']).with_columns([
    vectrill.functions.lag(vectrill.col('timestamp'), 1).over(vectrill.window.partition_by('user_id').order_by('timestamp')).alias('prev_timestamp')
]).with_columns([
    (vectrill.col('timestamp') - vectrill.col('prev_timestamp')).alias('time_diff_seconds'),
    vectrill.functions.when(vectrill.col('time_diff_seconds').is_null()).then(0).otherwise(vectrill.col('time_diff_seconds')).alias('time_diff_seconds'),
    vectrill.functions.when(vectrill.col('time_diff_seconds') < 60).then(1).otherwise(0).alias('rapid_transaction')
])

print("\nVectrill result:")
result_pd = vectrill_result.to_pandas()
print(result_pd.head(10))
print(f"Columns: {result_pd.columns.tolist()}")
