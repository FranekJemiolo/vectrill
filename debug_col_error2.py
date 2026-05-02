#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Let's try to run just the problematic part of the test
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd

# Create actual log data like in the test
log_data = pd.DataFrame({
    'timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:00:10'), pd.Timestamp('2023-01-01 10:00:20')],
    'level': ['INFO', 'ERROR', 'INFO'],
    'service': ['auth', 'payment', 'auth'],
    'response_time_ms': [100, 200, 150]
})

print('Original data:')
print(log_data)

# Try the vectrill implementation step by step
vectrill_df = vectrill.from_pandas(log_data)
print('\nVectrill df created successfully')

sorted_df = vectrill_df.sort(['service', 'timestamp'])
print('\nSorted successfully')

# Try just one column to isolate the issue
result = sorted_df.with_columns([
    functions.mean(col('response_time_ms')).over(window.partition_by('service')).alias('avg_response_time')
])
print('\nWindow function applied successfully')
print('Result:')
print(result.to_pandas())
