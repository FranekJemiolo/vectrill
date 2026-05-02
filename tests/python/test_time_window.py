#!/usr/bin/env python3
import sys
sys.path.insert(0, 'python')
import vectrill
import pandas as pd
import numpy as np

# Create test data similar to the log analysis test
np.random.seed(42)
num_logs = 100
data = []

log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
services = ['auth', 'payment', 'user']

for i in range(num_logs):
    timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(seconds=i * 0.1)
    level = np.random.choice(log_levels, p=[0.1, 0.6, 0.2, 0.08, 0.02])
    
    data.append({
        'timestamp': timestamp,
        'level': level,
        'service': np.random.choice(services),
    })

log_data = pd.DataFrame(data).sort_values('timestamp')

print("Test data:")
print(log_data.head(10))

# Test vectrill implementation
vectrill_df = vectrill.from_pandas(log_data)
vectrill_result = vectrill_df.sort('timestamp').with_columns([
    vectrill.functions.when(vectrill.col('level').is_in(['ERROR', 'FATAL'])).then(1).otherwise(0).alias('is_error')
]).with_columns([
    vectrill.functions.rolling_mean(vectrill.col('is_error'), window_size='1m').over(vectrill.window.order_by('timestamp')).alias('error_rate_1min')
])

print("\nVectrill result:")
result_pd = vectrill_result.to_pandas()
print(result_pd.head(10))
print(f"Error rate column created: {'error_rate_1min' in result_pd.columns}")
print(f"Error rate values: {result_pd['error_rate_1min'].dropna().head()}")
