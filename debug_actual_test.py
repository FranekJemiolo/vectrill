#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Try to reproduce the exact test scenario
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create log data similar to the test
np.random.seed(42)
log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
services = ['auth', 'payment', 'user', 'inventory', 'notification']

num_logs = 100
data = []

for i in range(num_logs):
    timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(seconds=i * 0.1)
    level = np.random.choice(log_levels, p=[0.1, 0.6, 0.2, 0.08, 0.02])
    service = np.random.choice(services)
    
    data.append({
        'timestamp': timestamp,
        'level': level,
        'service': service,
        'response_time_ms': np.random.exponential(100) if level in ['INFO', 'DEBUG'] else None
    })

log_data = pd.DataFrame(data).sort_values('timestamp')

print('Log data created successfully')

# Try the exact vectrill implementation from the test
print('Testing vectrill implementation...')
try:
    vectrill_df = vectrill.from_pandas(log_data)
    vectrill_result = vectrill_df.sort(['service', 'timestamp']).with_columns([
        functions.mean(col('response_time_ms')).over(window.partition_by('service')).alias('avg_response_time'),
        functions.median(col('response_time_ms')).over(window.partition_by('service')).alias('median_response_time'),
        functions.std(col('response_time_ms')).over(window.partition_by('service')).alias('std_response_time'),
        functions.sum(functions.when(col('level') == 'ERROR').then(1).otherwise(0)).over(window.partition_by('service')).alias('error_count')
    ])
    print('Vectrill implementation successful!')
    print('Result shape:', vectrill_result.to_pandas().shape)
    
except Exception as e:
    print('Error in vectrill implementation:', e)
    import traceback
    traceback.print_exc()
