#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Try to understand the scoping issue
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

print('Initial col function:', col)
print('col function id:', id(col))

# Try to simulate the exact test context
class TestLogAnalysisPipeline:
    def test_service_performance_metrics(self, log_data):
        """Test service performance metrics calculation"""
        print('Inside test method, col function:', col)
        print('Inside test method, col function id:', id(col))
        
        # Pandas implementation
        pandas_result = log_data.copy()
        pandas_result = pandas_result.sort_values(['service', 'timestamp'])
        
        # Calculate performance metrics per service
        service_metrics = pandas_result.groupby('service').agg({
            'response_time_ms': ['mean', 'median', 'std'],
            'level': lambda x: (x == 'ERROR').sum()
        }).reset_index()
        service_metrics.columns = ['service', 'avg_response_time', 'median_response_time', 'std_response_time', 'error_count']
        
        pandas_result = pandas_result.merge(service_metrics, on='service')
        
        print('Before vectrill implementation, col function:', col)
        
        # Vectrill implementation
        vectrill_df = vectrill.from_pandas(log_data)
        vectrill_result = vectrill_df.sort(['service', 'timestamp']).with_columns([
            functions.mean(col('response_time_ms')).over(window.partition_by('service')).alias('avg_response_time'),
            functions.median(col('response_time_ms')).over(window.partition_by('service')).alias('median_response_time'),
            functions.std(col('response_time_ms')).over(window.partition_by('service')).alias('std_response_time'),
            functions.sum(functions.when(col('level') == 'ERROR').then(1).otherwise(0)).over(window.partition_by('service')).alias('error_count')
        ])
        
        return vectrill_result

# Create test data
np.random.seed(42)
log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
services = ['auth', 'payment', 'user', 'inventory', 'notification']

num_logs = 20
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

# Run the test
test_instance = TestLogAnalysisPipeline()
try:
    result = test_instance.test_service_performance_metrics(log_data)
    print('Test passed!')
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
