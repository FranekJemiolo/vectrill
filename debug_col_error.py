#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Try to reproduce the error
try:
    from test_streaming_use_cases import TestLogAnalysisPipeline
    import vectrill
    from vectrill.dataframe import col, functions, window
    
    # Create a simple test case with timestamp
    import pandas as pd
    log_data = pd.DataFrame({
        'service': ['auth', 'payment', 'auth'],
        'response_time_ms': [100, 200, 150],
        'level': ['INFO', 'ERROR', 'INFO'],
        'timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:01:00'), pd.Timestamp('2023-01-01 10:02:00')]
    })
    
    # Try the vectrill implementation
    vectrill_df = vectrill.from_pandas(log_data)
    vectrill_result = vectrill_df.sort(['service', 'timestamp']).with_columns([
        functions.mean(col('response_time_ms')).over(window.partition_by('service')).alias('avg_response_time')
    ])
    print('Test passed')
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
