#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug WhenExpression handling
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data with NaN values
data = pd.DataFrame({
    'time_diff_seconds': [np.nan, 100.0, 50.0, 200.0, 30.0]
})

print('Original data:')
print(data)
print('Data types:')
print(data.dtypes)
print('Null values:')
print(data.isnull())

# Test vectrill WhenExpression
print('\nTesting vectrill WhenExpression...')

try:
    vectrill_df = vectrill.from_pandas(data)
    print('Vectrill DataFrame created')
    
    # Test the when expression
    result = vectrill_df.with_columns([
        functions.when(col('time_diff_seconds').is_null()).then(0).otherwise(col('time_diff_seconds')).alias('time_diff_seconds_fixed')
    ])
    
    print('WhenExpression result:')
    print(result.to_pandas())
    print('Fixed null values:')
    print(result.to_pandas()['time_diff_seconds_fixed'].isnull())

except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
