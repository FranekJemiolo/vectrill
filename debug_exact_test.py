#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug exact test execution
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Replicate the exact test logic
class TestComprehensiveFunctions:
    def __init__(self):
        self.VECTRILL_AVAILABLE = True
        
    def time_series_data(self):
        """Time series data for window operations"""
        np.random.seed(42)  # Make deterministic
        dates = pd.date_range('2023-01-01', periods=50, freq='D')
        return pd.DataFrame({
            'date': dates,
            'value': np.random.randn(50).cumsum() + 100,
            'group': np.random.choice(['X', 'Y', 'Z'], 50)
        })
    
    def test_window_functions_lag(self):
        """Test lag window function"""
        if not self.VECTRILL_AVAILABLE:
            print("Vectrill not available")
            return False
        
        # Get test data
        time_series_data = self.time_series_data()
        print("Test data shape:", time_series_data.shape)
        print("Test data groups:", time_series_data['group'].value_counts().sort_index())
        
        # Test pandas
        pandas_sorted = time_series_data.sort_values(['group', 'date'])
        pandas_sorted['lag_value'] = pandas_sorted.groupby('group')['value'].shift(1)
        pandas_result = pandas_sorted.sort_index()
        
        # Test Vectrill
        vectrill_df = vectrill.from_pandas(time_series_data)
        vectrill_result = vectrill_df.sort(['group', 'date']).with_columns([
            functions.lag(col('value'), 1).over(window.partition_by('group').order_by('date')).alias('lag_value')
        ]).to_pandas()
        
        print("Pandas result shape:", pandas_result.shape)
        print("Vectrill result shape:", vectrill_result.shape)
        
        # Compare results
        try:
            np.testing.assert_allclose(vectrill_result['lag_value'].values, pandas_result['lag_value'].values, rtol=1e-10, atol=1e-10, equal_nan=True)
            print("Test PASSED")
            return True
        except AssertionError as e:
            print(f"Test FAILED: {e}")
            print("Pandas lag_value (first 10):", pandas_result['lag_value'].head(10).tolist())
            print("Vectrill lag_value (first 10):", vectrill_result['lag_value'].head(10).tolist())
            
            # Check if the data is the same
            print("Data identical:", pandas_result[['group', 'date', 'value']].equals(vectrill_result[['group', 'date', 'value']]))
            
            # Check the sorted versions
            pandas_sorted_result = pandas_result.sort_values(['group', 'date']).reset_index(drop=True)
            vectrill_sorted_result = vectrill_result.sort_values(['group', 'date']).reset_index(drop=True)
            
            print("Pandas sorted lag_value (first 10):", pandas_sorted_result['lag_value'].head(10).tolist())
            print("Vectrill sorted lag_value (first 10):", vectrill_sorted_result['lag_value'].head(10).tolist())
            
            return False

# Run the test
test = TestComprehensiveFunctions()
result = test.test_window_functions_lag()
print(f"Test result: {result}")
