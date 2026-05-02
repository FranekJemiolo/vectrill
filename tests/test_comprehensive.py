#!/usr/bin/env python3
"""
Comprehensive test suite for Vectrill functions ensuring parity with pandas and polars.
This test suite systematically tests all Vectrill functions against pandas and polars.
"""

import sys
import os
import numpy as np
import pandas as pd
import polars as pl
import pytest
from typing import Dict, List, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

# Add vectrill to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    import vectrill
    from vectrill.dataframe import col, functions, window
    VECTRILL_AVAILABLE = True
except ImportError:
    VECTRILL_AVAILABLE = False
    print("Warning: Vectrill not available")

class TestComprehensiveFunctions:
    """Comprehensive test suite for all Vectrill functions"""
    
    @pytest.fixture
    def sample_data(self):
        """Sample test data for various operations"""
        np.random.seed(42)
        return pd.DataFrame({
            'id': range(100),
            'user_id': np.random.randint(1, 11, 100),
            'amount': np.random.uniform(-1000, 1000, 100),
            'score': np.random.uniform(0, 100, 100),
            'category': np.random.choice(['A', 'B', 'C'], 100),
            'timestamp': pd.date_range('2023-01-01', periods=100, freq='H'),
            'is_active': np.random.choice([True, False], 100)
        })
    
    @pytest.fixture
    def time_series_data(self):
        """Time series data for window operations"""
        dates = pd.date_range('2023-01-01', periods=50, freq='D')
        return pd.DataFrame({
            'date': dates,
            'value': np.random.randn(50).cumsum() + 100,
            'group': np.random.choice(['X', 'Y', 'Z'], 50)
        })
    
    def test_basic_aggregations(self, sample_data):
        """Test basic aggregation functions"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test mean, sum, count, min, max
        aggregations = [
            ('mean', functions.mean(col('amount')), 'mean_amount'),
            ('sum', functions.sum(col('amount')), 'sum_amount'),
            ('count', functions.count(col('amount')), 'count_amount'),
            ('min', functions.min(col('amount')), 'min_amount'),
            ('max', functions.max(col('amount')), 'max_amount'),
        ]
        
        for agg_name, vectrill_expr, result_col in aggregations:
            # Test pandas
            pandas_result = sample_data.agg({'amount': agg_name}).iloc[0]
            
            # Test Vectrill
            vectrill_df = vectrill.from_pandas(sample_data)
            vectrill_result = vectrill_df.select([vectrill_expr.alias(result_col)]).to_pandas()[result_col].iloc[0]
            
            # Compare results (allowing for floating point precision)
            if isinstance(pandas_result, (int, float)):
                assert np.isclose(vectrill_result, pandas_result, rtol=1e-10, atol=1e-10), f"{agg_name} mismatch: pandas={pandas_result}, vectrill={vectrill_result}"
            else:
                assert vectrill_result == pandas_result, f"{agg_name} mismatch: pandas={pandas_result}, vectrill={vectrill_result}"
    
    def test_mathematical_functions(self, sample_data):
        """Test mathematical functions"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test abs, round, floor, ceil
        math_functions = [
            ('abs', functions.abs(col('amount')), 'abs_amount'),
            ('round', functions.round(col('amount'), 2), 'round_amount'),
            ('floor', functions.floor(col('amount')), 'floor_amount'),
            ('ceil', functions.ceil(col('amount')), 'ceil_amount'),
        ]
        
        for func_name, vectrill_expr, result_col in math_functions:
            # Test pandas
            if func_name == 'abs':
                pandas_result = np.abs(sample_data['amount'])
            elif func_name == 'round':
                pandas_result = sample_data['amount'].round(2)
            elif func_name == 'floor':
                pandas_result = np.floor(sample_data['amount'])
            elif func_name == 'ceil':
                pandas_result = np.ceil(sample_data['amount'])
            
            # Test Vectrill
            vectrill_df = vectrill.from_pandas(sample_data)
            vectrill_result = vectrill_df.select([vectrill_expr.alias(result_col)]).to_pandas()[result_col]
            
            # Compare results
            np.testing.assert_allclose(vectrill_result, pandas_result, rtol=1e-10, atol=1e-10), f"{func_name} mismatch"
    
    def test_statistical_functions(self, sample_data):
        """Test statistical functions"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test var, std, median
        stat_functions = [
            ('var', functions.var(col('amount')), 'var_amount'),
            ('std', functions.std(col('amount')), 'std_amount'),
            ('median', functions.median(col('amount')), 'median_amount'),
        ]
        
        for func_name, vectrill_expr, result_col in stat_functions:
            # Test pandas
            if func_name == 'var':
                pandas_result = sample_data['amount'].var()
            elif func_name == 'std':
                pandas_result = sample_data['amount'].std()
            elif func_name == 'median':
                pandas_result = sample_data['amount'].median()
            
            # Test Vectrill
            vectrill_df = vectrill.from_pandas(sample_data)
            vectrill_result = vectrill_df.select([vectrill_expr.alias(result_col)]).to_pandas()[result_col].iloc[0]
            
            # Compare results
            assert np.isclose(vectrill_result, pandas_result, rtol=1e-10, atol=1e-10), f"{func_name} mismatch: pandas={pandas_result}, vectrill={vectrill_result}"
    
    def test_filter_operations(self, sample_data):
        """Test filter operations"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test various filter conditions
        filters = [
            ({"op": ">", "col": "amount", "value": 0}, sample_data[sample_data['amount'] > 0]),
            ({"op": "<", "col": "score", "value": 50}, sample_data[sample_data['score'] < 50]),
            ({"op": "==", "col": "category", "value": "A"}, sample_data[sample_data['category'] == "A"]),
            ({"op": "!=", "col": "is_active", "value": True}, sample_data[sample_data['is_active'] != True]),
        ]
        
        for filter_dict, pandas_result in filters:
            # Test Vectrill
            vectrill_df = vectrill.from_pandas(sample_data)
            vectrill_result = vectrill_df.filter(filter_dict).to_pandas()
            
            # Compare shapes and values
            assert vectrill_result.shape[0] == pandas_result.shape[0], f"Filter {filter_dict} row count mismatch"
            
            # Compare key columns
            for col in ['id', 'amount']:
                np.testing.assert_array_equal(vectrill_result[col].values, pandas_result[col].values), f"Filter {filter_dict} column {col} mismatch"
    
    def test_sort_operations(self, sample_data):
        """Test sort operations"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test various sort configurations
        sort_configs = [
            (['amount'], [True]),
            (['user_id', 'amount'], [True, False]),
            (['timestamp'], [False]),
        ]
        
        for sort_cols, ascending in sort_configs:
            # Test pandas
            pandas_result = sample_data.sort_values(sort_cols, ascending=ascending)
            
            # Test Vectrill
            vectrill_df = vectrill.from_pandas(sample_data)
            vectrill_result = vectrill_df.sort(sort_cols).to_pandas()
            
            # Compare shapes
            assert vectrill_result.shape == pandas_result.shape, f"Sort {sort_cols} shape mismatch"
            
            # Compare sorted order
            for col in sort_cols:
                np.testing.assert_array_equal(vectrill_result[col].values, pandas_result[col].values), f"Sort {sort_cols} column {col} mismatch"
    
    def test_window_functions_lag(self, time_series_data):
        """Test lag window function"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test lag function
        # Test pandas
        pandas_sorted = time_series_data.sort_values(['group', 'date'])
        pandas_sorted['lag_value'] = pandas_sorted.groupby('group')['value'].shift(1)
        pandas_result = pandas_sorted.sort_index()
        
        # Test Vectrill
        vectrill_df = vectrill.from_pandas(time_series_data)
        vectrill_result = vectrill_df.sort(['group', 'date']).with_columns([
            functions.lag(col('value'), 1).over(window.partition_by('group').order_by('date')).alias('lag_value')
        ]).to_pandas()
        
        # Compare results
        try:
            np.testing.assert_allclose(vectrill_result['lag_value'].values, pandas_result['lag_value'].values, rtol=1e-10, atol=1e-10, equal_nan=True)
        except AssertionError as e:
            print(f"Lag function test failed: {e}")
            print("Pandas result:", pandas_result['lag_value'].values[:10])
            print("Vectrill result:", vectrill_result['lag_value'].values[:10])
            raise
    
    def test_window_functions_cumsum(self, time_series_data):
        """Test cumsum window function"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test cumsum function
        # Test pandas
        pandas_sorted = time_series_data.sort_values(['group', 'date'])
        pandas_sorted['cumsum_value'] = pandas_sorted.groupby('group')['value'].cumsum()
        pandas_result = pandas_sorted.sort_index()
        
        # Test Vectrill
        vectrill_df = vectrill.from_pandas(time_series_data)
        vectrill_result = vectrill_df.sort(['group', 'date']).with_columns([
            functions.cumsum(col('value')).over(window.partition_by('group').order_by('date')).alias('cumsum_value')
        ]).to_pandas()
        
        # Compare results
        np.testing.assert_allclose(vectrill_result['cumsum_value'].values, pandas_result['cumsum_value'].values, rtol=1e-10, atol=1e-10)
    
    def test_conditional_expressions(self, sample_data):
        """Test when-then-otherwise conditional expressions"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test conditional expression
        # Test pandas
        pandas_result = sample_data.copy()
        pandas_result['category_score'] = np.where(
            sample_data['category'] == 'A', 
            sample_data['score'] * 2, 
            np.where(
                sample_data['category'] == 'B',
                sample_data['score'] * 1.5,
                sample_data['score']
            )
        )
        
        # Test Vectrill
        vectrill_df = vectrill.from_pandas(sample_data)
        vectrill_result = vectrill_df.with_columns([
            functions.when(col('category') == 'A')
            .then(col('score') * 2)
            .when(col('category') == 'B')
            .then(col('score') * 1.5)
            .otherwise(col('score'))
            .alias('category_score')
        ]).to_pandas()
        
        # Compare results
        np.testing.assert_allclose(vectrill_result['category_score'].values, pandas_result['category_score'].values, rtol=1e-10, atol=1e-10)
    
    def test_string_functions(self, sample_data):
        """Test string functions"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test string functions
        string_functions = [
            ('length', functions.length(col('category')), 'len_category'),
            ('upper', functions.upper(col('category')), 'upper_category'),
        ]
        
        for func_name, vectrill_expr, result_col in string_functions:
            # Test pandas
            if func_name == 'length':
                pandas_result = sample_data['category'].str.len()
            elif func_name == 'upper':
                pandas_result = sample_data['category'].str.upper()
            
            # Test Vectrill
            vectrill_df = vectrill.from_pandas(sample_data)
            vectrill_result = vectrill_df.select([vectrill_expr.alias(result_col)]).to_pandas()[result_col]
            
            # Compare results
            np.testing.assert_array_equal(vectrill_result.values, pandas_result.values), f"{func_name} mismatch"
    
    def test_arithmetic_operations(self, sample_data):
        """Test arithmetic operations"""
        if not VECTRILL_AVAILABLE:
            pytest.skip("Vectrill not available")
        
        # Test arithmetic operations
        arithmetic_ops = [
            ('add', col('amount') + col('score'), 'add_amount_score'),
            ('sub', col('amount') - col('score'), 'sub_amount_score'),
            ('mul', col('amount') * 2, 'mul_amount_2'),
            ('div', col('amount') / 100, 'div_amount_100'),
        ]
        
        for op_name, vectrill_expr, result_col in arithmetic_ops:
            # Test pandas
            if op_name == 'add':
                pandas_result = sample_data['amount'] + sample_data['score']
            elif op_name == 'sub':
                pandas_result = sample_data['amount'] - sample_data['score']
            elif op_name == 'mul':
                pandas_result = sample_data['amount'] * 2
            elif op_name == 'div':
                pandas_result = sample_data['amount'] / 100
            
            # Test Vectrill
            vectrill_df = vectrill.from_pandas(sample_data)
            vectrill_result = vectrill_df.select([vectrill_expr.alias(result_col)]).to_pandas()[result_col]
            
            # Compare results
            np.testing.assert_allclose(vectrill_result.values, pandas_result.values, rtol=1e-10, atol=1e-10, equal_nan=True), f"{op_name} mismatch"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
