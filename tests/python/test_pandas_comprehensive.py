#!/usr/bin/env python3
"""
Comprehensive test suite comparing Vectrill functionality with pandas
Tests cover: basic operations, aggregations, complex expressions, edge cases
"""

import pytest
import numpy as np
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import sys
import os

# Add the vectrill module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))

try:
    import vectrill
except ImportError:
    pytest.skip("vectrill module not available", allow_module_level=True)


class TestBasicOperations:
    """Test basic data operations against pandas"""
    
    @pytest.fixture
    def sample_data(self):
        """Create sample test data"""
        return pd.DataFrame({
            'id': range(1000),
            'value': np.random.randn(1000) * 100,
            'category': np.random.choice(['A', 'B', 'C'], 1000),
            'timestamp': pd.date_range('2023-01-01', periods=1000, freq='h'),
            'flag': np.random.choice([True, False], 1000)
        })
    
    def test_filter_operations(self, sample_data):
        """Test filtering operations"""
        # Test numeric filtering
        pandas_result = sample_data[sample_data['value'] > 50]
        
        # Convert to Vectrill and test
        vectrill_df = vectrill.from_pandas(sample_data)
        vectrill_result = vectrill_df.filter(vectrill.col('value') > 50)
        
        # Compare results
        assert len(vectrill_result.to_pandas()) == len(pandas_result)
        
        # Test string filtering
        pandas_result = sample_data[sample_data['category'] == 'A']
        vectrill_result = vectrill_df.filter(vectrill.col('category') == 'A')
        assert len(vectrill_result.to_pandas()) == len(pandas_result)
        
        # Test boolean filtering
        pandas_result = sample_data[sample_data['flag'] == True]
        vectrill_result = vectrill_df.filter(vectrill.col('flag') == True)
        assert len(vectrill_result.to_pandas()) == len(pandas_result)
    
    def test_arithmetic_operations(self, sample_data):
        """Test arithmetic operations"""
        # Test addition
        pandas_result = sample_data.copy()
        pandas_result['value_plus_10'] = pandas_result['value'] + 10
        
        vectrill_df = vectrill.from_pandas(sample_data)
        vectrill_result = vectrill_df.with_column(
            vectrill.col('value') + 10,
            'value_plus_10'
        )
        
        # Compare results (allowing for floating point precision)
        vectrill_pd = vectrill_result.to_pandas()
        np.testing.assert_allclose(
            vectrill_pd['value_plus_10'].values,
            pandas_result['value_plus_10'].values,
            rtol=1e-10
        )
        
        # Test multiplication
        pandas_result['value_times_2'] = pandas_result['value'] * 2
        vectrill_result = vectrill_df.with_column(
            vectrill.col('value') * 2,
            'value_times_2'
        )
        vectrill_pd = vectrill_result.to_pandas()
        np.testing.assert_allclose(
            vectrill_pd['value_times_2'].values,
            pandas_result['value_times_2'].values,
            rtol=1e-10
        )
    
    def test_string_operations(self, sample_data):
        """Test string operations"""
        # Test string length
        pandas_result = sample_data.copy()
        pandas_result['category_len'] = pandas_result['category'].str.len()
        
        vectrill_df = vectrill.from_pandas(sample_data)
        vectrill_result = vectrill_df.with_column(
            vectrill.functions.length('category'),
            'category_len'
        )
        
        vectrill_pd = vectrill_result.to_pandas()
        assert vectrill_pd['category_len'].tolist() == pandas_result['category_len'].tolist()
        
        # Test string upper
        pandas_result['category_upper'] = pandas_result['category'].str.upper()
        vectrill_result = vectrill_df.with_column(
            vectrill.functions.upper('category'),
            'category_upper'
        )
        vectrill_pd = vectrill_result.to_pandas()
        assert vectrill_pd['category_upper'].tolist() == pandas_result['category_upper'].tolist()


class TestAggregations:
    """Test aggregation operations against pandas"""
    
    @pytest.fixture
    def aggregation_data(self):
        """Create data suitable for aggregation tests"""
        return pd.DataFrame({
            'group': np.random.choice(['X', 'Y', 'Z'], 10000),
            'value1': np.random.randn(10000) * 1000,
            'value2': np.random.randint(1, 100, 10000),
            'category': np.random.choice(['A', 'B', 'C', 'D'], 10000)
        })
    
    def test_group_by_aggregations(self, aggregation_data):
        """Test GROUP BY aggregations"""
        # Test single aggregation
        pandas_result = aggregation_data.groupby('group')['value1'].sum().reset_index()
        
        vectrill_df = vectrill.from_pandas(aggregation_data)
        vectrill_result = vectrill_df.group_by('group').agg(
            vectrill.functions.sum('value1').alias('value1')
        )
        
        vectrill_pd = vectrill_result.to_pandas().sort_values('group')
        pandas_result = pandas_result.sort_values('group')
        
        # Compare results
        np.testing.assert_allclose(
            vectrill_pd['value1'].values,
            pandas_result['value1'].values,
            rtol=1e-10
        )
        
        # Test multiple aggregations
        pandas_result = aggregation_data.groupby('group').agg({
            'value1': ['sum', 'mean', 'min', 'max', 'count'],
            'value2': ['sum', 'mean']
        }).reset_index()
        
        vectrill_result = vectrill_df.group_by('group').agg([
            vectrill.functions.sum('value1').alias('value1_sum'),
            vectrill.functions.mean('value1').alias('value1_mean'),
            vectrill.functions.min('value1').alias('value1_min'),
            vectrill.functions.max('value1').alias('value1_max'),
            vectrill.functions.count('value1').alias('value1_count'),
            vectrill.functions.sum('value2').alias('value2_sum'),
            vectrill.functions.mean('value2').alias('value2_mean')
        ])
        
        vectrill_pd = vectrill_result.to_pandas().sort_values('group')
        pandas_result.columns = ['_'.join(col).strip() if col[1] else col[0] for col in pandas_result.columns.values]
        pandas_result = pandas_result.sort_values('group')
        
        # Compare each aggregation
        for col in ['value1_sum', 'value1_mean', 'value1_min', 'value1_max', 'value1_count', 'value2_sum', 'value2_mean']:
            np.testing.assert_allclose(
                vectrill_pd[col].values,
                pandas_result[col].values,
                rtol=1e-10,
                err_msg=f"Mismatch in column {col}"
            )
    
    def test_window_functions(self, aggregation_data):
        """Test window functions"""
        # Add id column for ordering
        data_with_id = aggregation_data.copy()
        data_with_id['id'] = range(len(data_with_id))
        
        # Test running sum
        pandas_result = data_with_id.copy()
        pandas_result['running_sum'] = pandas_result.groupby('group')['value1'].cumsum()
        
        vectrill_df = vectrill.from_pandas(data_with_id)
        vectrill_result = vectrill_df.with_column(
            vectrill.functions.sum('value1').over(vectrill.window.partition_by('group').order_by('id')),
            'running_sum'
        )
        
        vectrill_pd = vectrill_result.to_pandas()
        # Sort by group and id for comparison
        vectrill_pd = vectrill_pd.sort_values(['group', 'id']).reset_index(drop=True)
        pandas_result = pandas_result.sort_values(['group', 'id']).reset_index(drop=True)
        
        np.testing.assert_allclose(
            vectrill_pd['running_sum'].values,
            pandas_result['running_sum'].values,
            rtol=1e-10
        )


class TestComplexExpressions:
    """Test complex expressions and edge cases"""
    
    @pytest.fixture
    def complex_data(self):
        """Create data for complex expression tests"""
        np.random.seed(42)  # For reproducible results
        return pd.DataFrame({
            'a': np.random.randn(1000),
            'b': np.random.randn(1000),
            'c': np.random.randint(-10, 11, 1000),
            'd': np.random.choice(['low', 'medium', 'high'], 1000),
            'e': np.random.choice([None, 'valid', 'invalid'], 1000)
        })
    
    def test_nested_expressions(self, complex_data):
        """Test nested arithmetic expressions"""
        # Complex expression: sqrt(a^2 + b^2)
        pandas_result = complex_data.copy()
        pandas_result['magnitude'] = np.sqrt(pandas_result['a']**2 + pandas_result['b']**2)
        
        vectrill_df = vectrill.from_pandas(complex_data)
        vectrill_result = vectrill_df.with_column(
            vectrill.functions.sqrt(
                vectrill.functions.pow('a', 2) + vectrill.functions.pow('b', 2)
            ),
            'magnitude'
        )
        
        vectrill_pd = vectrill_result.to_pandas()
        np.testing.assert_allclose(
            vectrill_pd['magnitude'].values,
            pandas_result['magnitude'].values,
            rtol=1e-10
        )
    
    def test_conditional_expressions(self, complex_data):
        """Test conditional expressions"""
        # CASE WHEN expression
        pandas_result = complex_data.copy()
        conditions = [
            pandas_result['c'] < 0,
            pandas_result['c'] == 0,
            pandas_result['c'] > 0
        ]
        choices = ['negative', 'zero', 'positive']
        pandas_result['c_category'] = np.select(conditions, choices, 'unknown')
        
        vectrill_df = vectrill.from_pandas(complex_data)
        vectrill_result = vectrill_df.with_column(
            vectrill.functions.when(
                vectrill.col('c') < 0, 'negative'
            ).when(
                vectrill.col('c') == 0, 'zero'
            ).when(
                vectrill.col('c') > 0, 'positive'
            ).otherwise('unknown'),
            'c_category'
        )
        
        vectrill_pd = vectrill_result.to_pandas()
        assert vectrill_pd['c_category'].tolist() == pandas_result['c_category'].tolist()
    
    def test_null_handling(self, complex_data):
        """Test NULL handling"""
        # COALESCE function
        pandas_result = complex_data.copy()
        pandas_result['filled_e'] = pandas_result['e'].fillna('default')
        
        vectrill_df = vectrill.from_pandas(complex_data)
        vectrill_result = vectrill_df.with_column(
            vectrill.functions.coalesce('e', 'default'),
            'filled_e'
        )
        
        vectrill_pd = vectrill_result.to_pandas()
        assert vectrill_pd['filled_e'].tolist() == pandas_result['filled_e'].tolist()


class TestPerformanceComparison:
    """Test performance characteristics"""
    
    @pytest.fixture
    def large_dataset(self):
        """Create a large dataset for performance testing"""
        return pd.DataFrame({
            'id': range(100000),
            'value': np.random.randn(100000),
            'group': np.random.choice(['A', 'B', 'C', 'D', 'E'], 100000),
            'subcategory': np.random.choice([f'X{i}' for i in range(20)], 100000)
        })
    
    def test_filter_performance(self, large_dataset):
        """Test filter operation performance"""
        import time
        
        # Pandas timing
        start_time = time.time()
        pandas_result = large_dataset[large_dataset['value'] > 0]
        pandas_time = time.time() - start_time
        
        # Vectrill timing
        vectrill_df = vectrill.from_pandas(large_dataset)
        start_time = time.time()
        vectrill_result = vectrill_df.filter(vectrill.col('value') > 0)
        vectrill_time = time.time() - start_time
        
        # Verify correctness
        assert len(vectrill_result.to_pandas()) == len(pandas_result)
        
        # Performance should be competitive (within 10x for now)
        assert vectrill_time < pandas_time * 10, f"Vectrill too slow: {vectrill_time}s vs {pandas_time}s"
        
        print(f"Filter performance - Pandas: {pandas_time:.4f}s, Vectrill: {vectrill_time:.4f}s")
    
    def test_aggregation_performance(self, large_dataset):
        """Test aggregation performance"""
        import time
        
        # Pandas timing
        start_time = time.time()
        pandas_result = large_dataset.groupby(['group', 'subcategory'])['value'].agg(['sum', 'mean', 'count'])
        pandas_time = time.time() - start_time
        
        # Vectrill timing
        vectrill_df = vectrill.from_pandas(large_dataset)
        start_time = time.time()
        vectrill_result = vectrill_df.group_by(['group', 'subcategory']).agg([
            vectrill.functions.sum('value').alias('sum'),
            vectrill.functions.mean('value').alias('mean'),
            vectrill.functions.count('value').alias('count')
        ])
        vectrill_time = time.time() - start_time
        
        # Verify correctness (sample check)
        vectrill_pd = vectrill_result.to_pandas()
        assert len(vectrill_pd) == len(pandas_result)
        
        # Performance should be competitive
        assert vectrill_time < pandas_time * 10, f"Vectrill too slow: {vectrill_time}s vs {pandas_time}s"
        
        print(f"Aggregation performance - Pandas: {pandas_time:.4f}s, Vectrill: {vectrill_time:.4f}s")


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrames"""
        empty_df = pd.DataFrame({'a': [], 'b': []})
        
        vectrill_df = vectrill.from_pandas(empty_df)
        
        # Test operations on empty DataFrame
        result = vectrill_df.filter(vectrill.col('a') > 0)
        assert len(result.to_pandas()) == 0
        
        result = vectrill_df.with_column(vectrill.col('a') + 1, 'a_plus_1')
        assert len(result.to_pandas()) == 0
    
    def test_single_row(self):
        """Test handling of single row DataFrames"""
        single_df = pd.DataFrame({'a': [1], 'b': [2]})
        
        vectrill_df = vectrill.from_pandas(single_df)
        result = vectrill_df.with_column(vectrill.col('a') + vectrill.col('b'), 'sum')
        
        assert len(result.to_pandas()) == 1
        assert result.to_pandas()['sum'].iloc[0] == 3
    
    def test_all_nulls(self):
        """Test handling of all-null columns"""
        null_df = pd.DataFrame({
            'a': [None] * 100,
            'b': [1, 2, 3, 4, 5] * 20
        })
        
        vectrill_df = vectrill.from_pandas(null_df)
        result = vectrill_df.with_column(
            vectrill.functions.coalesce('a', 'b'),
            'filled_a'
        )
        
        vectrill_pd = result.to_pandas()
        assert vectrill_pd['filled_a'].tolist() == [1, 2, 3, 4, 5] * 20
    
    def test_extreme_values(self):
        """Test handling of extreme values"""
        extreme_df = pd.DataFrame({
            'large': [1e10, -1e10, 1e-10, -1e-10],
            'inf': [float('inf'), float('-inf'), 0, 1],
            'nan': [float('nan'), 1, 2, 3]
        })
        
        vectrill_df = vectrill.from_pandas(extreme_df)
        
        # Test operations with extreme values
        result = vectrill_df.with_column(vectrill.col('large') * 2, 'large_times_2')
        vectrill_pd = result.to_pandas()
        
        # Check infinities and NaNs are preserved
        assert np.isinf(vectrill_pd['large_times_2'].iloc[0])
        assert np.isinf(vectrill_pd['large_times_2'].iloc[1])


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
