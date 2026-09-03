#!/usr/bin/env python3
"""
Dedicated unit test suite for native Arrow compute performance optimizations.
Verifies exact parity with pandas while testing the high-performance Arrow compute paths.
"""

import pytest
import numpy as np
import pandas as pd
import pyarrow as pa
import sys
import os

# Add the vectrill module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))

import vectrill
from vectrill.dataframe import VectrillDataFrame, col, functions


class TestArrowComputeOptimizations:
    """Test optimized Arrow compute execution paths"""

    @pytest.fixture
    def benchmark_data(self):
        """Create benchmark-sized test data"""
        np.random.seed(42)
        N = 10000
        return pd.DataFrame({
            'id': range(N),
            'group': np.random.choice(['A', 'B', 'C', 'D'], N),
            'value1': np.random.randn(N) * 100,
            'value2': np.random.randint(1, 1000, N),
            'text': np.random.choice(['hello', 'world', 'vectrill', 'arrow'], N),
        })

    def test_native_arrow_filters(self, benchmark_data):
        """Verify native Arrow compute filtering for all comparison operators"""
        vdf = VectrillDataFrame(benchmark_data)

        # >
        res_v = vdf.filter(col('value1') > 0).to_pandas()
        res_p = benchmark_data[benchmark_data['value1'] > 0]
        assert len(res_v) == len(res_p)
        np.testing.assert_allclose(res_v['value1'].values, res_p['value1'].values)

        # <
        res_v = vdf.filter(col('value1') < 0).to_pandas()
        res_p = benchmark_data[benchmark_data['value1'] < 0]
        assert len(res_v) == len(res_p)

        # ==
        res_v = vdf.filter(col('group') == 'B').to_pandas()
        res_p = benchmark_data[benchmark_data['group'] == 'B']
        assert len(res_v) == len(res_p)

        # !=
        res_v = vdf.filter(col('group') != 'A').to_pandas()
        res_p = benchmark_data[benchmark_data['group'] != 'A']
        assert len(res_v) == len(res_p)

        # >=
        res_v = vdf.filter(col('value2') >= 500).to_pandas()
        res_p = benchmark_data[benchmark_data['value2'] >= 500]
        assert len(res_v) == len(res_p)

        # <=
        res_v = vdf.filter(col('value2') <= 500).to_pandas()
        res_p = benchmark_data[benchmark_data['value2'] <= 500]
        assert len(res_v) == len(res_p)

    def test_native_arrow_with_column_arithmetic(self, benchmark_data):
        """Verify native Arrow compute column arithmetic"""
        vdf = VectrillDataFrame(benchmark_data)

        # Addition
        res_v = vdf.with_column(col('value1') + 10.5, 'val_add').to_pandas()
        res_p = benchmark_data.assign(val_add=benchmark_data['value1'] + 10.5)
        np.testing.assert_allclose(res_v['val_add'].values, res_p['val_add'].values)

        # Multiplication
        res_v = vdf.with_column(col('value1') * 2.0, 'val_mul').to_pandas()
        res_p = benchmark_data.assign(val_mul=benchmark_data['value1'] * 2.0)
        np.testing.assert_allclose(res_v['val_mul'].values, res_p['val_mul'].values)

        # Subtraction
        res_v = vdf.with_column(col('value2') - 50, 'val_sub').to_pandas()
        res_p = benchmark_data.assign(val_sub=benchmark_data['value2'] - 50)
        np.testing.assert_allclose(res_v['val_sub'].values, res_p['val_sub'].values)

        # Division
        res_v = vdf.with_column(col('value2') / 2.0, 'val_div').to_pandas()
        res_p = benchmark_data.assign(val_div=benchmark_data['value2'] / 2.0)
        np.testing.assert_allclose(res_v['val_div'].values, res_p['val_div'].values)

    def test_native_arrow_functions(self, benchmark_data):
        """Verify native Arrow string and math functions"""
        vdf = VectrillDataFrame(benchmark_data)

        # utf8 length
        res_v = vdf.with_column(functions.length('text'), 'text_len').to_pandas()
        res_p = benchmark_data.assign(text_len=benchmark_data['text'].str.len())
        assert res_v['text_len'].tolist() == res_p['text_len'].tolist()

        # utf8 upper
        res_v = vdf.with_column(functions.upper('text'), 'text_up').to_pandas()
        res_p = benchmark_data.assign(text_up=benchmark_data['text'].str.upper())
        assert res_v['text_up'].tolist() == res_p['text_up'].tolist()

        # abs
        res_v = vdf.with_column(functions.abs(col('value1')), 'val_abs').to_pandas()
        res_p = benchmark_data.assign(val_abs=benchmark_data['value1'].abs())
        np.testing.assert_allclose(res_v['val_abs'].values, res_p['val_abs'].values)

        # round
        res_v = vdf.with_column(functions.round(col('value1'), 1), 'val_round').to_pandas()
        res_p = benchmark_data.assign(val_round=benchmark_data['value1'].round(1))
        np.testing.assert_allclose(res_v['val_round'].values, res_p['val_round'].values)

        # floor
        res_v = vdf.with_column(functions.floor(col('value1')), 'val_floor').to_pandas()
        res_p = benchmark_data.assign(val_floor=np.floor(benchmark_data['value1']))
        np.testing.assert_allclose(res_v['val_floor'].values, res_p['val_floor'].values)

        # ceil
        res_v = vdf.with_column(functions.ceil(col('value1')), 'val_ceil').to_pandas()
        res_p = benchmark_data.assign(val_ceil=np.ceil(benchmark_data['value1']))
        np.testing.assert_allclose(res_v['val_ceil'].values, res_p['val_ceil'].values)

    def test_native_arrow_groupby_multi_agg(self, benchmark_data):
        """Verify native Arrow compute group-by engine with multi-aggregations"""
        vdf = VectrillDataFrame(benchmark_data)

        res_v = vdf.group_by('group').agg([
            functions.sum('value1').alias('v1_sum'),
            functions.mean('value1').alias('v1_mean'),
            functions.min('value1').alias('v1_min'),
            functions.max('value1').alias('v1_max'),
            functions.count('value1').alias('v1_count'),
            functions.sum('value2').alias('v2_sum'),
        ]).to_pandas().sort_values('group').reset_index(drop=True)

        res_p = benchmark_data.groupby('group').agg(
            v1_sum=('value1', 'sum'),
            v1_mean=('value1', 'mean'),
            v1_min=('value1', 'min'),
            v1_max=('value1', 'max'),
            v1_count=('value1', 'count'),
            v2_sum=('value2', 'sum'),
        ).reset_index().sort_values('group').reset_index(drop=True)

        assert res_v['group'].tolist() == res_p['group'].tolist()
        for c in ['v1_sum', 'v1_mean', 'v1_min', 'v1_max', 'v1_count', 'v2_sum']:
            np.testing.assert_allclose(res_v[c].values, res_p[c].values, rtol=1e-8)

    def test_native_arrow_sort(self, benchmark_data):
        """Verify native Arrow compute sorting with ascending/descending keys"""
        vdf = VectrillDataFrame(benchmark_data)

        # Single column sort
        res_v = vdf.sort('value1', ascending=True).to_pandas().reset_index(drop=True)
        res_p = benchmark_data.sort_values('value1', ascending=True).reset_index(drop=True)
        np.testing.assert_allclose(res_v['value1'].values, res_p['value1'].values)

        # Multi-column sort with mixed directions
        res_v = vdf.sort(['group', 'value2'], ascending=[True, False]).to_pandas().reset_index(drop=True)
        res_p = benchmark_data.sort_values(['group', 'value2'], ascending=[True, False]).reset_index(drop=True)
        assert res_v['group'].tolist() == res_p['group'].tolist()
        np.testing.assert_allclose(res_v['value2'].values, res_p['value2'].values)

    def test_native_arrow_select(self, benchmark_data):
        """Verify zero-copy Arrow native column selection"""
        vdf = VectrillDataFrame(benchmark_data)
        res_v = vdf.select(['group', 'value1'])

        assert res_v._arrow_table.column_names == ['group', 'value1']
        assert len(res_v) == len(benchmark_data)
        assert res_v.to_pandas().equals(benchmark_data[['group', 'value1']])
