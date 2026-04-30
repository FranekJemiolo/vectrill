"""Cross-reference tests comparing vectrill with pandas and polars."""

import pandas as pd
import polars as pl
import pyarrow as pa
import numpy as np
import pytest


class TestExpressionCrossReference:
    """Test expression evaluation against pandas/polars for correctness."""

    def test_binary_operations_add(self):
        """Test addition operation."""
        # Create test data
        data = {"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}
        
        # Polars result
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select((pl.col("a") + pl.col("b")).alias("sum"))
        
        # Pandas result
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"] + df_pandas["b"]
        
        # Compare results
        expected = result_pandas.to_numpy()
        actual = result_polars["sum"].to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_binary_operations_subtract(self):
        """Test subtraction operation."""
        data = {"a": [10, 20, 30, 40, 50], "b": [1, 2, 3, 4, 5]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select((pl.col("a") - pl.col("b")).alias("diff"))
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"] - df_pandas["b"]
        
        expected = result_pandas.to_numpy()
        actual = result_polars["diff"].to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_binary_operations_multiply(self):
        """Test multiplication operation."""
        data = {"a": [2, 3, 4, 5, 6], "b": [7, 8, 9, 10, 11]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select((pl.col("a") * pl.col("b")).alias("product"))
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"] * df_pandas["b"]
        
        expected = result_pandas.to_numpy()
        actual = result_polars["product"].to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_binary_operations_divide(self):
        """Test division operation."""
        data = {"a": [20, 40, 60, 80, 100], "b": [2, 4, 6, 8, 10]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select((pl.col("a") / pl.col("b")).alias("quotient"))
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"] / df_pandas["b"]
        
        expected = result_pandas.to_numpy()
        actual = result_polars["quotient"].to_numpy()
        np.testing.assert_array_almost_equal(actual, expected)

    def test_comparison_operations_gt(self):
        """Test greater than comparison."""
        data = {"a": [5, 10, 15, 20, 25], "b": [10, 5, 20, 15, 30]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select((pl.col("a") > pl.col("b")).alias("gt"))
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"] > df_pandas["b"]
        
        expected = result_pandas.to_numpy()
        actual = result_polars["gt"].to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_comparison_operations_lt(self):
        """Test less than comparison."""
        data = {"a": [5, 10, 15, 20, 25], "b": [10, 5, 20, 15, 30]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select((pl.col("a") < pl.col("b")).alias("lt"))
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"] < df_pandas["b"]
        
        expected = result_pandas.to_numpy()
        actual = result_polars["lt"].to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_filter_operations(self):
        """Test filter operations."""
        data = {"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.filter(pl.col("a") > 2)
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas[df_pandas["a"] > 2]
        
        np.testing.assert_array_equal(result_polars["a"].to_numpy(), result_pandas["a"].to_numpy())
        np.testing.assert_array_equal(result_polars["b"].to_numpy(), result_pandas["b"].to_numpy())

    def test_aggregation_sum(self):
        """Test sum aggregation."""
        data = {"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select(pl.col("a").sum()).item()
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"].sum()
        
        assert result_polars == result_pandas

    def test_aggregation_mean(self):
        """Test mean aggregation."""
        data = {"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select(pl.col("a").mean()).item()
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"].mean()
        
        assert result_polars == result_pandas

    def test_aggregation_min_max(self):
        """Test min and max aggregations."""
        data = {"a": [5, 2, 8, 1, 9], "b": [10, 20, 30, 40, 50]}
        
        df_polars = pl.DataFrame(data)
        min_polars = df_polars.select(pl.col("a").min()).item()
        max_polars = df_polars.select(pl.col("a").max()).item()
        
        df_pandas = pd.DataFrame(data)
        min_pandas = df_pandas["a"].min()
        max_pandas = df_pandas["a"].max()
        
        assert min_polars == min_pandas
        assert max_polars == max_pandas

    def test_boolean_operations_and(self):
        """Test boolean AND operation."""
        data = {"a": [True, True, False, False, True], "b": [True, False, True, False, True]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select((pl.col("a") & pl.col("b")).alias("and"))
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"] & df_pandas["b"]
        
        expected = result_pandas.to_numpy()
        actual = result_polars["and"].to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_boolean_operations_or(self):
        """Test boolean OR operation."""
        data = {"a": [True, True, False, False, True], "b": [True, False, True, False, True]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select((pl.col("a") | pl.col("b")).alias("or"))
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["a"] | df_pandas["b"]
        
        expected = result_pandas.to_numpy()
        actual = result_polars["or"].to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_string_operations(self):
        """Test string operations."""
        data = {"names": ["alice", "bob", "charlie", "david"]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.select(pl.col("names").str.to_uppercase().alias("upper"))
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas["names"].str.upper()
        
        expected = result_pandas.to_numpy()
        actual = result_polars["upper"].to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_null_handling(self):
        """Test null handling."""
        data = {"a": [1, 2, None, 4, 5], "b": [10, None, 30, 40, 50]}
        
        df_polars = pl.DataFrame(data)
        result_polars = df_polars.filter(pl.col("a").is_not_null())
        
        df_pandas = pd.DataFrame(data)
        result_pandas = df_pandas[df_pandas["a"].notna()]
        
        assert len(result_polars) == len(result_pandas)


class TestArrowComputeCrossReference:
    """Test Arrow compute kernels against polars/pandas."""

    def test_arrow_add_array(self):
        """Test Arrow array addition."""
        a = pa.array([1, 2, 3, 4, 5])
        b = pa.array([10, 20, 30, 40, 50])
        
        result = pc.add(a, b)
        
        # Compare with numpy
        expected = np.array([11, 22, 33, 44, 55])
        actual = result.to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_arrow_subtract_array(self):
        """Test Arrow array subtraction."""
        a = pa.array([10, 20, 30, 40, 50])
        b = pa.array([1, 2, 3, 4, 5])
        
        result = pc.subtract(a, b)
        
        expected = np.array([9, 18, 27, 36, 45])
        actual = result.to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_arrow_multiply_array(self):
        """Test Arrow array multiplication."""
        a = pa.array([2, 3, 4, 5, 6])
        b = pa.array([7, 8, 9, 10, 11])
        
        result = pc.multiply(a, b)
        
        expected = np.array([14, 24, 36, 50, 66])
        actual = result.to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_arrow_divide_array(self):
        """Test Arrow array division."""
        a = pa.array([20, 40, 60, 80, 100])
        b = pa.array([2, 4, 6, 8, 10])
        
        result = pc.divide(a, b)
        
        expected = np.array([10, 10, 10, 10, 10], dtype=np.float64)
        actual = result.to_numpy()
        np.testing.assert_array_almost_equal(actual, expected)

    def test_arrow_comparison_gt(self):
        """Test Arrow greater than comparison."""
        a = pa.array([5, 10, 15, 20, 25])
        b = pa.array([10, 5, 20, 15, 30])
        
        result = pc.greater(a, b)
        
        expected = np.array([False, True, False, True, False])
        actual = np.array(result.to_pylist())
        np.testing.assert_array_equal(actual, expected)

    def test_arrow_comparison_lt(self):
        """Test Arrow less than comparison."""
        a = pa.array([5, 10, 15, 20, 25])
        b = pa.array([10, 5, 20, 15, 30])
        
        result = pc.less(a, b)
        
        expected = np.array([True, False, True, False, True])
        actual = np.array(result.to_pylist())
        np.testing.assert_array_equal(actual, expected)

    def test_arrow_filter(self):
        """Test Arrow filter operation."""
        data = pa.array([1, 2, 3, 4, 5])
        mask = pa.array([False, False, True, True, True])
        
        result = data.filter(mask)
        
        expected = np.array([3, 4, 5])
        actual = result.to_numpy()
        np.testing.assert_array_equal(actual, expected)

    def test_arrow_sum_aggregate(self):
        """Test Arrow sum aggregation."""
        data = pa.array([1, 2, 3, 4, 5])
        
        result = pc.sum(data)
        
        assert result.as_py() == 15

    def test_arrow_mean_aggregate(self):
        """Test Arrow mean aggregation."""
        data = pa.array([1, 2, 3, 4, 5])
        
        result = pc.mean(data)
        
        assert result.as_py() == 3.0

    def test_arrow_min_max_aggregate(self):
        """Test Arrow min and max aggregations."""
        data = pa.array([5, 2, 8, 1, 9])
        
        min_result = pc.min(data)
        max_result = pc.max(data)
        
        assert min_result.as_py() == 1
        assert max_result.as_py() == 9


# Import pyarrow compute functions
import pyarrow.compute as pc
