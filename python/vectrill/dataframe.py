"""DataFrame-like API for Vectrill compatibility with pandas tests"""

import pandas as pd
import polars as pl
from typing import Any, Union, Optional


class ColumnExpression:
    """Column expression for filtering and operations"""
    
    def __init__(self, name: str):
        self.name = name
    
    def __gt__(self, other: Any) -> dict:
        """Greater than comparison"""
        return {"op": ">", "col": self.name, "value": other}
    
    def __lt__(self, other: Any) -> dict:
        """Less than comparison"""
        return {"op": "<", "col": self.name, "value": other}
    
    def __eq__(self, other: Any) -> dict:
        """Equality comparison"""
        return {"op": "==", "col": self.name, "value": other}
    
    def __ne__(self, other: Any) -> dict:
        """Inequality comparison"""
        return {"op": "!=", "col": self.name, "value": other}
    
    def __ge__(self, other: Any) -> dict:
        """Greater than or equal comparison"""
        return {"op": ">=", "col": self.name, "value": other}
    
    def __le__(self, other: Any) -> dict:
        """Less than or equal comparison"""
        return {"op": "<=", "col": self.name, "value": other}


class VectrillDataFrame:
    """DataFrame-like class for compatibility with pandas tests"""
    
    def __init__(self, data: Union[pd.DataFrame, pl.DataFrame]):
        if isinstance(data, pd.DataFrame):
            self._df = pl.from_pandas(data)
        else:
            self._df = data
    
    def filter(self, condition: dict) -> 'VectrillDataFrame':
        """Filter DataFrame based on condition"""
        op = condition.get("op")
        col_name = condition.get("col")
        value = condition.get("value")
        
        if op == ">" and col_name in self._df.columns:
            filtered_df = self._df.filter(pl.col(col_name) > value)
            return VectrillDataFrame(filtered_df)
        elif op == "<" and col_name in self._df.columns:
            filtered_df = self._df.filter(pl.col(col_name) < value)
            return VectrillDataFrame(filtered_df)
        elif op == "==" and col_name in self._df.columns:
            filtered_df = self._df.filter(pl.col(col_name) == value)
            return VectrillDataFrame(filtered_df)
        elif op == "!=" and col_name in self._df.columns:
            filtered_df = self._df.filter(pl.col(col_name) != value)
            return VectrillDataFrame(filtered_df)
        elif op == ">=" and col_name in self._df.columns:
            filtered_df = self._df.filter(pl.col(col_name) >= value)
            return VectrillDataFrame(filtered_df)
        elif op == "<=" and col_name in self._df.columns:
            filtered_df = self._df.filter(pl.col(col_name) <= value)
            return VectrillDataFrame(filtered_df)
        else:
            # Return empty DataFrame for unsupported operations
            return VectrillDataFrame(self._df.limit(0))
    
    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame"""
        return self._df.to_pandas()
    
    def __len__(self) -> int:
        """Get length of DataFrame"""
        return len(self._df)


def from_pandas(df: pd.DataFrame) -> VectrillDataFrame:
    """Create a VectrillDataFrame from a pandas DataFrame"""
    return VectrillDataFrame(df)


def col(name: str) -> ColumnExpression:
    """Create a column expression"""
    return ColumnExpression(name)
