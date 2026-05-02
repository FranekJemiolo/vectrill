"""Vector functions module for Vectrill compatibility with pandas tests"""

import numpy as np
from typing import Any, List


# Define expression classes locally to avoid circular imports
class ColumnExpression:
    """Column expression for filtering and operations"""
    
    def __init__(self, name: str):
        self.name = name
        self.alias_name = None
    
    def alias(self, name: str):
        """Set alias for the expression"""
        self.alias_name = name
        return self
    
    def over(self, window_transform):
        """Window function specification"""
        from .dataframe import WindowExpression
        return WindowExpression(self, window_transform)
    
    def __gt__(self, other: Any):
        """Greater than comparison"""
        return {"op": ">", "col": self.name, "value": other}
    
    def __lt__(self, other: Any):
        """Less than comparison"""
        return {"op": "<", "col": self.name, "value": other}
    
    def __eq__(self, other: Any):
        """Equality comparison"""
        return {"op": "==", "col": self.name, "value": other}
    
    def __ne__(self, other: Any):
        """Inequality comparison"""
        return {"op": "!=", "col": self.name, "value": other}
    
    def __ge__(self, other: Any):
        """Greater than or equal comparison"""
        return {"op": ">=", "col": self.name, "value": other}
    
    def __le__(self, other: Any):
        """Less than or equal comparison"""
        return {"op": "<=", "col": self.name, "value": other}
    
    def __add__(self, other: Any):
        """Addition operation"""
        if isinstance(other, ColumnExpression):
            from .dataframe import BinaryExpression
            return BinaryExpression(self, "+", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "+", other)
    
    def __mul__(self, other: Any):
        """Multiplication operation"""
        if isinstance(other, ColumnExpression):
            from .dataframe import BinaryExpression
            return BinaryExpression(self, "*", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "*", other)
    
    def __rmul__(self, other: Any):
        """Right multiplication operation (for int * ColumnExpression)"""
        if isinstance(other, ColumnExpression):
            from .dataframe import BinaryExpression
            return BinaryExpression(other, "*", self)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "*", other)
    
    def __sub__(self, other: Any):
        """Subtraction operation"""
        if isinstance(other, ColumnExpression):
            from .dataframe import BinaryExpression
            return BinaryExpression(self, "-", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "-", other)
    
    def __truediv__(self, other: Any):
        """Division operation"""
        if isinstance(other, ColumnExpression):
            from .dataframe import BinaryExpression
            return BinaryExpression(self, "/", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "/", other)
    
    def __pow__(self, other: Any):
        """Power operation"""
        if isinstance(other, ColumnExpression):
            from .dataframe import BinaryExpression
            return BinaryExpression(self, "**", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "**", other)
    
    def is_null(self):
        """Check if column values are null"""
        return ColumnExpression(f"is_null({self.name})")
    
    def is_in(self, values):
        """Check if column values are in the given list"""
        if isinstance(values, list):
            # Convert list to string representation
            values_str = str(values)
        else:
            values_str = str(values)
        return ColumnExpression(f"is_in({self.name}, {values_str})")


class ArithmeticExpression:
    """Arithmetic expression between column and value"""
    
    def __init__(self, col, op: str, value: Any):
        self.col = col
        self.op = op
        self.value = value
        self.alias_name = None
    
    def alias(self, name: str):
        """Set alias for the expression"""
        self.alias_name = name
        return self


class BinaryExpression:
    """Binary expression between two columns"""
    
    def __init__(self, left, op: str, right):
        self.left = left
        self.op = op
        self.right = right
        self.alias_name = None
    
    def alias(self, name: str):
        """Set alias for the expression"""
        self.alias_name = name
        return self
    
    def __truediv__(self, other: Any):
        """Division operation"""
        if isinstance(other, ColumnExpression):
            return BinaryExpression(self, "/", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "/", other)
    
    def __mul__(self, other: Any):
        """Multiplication operation"""
        if isinstance(other, ColumnExpression):
            return BinaryExpression(self, "*", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "*", other)
    
    def __add__(self, other: Any):
        """Addition operation"""
        if isinstance(other, ColumnExpression):
            return BinaryExpression(self, "+", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "+", other)
    
    def __sub__(self, other: Any):
        """Subtraction operation"""
        if isinstance(other, ColumnExpression):
            return BinaryExpression(self, "-", other)
        else:
            from .dataframe import ArithmeticExpression
            return ArithmeticExpression(self, "-", other)


class WhenExpression:
    """When-then-else expression for conditional logic"""
    
    def __init__(self, condition, then_value=None):
        self.conditions = [condition]
        # If then_value is provided, it's the then value for the first condition
        if then_value is not None:
            self.then_values = [then_value]
        else:
            self.then_values = []  # Will be set by subsequent .then() call
        self.otherwise_value = None
        self.alias_name = None
    
    def then(self, then_value):
        """Set the then value for the last condition"""
        if len(self.then_values) == 0:
            self.then_values.append(then_value)
        else:
            self.then_values[-1] = then_value
        return self
    
    def when(self, condition, then_value=None):
        """Chain another when-then"""
        self.conditions.append(condition)
        if then_value is not None:
            self.then_values.append(then_value)
        else:
            # Add placeholder for then_value, will be set by subsequent .then() call
            self.then_values.append(None)
        return self
    
    def otherwise(self, else_value):
        """Set else value"""
        self.otherwise_value = else_value
        return self
    
    def alias(self, name: str):
        """Set alias for the expression"""
        self.alias_name = name
        return self


def col(name: str) -> ColumnExpression:
    """Create a column reference"""
    return ColumnExpression(name)


def sqrt(expr) -> ColumnExpression:
    """Square root function"""
    result = ColumnExpression("sqrt")
    result.nested_expr = expr
    return result


def abs(expr) -> ColumnExpression:
    """Absolute value function"""
    result = ColumnExpression("abs")
    result.nested_expr = expr
    return result


def pow(col_name: str, exponent: int) -> ColumnExpression:
    """Power function"""
    return ColumnExpression(f"pow({col_name}, {exponent})")


def length(col_name: str) -> ColumnExpression:
    """String length function"""
    return ColumnExpression(f"length({col_name})")


def upper(col_name: str) -> ColumnExpression:
    """String uppercase function"""
    return ColumnExpression(f"upper({col_name})")


def lower(col_name: str) -> ColumnExpression:
    """String lowercase function"""
    return ColumnExpression(f"lower({col_name})")


def round(col_name: str, decimals: int = 0) -> ColumnExpression:
    """Round function"""
    return ColumnExpression(f"round({col_name}, {decimals})")


def floor(col_name: str) -> ColumnExpression:
    """Floor function"""
    return ColumnExpression(f"floor({col_name})")


def ceil(col_name: str) -> ColumnExpression:
    """Ceiling function"""
    return ColumnExpression(f"ceil({col_name})")


def sum(col_name: str) -> ColumnExpression:
    """Sum function"""
    return ColumnExpression(f"sum({col_name})")


def mean(col_name: str) -> ColumnExpression:
    """Mean function"""
    return ColumnExpression(f"mean({col_name})")


def median(col_name: str) -> ColumnExpression:
    """Median function"""
    return ColumnExpression(f"median({col_name})")


def min(col_name: str) -> ColumnExpression:
    """Minimum function"""
    return ColumnExpression(f"min({col_name})")


def max(col_name: str) -> ColumnExpression:
    """Maximum function"""
    return ColumnExpression(f"max({col_name})")


def std(col_name: str) -> ColumnExpression:
    """Standard deviation function"""
    return ColumnExpression(f"std({col_name})")


def var(col_name: str) -> ColumnExpression:
    """Variance function"""
    return ColumnExpression(f"var({col_name})")


def count(col_name: str = None) -> ColumnExpression:
    """Count function"""
    if col_name:
        return ColumnExpression(f"count({col_name})")
    else:
        return ColumnExpression("count()")


def lag(col_name: str, offset: int = 1) -> ColumnExpression:
    """Lag function"""
    return ColumnExpression(f"lag({col_name}, {offset})")


def lead(col_name: str, offset: int = 1) -> ColumnExpression:
    """Lead function"""
    return ColumnExpression(f"lead({col_name}, {offset})")


def rolling_mean(col_name: str, window_size) -> ColumnExpression:
    """Rolling mean function"""
    if isinstance(window_size, str):
        return ColumnExpression(f"rolling_mean({col_name}, '{window_size}')")
    else:
        return ColumnExpression(f"rolling_mean({col_name}, {window_size})")


def rolling_std(col_name: str, window_size: int) -> ColumnExpression:
    """Rolling standard deviation function"""
    return ColumnExpression(f"rolling_std({col_name}, {window_size})")


def coalesce(*args) -> ColumnExpression:
    """Coalesce function - returns first non-null value"""
    if len(args) >= 2:
        col_name = args[0]
        default = args[1]
        if isinstance(default, str):
            default = f"'{default}'"
        return ColumnExpression(f"coalesce({col_name}, {default})")
    else:
        raise ValueError("coalesce requires at least 2 arguments")


def when(condition, then_value=None) -> WhenExpression:
    """When-then expression for conditional logic"""
    return WhenExpression(condition, then_value)


class WhenExpression:
    """When-then-else expression for conditional logic"""
    
    def __init__(self, condition, then_value=None):
        self.conditions = [condition]
        self.then_values = [then_value]
        self.otherwise_value = None
    
    def when(self, condition, then_value) -> 'WhenExpression':
        """Add another when-then condition"""
        self.conditions.append(condition)
        self.then_values.append(then_value)
        return self
    
    def then(self, value) -> 'WhenExpression':
        """Set the then value for the last when condition"""
        if self.then_values:
            self.then_values[-1] = value
        else:
            self.then_values.append(value)
        return self
    
    def otherwise(self, value) -> WhenExpression:
        """Set the else value"""
        self.otherwise_value = value
        return self
    
    def alias(self, name: str) -> 'WhenExpression':
        """Set alias for the expression"""
        self.alias_name = name
        return self
