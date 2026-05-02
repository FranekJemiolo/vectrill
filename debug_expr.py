#!/usr/bin/env python3
import sys
sys.path.insert(0, 'python')
import vectrill
import pandas as pd
import numpy as np

# Create test data
np.random.seed(42)
data = pd.DataFrame({
    'a': np.random.randn(5),
    'b': np.random.randn(5)
})

print('Test data:')
print(data)

# Test the expression
vectrill_df = vectrill.from_pandas(data)
expr = vectrill.functions.sqrt(
    vectrill.functions.pow('a', 2) + vectrill.functions.pow('b', 2)
)
print('Expression:', expr)
print('Expression name:', expr.name)
print('Has nested_expr:', hasattr(expr, 'nested_expr'))
if hasattr(expr, 'nested_expr'):
    print('Nested expr type:', type(expr.nested_expr))
    if hasattr(expr.nested_expr, 'left'):
        print('Nested left:', expr.nested_expr.left, type(expr.nested_expr.left))
        print('Nested left name:', getattr(expr.nested_expr.left, 'name', 'NO NAME'))
    if hasattr(expr.nested_expr, 'right'):
        print('Nested right:', expr.nested_expr.right, type(expr.nested_expr.right))
        print('Nested right name:', getattr(expr.nested_expr.right, 'name', 'NO NAME'))
