#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug sort operations
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create test data
np.random.seed(42)
data = pd.DataFrame({
    'user_id': np.random.randint(1, 11, 100),
    'amount': np.random.uniform(-1000, 1000, 100),
    'score': np.random.uniform(0, 100, 100),
    'category': np.random.choice(['A', 'B', 'C'], 100),
    'timestamp': pd.date_range('2023-01-01', periods=100, freq='H'),
    'is_active': np.random.choice([True, False], 100)
})

print("Testing sort operations:")

# Test various sort configurations
sort_configs = [
    (['amount'], [True]),
    (['user_id', 'amount'], [True, False]),
    (['timestamp'], [False]),
]

for sort_cols, ascending in sort_configs:
    print(f"\n=== Testing sort by {sort_cols} with ascending={ascending} ===")
    
    # Test pandas
    pandas_result = data.sort_values(sort_cols, ascending=ascending)
    print(f"Pandas result shape: {pandas_result.shape}")
    print(f"Pandas first 5 rows of {sort_cols[0]}: {pandas_result[sort_cols[0]].head().values}")
    
    # Test Vectrill
    vectrill_df = vectrill.from_pandas(data)
    vectrill_result = vectrill_df.sort(sort_cols).to_pandas()
    print(f"Vectrill result shape: {vectrill_result.shape}")
    print(f"Vectrill first 5 rows of {sort_cols[0]}: {vectrill_result[sort_cols[0]].head().values}")
    
    # Compare shapes
    if vectrill_result.shape == pandas_result.shape:
        print("✓ Shapes match")
    else:
        print("✗ Shapes don't match")
    
    # Compare sorted order
    try:
        np.testing.assert_array_equal(vectrill_result[sort_cols[0]].values, pandas_result[sort_cols[0]].values)
        print(f"✓ {sort_cols[0]} order matches")
    except AssertionError as e:
        print(f"✗ {sort_cols[0]} order doesn't match")
        print(f"  Pandas: {pandas_result[sort_cols[0]].head().values}")
        print(f"  Vectrill: {vectrill_result[sort_cols[0]].head().values}")
    
    # Check if the issue is with ascending parameter
    print(f"  Pandas ascending: {ascending}")
    print(f"  Vectrill ascending: Not specified in sort method")
