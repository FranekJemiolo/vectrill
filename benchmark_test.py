#!/usr/bin/env python3
"""
Quick test of the benchmark script to verify it works
"""

import sys
import os
import pandas as pd
import polars as pl

# Add vectrill to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

try:
    from vectrill.dataframe import VectrillDataFrame, col, functions
    VECTRILL_AVAILABLE = True
    print("✓ Vectrill available")
except ImportError as e:
    print(f"✗ Vectrill not available - {e}")
    VECTRILL_AVAILABLE = False

# Test basic operations with small dataset
print("\nTesting basic operations...")

# Create small test data
data = {
    'id': range(100),
    'group': ['A', 'B', 'C', 'D'] * 25,
    'value1': [i * 0.1 for i in range(100)],
    'value2': list(range(100))
}

pandas_df = pd.DataFrame(data)
polars_df = pl.DataFrame(data)

print(f"Created test data with {len(pandas_df)} rows")

# Test pandas operations
print("\nPandas tests:")
try:
    filtered_pandas = pandas_df[pandas_df['value1'] > 5]
    print(f"  Filter: {len(filtered_pandas)} rows")
    
    grouped_pandas = pandas_df.groupby('group')['value1'].sum().reset_index()
    print(f"  Groupby: {len(grouped_pandas)} groups")
    
    with_col_pandas = pandas_df.assign(new_col=pandas_df['value1'] * 2)
    print(f"  With column: {len(with_col_pandas.columns)} columns")
    
except Exception as e:
    print(f"  ✗ Pandas error: {e}")

# Test polars operations
print("\nPolars tests:")
try:
    filtered_polars = polars_df.filter(pl.col('value1') > 5)
    print(f"  Filter: {len(filtered_polars)} rows")
    
    grouped_polars = polars_df.groupby('group').agg(pl.col('value1').sum())
    print(f"  Groupby: {len(grouped_polars)} groups")
    
    with_col_polars = polars_df.with_columns((pl.col('value1') * 2).alias('new_col'))
    print(f"  With column: {len(with_col_polars.columns)} columns")
    
except Exception as e:
    print(f"  ✗ Polars error: {e}")

# Test vectrill operations
if VECTRILL_AVAILABLE:
    print("\nVectrill tests:")
    try:
        vectrill_df = VectrillDataFrame(pandas_df)
        print(f"  Created VectrillDataFrame: {len(vectrill_df)} rows")
        
        filtered_vectrill = vectrill_df.filter(col('value1') > 5)
        print(f"  Filter: {len(filtered_vectrill)} rows")
        
        grouped_vectrill = vectrill_df.group_by('group').agg(functions.sum('value1').alias('value1_sum'))
        print(f"  Groupby: {len(grouped_vectrill)} groups")
        
        with_col_vectrill = vectrill_df.with_column(col('value1') * 2, 'new_col')
        print(f"  With column: {len(with_col_vectrill.to_pandas().columns)} columns")
        
    except Exception as e:
        print(f"  ✗ Vectrill error: {e}")

print("\n✓ Basic tests completed successfully!")
