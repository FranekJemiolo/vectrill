#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug when function test
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create test data (same as in the test)
data = pd.DataFrame({
    'user_id': [1, 1, 1, 2, 2, 2],
    'amount': [100, 200, 300, 150, 250, 350],
    'score': [10, 20, 30, 40, 50, 60],
    'category': ['A', 'B', 'C', 'A', 'B', 'C']
})

print("Testing when function with test data:")
print("Original data:")
print(data)

try:
    # Test the exact same expression as in the test
    print("\n=== Testing when expression ===")
    when_expr = functions.when(col('category') == 'A')
    print(f"when expression created: {when_expr}")
    
    # Test then method
    then_expr = when_expr.then(col('score') * 2)
    print(f"then expression created: {then_expr}")
    
    # Test otherwise method
    otherwise_expr = then_expr.otherwise(col('score'))
    print(f"otherwise expression created: {otherwise_expr}")
    
    # Test with_columns
    print("\n=== Testing with_columns ===")
    vectrill_df = vectrill.from_pandas(data)
    result_df = vectrill_df.with_columns([
        otherwise_expr.alias('category_score')
    ])
    print("Result:")
    print(result_df.to_pandas())
    
    # Compare with pandas
    print("\n=== Pandas comparison ===")
    pandas_result = data.copy()
    pandas_result['category_score'] = np.where(
        pandas_result['category'] == 'A', 
        pandas_result['score'] * 2, 
        pandas_result['score']
    )
    print("Pandas result:")
    print(pandas_result[['category', 'score', 'category_score']])
    
    # Check if they match
    vectrill_result = result_df.to_pandas()
    match = np.array_equal(vectrill_result['category_score'].values, pandas_result['category_score'].values)
    print(f"Results match: {match}")
    
    if not match:
        print("Differences:")
        for i in range(len(data)):
            v_val = vectrill_result.iloc[i]['category_score']
            p_val = pandas_result.iloc[i]['category_score']
            if v_val != p_val:
                print(f"  Row {i}: vectrill={v_val}, pandas={p_val}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
