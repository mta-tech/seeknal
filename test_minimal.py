#!/usr/bin/env python
"""Minimal test for DuckDBTask."""
import sys
sys.path.insert(0, 'src')

import pandas as pd
import pyarrow as pa
from seeknal.tasks.duckdb import DuckDBTask

# Create test data
data = pd.DataFrame({
    'user_id': ['A', 'B', 'C'],
    'amount': [100, 200, 150]
})
table = pa.Table.from_pandas(data)

print("Test data created")
print(table.to_pandas())

# Test basic pipeline
try:
    print("\nCreating DuckDBTask...")
    task = DuckDBTask(name='test')
    print("✓ Task created")
    
    print("\nAdding input...")
    task.add_input(dataframe=table)
    print("✓ Input added")
    
    print("\nAdding SQL stage...")
    task.add_sql('SELECT * FROM __THIS__ WHERE amount > 100')
    print("✓ SQL stage added")
    
    print("\nExecuting transform...")
    result = task.transform()
    print(f"✓ Transform completed! Result type: {type(result)}")
    
    # Convert to pandas if it's Arrow
    import pyarrow as pa
    if isinstance(result, pa.Table):
        print(f"Result has {len(result)} rows")
        print(result.to_pandas())
    elif isinstance(result, pd.DataFrame):
        print(f"Result has {len(result)} rows")
        print(result)
    else:
        print(f"Result: {result}")

    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
