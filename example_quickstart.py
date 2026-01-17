"""
Quick Start: xarray-sql in 5 minutes

This minimal example shows the core workflow of using xarray-sql.
"""

import xarray as xr
from xarray_sql import XarrayContext

# 1. Load xarray Dataset (using tutorial data)
print("Loading air temperature dataset...")
ds = xr.tutorial.open_dataset('air_temperature')
print(f"Dataset dimensions: {dict(ds.sizes)}")
print(f"Variables: {list(ds.data_vars)}\n")

# 2. Create SQL context
ctx = XarrayContext()

# 3. Register dataset with chunking strategy
print("Registering dataset with SQL context...")
ctx.from_dataset(
    table_name='air_temp',
    input_table=ds,
    chunks={'time': 100, 'lat': 10, 'lon': 10}
)
print("✓ Registered as 'air_temp' table\n")

# 4. Query with SQL!
print("="*60)
print("Example queries:")
print("="*60)

# Query 1: Simple SELECT
print("\n1. Get sample rows:")
result = ctx.sql("""
    SELECT time, lat, lon, air
    FROM air_temp
    LIMIT 5
""")
print(result.to_pandas())

# Query 2: Aggregation
print("\n2. Average temperature by latitude:")
result = ctx.sql("""
    SELECT
        FLOOR(lat / 10) * 10 as lat_band,
        AVG(air) as avg_temp,
        MIN(air) as min_temp,
        MAX(air) as max_temp
    FROM air_temp
    GROUP BY lat_band
    ORDER BY lat_band DESC
""")
print(result.to_pandas())

# Query 3: Filtering
print("\n3. Find hot locations (air > 300K):")
result = ctx.sql("""
    SELECT time, lat, lon, air
    FROM air_temp
    WHERE air > 300
    ORDER BY air DESC
    LIMIT 10
""")
print(result.to_pandas())

print("\n✓ That's it! You can now query multi-dimensional arrays with SQL.")
