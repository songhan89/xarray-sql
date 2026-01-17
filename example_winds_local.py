"""
Example: Query atmospheric data using SQL with xarray-sql

This script demonstrates how to use xarray-sql to query multi-dimensional
atmospheric data using SQL syntax. We'll use xarray's tutorial data as a
demonstration, but the same approach works with real ERA5 data.
"""

import xarray as xr
import numpy as np
from xarray_sql import XarrayContext

print("="*70)
print("XARRAY-SQL DEMO: Querying Atmospheric Data with SQL")
print("="*70)

# Create a synthetic dataset that mimics ERA5 structure
# This represents wind components at different pressure levels
print("\n1. Creating synthetic atmospheric dataset...")
print("   (Simulating ERA5-like structure with pressure levels)")

# Define coordinates
times = np.arange('2020-01-01', '2020-01-03', dtype='datetime64[h]')
lats = np.arange(-90, 91, 2.5)
lons = np.arange(-180, 180, 2.5)
levels = [1000, 925, 850, 700, 500, 300, 200]  # hPa

# Create synthetic wind data
np.random.seed(42)
u_wind = 10 * np.random.randn(len(times), len(levels), len(lats), len(lons))
v_wind = 8 * np.random.randn(len(times), len(levels), len(lats), len(lons))

# Add realistic patterns: stronger winds at jet stream level (200-300 hPa)
for i, level in enumerate(levels):
    if level <= 300:
        u_wind[:, i, :, :] += 30 * (1 - level/300)  # Westerly jet
        v_wind[:, i, :, :] += 5 * np.sin(lats * np.pi / 180).reshape(1, -1, 1)

# Create xarray Dataset
ds = xr.Dataset(
    {
        'u_component_of_wind': (['time', 'level', 'latitude', 'longitude'], u_wind),
        'v_component_of_wind': (['time', 'level', 'latitude', 'longitude'], v_wind),
    },
    coords={
        'time': times,
        'level': levels,
        'latitude': lats,
        'longitude': lons,
    }
)

# Add metadata
ds['u_component_of_wind'].attrs = {'units': 'm/s', 'long_name': 'U component of wind'}
ds['v_component_of_wind'].attrs = {'units': 'm/s', 'long_name': 'V component of wind'}
ds['level'].attrs = {'units': 'hPa', 'long_name': 'Pressure level'}

print(f"\n   Dataset created:")
print(f"   - Dimensions: {dict(ds.dims)}")
print(f"   - Variables: {list(ds.data_vars.keys())}")
print(f"   - Pressure levels: {ds.level.values} hPa")
print(f"   - Time range: {ds.time.values[0]} to {ds.time.values[-1]}")
print(f"   - Spatial extent: {ds.latitude.min().values}° to {ds.latitude.max().values}° lat")

# Filter for 850 hPa (typical level for low-level jets and weather systems)
print("\n2. Filtering for 850 hPa level...")
ds_850 = ds.sel(level=850)

print(f"   Filtered dimensions: {dict(ds_850.dims)}")
print(f"   Shape: {ds_850['u_component_of_wind'].shape}")
print(f"   Memory size: ~{ds_850.nbytes / 1024 / 1024:.1f} MB")

# Create SQL context
print("\n3. Creating XarrayContext and registering dataset...")
ctx = XarrayContext()

# Choose chunking strategy for efficient processing
chunks = {'time': 12, 'latitude': 30, 'longitude': 30}
print(f"   Using chunks: {chunks}")

ctx.from_dataset('winds_850', ds_850, chunks=chunks)
print("   ✓ Dataset registered as 'winds_850' table")

# Execute SQL queries
print("\n" + "="*70)
print("QUERY 1: Preview the data")
print("="*70)

query1 = """
SELECT time, latitude, longitude,
       u_component_of_wind as u_wind,
       v_component_of_wind as v_wind
FROM winds_850
LIMIT 10
"""

result1 = ctx.sql(query1)
df1 = result1.to_pandas()
print(df1.to_string())

print("\n" + "="*70)
print("QUERY 2: Find locations with strong westerly winds (u > 40 m/s)")
print("="*70)

query2 = """
SELECT time, latitude, longitude,
       u_component_of_wind as u_wind,
       v_component_of_wind as v_wind,
       SQRT(u_component_of_wind * u_component_of_wind +
            v_component_of_wind * v_component_of_wind) as wind_speed
FROM winds_850
WHERE u_component_of_wind > 40
ORDER BY u_component_of_wind DESC
LIMIT 15
"""

result2 = ctx.sql(query2)
df2 = result2.to_pandas()
print(df2.to_string())
print(f"\n   Found {len(df2)} locations with strong westerly winds")

print("\n" + "="*70)
print("QUERY 3: Average winds by latitude band (10° bins)")
print("="*70)

query3 = """
SELECT
    FLOOR(latitude / 10) * 10 as lat_band,
    AVG(u_component_of_wind) as mean_u_wind,
    AVG(v_component_of_wind) as mean_v_wind,
    MIN(u_component_of_wind) as min_u_wind,
    MAX(u_component_of_wind) as max_u_wind,
    COUNT(*) as num_points
FROM winds_850
GROUP BY lat_band
ORDER BY lat_band DESC
"""

result3 = ctx.sql(query3)
df3 = result3.to_pandas()
print(df3.to_string())

print("\n" + "="*70)
print("QUERY 4: Wind statistics by time")
print("="*70)

query4 = """
SELECT
    time,
    AVG(u_component_of_wind) as mean_u,
    AVG(v_component_of_wind) as mean_v,
    STDDEV(u_component_of_wind) as std_u,
    STDDEV(v_component_of_wind) as std_v
FROM winds_850
GROUP BY time
ORDER BY time
LIMIT 20
"""

result4 = ctx.sql(query4)
df4 = result4.to_pandas()
print(df4.to_string())

print("\n" + "="*70)
print("QUERY 5: Find jet stream locations (regions with |u| > 35 m/s)")
print("         Group by time to see temporal evolution")
print("="*70)

query5 = """
SELECT
    time,
    COUNT(*) as jet_locations,
    AVG(u_component_of_wind) as avg_jet_u,
    MAX(u_component_of_wind) as max_jet_u,
    AVG(latitude) as avg_jet_latitude
FROM winds_850
WHERE ABS(u_component_of_wind) > 35
GROUP BY time
ORDER BY time
"""

result5 = ctx.sql(query5)
df5 = result5.to_pandas()
print(df5.to_string())

print("\n" + "="*70)
print("QUERY 6: Regional analysis - Compare winds in different regions")
print("="*70)

query6 = """
SELECT
    CASE
        WHEN latitude > 30 THEN 'Northern Mid-Latitudes'
        WHEN latitude > 0 THEN 'Northern Tropics'
        WHEN latitude > -30 THEN 'Southern Tropics'
        ELSE 'Southern Mid-Latitudes'
    END as region,
    AVG(u_component_of_wind) as mean_u_wind,
    AVG(v_component_of_wind) as mean_v_wind,
    STDDEV(u_component_of_wind) as u_variability,
    COUNT(*) as num_points
FROM winds_850
GROUP BY region
ORDER BY mean_u_wind DESC
"""

result6 = ctx.sql(query6)
df6 = result6.to_pandas()
print(df6.to_string())

print("\n" + "="*70)
print("✓ Demo completed successfully!")
print("="*70)
print("\nKey Takeaways:")
print("  • xarray-sql enables SQL queries on multi-dimensional arrays")
print("  • Chunking strategy controls memory usage and parallelism")
print("  • Use standard SQL: SELECT, WHERE, GROUP BY, aggregations, etc.")
print("  • Works with any xarray Dataset (NetCDF, Zarr, HDF5, etc.)")
print("\nFor 850 hPa ERA5 data from GCS, use:")
print("  ds = xr.open_zarr('gs://gcp-public-data-arco-era5/...')")
print("  ds_850 = ds.sel(level=850)")
print("  ctx.from_dataset('winds', ds_850)")
