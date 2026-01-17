"""
Example: Query ERA5 850 hPa wind data using SQL

This script demonstrates how to use xarray-sql to query ERA5 reanalysis data
stored in Zarr format on Google Cloud Storage.
"""

import xarray as xr
from xarray_sql import XarrayContext

# Open the ERA5 dataset from GCS
print("Opening ERA5 dataset from GCS...")
ds = xr.open_zarr(
    'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
    chunks=None,
    storage_options=dict(token='anon'),
)

print(f"\nDataset loaded:")
print(f"  Dimensions: {dict(ds.dims)}")
print(f"  Coordinates: {list(ds.coords.keys())}")
print(f"  Data variables: {list(ds.data_vars.keys())}")

# Check what pressure levels are available
if 'level' in ds.coords:
    print(f"\nPressure levels (hPa): {ds.level.values}")

# Filter for 850 hPa and wind components (u and v)
print("\nFiltering for 850 hPa and wind components...")
ds_850 = ds.sel(level=850)

# Check if wind components exist
wind_vars = []
if 'u_component_of_wind' in ds_850.data_vars:
    wind_vars.append('u_component_of_wind')
if 'v_component_of_wind' in ds_850.data_vars:
    wind_vars.append('v_component_of_wind')

if not wind_vars:
    print("Wind components not found. Available variables:")
    print(list(ds_850.data_vars.keys()))
else:
    print(f"Found wind variables: {wind_vars}")

    # Select only wind components
    ds_winds = ds_850[wind_vars]

    print(f"\nFiltered dataset:")
    print(f"  Dimensions: {dict(ds_winds.dims)}")
    print(f"  Variables: {list(ds_winds.data_vars.keys())}")
    print(f"  Chunks: {ds_winds.chunks}")

    # Create SQL context
    print("\nCreating SQL context...")
    ctx = XarrayContext()

    # Choose appropriate chunking strategy
    # Use a subset to make it runnable in reasonable time
    print("\nTaking a time slice for demonstration (first 24 hours)...")
    ds_subset = ds_winds.isel(time=slice(0, 24))

    # Register the dataset with chunking
    chunks = {'time': 24, 'latitude': 50, 'longitude': 50}
    print(f"Registering dataset with chunks: {chunks}")
    ctx.from_dataset('winds_850', ds_subset, chunks=chunks)

    # Example SQL queries
    print("\n" + "="*60)
    print("QUERY 1: Get sample of wind data")
    print("="*60)

    query1 = """
    SELECT time, latitude, longitude,
           u_component_of_wind, v_component_of_wind
    FROM winds_850
    LIMIT 10
    """

    result1 = ctx.sql(query1)
    df1 = result1.to_pandas()
    print(df1)

    print("\n" + "="*60)
    print("QUERY 2: Average wind components by latitude band")
    print("="*60)

    query2 = """
    SELECT
        FLOOR(latitude / 10) * 10 as lat_band,
        AVG(u_component_of_wind) as avg_u_wind,
        AVG(v_component_of_wind) as avg_v_wind,
        COUNT(*) as num_points
    FROM winds_850
    GROUP BY lat_band
    ORDER BY lat_band
    """

    result2 = ctx.sql(query2)
    df2 = result2.to_pandas()
    print(df2)

    print("\n" + "="*60)
    print("QUERY 3: Find locations with strong winds (|u| > 20 m/s)")
    print("="*60)

    query3 = """
    SELECT time, latitude, longitude,
           u_component_of_wind, v_component_of_wind
    FROM winds_850
    WHERE ABS(u_component_of_wind) > 20
    ORDER BY ABS(u_component_of_wind) DESC
    LIMIT 10
    """

    result3 = ctx.sql(query3)
    df3 = result3.to_pandas()
    print(df3)

    print("\n" + "="*60)
    print("QUERY 4: Wind statistics by time")
    print("="*60)

    query4 = """
    SELECT
        time,
        AVG(u_component_of_wind) as mean_u,
        AVG(v_component_of_wind) as mean_v,
        MIN(u_component_of_wind) as min_u,
        MAX(u_component_of_wind) as max_u
    FROM winds_850
    GROUP BY time
    ORDER BY time
    """

    result4 = ctx.sql(query4)
    df4 = result4.to_pandas()
    print(df4)

    print("\n✓ Example completed successfully!")
