# xarray-sql Examples

This directory contains working examples demonstrating how to use xarray-sql to query multi-dimensional array data with SQL.

## Quick Start (5 minutes)

Run the simplest example:

```bash
uv run python example_quickstart.py
```

**What it does**: Loads xarray's built-in air temperature dataset and runs basic SQL queries (SELECT, WHERE, GROUP BY).

**Key code**:
```python
from xarray_sql import XarrayContext
import xarray as xr

# Load data
ds = xr.tutorial.open_dataset('air_temperature')

# Create SQL context
ctx = XarrayContext()
ctx.from_dataset('air_temp', ds, chunks={'time': 100, 'lat': 10, 'lon': 10})

# Query!
result = ctx.sql("SELECT * FROM air_temp WHERE air > 300 LIMIT 10")
df = result.to_pandas()
```

## Wind Analysis Demo

Run the comprehensive atmospheric wind analysis:

```bash
uv run python example_winds_local.py
```

**What it does**: Creates a synthetic ERA5-like dataset with wind components (u, v) at 850 hPa and demonstrates:
- Filtering for specific pressure levels
- Finding strong winds with WHERE clauses
- Aggregating by latitude bands
- Temporal analysis with GROUP BY time
- Regional comparisons with CASE statements

**Use cases demonstrated**:
1. Preview data (`SELECT ... LIMIT`)
2. Find extreme events (`WHERE u_component_of_wind > 40`)
3. Climatological averages (`GROUP BY latitude`)
4. Time series analysis (`GROUP BY time`)
5. Jet stream detection (`WHERE ABS(u) > 35`)
6. Regional statistics (`CASE WHEN ... GROUP BY region`)

## ERA5 from Google Cloud Storage (requires network)

To use real ERA5 data from GCS:

```python
import xarray as xr
from xarray_sql import XarrayContext

# Open ERA5 Zarr from GCS
ds = xr.open_zarr(
    'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
    chunks=None,
    storage_options=dict(token='anon'),
)

# Filter for 850 hPa
ds_850 = ds.sel(level=850)

# Select wind components
ds_winds = ds_850[['u_component_of_wind', 'v_component_of_wind']]

# Register with SQL
ctx = XarrayContext()
ctx.from_dataset('winds_850', ds_winds, chunks={'time': 24, 'latitude': 50, 'longitude': 50})

# Query
result = ctx.sql("""
    SELECT time, latitude, longitude,
           u_component_of_wind, v_component_of_wind
    FROM winds_850
    WHERE ABS(u_component_of_wind) > 20
    LIMIT 100
""")
```

See `example_era5_winds.py` for the full implementation (requires GCS access).

## Chunking Strategy

The `chunks` parameter controls memory usage and parallelism:

```python
# Small chunks = less memory, more overhead
ctx.from_dataset('data', ds, chunks={'time': 10, 'lat': 10, 'lon': 10})

# Large chunks = more memory, less overhead
ctx.from_dataset('data', ds, chunks={'time': 1000, 'lat': 100, 'lon': 100})

# Rule of thumb: aim for ~10-100 MB per chunk
chunk_size_bytes = chunk_time * chunk_lat * chunk_lon * num_vars * 8  # for float64
```

## SQL Features Supported

All standard DataFusion SQL features work:

**Aggregations**: `AVG()`, `SUM()`, `MIN()`, `MAX()`, `COUNT()`, `STDDEV()`

**Filtering**: `WHERE`, comparison operators (`>`, `<`, `=`, `!=`, `BETWEEN`)

**Grouping**: `GROUP BY`, `HAVING`

**Sorting**: `ORDER BY ... ASC/DESC`

**Functions**: `ABS()`, `SQRT()`, `FLOOR()`, `CEIL()`, `ROUND()`, math functions

**Conditionals**: `CASE WHEN ... THEN ... ELSE ... END`

**Joins**: `JOIN`, `LEFT JOIN`, `CROSS JOIN` (between multiple datasets)

**Subqueries**: Supported via DataFusion

## Tips

1. **Always specify chunks** when registering datasets to control memory usage
2. **Filter early** in queries (`WHERE`) to reduce data scanned
3. **Use LIMIT** when exploring to avoid materializing huge results
4. **Check dataset structure** with `ds.dims`, `ds.data_vars`, `ds.coords` before querying
5. **Column names** come from dimension coordinates (e.g., `time`, `lat`, `lon`) and data variables

## Next Steps

- Read the main [README.md](README.md) for installation and API reference
- Check out [tests](xarray_sql/sql_test.py) for more query examples
- See the [roadmap](README.md#roadmap) for upcoming features
