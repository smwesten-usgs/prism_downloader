# prism_downloader
[WIP] Python package to download daily PRISM files, stack them on a monthly or annual basis, and subset spatially if desired.

A script for downloading daily PRISM climate grids (via the PRISM time-series web service), optionally subsetting them to a bounding box, and assembling single-variable monthly or annual CF-compliant NetCDF files.
This project is intentionally minimal: one variable per run, clear directory structure, resumable downloads.

## Goals of this package

Provide a clean, reliable workflow for acquiring PRISM daily datasets using the official web service (?format=nc).
Convert daily NetCDF bundles (each containing Band1(lat, lon)) into well-structured monthly or yearly NetCDF stacks.
Support optional spatial subsetting (lon/lat bbox) before stacking to keep data manageable.
Keep all logic simple, readable, and easy to adapt.
Expose a CLI.

# Installation
1. Clone the repository
git clone https://github.com/smwesten-usgs/prism_downloader.git
cd prism_downloader

2. Install in editable mode
python -m pip install -e .

This installs:

The importable Python package prism_downloader.
A CLI command named:
prism_downloader

Command-line usage
```
prism_downloader -h

usage: prism_downloader {ppt,tmin,tmax} start_date end_date
                        [-h] 
                        [--output_dir OUTPUT_DIR] 
                        [--zipfile_dir ZIPFILE_DIR]
                         [--netcdf_dir NETCDF_DIR]
                        [--ca_bundle CA_BUNDLE] 
                        [--resolution {4km,800m}]
                        [--stack_by {monthly,yearly}]
                        [--bbox LON_MIN LAT_MIN LON_MAX LAT_MAX] [--sleep_seconds SLEEP_SECONDS]
                        [--output_prefix OUTPUT_PREFIX]

Download PRISM daily grids and assemble single-variable monthly/annual NetCDFs.

positional arguments:
  {ppt,tmin,tmax}       PRISM variable to download.
  start_date            Start date (YYYY-MM-DD).
  end_date              End date (YYYY-MM-DD).

options:
  -h, --help            show this help message and exit
  --output_dir OUTPUT_DIR
                        Directory where final monthly/yearly NetCDF(s) will be written (default: current working
                        directory).
  --zipfile_dir ZIPFILE_DIR
                        Directory to store downloaded PRISM ZIP bundles. Default: a subfolder under --output_dir.
  --netcdf_dir NETCDF_DIR
                        Directory to store extracted daily NetCDF files. Default: same as --zipfile_dir.
  --ca_bundle CA_BUNDLE
                        Path to a certificate file (or directory) that tells the downloader which corporate or internal
                        Certificate Authorities (CAs) to trust when making HTTPS requests. Use this when your network
                        performs TLS inspection or uses a private CA and you see CERTIFICATE_VERIFY_FAILED errors.
  --resolution {4km,800m}
                        Spatial resolution (default: 4km).
  --stack_by {monthly,yearly}
                        Stack output by 'monthly' or 'yearly' (default: yearly).
  --bbox LON_MIN LAT_MIN LON_MAX LAT_MAX
                        Optional bounding box in degrees for subsetting.
  --sleep_seconds SLEEP_SECONDS
                        Delay between requests (default: 2.0).
  --output_prefix OUTPUT_PREFIX
                        Optional prefix added to output filenames (e.g., 'wisconsin').
```

# Examples
1. Annual, full CONUS, 4km precipitation:
prism_downloader ppt 1999-01-01 1999-12-31 \
    --output_dir ./out                     \
    --resolution 4km --stack-by yearly

Produces:
out/period_ppt_4km_yearly/PRISM_ppt_us_4km_1999.nc

2. Monthly, 800m tmin, Wisconsin subset:
prism_downloader tmin 1999-01-01 1999-03-31 \
    --output_dir ./out                      \
    --resolution 800m --stack-by monthly    \
    --bbox -93 42 -86 46 --output-prefix wisconsin

Produces:
out/period_tmin_800m_monthly/wisconsin__PRISM_tmin_us_800m_199901.nc
out/period_tmin_800m_monthly/wisconsin__PRISM_tmin_us_800m_199902.nc
out/period_tmin_800m_monthly/wisconsin__PRISM_tmin_us_800m_199903.nc


# Design and Architecture Summary
Below is a high-level description of how the package works internally. This section is meant for developers, maintainers, and curious users.

1. Data Source Overview
PRISM’s time-series web service provides one daily grid per HTTP request:

URL format: https://services.nacse.org/prism/data/get/<region>/<resolution>/<var>/<yyyymmdd>?format=nc
The ?format=nc option returns a ZIP containing: A NetCDF file (Band1(lat, lon))
Metadata files (.xml, .prj, etc.)
Supported variables include: ppt, tmin, tmax, tmean, etc.
Supported resolutions: 4km and 800m (API tokens).
The downloader uses polite request pacing to avoid server throttling.

2. Daily Workflow
For each date in the user-provided range:

Construct the proper URL.
Download the ZIP — unless it already exists.
Extract the daily NetCDF — unless it already exists.
Register the daily file with its appropriate monthly/yearly bucket.
All downloads and extractions are efficient and resumable; code does
not re-download anything if it already exists on local disk.


3. NetCDF Bundles and the Band1 Raster
Assumes that PRISM’s NetCDF bundle always contains:

lat(lat) and lon(lon) coordinate variables.
A single raster variable named Band1(lat, lon).
A crs variable holding the grid-mapping attributes.
This makes the stacking logic predictable.

4. Bounding Box Subsetting
If the user specifies:
--bbox lon_min lat_min lon_max lat_max

Then each daily Band1 array is subset before stacking.
Key details:

The code automatically detects whether latitude is ascending or descending.
Subsetting happens on the DataArray (not the Dataset), ensuring only the cropped region is concatenated.
The subset grid preserves CRS, metadata, and chunking.
This dramatically reduces RAM and disk footprint when working on a subregion like a basin or state.


5. Stacking: Monthly or Annual
After all days are processed and grouped:

For each period (YYYYMM or YYYY), the selected daily rasters are: Opened lazily with xarray + dask.
Given a singleton time dimension.
Concatenated along time.
Saved as a single-variable NetCDF file.
Each output file contains:

One data variable (e.g., ppt).
time, lat, lon dimensions.
crs grid-mapping variable.
Minimal but useful CF attributes (units, cell_methods).
Chunking and compression are applied to keep file sizes reasonable and writes efficient.


6. Chunking Strategy
Defaults:
time: 31
lat: 512
lon: 512

Works well for typical monthly stacks.
Scales up to annual runs without blowing out memory.
Compatible with dask for lazy evaluation.
Advanced users can override chunking via CLI or through direct function calls.

7. Output Naming
Outputs follow the form:
[optional prefix]__PRISM_<var>_<region>_<resolution>_<period>.nc

where <period> is YYYYMM (monthly) or YYYY (yearly).
Example:
wisco__PRISM_ppt_us_4km_1999.nc


The prefix is optional. If omitted, filenames have no leading underscores.

8. Project Layout (src-style)
prism_downloader/
├─ pyproject.toml
├─ README.md
├─ src/
│  └─ prism_downloader/
│     ├─ __init__.py
│     ├─ prism_downloader.py
│     └─ __main__.py
└─ tests/      (optional)


This layout prevents accidental import shadowing and supports modern Python packaging.

9. Philosophy
The project intentionally avoids complexity:

No huge frameworks.
No magical behavior.
No multi-variable output files.
No opaque abstractions.
Keep it simple, stupid!

10. Ideas for Future Extensions

* Unit tests for bbox slicing and period logic.
* Logging (--verbose flag).
* Parallel monthly writes.
* A conda package.
* Automatic handling of PRISM provisional/final versions.
