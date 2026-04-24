#!/usr/bin/env python
"""
PRISM daily downloader and monthly/yearly netCDF assembler (single variable, simple).

Assumptions (based on the PRISM daily netCDF bundle):
- Raster variable is named 'Band1(lat, lon)'.
- Coordinates are 'lat(lat)' and 'lon(lon)'.
- Grid mapping variable is 'crs' with CF attributes.

We:
- Request NetCDF via PRISM's web service (?format=nc), one grid per day.  # [1](https://acdguide.github.io/Governance/tech/compression.html)
- Skip downloading if the expected ZIP already exists.
- Extract the daily .nc, select 'Band1', add time, rename to user element (e.g., 'ppt').
- Concatenate lazily with dask, write monthly or yearly single-variable netCDF.
- Add minimal CF attributes (units, cell_methods) and keep 'crs' grid mapping.

References:
- PRISM web service doc (endpoint, ?format=nc, one grid per request): https://prism.oregonstate.edu/documents/PRISM_downloads_web_service.pdf  # [1](https://acdguide.github.io/Governance/tech/compression.html)
- PRISM formats (bundled files and naming conventions): https://prism.oregonstate.edu/formats/  # [2](https://data-infrastructure-services.gitlab-pages.dkrz.de/tutorials-and-use-cases/tutorial_compression_netcdf.html)

Background about PRISM file naming (valid as of April 2026), pertaining to the FTP dir structure:

    * Raster data are now distributed in COG (Cloud-Optimized GeoTIFF) format (see https://cogeo.org)
    * Filenames use the internal PRISM naming scheme (e.g., prism_ppt_us_30s_20250101.zip)
        - note that 15s ≡ 400m, 30s ≡ 800m, 25m ≡ 4km (s = seconds, m = minutes)
    * Regions (us, hi, ak, pr) added at second level of time_series & normals directory structures
        - PRISM data for Hawaii and Alaska will be added as they are produced
        - "us" contains data for the 48 conterminous states
        - "hi" and "ak" will contain regional data as they are completed
    * Spatial resolution (400m, 800m, 4km) added at third level 
        - added 800m, which is now available to the public in addition to 4km
        - added 400m, which will be used for Hawaii
    * PRISM variable (ppt, tmin, tmax, etc.) added at fourth level
    * Temporal resolution (daily, monthly) added at fifth level
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import time
from tqdm import tqdm
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import xarray as xr
import dask
dask.config.set(scheduler="threads")


PRISM_BASE = "https://services.nacse.org/prism/data/get"  # base for time-series grids  # [1](https://acdguide.github.io/Governance/tech/compression.html)



def parse_date(s: str) -> dt.date:
    """Parse 'YYYY-MM-DD' into a datetime.date."""
    return dt.date.fromisoformat(s)


def daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    """Yield dates from start to end inclusive.

    Args:
        start: Inclusive start date.
        end: Inclusive end date.

    Yields:
        Each date in [start, end].
    """
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def build_bbox_slices(ds: xr.Dataset,
                      bbox: Tuple[float, float, float, float],
                      lat_name: str = "lat",
                      lon_name: str = "lon") -> Tuple[slice, slice]:
    """Return (lat_slice, lon_slice) for bbox in degrees, respecting coord order.

    Args:
        ds: Dataset that contains the coordinate variables.
        bbox: (lon_min, lat_min, lon_max, lat_max) in degrees, same CRS as ds.
        lat_name: Name of the latitude axis in ds.
        lon_name: Name of the longitude axis in ds.

    Returns:
        (lat_slice, lon_slice) suitable for .sel(...).
    """
    lon_min, lat_min, lon_max, lat_max = bbox

    lat_vals = ds[lat_name].values
    lon_vals = ds[lon_name].values
    lat_ascending = lat_vals[0] < lat_vals[-1]
    lon_ascending = lon_vals[0] < lon_vals[-1]

    lat_slice = slice(lat_min, lat_max) if lat_ascending else slice(lat_max, lat_min)
    lon_slice = slice(lon_min, lon_max) if lon_ascending else slice(lon_max, lon_min)
    return lat_slice, lon_slice

def build_url(region: Literal["us"],
              resolution: Literal["4km", "800m"],
              element: Literal["ppt", "tmin", "tmax"],
              date: dt.date) -> str:
    """Construct PRISM web-service URL for a single daily grid (request NetCDF).

    PRISM service returns one grid per request; add '?format=nc' to get a ZIP containing
    the daily netCDF ('Band1', lat/lon, crs).  # [1](https://acdguide.github.io/Governance/tech/compression.html)

    Args:
        region: Region token, currently 'us' (CONUS).
        resolution: '4km' or '800m'.
        element: Requested climate variable ('ppt', 'tmin', 'tmax').
        date: Daily date.

    Returns:
        Fully-qualified URL string.
    """
    return f"{PRISM_BASE}/{region}/{resolution}/{element}/{date.strftime('%Y%m%d')}?format=nc"  # [1](https://acdguide.github.io/Governance/tech/compression.html)


def expected_zip_name(region: str,
                      resolution: str,
                      element: str,
                      date: dt.date) -> str:
    """Return expected ZIP filename (PRISM naming convention).

    Naming pattern: prism_<var>_<region>_<resolution>_<yyyymmdd>.zip.  # [2](https://data-infrastructure-services.gitlab-pages.dkrz.de/tutorials-and-use-cases/tutorial_compression_netcdf.html)

    Args:
        region: Region token (e.g., 'us').
        resolution: '4km' or '800m'.
        element: Variable name ('ppt', 'tmin', 'tmax').
        date: Daily date.

    Returns:
        Canonical ZIP filename.
    """
    return f"prism_{element}_{region}_{resolution}_{date.strftime('%Y%m%d')}.zip"


def polite_get(url: str, sleep_seconds: float = 2.0) -> requests.Response:
    """Streaming GET with a short delay (polite to PRISM servers).

    Args:
        url: Resource URL.
        sleep_seconds: Delay after the request.

    Returns:
        HTTP response (streaming enabled).

    Raises:
        requests.HTTPError: If request fails.
    """
    resp = requests.get(url, stream=True, timeout=60)
    time.sleep(sleep_seconds)
    resp.raise_for_status()
    return resp


def download_daily_zip(region: Literal["us"],
                       resolution: Literal["4km", "800m"],
                       element: Literal["ppt", "tmin", "tmax"],
                       date: dt.date,
                       out_dir: Path,
                       sleep_seconds: float = 2.0) -> Path:
    """Download a single daily ZIP (NetCDF bundle) if not present; else return it.

    Args:
        region: 'us' (CONUS).
        resolution: '4km' or '800m'.
        element: 'ppt', 'tmin', or 'tmax'.
        date: Daily date.
        out_dir: Destination folder for ZIPs.
        sleep_seconds: Delay between requests.

    Returns:
        Path to ZIP file (existing or newly downloaded).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    zipname = expected_zip_name(region, resolution, element, date)
    out_zip = out_dir / zipname
    if out_zip.exists():
        print(f"zip exists: {out_zip}")
        return out_zip

    # call build_url - obtain url to the file we are trying to download
    # from the PRISM website
    url = build_url(region, resolution, element, date)
    resp = polite_get(url, sleep_seconds=sleep_seconds)
    with open(out_zip, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)
    return out_zip

import re
from pathlib import Path
from typing import Optional

def extract_nc_from_zip(zip_path: Path, extract_dir: Path) -> Path:
    """Return the daily NetCDF without re-extracting if it already exists.

    Behavior:
      - Parse YYYYMMDD from ZIP filename.
      - If a *.nc in extract_dir already matches that date, return it (skip extraction).
      - Else open ZIP and extract the first *.nc member.

    Args:
        zip_path: Path to the daily ZIP (e.g., prism_ppt_us_4km_19990101.zip).
        extract_dir: Folder where daily .nc files are stored.

    Returns:
        Path to the daily .nc (existing or newly extracted).

    Raises:
        FileNotFoundError: If no .nc member exists in the ZIP.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)

    # 1) Try to infer the date from the ZIP filename (YYYYMMDD)
    m = re.search(r'(\d{8})', zip_path.name)
    date_token: Optional[str] = m.group(1) if m else None
    
    # 2) If we can infer the date, see if a matching .nc already exists
    if date_token:
        existing = list(extract_dir.glob(f"*{date_token}*.nc"))
        if existing:
            # Return the first match; you can tighten this if there is
            # more than one *.nc expected per zip
            #print(f"netCDF already unzipped: {existing}")
            return existing[0]

    # 3) Otherwise, extract the first *.nc member from the ZIP
    with zipfile.ZipFile(zip_path, "r") as z:
        nc_members = [m for m in z.namelist() if m.lower().endswith(".nc")]
        if not nc_members:
            raise FileNotFoundError(f"No .nc found in {zip_path}")
        member = nc_members[0]
        z.extract(member, path=extract_dir)
        return extract_dir / member


def stack_period(nc_files: List[Path],
                 out_var_name: Literal["ppt", "tmin", "tmax"],
                 out_nc: Path,
                 chunks: Dict[str, int],
                 bbox: Optional[Tuple[float, float, float, float]] = None) -> None:
    """Stack daily 'Band1(lat, lon)' into one single-variable netCDF file (monthly/yearly).

    We:
    - open each daily netCDF lazily with dask chunks,
    - select 'Band1', add a 'time' coordinate,
    - rename to user-chosen output variable name (e.g., 'ppt'),
    - concatenate along 'time',
    - write a compressed netCDF keeping the 'crs' grid_mapping variable.

    Args:
        nc_files: List of daily netCDF paths for the output period.
        out_var_name: Output variable name ('ppt', 'tmin', or 'tmax').
        out_nc: Destination netCDF file path.
        chunks: Dask chunk sizes, e.g., {'time': 31, 'lat': 512, 'lon': 512}.
        bbox: bounding box, optional, e.g. (lon_min, lat_min, lon_max, lat_max).

    Raises:
        ValueError: If no files or date cannot be parsed.
    """
    if not nc_files:
        raise ValueError("No NetCDF files to stack for this period.")

    dataarrays: List[xr.DataArray] = []

    #for p in sorted(nc_files):
    for p in tqdm(
            nc_files,
            desc=f"Stacking {out_var_name} → {out_nc.name}", 
            unit="file", 
            leave=False):
        m = re.search(r'(\d{8})', p.name)
        if not m:
            raise ValueError(f"Could not parse date from {p.name}")
        ts = pd.to_datetime(m.group(1), format="%Y%m%d")

        # Open lazily with dask chunks
        ds = xr.open_dataset(p, chunks=chunks)

        # Always take the numeric raster from *this* ds
        if "Band1" not in ds.data_vars:
            raise ValueError(f"'Band1' not found in {p.name}. Inspect ds.data_vars.")
        band = ds["Band1"]  # DataArray

        # Robust dim-name detection: PRISM daily netCDF uses 'lat'/'lon', but guard for 'y'/'x'
        dims = band.dims
        lat_name = "lat" if "lat" in dims else ("y" if "y" in dims else None)
        lon_name = "lon" if "lon" in dims else ("x" if "x" in dims else None)
        if lat_name is None or lon_name is None:
            raise ValueError(f"Can't find lat/lon dims in {p.name}. Found dims={dims}")
   
        # Optional subsetting
        if bbox is not None:
            tqdm.write(f"subsetting the dataset to {bbox}")
            lat_slice, lon_slice = build_bbox_slices(ds, bbox, lat_name=lat_name, lon_name=lon_name)
            band = band.sel({lat_name: lat_slice, lon_name: lon_slice})

        # Add the time axis
        da = band.rename(out_var_name).expand_dims({"time": [ts]})
        dataarrays.append(da)
 
    combined = xr.concat(dataarrays, dim="time")

    # Minimal CF attributes
    combined.attrs["long_name"] = {
        "ppt": "Daily precipitation amount",
        "tmin": "Daily minimum air temperature",
        "tmax": "Daily maximum air temperature",
    }[out_var_name]
    combined.attrs["units"] = {
        "ppt": "mm",         # PRISM grids use metric units  # [2](https://data-infrastructure-services.gitlab-pages.dkrz.de/tutorials-and-use-cases/tutorial_compression_netcdf.html)
        "tmin": "degree_C",
        "tmax": "degree_C",
    }[out_var_name]
    # Document PRISM's daily window ending 12:00 GMT (cell methods)
    combined.attrs["cell_methods"] = {
        "ppt": "time: sum (interval: 24 hours, end at 12:00 GMT)",
        "tmin": "time: minimum (interval: 24 hours, end at 12:00 GMT)",
        "tmax": "time: maximum (interval: 24 hours, end at 12:00 GMT)",
    }[out_var_name]

    # Keep the 'crs' grid mapping variable if present
    ds_out = combined.to_dataset(name=out_var_name)

    # Choose a small reference date so time values fit in int32 for a single year/month
    ref_date = pd.to_datetime(ds_out['time'].values[0]).strftime('%Y-%m-%d 00:00:00')

    # Variable (data) encoding as you already have
    t_chunk = min(31, ds_out.sizes.get("time", 31))
    lat_chunk = min(512, ds_out.sizes.get("lat", 512))
    lon_chunk = min(512, ds_out.sizes.get("lon", 512))

    var_enc = {
        out_var_name: {
            "zlib": True,
            "complevel": 3,
            "shuffle": True,
            "chunksizes": (t_chunk, lat_chunk, lon_chunk),
        }
    }

    # Time coordinate encoding: int16 or int32 + CF units/calendar
    time_enc = {
        "time": {
            "dtype": "int32",  # or "int32" if you prefer
            "units": f"days since {ref_date}",
            "calendar": "proleptic_gregorian",
        }
    }

    # CF-friendly numeric grid-mapping variable (no char => no string1 dim)
    ds_out[out_var_name].attrs["grid_mapping"] = "crs"
    if "crs" in ds:
        ds_out["crs"] = xr.DataArray(
            data=np.int32(0),  # any scalar numeric type is fine (int32 is common)
            attrs=ds["crs"].attrs
        )

    # Global attrs
    ds_out.attrs.update({
        "title": f"PRISM daily {out_var_name} ({out_nc.stem})",
        "institution": "PRISM Climate Group, Oregon State University",
        "source": "PRISM time-series web service",
        "references": "https://www.prism.oregonstate.edu/; https://prism.oregonstate.edu/downloads/",
        "Conventions": "CF-1.9",
    })  # [1](https://acdguide.github.io/Governance/tech/compression.html)[2](https://data-infrastructure-services.gitlab-pages.dkrz.de/tutorials-and-use-cases/tutorial_compression_netcdf.html)

    enc = {**var_enc, **time_enc}

    print(f"writing data to netCDF: {out_nc}")
    ds_out.to_netcdf(out_nc, encoding=enc, engine="h5netcdf")


def run_single_variable(element: Literal["ppt", "tmin", "tmax"],
                        region: Literal["us"],
                        resolution: Literal["4km", "800m"],
                        start_date: dt.date,
                        end_date: dt.date,
                        output_dir: Path,
                        zipfile_dir: Optional[Path] = None,
                        netcdf_dir: Optional[Path] = None,
                        stack_by: Literal["monthly", "yearly"] = "monthly",
                        chunks: Optional[Dict[str, int]] = None,
                        bbox: Optional[Tuple[float, float, float, float]] = None,
                        sleep_seconds: float = 2.0,
                        output_prefix: str = "",
                        ) -> None:
    """Download daily ZIPs (NetCDF), extract, and stack 'Band1' monthly or yearly.

    Args:
        element: 'ppt', 'tmin', or 'tmax' (one variable per run).
        region: 'us' for CONUS.
        resolution: '4km' or '800m'.
        start_date: Inclusive start date.
        end_date: Inclusive end date.
        output_dir: Output directory.
        stack_by: 'monthly' (test smaller subsets first) or 'yearly'.
        chunks: Dask chunk sizes (default reasonable values for lat/lon/time).
        bbox: 
        sleep_seconds: Delay between HTTP requests (server etiquette).
        output_prefix: text string to prepend to the output filenames.

    Returns:
        None
    """

    prefix_clean = (output_prefix or "").strip().replace(" ", "_")
    prefix = f"{prefix_clean}__" if prefix_clean else ""

    if chunks is None:
        # Simple defaults that keep memory bounded
        chunks = {"time": 31, "lat": 512, "lon": 512}

    # Decide where ZIPs and daily NetCDFs live
    zipfile_dir = zipfile_dir or (output_dir / f"zip_{element}_{resolution}")
    netcdf_dir  = netcdf_dir  or zipfile_dir

    # Ensure directories exist
    zipfile_dir.mkdir(parents=True, exist_ok=True)
    netcdf_dir.mkdir(parents=True, exist_ok=True)

    # Final outputs will go to output_dir (no pdir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group daily netCDFs by period key (YYYYMM or YYYY)
    by_period: Dict[str, List[Path]] = {}

    ndays = (end_date - start_date).days + 1

    for day in tqdm(
        daterange(start_date, end_date),
        total=ndays,
        desc=f"Downloading {element} {resolution} ({start_date}→{end_date})",
        unit="day",
        leave=False,
    ):
    #for day in daterange(start_date, end_date):
        # Download ZIP (skip if present)
        zip_path = download_daily_zip(region, resolution, element, day, zipfile_dir, sleep_seconds=sleep_seconds)

        # Extract .nc (skip if already extracted)
        daily_nc_name = f"{element}_{region}_{resolution}_{day.strftime('%Y%m%d')}.nc"
        daily_nc_path = netcdf_dir / daily_nc_name
        if not daily_nc_path.exists():
            daily_nc_path = extract_nc_from_zip(zip_path, netcdf_dir)

        # Assign to monthly/yearly key
        key = day.strftime("%Y%m") if stack_by == "monthly" else str(day.year)
        by_period.setdefault(key, []).append(daily_nc_path)

        #time.sleep(0.5)  # small extra delay

    # Stack per period
    for key, files in sorted(by_period.items()):
        out_nc = output_dir / f"{prefix}PRISM_{element}_{region}_{resolution}_{key}.nc"
        if out_nc.exists():
            continue
        stack_period(files, element, out_nc, chunks, bbox)
        tqdm.write(f"Wrote {out_nc}")

def build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="prism_downloader",
        description="Download PRISM daily grids and assemble single-variable monthly/annual NetCDFs."
    )
    p.add_argument("element", choices=["ppt", "tmin", "tmax"],
                   help="PRISM variable to download.")
    p.add_argument("start_date", type=parse_date,
                   help="Start date (YYYY-MM-DD).")
    p.add_argument("end_date", type=parse_date,
                   help="End date (YYYY-MM-DD).")

    p.add_argument(
        "--output_dir",
        type=Path,
        default=Path.cwd(),
        help="Directory where final monthly/yearly NetCDF(s) will be written (default: current working directory).",
    )

    p.add_argument(
        "--zipfile_dir",
        type=Path,
        help="Directory to store downloaded PRISM ZIP bundles. Default: a subfolder under --output_dir."
    )

    p.add_argument(
        "--netcdf_dir",
        type=Path,
        help="Directory to store extracted daily NetCDF files. Default: same as --zipfile_dir."
    )

    p.add_argument("--resolution", choices=["4km", "800m"], default="4km",
                   help="Spatial resolution (default: 4km).")
    p.add_argument("--stack-by", choices=["monthly", "yearly"], default="yearly",
                   help="Stack output by 'monthly' or 'yearly' (default: yearly).")

    # Optional bbox: lon_min lat_min lon_max lat_max
    p.add_argument("--bbox", nargs=4, type=float,
                   metavar=("LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"),
                   help="Optional bounding box in degrees for subsetting.")

    # Keep your sleep/delay as-is; expose if you want
    p.add_argument("--sleep-seconds", type=float, default=2.0,
                   help="Delay between requests (default: 2.0).")

    p.add_argument("--output-prefix", type=str, default="",
                   help="Optional prefix added to output filenames (e.g., 'wisconsin').")

    return p


def main():
    cli = build_cli()
    args = cli.parse_args()

    run_single_variable(
        element=args.element,
        region="us",
        resolution=args.resolution,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        zipfile_dir=args.zipfile_dir,
        netcdf_dir=args.netcdf_dir,
        stack_by=args.stack_by,
        bbox=tuple(args.bbox) if args.bbox else None,
        sleep_seconds=args.sleep_seconds,
        output_prefix=args.output_prefix,
    )


if __name__ == "__main__":
    main()