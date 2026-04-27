set ZIPFILE_DIR=D:\weather_and_geodata\prism\daily\zip_ppt_4km
set NETCDF_DIR=D:\weather_and_geodata\prism\daily\nc_ppt_4km
set OUTPUT_DIR=D:\weather_and_geodata\prism\daily\wi
set OUTPUT_PREFIX=wisconsin
set SSL_CERT=<put path to your SSL_CERT here...>
set BBOX=-93 42 -86 47.5

prism_downloader tmin 1999-01-01 1999-12-31 ^
  --output_dir %OUTPUT_DIR% ^
  --zipfile_dir %ZIPFILE_DIR% ^
  --netcdf_dir %NETCDF_DIR% ^
  --ca_bundle %SSL_CERT% ^
  --resolution 4km ^
  --stack_by yearly ^
  --bbox %BBOX% ^
  --output_prefix %OUTPUT_PREFIX%
