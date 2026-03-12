"""Core functionality for CINEI emission integration."""

import pandas as pd
import rioxarray
import xarray as xr
import numpy as np
import geopandas as gpd
import fnmatch
import os
from .utils import ll_area


def emis_union(ceds_dir, meic_dir, save_dir, spec_ceds, spec_meic,
               mon, mon_id, mon_agg, year,
               mapper_path, country_shp, province_shp, agg_dir):
    """
    Integrate emissions data from CEDS and MEIC inventories.

    Parameters
    ----------
    ceds_dir : str
        Directory path for CEDS data files.
        Expected file pattern: CEDS_Glb_0.5x0.5_anthro_{spec}_monthly_{year}.nc
    meic_dir : str
        Directory path for MEIC data files.
        Expected file pattern: *_{mon}_*_{spec}.nc
    save_dir : str
        Directory path to save the integrated output NetCDF files.
    spec_ceds : str
        CEDS species name (e.g. 'SO2', 'NOx', 'BC').
    spec_meic : str
        MEIC species name (e.g. 'SO2', 'NOx', 'PMcoarse').
    mon : str
        Month name abbreviation (e.g. 'Jan', 'Feb').
    mon_id : int
        Month index, 0-based (e.g. 0 for January).
    mon_agg : str
        Month aggregation identifier string (e.g. '01', '02').
    year : str
        Year of data as string (e.g. '2017').
    mapper_path : str
        Path to the species mapper CSV file (Integrated_mapper.csv).
        Required columns: MEIC, partition, weight, if VOC, output species.
    country_shp : str
        Path to the world country shapefile (country.shp).
        Must contain 'CNTRY_NAME' column.
    province_shp : str
        Path to the China province shapefile.
        Must contain column for province names including Taiwan.
    agg_dir : str
        Directory path for pre-aggregated sector emission files.
        Expected file pattern: regridded_aggregated_sectors1{year}{mon_agg}_{spec}.nc

    Returns
    -------
    str
        Path to the output integrated NetCDF file.

    Examples
    --------
    >>> import cinei
    >>> output = cinei.emis_union(
    ...     ceds_dir='/work/bb1554/data/CEDS',
    ...     meic_dir='/work/bb1554/data/MEIC/2017',
    ...     save_dir='/work/bb1554/output/cinei',
    ...     spec_ceds='SO2',
    ...     spec_meic='SO2',
    ...     mon='Jan',
    ...     mon_id=0,
    ...     mon_agg='01',
    ...     year='2017',
    ...     mapper_path='/work/bb1554/data/Integrated_mapper.csv',
    ...     country_shp='/work/bb1554/data/shapefiles/country.shp',
    ...     province_shp='/work/bb1554/data/shapefiles/province.shp',
    ...     agg_dir='/work/bb1554/data/agg_sectors'
    ... )
    """
    # ── Validate input paths ──────────────────────────────────────────
    _check_path(ceds_dir, 'ceds_dir')
    _check_path(meic_dir, 'meic_dir')
    _check_path(mapper_path, 'mapper_path')
    _check_path(country_shp, 'country_shp')
    _check_path(province_shp, 'province_shp')
    _check_path(agg_dir, 'agg_dir')
    os.makedirs(save_dir, exist_ok=True)

    # ── Read CEDS data ────────────────────────────────────────────────
    pre_ceds = 'CEDS_Glb_0.5x0.5_anthro_'
    post_ceds = '__monthly_' + year + '.nc'
    CEDS = os.path.join(ceds_dir, pre_ceds + spec_ceds + post_ceds)
    if spec_ceds == 'BC':
        CEDS = os.path.join(ceds_dir, 'CEDS_Glb_0.5x0.5_anthro_BC__monthly_2016.nc')

    ds = rioxarray.open_rasterio(CEDS, masked=True)
    re_lat = np.arange(ds.y.values.min(), ds.y.values.max(), 0.25)
    re_lon = np.arange(ds.x.values.min(), ds.x.values.max(), 0.25)
    emis_all = ds.interp(y=re_lat, x=re_lon)
    emis_all = emis_all.isel(time=mon_id)
    lon_arange = np.arange(70.125, 150, 0.25, dtype=np.float32)
    lat_arange = np.arange(10.125, 60, 0.25, dtype=np.float32)
    res_tomeic = emis_all.sel(x=lon_arange, y=lat_arange, method="nearest")

    # ── Calculate grid cell area ──────────────────────────────────────
    lon_2d, lat_2d = np.meshgrid(lon_arange, lat_arange)
    area = ll_area(lat_2d, 0.25)

    # ── Unit conversion using mapper ─────────────────────────────────
    mapper = pd.read_csv(mapper_path)
    mapper = mapper.set_index('MEIC')
    meic_spec = spec_meic
    par = mapper.loc[meic_spec, 'partition']
    M   = mapper.loc[meic_spec, 'weight']
    V   = mapper.loc[meic_spec, 'if VOC']
    if V == 'Y':
        unit_tomeic = res_tomeic * 0.001 * area * 2678400 * 1000000 * par / M
    else:
        unit_tomeic = res_tomeic * 0.001 * area * 2678400 * 1000000 * par

    # ── Clip to China (excluding Taiwan) ─────────────────────────────
    country  = gpd.read_file(country_shp)
    China_shp = country[country['CNTRY_NAME'] == 'China']
    province = gpd.read_file(province_shp)
    Taiwan_shp = province[province['行政区划_c'] == '台湾省']
    mChina = gpd.overlay(China_shp, Taiwan_shp, how='difference')
    unit_tomeic.rio.write_crs("epsg:4326", inplace=True)
    ceds_clipped = unit_tomeic.rio.clip(
        mChina.geometry, mChina.crs, drop=False, invert=True)

    # ── Read aggregated sector data ───────────────────────────────────
    agg_path = os.path.join(
        agg_dir,
        f'regridded_aggregated_sectors1{year}{mon_agg}_{meic_spec}.nc')
    _check_path(agg_path, 'agg_path (derived)')
    ds_agg = rioxarray.open_rasterio(agg_path, masked=True)
    ds_agg.rio.write_crs("epsg:4326", inplace=True)
    agg_clipped = ds_agg.rio.clip(
        mChina.geometry, mChina.crs, drop=False, invert=True)
    DS_agg = xr.open_dataset(agg_path)

    # ── Extract sector arrays ─────────────────────────────────────────
    allwst    = DS_agg['waste'].values
    alldoshp  = DS_agg['shipping'].values
    all_avi   = DS_agg['aviation'].values
    doagr     = DS_agg['agriculture'].values
    doagr_clip = np.nan_to_num(agg_clipped['agriculture'].values, nan=0)
    dms_agr   = doagr - doagr_clip[0][::-1]

    # ── Read MEIC sector files ────────────────────────────────────────
    if spec_meic == 'PMcoarse':
        spec_meic = 'PM10'
    i = f'*_{mon}_*_{spec_meic}.*'

    fn_act = os.path.join(meic_dir, fnmatch.filter(
        fnmatch.filter(os.listdir(meic_dir), i), '*agr*nc')[0])
    fn_idt = os.path.join(meic_dir, fnmatch.filter(
        fnmatch.filter(os.listdir(meic_dir), i), '*ind*nc')[0])
    fn_pwr = os.path.join(meic_dir, fnmatch.filter(
        fnmatch.filter(os.listdir(meic_dir), i), '*pow*nc')[0])
    fn_rdt = os.path.join(meic_dir, fnmatch.filter(
        fnmatch.filter(os.listdir(meic_dir), i), '*res*nc')[0])
    fn_tpt = os.path.join(meic_dir, fnmatch.filter(
        fnmatch.filter(os.listdir(meic_dir), i), '*tra*nc')[0])

    act = xr.open_dataset(fn_act)['z'][:].values.reshape((200, 320))[::-1]
    idt = xr.open_dataset(fn_idt)['z'][:].values.reshape((200, 320))[::-1]
    pwr = xr.open_dataset(fn_pwr)['z'][:].values.reshape((200, 320))[::-1]
    rdt = xr.open_dataset(fn_rdt)['z'][:].values.reshape((200, 320))[::-1]
    tpt = xr.open_dataset(fn_tpt)['z'][:].values.reshape((200, 320))[::-1]

    act = np.where(act > 0.0, act, 0.0)
    idt = np.where(idt > 0.0, idt, 0.0)
    pwr = np.where(pwr > 0.0, pwr, 0.0)
    rdt = np.where(rdt > 0.0, rdt, 0.0)
    tpt = np.where(tpt > 0.0, tpt, 0.0)

    # ── Merge sectors ─────────────────────────────────────────────────
    pwr_union = np.nan_to_num(ceds_clipped['energy'], nan=0) + pwr
    res_union = (np.nan_to_num(ceds_clipped['residential'], nan=0) +
                 np.nan_to_num(ceds_clipped['solvents'], nan=0) + rdt)
    idt_union = np.nan_to_num(ceds_clipped['industrial'], nan=0) + idt
    shp_union = np.nan_to_num(ceds_clipped['ships'], nan=0) + alldoshp
    tpt_union = np.nan_to_num(ceds_clipped['transportation'], nan=0) + tpt
    act_union = np.nan_to_num(ceds_clipped['agriculture'], nan=0) + dms_agr
    swd_union = allwst
    avi_union = all_avi
    sum_union = (pwr_union + res_union + idt_union + shp_union +
                 swd_union + tpt_union + act_union)

    # ── Build output xarray Dataset ───────────────────────────────────
    myds = xr.Dataset(
        {"energy":        (("lat", "lon"), pwr_union),
         "residential":   (("lat", "lon"), res_union),
         "industry":      (("lat", "lon"), idt_union),
         "agriculture":   (("lat", "lon"), act_union),
         "transportation":(("lat", "lon"), tpt_union),
         "waste":         (("lat", "lon"), swd_union),
         "shipping":      (("lat", "lon"), shp_union),
         "aviation":      (("lat", "lon"), avi_union),
         "sum":           (("lat", "lon"), sum_union)},
        coords={'lon': lon_arange, 'lat': lat_arange})

    myds.attrs['unit'] = ('million mole/month/grid'
                          if V == 'Y' else 'ton/month/grid')
    myds.attrs['conventions'] = 'NETCDF3_CLASSIC'
    myds.attrs['comments'] = (
        'Integrated inventories include sectoral emissions from both global '
        'and regional inventories with uniform VOC speciation (MOZART mechanism).')
    myds.attrs['projection'] = (
        'Latitude-Longitude gridded data at 0.25 x 0.25 decimal degrees.')
    myds.attrs['authors'] = 'Yijuan Zhang, University of Bremen.'
    myds.attrs['title'] = f'Integrated anthropogenic emission inventory for China in {year}'

    # ── Write output ──────────────────────────────────────────────────
    output_spec = mapper.loc[spec_meic, 'output species']
    output = os.path.join(
        save_dir,
        f'Integrated_Anthropogenic_{year}_{mon}_{output_spec}_0.25x0.25.nc')
    myds.to_netcdf(output, format="NETCDF3_CLASSIC")
    print(f"✅ Output saved: {output}")
    return output


def _check_path(path, name):
    """Raise a clear error if a required path does not exist."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"[CINEI] Required path not found: '{path}'\n"
            f"  Parameter: {name}\n"
            f"  Please provide a valid path when calling emis_union().")
