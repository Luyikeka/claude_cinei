"""Core functionality for emission integration."""

import pandas as pd
import rioxarray
import xarray as xr
import numpy as np
import geopandas as gpd
import fnmatch
import os
from .utils import ll_area

def emis_union(ceds_dir, meic_dir, save_dir, spec_ceds, spec_meic, mon, mon_id, mon_agg, year, 
               mapper_path=None, country_shp=None, province_shp=None):
    """
    Integrate emissions data from CEDS and MEIC inventories.
    
    Parameters
    ----------
    ceds_dir : str
        Directory path for CEDS data
    meic_dir : str
        Directory path for MEIC data
    save_dir : str
        Directory path to save the integrated data
    spec_ceds : str
        CEDS species name
    spec_meic : str
        MEIC species name
    mon : str
        Month name
    mon_id : int
        Month index
    mon_agg : str
        Month aggregation identifier
    year : str
        Year of data
    mapper_path : str, optional
        Path to the species mapper file
    country_shp : str, optional
        Path to country shapefile
    province_shp : str, optional
        Path to province shapefile
        
    Returns
    -------
    str
        Path to the output file
    """
    # Set default paths if not provided
    if mapper_path is None:
        mapper_path = '/home/yjzhang/meic2wrf/add_bg_emis/integrated_all/Integrated_mapper.csv'
    if country_shp is None:
        country_shp = '/home/yjzhang/shpfiles/World_GIS_data/country.shp'
    if province_shp is None:
        province_shp = '/home/yjzhang/shpfiles/全国数据shp/全国数据/分省.shp'
    
    #####-------------------------------CAMs----------------------------------------####
    pre_ceds = 'CEDS_Glb_0.5x0.5_anthro_'
    post_ceds = '__monthly_'+year+'.nc'
    CEDS = ceds_dir + '/' + pre_ceds + spec_ceds + post_ceds
    if spec_ceds == 'BC': 
        CEDS = ceds_dir + '/' + 'CEDS_Glb_0.5x0.5_anthro_' + 'BC__monthly_'+'2016'+'.nc'
    
    ds = rioxarray.open_rasterio(CEDS, masked=True)
    re_lat = np.arange(ds.y.values.min(), ds.y.values.max(), 0.25)
    re_lon = np.arange(ds.x.values.min(), ds.x.values.max(), 0.25)
    emis_all = ds.interp(y=re_lat, x=re_lon)
    emis_all = emis_all.isel(time=mon_id)
    lon_arange = np.arange(70.125, 150, 0.25, dtype=np.float32)
    lat_arange = np.arange(10.125, 60, 0.25, dtype=np.float32)
    res_tomeic = emis_all.sel(x=lon_arange, y=lat_arange, method="nearest")
    
    #########------------------------calculate area of meic grids 0.25 degree---------------########
    lon_2d, lat_2d = np.meshgrid(lon_arange, lat_arange) 
    area = ll_area(lat_2d, 0.25)

    #########---------------convert units same as meic---------------########
    mapper = pd.read_csv(mapper_path)
    mapper = mapper.set_index('MEIC')
    meic_spec = spec_meic
    par = mapper.loc[meic_spec, 'partition']
    M = mapper.loc[meic_spec, 'weight']  
    V = mapper.loc[meic_spec, 'if VOC'] 
    if V == 'Y':
        unit_tomeic = res_tomeic*0.001*area*2678400*1000000*par/M
    else:
        unit_tomeic = res_tomeic*0.001*area*2678400*1000000*par

    #########---------------clip by shape file---------------########
    country = gpd.read_file(country_shp)
    China_shp = country[country['CNTRY_NAME'] == 'China']
    province = gpd.read_file(province_shp)
    Taiwan_shp = province[province['行政区划_c'] == '台湾省']
    mChina = gpd.overlay(China_shp, Taiwan_shp, how='difference')
    unit_tomeic.rio.write_crs("epsg:4326", inplace=True)
    ceds_clipped = unit_tomeic.rio.clip(mChina.geometry, mChina.crs, drop=False, invert=True)
    
    ###read the ready intermediate aggregation emission data
    agg_path = '/mnt/beegfs/user/yjzhang/emission/integrated_all/agg_sectors/regridded_aggregated_sectors1'+year+mon_agg+'_'+meic_spec+'.nc'
    ds_agg = rioxarray.open_rasterio(agg_path, masked=True)
    ds_agg.rio.write_crs("epsg:4326", inplace=True)
    agg_clipped = ds_agg.rio.clip(mChina.geometry, mChina.crs, drop=False, invert=True)
    DS_agg = xr.open_dataset(agg_path)

    ####masking waste emission
    allwst = DS_agg['waste'].values
    
    ####读取CEDS数据需要考虑mapper table 中的空值
    doagr = DS_agg['agriculture'].values
    doagr_clip = np.nan_to_num(agg_clipped['agriculture'].values, nan=0)
    dms_agr = doagr - doagr_clip[0][::-1]

    ####masking shipping emission
    alldoshp = DS_agg['shipping'].values

    ####masking aviation emission
    all_avi = DS_agg['aviation'].values
    
    #########---------------read meic files---------------########
    if spec_meic == 'PMcoarse': 
        spec_meic = 'PM10'
    ent_dir = meic_dir
    i = '*_' + mon + '_*' + '_' + spec_meic+ '.' + '*'
    
    fn_act = ent_dir+'/' + fnmatch.filter(fnmatch.filter(os.listdir(ent_dir), i), '*agr*nc')[0]
    fn_idt = ent_dir+'/' + fnmatch.filter(fnmatch.filter(os.listdir(ent_dir), i), '*ind*nc')[0]
    fn_pwr = ent_dir+'/' + fnmatch.filter(fnmatch.filter(os.listdir(ent_dir), i), '*pow*nc')[0]
    fn_rdt = ent_dir+'/' + fnmatch.filter(fnmatch.filter(os.listdir(ent_dir), i), '*res*nc')[0]
    fn_tpt = ent_dir+'/' + fnmatch.filter(fnmatch.filter(os.listdir(ent_dir), i), '*tra*nc')[0]
    
    f_act = xr.open_dataset(fn_act)
    f_idt = xr.open_dataset(fn_idt)
    f_pwr = xr.open_dataset(fn_pwr)
    f_rdt = xr.open_dataset(fn_rdt)
    f_tpt = xr.open_dataset(fn_tpt)
    
    act = f_act['z'][:].values.reshape((200, 320),)[::-1]
    act = np.where(act > 0.0, act*1, 0.0)
    idt = f_idt['z'][:].values.reshape((200, 320),)[::-1]
    idt = np.where(idt > 0.0, idt*1, 0.0)
    pwr = f_pwr['z'][:].values.reshape((200, 320),)[::-1]
    pwr = np.where(pwr > 0.0, pwr*1, a=0.0)
    rdt = f_rdt['z'][:].values.reshape((200, 320),)[::-1]
    rdt = np.where(rdt > 0.0, rdt*1, 0.0)
    tpt = f_tpt['z'][:].values.reshape((200, 320),)[::-1]
    tpt = np.where(tpt > 0.0, tpt*1, 0.0)
    
    ###---------------re calculate sectors---------------####
    pwr_union = np.nan_to_num(ceds_clipped['energy'], nan=0) + pwr
    res_union = np.nan_to_num(ceds_clipped['residential'], nan=0) + np.nan_to_num(ceds_clipped['solvents'], nan=0) + rdt
    idt_union = np.nan_to_num(ceds_clipped['industrial'], nan=0) + idt
    shp_ocean = np.nan_to_num(ceds_clipped['ships'], nan=0)
    shp_union = shp_ocean + alldoshp
    swd_union = allwst
    avi_union = all_avi
    tpt_union = np.nan_to_num(ceds_clipped['transportation'], nan=0) + tpt
    act_union = np.nan_to_num(ceds_clipped['agriculture'], nan=0) + dms_agr
    sum_union = pwr_union + res_union + idt_union + shp_union + swd_union + tpt_union + act_union

    #==============Create a xarray file to serve reunion emission data================# 
    myds = xr.Dataset(
        {"energy": (("lat", "lon"), pwr_union),
         "residential": (("lat", "lon"), res_union),
         "industry": (("lat", "lon"), idt_union),
         "agriculture": (("lat", "lon"), act_union),
         "transportation": (("lat", "lon"), tpt_union),
         "waste": (("lat", "lon"), swd_union),
         "shipping": (("lat", "lon"), shp_union),
         "aviation": (("lat", "lon"), avi_union),
         "sum": (("lat", "lon"), sum_union),},
        coords = {'lon': lon_arange,
                  'lat': lat_arange})
    
    if V == 'Y':
        myds.attrs['unit'] = 'million mole/month/grid'
    else:
        myds.attrs['unit'] = 'ton/month/grid'

    myds.attrs['conventions'] = 'NETCDF3_CLASSIC'
    myds.attrs['comments'] = 'Integrated inventories include more sectoral emission from both global inventories and regional inventories and uniform VOC speciation as MOZART chemistry mechanism.'
    myds.attrs['projection'] = 'Latitude-Longitude gridded data at a 0.25 x 0.25 decimal degrees spatial resolution.'
    myds.attrs['authors'] = 'Files prepared by Yijuan Zhang: University of Bremen.'
    myds.attrs['source'] = 'Data are available at the '
    myds.attrs['title'] = 'Integrated anthropogenic emission inventory for China in ' + year
    
    #==============Output as NETCDF files================# 
    output_spec = mapper.loc[meic_spec, 'output species']
    output = save_dir + '/' + 'Integrated_Anthropogenic_' + year + '_' + mon + '_' + output_spec + '_0.25x0.25' + '.nc'
    myds.to_netcdf(output, format="NETCDF3_CLASSIC")
    
    return output