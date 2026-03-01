# CINEI - Coupled and Integrated Emission Inventory

[![PyPI version](https://badge.fury.io/py/cinei.svg)](https://pypi.org/project/cinei/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

A Python package for integrating global and regional anthropogenic emission inventories (CEDS + MEIC) for atmospheric chemistry research.

## Installation
```bash
pip install cinei
```

On HPC systems (e.g. DKRZ Levante):
```bash
pip install cinei --user
```

## Quick Start
```python
import cinei

# Integrate CEDS and MEIC emissions
cinei.emis_union(
    ceds_dir='/path/to/ceds',
    meic_dir='/path/to/meic',
    save_dir='/path/to/output',
    spec_ceds='SO2',
    spec_meic='SO2',
    mon='Jan',
    mon_id=0,
    mon_agg='01',
    year='2017'
)

# Visualize emission map
cinei.plot_emission_map('/path/to/output/file.nc', variable='sum')

# Calculate grid cell area
import numpy as np
lat = np.arange(10, 60, 0.25)
area = cinei.ll_area(lat, 0.25)
```

## Functions

| Function | Description |
|----------|-------------|
| `emis_union()` | Integrate CEDS and MEIC emission inventories |
| `ll_area()` | Calculate grid cell area from latitude and resolution |
| `plot_emission_map()` | Visualize emission data from NetCDF files |

## Dependencies

- numpy, pandas, xarray, rioxarray
- geopandas, shapely, matplotlib
- netcdf4, rasterio

## Citation

If you use CINEI in your research, please cite:

> Zhang, Y.: CINEI V1.1: Python code for creating an integrated inventory
> of anthropogenic emission for China, https://doi.org/10.5281/zenodo.15000795, 2025.

## Acknowledgements

This work used resources of the Deutsches Klimarechenzentrum (DKRZ)
granted by its Scientific Steering Committee (WLA) under project ID **bb1554**.

## Author

**Yijuan Zhang**
Institute of Environmental Physics (IUP), University of Bremen
/ Max Planck Institute for Meteorology

## License

MIT License
