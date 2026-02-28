# CINEI - Coupled and Integrated Emission Inventory

A Python package for integrating global and regional anthropogenic emission inventories.

## Installation
```bash
pip install cinei
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

# Visualize results
cinei.plot_emission_map('/path/to/output/file.nc', variable='sum')
```

## Citation

Zhang, Y.: CINEI V1.1, https://doi.org/10.5281/zenodo.15000795, 2025.

## Author

Yijuan Zhang, University of Bremen / IUP
