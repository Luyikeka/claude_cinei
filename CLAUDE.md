# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Emission Integrator is a Python package for integrating atmospheric emissions data from different global and regional anthropogenic emission inventories, specifically designed to combine CEDS (Community Emissions Data System) and MEIC (Multi-resolution Emission Inventory for China) datasets.

## Architecture

The package has a simple, flat structure with three main modules:

- **core.py**: Contains the main `emis_union()` function that performs the complex integration workflow
- **utils.py**: Utility functions including `ll_area()` for calculating grid cell areas based on latitude/longitude
- **Visualization.py**: Contains `plot_emission_map()` for creating visualizations of emission data

The main workflow in `emis_union()` follows these steps:
1. Loads and processes CEDS global inventory data
2. Interpolates CEDS data to MEIC grid resolution (0.25°)
3. Applies unit conversions using species mapping tables
4. Clips data to China boundaries using shapefiles
5. Loads MEIC regional data by sector (agriculture, industry, power, residential, transportation)
6. Integrates datasets by combining sectors appropriately
7. Outputs integrated data as NetCDF files

## Key Dependencies

The package relies on geospatial and scientific computing libraries:
- **xarray/rioxarray**: For handling NetCDF climate data files
- **geopandas**: For geographic boundary operations
- **pandas**: For species mapping and data manipulation
- **numpy**: For numerical operations
- **matplotlib**: For visualization (in Visualization.py)

## Development Commands

This is a simple Python package without complex build tools:

- **Install package**: `pip install -e .` (development mode)
- **Install dependencies**: `pip install pandas rioxarray xarray numpy geopandas matplotlib`
- **Package build**: `python setup.py sdist bdist_wheel`

No specific test framework, linting, or CI/CD configuration is present in the codebase.

## Important Notes

- The package contains hardcoded paths (lines 51-55 in core.py) that point to specific file systems
- Default paths reference `/home/yjzhang/` directories for mapper files and shapefiles
- The integration focuses specifically on China's geographic boundaries
- Data processing assumes 0.25° x 0.25° grid resolution for output
- Units are automatically converted based on species type (VOCs vs non-VOCs)
