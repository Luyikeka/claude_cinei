"""
CINEI - Coupled and Integrated Emission Inventory

A Python package for integrating global and regional anthropogenic 
emission inventories (CEDS, MEIC, and others).

Author: Yijuan Zhang, University of Bremen
"""

from .core import emis_union
from .utils import ll_area
from .visualization import plot_emission_map
from .__version__ import __version__

__all__ = ['emis_union', 'll_area', 'plot_emission_map', '__version__']
