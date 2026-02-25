from setuptools import setup, find_packages
import os

# Read version from __version__.py
version = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "__version__.py")) as f:
    exec(f.read(), version)

# Read long description from README
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="emis_integrator",
    version=version["__version__"],
    author="Yijuan Zhang",
    author_email="yijuancham@gmail.com",
    description="A Python package for integrating atmospheric emissions data from global and regional inventories",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Luyikeka/claude_cinei",
    project_urls={
        "Bug Reports": "https://github.com/Luyikeka/claude_cinei/issues",
        "Source": "https://github.com/Luyikeka/claude_cinei",
        "Documentation": "https://github.com/Luyikeka/claude_cinei#readme",
    },
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    keywords="emissions, atmospheric science, climate, CEDS, MEIC, inventory, geospatial",
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.0",
        "rioxarray>=0.11.0",
        "xarray>=0.20.0",
        "numpy>=1.20.0",
        "geopandas>=0.10.0",
        "matplotlib>=3.5.0",
        "shapely>=1.8.0",
        "netcdf4>=1.5.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.10",
            "black>=21.0",
            "flake8>=3.8",
            "isort>=5.0",
            "mypy>=0.800",
            "pre-commit>=2.0",
        ],
        "docs": [
            "sphinx>=4.0",
            "sphinx-rtd-theme>=1.0",
            "sphinx-autodoc-typehints>=1.12",
        ],
        "test": [
            "pytest>=6.0",
            "pytest-cov>=2.10",
            "pytest-mock>=3.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "emis-integrate=emis_integrator.core:main",
            "emis-plot=emis_integrator.Visualization:plot_main",
        ],
    },
    zip_safe=False,
)