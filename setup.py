from setuptools import setup, find_packages
import os

here = os.path.abspath(os.path.dirname(__file__))
version = {}
with open(os.path.join(here, "cinei", "__version__.py")) as f:
    exec(f.read(), version)

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="cinei",
    version=version["__version__"],
    author="Yijuan Zhang",
    author_email="yijuancham@gmail.com",
    description="Coupled and Integrated Emission Inventory for atmospheric research",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Luyikeka/claude_cinei",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="emissions atmospheric-science climate CEDS MEIC inventory",
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
)
