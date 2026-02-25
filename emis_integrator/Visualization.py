"""Visualization tools for emission data."""

import matplotlib.pyplot as plt
import xarray as xr
import numpy as np

def plot_emission_map(file_path, variable='sum', cmap='viridis', 
                     figsize=(12, 8), title=None, save_path=None):
    """
    Plot a map of emission data.
    
    Parameters
    ----------
    file_path : str
        Path to the netCDF file containing emission data
    variable : str, optional
        Variable to plot, default is 'sum'
    cmap : str, optional
        Matplotlib colormap name, default is 'viridis'
    figsize : tuple, optional
        Figure size (width, height) in inches
    title : str, optional
        Plot title, if None, will be derived from the file
    save_path : str, optional
        Path to save the figure, if None, the figure is displayed but not saved
        
    Returns
    -------
    matplotlib.figure.Figure
        The figure object
    """
    ds = xr.open_dataset(file_path)
    
    if title is None:
        title = f"Emission map of {variable} from {file_path.split('/')[-1]}"
    
    fig, ax = plt.subplots(figsize=figsize)
    
    # Create a masked array for better visualization
    data = ds[variable].values
    data = np.ma.masked_where(data <= 0, data)
    
    # Plot data
    im = ax.pcolormesh(ds.lon, ds.lat, data, cmap=cmap, shading='auto')
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label(ds.attrs.get('unit', 'Emission'))
    
    # Set labels and title
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(title)
    
    # Add grid
    ax.grid(True, linestyle='--', alpha=0.5)
    
    # Set extent to cover China and surrounding areas
    ax.set_xlim(70, 150)
    ax.set_ylim(10, 60)
    
    # Save figure if path is provided
    if save_path is not None:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
    
    return fig