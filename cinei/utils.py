"""Utility functions for the CINEI package."""
import numpy as np

def ll_area(lat, res):
    """
    Calculate grid cell area based on latitude and resolution.
    
    Parameters
    ----------
    lat : ndarray
        Latitude values in degrees
    res : float
        Resolution in degrees
        
    Returns
    -------
    ndarray
        Grid cell areas in square kilometers
    """
    Re = 6371.392  # Earth radius in km
    X = Re * np.cos(lat * (np.pi/180)) * (np.pi/180) * res
    Y = Re * (np.pi/180) * res
    return X * Y  # Bug fix: added return statement
