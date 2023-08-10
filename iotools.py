# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod
from pyhdf.SD import SD, SDC
from datetime import datetime, timedelta

import h5py
import os

import netCDF4 as nc
import numpy as np
import pyresample as pr
import pandas as pd


""" Listing """
# In[]
class ListingIO(object):
    """
    Class for all listing-related I/O
    """
    def __init__(self, out: str):
        self.OUTPUT_PATH = out
        
    def set_listing_file_name(self, lfn: str) -> None:
        self.path = os.path.join(self.OUTPUT_PATH, lfn)

    def to_csv(self, df: pd.DataFrame) -> None:
        df.to_csv(self.path, index=False)

    def from_csv(self) -> pd.DataFrame:
        return pd.read_csv(self.path)


""" Swath handling """
# In[]
class SwathIO(ABC):
    """
    Parentclass for all swath-related I/O
    """
    def __init__(self, out: str):
        self.OUTPUT_PATH = out

    @abstractmethod
    def load(self, path: str) -> None:
        """
        Parameters
        ----------
        path : str
            Path to the file/swath to load

        Returns
        -------
        None
            Stores the specific file handle, e.g., within self.fh
        """
        pass
    
    @abstractmethod
    def get_var(self, var: str, grp: str, meta: list) -> np.array:
        """
        Parameters
        ----------
        var : str
            Variable name within the data set taken from the variables to 
            process dictionary provided within the sensor-specific meta data
        grp : str
            Group name within the data set taken from the variables to 
            process dictionary provided within the sensor-specific meta data
        meta : list
            List entry from the dictionary containign all the meta information
            for a specific sensor and its channel configuration and storage 
            specification

        Returns
        -------
        np.array
            Returns numpy array with corresponding data
        """
        pass
    
    def close(self) -> None:
        try:
            self.fh.end()
        except:
            self.fh.close()
    
    def save(self, path: str) -> None:
        #create file and open file handle
        self.fh = h5py.File(path,'w')
        #set global attributes
        AUTHOR = "Dr. Stephan Paul (AWI/iceXai)"
        self.fh.attrs.create("author", AUTHOR)
        EMAIL = "stephan.paul@posteo.net"
        self.fh.attrs.create("contact", EMAIL)
        TIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.fh.attrs.create("created", TIMESTAMP)
    
    def set_var(self, inpath: str, ds: np.array, attr: str) -> None:
        #create dataset in file
        h5ds = self.fh.create_dataset(inpath,
                                      data=ds,
                                      compression="gzip",
                                      compression_opts=9)
        #set data attributes
        h5ds.attrs.create("long_name",attr)
        h5ds.attrs.create("valid_range",[np.nanmin(ds),np.nanmax(ds)])
    
    def cleanup(self, path: str) -> None:
        os.remove(path)  
    
    
class ModisSwathIO(SwathIO):
    """
    Childclass for all MODIS swath-related I/O
    """
    def load(self, path: str) -> None:
        self.fh = SD(path,SDC.READ)
        
    def get_var(self, var: str, grp: str, meta: list) -> np.array:
        #select scientific data set (sds) and corresponding attributes
        sds = self.fh.select(var)
        attributes = sds.attributes(full=1)
        
        #index in sds
        if meta[0] is not None:
            variable = sds[meta[0],:,:].astype(np.float32)
        else:
            variable = sds[:,:].astype(np.float32)
        #scale factor
        if meta[1] is not None:
            scale = attributes[meta[1]][0]
        else:
            scale = 1
        #offset
        if meta[2] is not None:
            offset = attributes[meta[2]][0]
        else:
            offset = 0
        #and retrieve the rest
        fill = attributes['_FillValue'][0]
        valid_min = attributes['valid_range'][0][0]
        valid_max = attributes['valid_range'][0][1]
        
        #mask invalid entries
        invalid = np.logical_or(variable>valid_max, 
                                variable<valid_min, 
                                variable==fill)
        variable[invalid] = np.nan
        
        #apply scale/offset
        if meta[0] is not None:
            variable = (variable - offset[meta[0]]) * scale[meta[0]]
        else:
            variable = (variable - offset) * scale
        
        #compute brightness temperature
        if meta[3] is not None:
            #mask values below 0 that sometimes appear
            non_nan = ~np.isnan(variable)
            non_nan[non_nan] = np.less(variable[non_nan],0)
            variable[non_nan] = np.nan
            variable = self._calculate_Tb(variable, meta[3])
        
        #return variable to caller
        return variable[:,:]
             

    #calculate TB specific for this sensor?
    def _calculate_Tb(self, variable, wavelength):
        #define constants
        H_PLANCK = 6.62607004 * 10**-34  #[Js]
        C_SOL    = 2.99792458 * 10**8  #[m/s]
        K_BOLTZ  = 1.38064852 * 10**-23  #[J/K]
        
        #calculate and return brightness temperature
        #Tb = np.zeros(variable.shape).astype(np.float32)
        Tb = H_PLANCK * C_SOL / (K_BOLTZ * wavelength * 
             np.log((2.0 * H_PLANCK * C_SOL**2 * wavelength**(-5)) / 
                    (variable * 10**6) + 1.0))
        return Tb


class SlstrSwathIO(SwathIO):
    """
    Childclass for all SLSTR swath-related I/O
    """
    def load(self, path: str) -> None:
        pass
    
    def get_var(self, var: str, grp: str, meta: list) -> np.array:
        pass
    