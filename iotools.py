# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod
from pyhdf.SD import SD, SDC

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
    
    @abstractmethod
    def create(self, path: str) -> None:
        pass
    
    @abstractmethod
    def set_var(self, var: str, grp: str) -> None:
        pass
    
    @abstractmethod
    def close(self, path: str) -> None:
        pass
    
    @abstractmethod
    def save(self, path: str) -> None:
        pass
    
    @abstractmethod
    def cleanup(self, path: str) -> None:
        pass
    
    
class ModisSwathIO(SwathIO):
    """
    Childclass for all MODIS swath-related I/O
    """
    def load(self, path: str) -> None:
        self.fh = SD(file_key,SDC.READ)
        
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

        #apply attributes to variable
        
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
    
    def create(path: str) -> None:
        pass
    
    def set_var(self, var: str, grp: str) -> None:
        pass
        
    def close(self, path: str) -> None:
        self.fh.end()
    
    def save(self, path: str) -> None:
        pass
    
    def cleanup(self, path: str) -> None:
        pass        

    #calculate TB specific for this sensor?
    def _calculate_Tb(self, variable, wavelength):
        #define constants
        H_PLANCK = 6.62607004 * 10**-34  #[Js]
        C_SOL    = 2.99792458 * 10**8  #[m/s]
        K_BOLTZ  = 1.38064852 * 10**-23  #[J/K]
        
        #calculate and return brightness temperature
        #Tb = np.zeros(variable.shape).astype(np.float32)
        Tb = h_planck * c_sol / (k_boltz * wavelength * 
             np.log((2.0 * h_planck * c_sol**2 * wavelength**(-5)) / 
                    (variable * 10**6) + 1.0))
        return Tb


# In[]
"""
I/O
"""
class SwathIO(ABC):
    """
    Abstract base class for each sensor-based I/O operation
    """
    



class ModisIO(IO):
    """
    Terra/Aqua MODIS input/output handler instance tailored to the specific
    needs of the sensor data processing
    """
    def __init__(self):
        pass
        
    def load(self, path: str) -> None:
        pass
        
    def get_var(self, var: str) -> None:
        
        pass        
    def close(self, path: str) -> None:
        print(f'this closes {path}')
        
    def set_var(self, var: str) -> None:
        print(f'this sets {var}')
        
    def save(self, path: str) -> None:
        print(f'this saves to {path}')
        
    def cleanup(self, path: str) -> None:
        pass
        


class SlstrIO(IO):
    """
    Sentinel3-A/B SLSTR input/output handler instance tailored to the specific
    needs of the sensor data processing
    """
    def __init__(self):
        #will store all loaded data from the swaths
        self.data_dict = {}
        #will store the finalized data per specified aoi
        self.finalized_aoi_dict = {}
    
    def load(self, file_path: str) -> None:
        self.fh = nc.Dataset(file_path,'r')
        
    def get_var(self, var: str) -> None:
        return self.fh.variables[variable_key][:,:]
        
    def set_var(self, var: str) -> None:
        print(f'this sets {var}')
        
    def close(self, path: str) -> None:
        self.fh.close()
    
    def save(self, path: str) -> None:
        print(f'this saves to {path}')

    def cleanup(self, path: str) -> None:
        pass
        

class OlciIO(IO):
    """
    Sentinel3-A/B OLCI input/output handler instance tailored to the specific
    needs of the sensor data processing
    
    NOTE: Not yet implemented - PLACEHOLDER!
    """
    def __init__(self):
        #will store all loaded data from the swaths
        self.data_dict = {}
        #will store the finalized data per specified aoi
        self.finalized_aoi_dict = {}
    
    def load(self, path: str) -> None:
        print(f'this loads {path}')
        
    def get_var(self, var: str) -> None:
        print(f'this gets {var}')
        
    def set_var(self, var: str) -> None:
        print(f'this sets {var}')
        
    def close(self, path: str) -> None:
        print(f'this closes {path}')

    def save(self, path: str) -> None:
        print(f'this saves to {path}')
        
    def cleanup(self, path: str) -> None:
        pass