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



# In[]
class ListingIO(object):
    def __init__(self, out: str):
        self.OUTPUT_PATH = out
        
    def set_listing_file_name(self, lfn: str) -> None:
        self.path = os.path.join(self.OUTPUT_PATH, lfn)

    def to_csv(self, df: pd.DataFrame) -> None:
        df.to_csv(self.path)

    def from_csv(self) -> pd.DataFrame:
        return pd.read_csv(self.path)

# In[]
"""
I/O
"""
class IO(ABC):
    """
    Abstract base class for each sensor-based I/O operation
    """
    
    @abstractmethod
    def load(self, path: str) -> None:
        pass
    
    @abstractmethod
    def get_var(self, var: str) -> None:
        pass
    
    @abstractmethod
    def set_var(self, var: str) -> None:
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