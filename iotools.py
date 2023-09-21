# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod
from pyhdf.SD import SD, SDC
from datetime import datetime, timedelta
from loguru import logger
from dataclasses import dataclass

import h5py
import os

import netCDF4 as nc
import numpy as np
import pyresample as pr
import pandas as pd


""" Listing """
# In[]
class ListingIO(object):
    """ Class for all listing-related I/O """
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
@dataclass
class SwathVariable:
    """ Dataclass to keep track of and process a loaded variable """
    data: np.array
    attrs 
    variable: str
    
#TODO have several data classes per sensor-type variable?! that sports 
#     necessary functionality for all its processing and maybe also 
#     resampling and storing? - also rewrite data class to store serveral 
#     of these dataclasses?
#
#     also have a meta dataclass and reorder all meta yaml info along 
#     the channels?
    

class SwathInput(ABC):
    """ Parentclass for all input-related swath operations """
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
    def get_var(self, var: str, grp: str, chspecs: list) -> np.array:
        """
        Parameters
        ----------
        var : str
            Variable name within the data set taken from the variables to 
            process dictionary provided within the sensor-specific meta data
        grp : str
            Group name within the data set taken from the variables to 
            process dictionary provided within the sensor-specific meta data
        chspecs : list
            List entry with channel specifications from the sensor-specific 
            meta file

        Returns
        -------
        np.array
            Returns numpy array with corresponding data
        """
        pass
    
    @abstractmethod
    def process_variable(self, ds: np.array, chspecs: list) -> np.array:
        """
        Parameters
        ----------
        ds : np.array
            Dataset to be processed; output of the get_var() function
        chspecs : list
            List entry with channel specifications from the sensor-specific 
            meta file
            
        Returns
        -------
        np.array
            Returns numpy array with corresponding data
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Returns
        -------
        None
            Closes the opened file handle by load()
        """
        pass
    

class SwathOutput(ABC):
    """ Parentclass for all output-related swath operations """
    @abstractmethod
    def save(self, path: str) -> None:
        """
        Parameters
        ----------
        path : str
            Path to the output file (to be created)

        Returns
        -------
        None
            Creates the output file together with global attributes
        """
        pass
    
    @abstractmethod
    def set_var(self, var: str, grp: str, ds: np.array, attr: str) -> None:
        """
        Parameters
        ----------
        var : str
            Variable name used for within the output file
        grp : str
            Group name used for the variable within the output file 
        dataset : np.array
            Dataset to be placed within the output file
        attr : str
            Corresponding attribute/longname used within the output file

        Returns
        -------
        None
            DESCRIPTION.
        """
        pass
    
    @abstractmethod
    def cleanup(self, path: str) -> None:
        """
        Parameters
        ----------
        path : str
            Path to the raw swath file that shall be removed after processing

        Returns
        -------
        None
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Returns
        -------
        None
            Closes the opened file handle by save()
        """
        pass

# In[]
class HDF4SwathInput(SwathInput):
    def load(self, path: str) -> None:
        self.fh = SD(path,SDC.READ)
    
    def get_var(self, **kwargs) -> None:
        var = kwargs['variable']
        #select scientific data set (sds) and corresponding attributes
        sds = self.fh.select(var)
        import pdb; pdb.set_trace()
        self.attrs = sds.attributes(full=1)
        #index in sds
        if 'index' in kwargs.keys():
            idx = kwargs['index']
            self.data = sds[idx,:,:].astype(np.float32)
        else:
            self.data = sds[:,:].astype(np.float32)
    
    def process_var(self, var: str, chspecs: list) -> np.array:
        pass
    
    def close(self):
        pass

# class NetCDFSwathInput(SwathInput):


# class HDF5SwathInput(SwathInput):
    

class HDF5SwathOutput(SwathOutput):
    def save(self, path: str) -> None:
        pass
    
    def set_var(self, var: str, grp: str, ds: np.array, attr: str) -> None:
        pass

    def cleanup(self, path: str) -> None:
        pass
    
    def close(self) -> None:
        pass


# In[]
class SwathIO(object):
    def __init__(self, swath_in: SwathInput, swath_out: SwathOutput) -> None:
        self.swath_in = swath_in
        self.swath_out = swath_out
        
    def open_input_swath(self, path: str) -> None:
        self.swath_in.load(path)
        
    def get_variable(self, **kwargs: dict) -> None:
        self.swath_in.get_var(**kwargs)
        
    def process_variable(self, **kwargs: dict) -> np.array:
        self.swath_in.process_var(**kwargs)
        
        
    # """
    # Parentclass for all swath-related I/O
    # """
    # def __init__(self, out: str):
    #     self.OUTPUT_PATH = out

    # @abstractmethod
    # def load(self, path: str) -> None:
    #     """
    #     Parameters
    #     ----------
    #     path : str
    #         Path to the file/swath to load

    #     Returns
    #     -------
    #     None
    #         Stores the specific file handle, e.g., within self.fh
    #     """
    #     pass
    
    # @abstractmethod
    # def get_var(self, var: str, grp: str, chspecs: list) -> np.array:
    #     """
    #     Parameters
    #     ----------
    #     var : str
    #         Variable name within the data set taken from the variables to 
    #         process dictionary provided within the sensor-specific meta data
    #     grp : str
    #         Group name within the data set taken from the variables to 
    #         process dictionary provided within the sensor-specific meta data
    #     chspecs : list
    #         List entry with channel specifications from the sensor-specific 
    #         meta file

    #     Returns
    #     -------
    #     np.array
    #         Returns numpy array with corresponding data
    #     """
    #     pass        
    
    # def close(self) -> None:
    #     try:
    #         self.fh.end()
    #     except:
    #         self.fh.close()
    
    # def save(self, path: str) -> None:
    #     #create file and open file handle
    #     self.fh = h5py.File(path,'w')
    #     #set global attributes
    #     AUTHOR = "Dr. Stephan Paul (AWI/iceXai)"
    #     self.fh.attrs.create("author", AUTHOR)
    #     EMAIL = "stephan.paul@posteo.net"
    #     self.fh.attrs.create("contact", EMAIL)
    #     TIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #     self.fh.attrs.create("created", TIMESTAMP)
    
    # def set_var(self, inpath: str, ds: np.array, attr: str) -> None:
    #     #create dataset in file
    #     h5ds = self.fh.create_dataset(inpath,
    #                                   data=ds,
    #                                   compression="gzip",
    #                                   compression_opts=9)
    #     #set data attributes
    #     h5ds.attrs.create("long_name",attr)
    #     h5ds.attrs.create("valid_range",[np.nanmin(ds),np.nanmax(ds)])
    
    # def cleanup(self, path: str) -> None:
    #     os.remove(path)  
    
    
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
        self.fh = nc.Dataset(path, 'r')
    
    def get_var(self, var: str, grp: str, meta: list) -> np.array:
        if grp is None:
            return self.fh.variables[var][:,:]
        else:
            return self.fh.groups[grp].variables[var][:,:]
        

class OlciSwathIO(SwathIO):
    """
    Childclass for all OLCI swath-related I/O
    """
    def load(self, path: str) -> None:
        self.fh = nc.Dataset(path, 'r')
    
    def get_var(self, var: str, grp: str, meta: list) -> np.array:
        if grp is None:
            return self.fh.variables[var][:,:]
        else:
            return self.fh.groups[grp].variables[var][:,:]