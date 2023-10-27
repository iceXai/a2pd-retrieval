# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[]
# from __future__ import annotations

from meta import MetaVariable
from data import SwathVariable
from data import HDF4DataVariable
from data import NetCDFDataVariable
from data import HDF5DataVariable

from abc import ABC, abstractmethod
from pyhdf.SD import SD, SDC
from datetime import datetime, timedelta
from loguru import logger
from typing import List, Dict

import h5py
import os

import numpy as np
import pandas as pd
import xarray as xr


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
    def get_var(self, metavar: MetaVariable) -> SwathVariable:
        """
        Parameters
        ----------
        metavar : MetaVariable
            MetaVariable dataclass corresponding to the variable to retrieve 
            from the swath file

        Returns
        -------
        SwathDataVariable
            Returns a swath DataVariable dataclass or it child class with the 
            corresponding data
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
    def create(self, path: str) -> None:
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
    def set_var(self, data: np.array, group: str, variable: str,
                longname: str) -> None:
        """
        Parameters
        ----------
        data : np.array
            Variable data to be set in the output file
        group : str
            Group name to be used in the output file
        variable : str
            Variable name to be used in the output file
        longname : str
            Attribute long name to better explain the variable name in the 
            output file

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
    
    def get_var(self, metavar: MetaVariable) -> HDF4DataVariable:
        #select scientific data set (sds) and corresponding attributes
        VAR = metavar.input_parameter['variable']
        sds = self.fh.select(VAR)
        attributes = sds.attributes(full=1)
        #index in sds
        IDX = metavar.stack_index
        if IDX is not None:
            data = sds[IDX,:,:].astype(np.float32)
        else:
            data = sds[:,:].astype(np.float32)
        GRID = metavar.grid_parameter
        OUT = metavar.output_parameter
        META = {'grid': GRID,
                'out': OUT,
                }
        #initialize swath variable data class
        DATA = {'name': metavar.name,
                'datatype': metavar.datatype,
                'meta': META,
                'data': data,
                'attributes': attributes,
                }
        return HDF4DataVariable(**DATA)
    
    def close(self):
        self.fh.end()


class NetCDFSwathInput(SwathInput):
    def load(self, path: str) -> None:
        self.fh = xr.open_dataset(path)
    
    def get_var(self, metavar: MetaVariable) -> NetCDFDataVariable:
        VAR = metavar.input_parameter['variable']
        #get data
        data = self.fh[VAR].values
        attributes = {}
        attributes['data'] = self.fh[VAR].attrs
        #retrieve exclusion data 
        if metavar.process_parameter is not None:
            EXCLUDE_VAR = metavar.process_parameter['exclusion_variable']
            exclusion_data = self.fh.variables[EXCLUDE_VAR].values
            attributes['exclusion'] = self.fh.variables[EXCLUDE_VAR].attrs
        else:
            exclusion_data = None
        #compile meta data
        GRID = metavar.grid_parameter
        OUT = metavar.output_parameter
        META = {'grid': GRID,
                'out': OUT,
                }
        #check datatype
        DATATYPE = metavar.datatype
        if DATATYPE != 'geo':
            DATATYPE = f'{DATATYPE}_{GRID["longitude"]}_{GRID["latitude"]}'
        #initialize swath variable data class
        DATA = {'name': metavar.name,
                'datatype': DATATYPE,
                'meta': META,
                'attributes': attributes,
                'data': data,
                'exclude': exclusion_data,
                }
        return NetCDFDataVariable(**DATA)

    def close(self) -> None:
        self.fh.close()
        
        
class HDF5SwathInput(SwathInput):
    def load(self, path: str) -> None:
        self.fh = h5py.File(path, "r")
    
    def get_var(self, metavar: MetaVariable) -> HDF5DataVariable:
        pass
    
    def get_var_by_name(self, var: str, group: str) -> HDF5DataVariable:
        grps = self.fh.keys()
        if group in grps:
            variables = self.fh[group].keys()
            if var in variables:
                data = self.fh[group][var][:]
                #initialize swath variable data class
                DATA = {'name': var,
                        'datatype': group,
                        'meta': None,
                        'attributes': None,
                        'data': data,
                        }
                return HDF5DataVariable(**DATA)
            else:
                logger.info(f'Specified variable {var} does not exist in'+
                            f' group {group}')
                return None
        else:
            logger.info(f'Specified group {group} does not exist in file.')
            return None

    def close(self) -> None:
        self.fh.close()
        

class HDF5SwathOutput(SwathOutput):
    def load(self, path: str) -> None:
        #open file handle
        self.fh = h5py.File(path,'r+')
        
    def create(self, path: str) -> None:
        #create file and open file handle
        self.fh = h5py.File(path,'w')
        #set global attributes
        AUTHOR = "Dr. Stephan Paul (AWI/iceXai)"
        self.fh.attrs.create("author", AUTHOR)
        EMAIL = "stephan.paul@posteo.net"
        self.fh.attrs.create("contact", EMAIL)
        TIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.fh.attrs.create("created", TIMESTAMP)
    
    def set_var(self, data: np.array, group: str, variable: str,
                longname: str) -> None:
        try:
            #create dataset in file
            h5ds = self.fh.create_dataset(f'{group}/{variable}',
                                          data=data,
                                          compression="gzip",
                                          compression_opts=9)
            #set data attributes
            h5ds.attrs.create("long_name", longname)
            h5ds.attrs.create("valid_range", [np.nanmin(data),
                                              np.nanmax(data)])
        except OSError:
            logger.info(f'Variable {group}/{variable} exists already'+
                        f' in output file')
    
    def close(self) -> None:
        self.fh.close()


# In[]
class SwathIO(object):
    def __init__(self, swath_in: SwathInput, swath_out: SwathOutput) -> None:
        self.swath_in = swath_in
        self.swath_out = swath_out
        
    def open_input_swath(self, path: str) -> None:
        self.swath_in.load(path)
        
    def get_variable(self, metavar: MetaVariable) -> SwathVariable:
        return self.swath_in.get_var(metavar)
        
    def close_input_swath(self) -> None:
        self.swath_in.close()
        
    def open_output_swath(self, path: str) -> None:
        self.swath_out.load(path)
        
    def create_output_swath(self, path: str) -> None:
        self.swath_out.create(path)
        
    def set_variable(self, data: np.array, group: str, variable: str,
                     longname: str) -> None:
        self.swath_out.set_var(data, group, variable, longname)
        
    def close_output_swath(self) -> None:
        self.swath_out.close()
        
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
        os.remove(path) 
