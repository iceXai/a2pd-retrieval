# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[]
from __future__ import annotations

from meta import MetaVariable

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict
 
import pandas as pd
import numpy as np

# In[] 

@dataclass
class SwathVariable(ABC):
    name: str
    datatype: str
    meta: Dict[str, str]

@dataclass
class DataVariable(SwathVariable):
    """ Databaseclass to keep track of and process a loaded variable """
    data: np.array
    attributes: dict

    @property
    def shape(self) -> tuple:
        return self.data.shape
    
    def process(self, metavar: MetaVariable) -> None:
        if metavar.process_parameter is not None:
            self._process(metavar)
    
    @abstractmethod
    def _process(self, metavar: MetaVariable) -> None:
        pass
    
@dataclass
class HDF4DataVariable(DataVariable):
    """ HDF4-based data class to handle the individual processing """
    def _process(self, metavar: MetaVariable) -> None:
        #limit data
        FILL_VALUE = self.attributes['_FillValue'][0]
        VALID_MIN = self.attributes['valid_range'][0][0]
        VALID_MAX = self.attributes['valid_range'][0][1]
        DATA = self.data        
        #mask invalid entries
        INVALID = np.logical_or(DATA > VALID_MAX, 
                                DATA < VALID_MIN, 
                                DATA == FILL_VALUE)
        DATA[INVALID] = np.nan
        #apply offset if available
        PROCESS_SPECS = metavar.process_parameter
        IDX = metavar.stack_index
        if 'offset' in PROCESS_SPECS.keys():
            OFFSET = self.attributes[PROCESS_SPECS['offset']][0]
            DATA -= OFFSET[IDX]
        #apply scale if available
        if 'scale' in PROCESS_SPECS.keys() and IDX is None:
            SCALE = self.attributes[PROCESS_SPECS['scale']][0]
            DATA *= SCALE
        if 'scale' in PROCESS_SPECS.keys() and IDX is not None:
            SCALE = self.attributes[PROCESS_SPECS['scale']][0]
            DATA *= SCALE[IDX]
        #compute brightness temperature where applicable
        if 'wavelength' in PROCESS_SPECS.keys():
            #mask values below 0 that sometimes appear
            non_nan = ~np.isnan(DATA)
            non_nan[non_nan] = np.less(DATA[non_nan],0)
            DATA[non_nan] = np.nan
            DATA = self._calculate_Tb(DATA, PROCESS_SPECS['wavelength'])
        #override it
        self.data = DATA
        
    def _calculate_Tb(self, var, wavelength):
        #define constants
        H_PLANCK = 6.62607004 * 10**-34  #[Js]
        C_SOL    = 2.99792458 * 10**8  #[m/s]
        K_BOLTZ  = 1.38064852 * 10**-23  #[J/K]
        
        #calculate and return brightness temperature
        Tb = H_PLANCK * C_SOL / (K_BOLTZ * wavelength * 
             np.log((2.0 * H_PLANCK * C_SOL**2 * wavelength**(-5)) / 
                    (var * 10**6) + 1.0))
        return Tb

@dataclass
class NetCDFDataVariable(DataVariable):
    """ HDF4-based data class to handle the individual processing """
    exclude: np.array = None

    def _process(self, metavar: MetaVariable) -> None:
        #set all unsigned 8bit binary values in the exclusion variable to NaN
        #1UB, 2UB, 4UB, 8UB, 16UB, 32UB, 64UB, 128UB
        # #limit data
        # if self.exclude is not None:
        #     self.data[np.where(self.exclude > 0)] = np.nan
        pass

        
@dataclass
class HDF5DataVariable(DataVariable):
    """ HDF5-based data class to handle the individual processing """
    def _process(self, metavar: MetaVariable) -> None:
        pass

    
@dataclass
class DataStack:
    """ Container class to store and handle individual swath variables """
    variables: List[SwathVariable]
    
    def __len__(self) -> int:
        return len(self.variables)
    
    def __iter__(self):
        return iter(self.variables)
    
    def __getitem__(self, item) -> SwathVariable:
        if type(item) == str and item in self.names:
            return [var for var in self.variables if item == var.name][0]
        elif type(item) == int:
            return self.variables[item]
        else:
            return None
    
    @property
    def names(self) -> List[str]:
        return [var.name for var in self.variables]
    
    @property
    def datatypes(self) -> List[str]:
        return [var.datatype for var in self.variables]
    
    @property
    def aois(self) -> Dict[str, np.array]:
        aoi_dict = {}
        aoi_list = [var.aoi for var in self.variables if hasattr(var, 'aoi')]
        aoi_array = np.array(aoi_list)
        unique_aois = np.unique(aoi_array)
        for aoi in unique_aois:
            aoi_dict[aoi] = np.where(aoi_array==aoi)[0]
        return aoi_dict
    
    @property
    def size(self) -> int:
        return self.__len__()
    
    def subset_by_datatype(self, datatype: str) -> DataStack:
        datatypes = self.datatypes
        if datatype in datatypes:
            idx = [idx for idx, dt in enumerate(datatypes) if dt == datatype]
            subset_vars = [self.variables[i] for i in idx]
            return DataStack(subset_vars)
        else:
            return None
        