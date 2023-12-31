# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from dataclasses import asdict
from typing import List

import os
import yaml

import pandas as pd


# In[]

"""
Meta Data Classes/Container
"""

@dataclass
class MetaVariable:
    """ 
    Meta variable class containing variable name, datatype, and all 
    other relevant meta data needed for the input/output and processing 
    of the data
    """
    name: str
    datatype: str
    input_parameter: dict
    output_parameter: dict = None
    grid_parameter: dict = None
    process_parameter: dict = None
    
    @property
    def input_file(self):
        return self.input_parameter['file']
    
    @property
    def stack_index(self):
        if 'index' in self.input_parameter.keys():
            return self.input_parameter['index']
        else: 
            return None

    
@dataclass
class MetaStack:
    """ Container class to store and handle individual meta variables """
    variables: List[MetaVariable]
    
    def __len__(self) -> int:
        return len(self.variables)
    
    def __iter__(self):
        return iter(self.variables)
    
    def __getitem__(self, item: int | str) -> MetaVariable:
        if type(item) == str and item in self.names:
            return [var for var in self.variables if item == var.name][0]
        elif type(item) == int:
            return self.variables[item]
        else:
            return None
        
    @property
    def datatypes(self) -> List[str]:
        return [var.datatype for var in self.variables]
    
    @property
    def size(self) -> int:
        return self.__len__()
    
    @property
    def names(self) -> list:
        return [var.name for var in self.variables]
    
    def subset_by_datatype(self, datatype: str) -> MetaStack:
        datatypes = self.datatypes
        if datatype in datatypes:
            idx = [idx for idx, dt in enumerate(datatypes) if dt == datatype]
            subset_vars = [self.variables[i] for i in idx]
            return MetaStack(subset_vars)
        else:
            return None



# In[]

"""
Meta Information on Sensor/Carrier
"""

class Meta(ABC):
    """
    Abstract base class for all meta information regarding the supported 
    sensor/carrier combinations
    """    
    def __init__(self, sensor: str, carrier: str, version: str):
        self.carrier = carrier
        #loads the yaml file and reads it content
        fn = f'{sensor}_{version}.yaml'
        with open(os.path.join(os.getcwd(), 'meta', fn)) as f:
            self.meta = yaml.safe_load(f)
        self._import_variables()
        
    def _import_variables(self) -> None:
        variables = []
        for var in self.meta['variables'].keys():
            var_meta = self.meta['variables'][var]
            data = MetaVariable(var, **var_meta)
            variables.append(data)
        self.metadata = MetaStack(variables)

    @property
    def urls(self) -> dict:
        return self.meta['urls'][self.carrier]
    
    @property
    def variables(self) -> List[str]:
        return [datavar.name for datavar in self.metadata]
    
    @property
    def datatypes(self) -> List[str]:
        datatypes = self.metadata.datatypes
        return [dt for idx, dt in enumerate(datatypes)]
    
    @property
    def indices(self) -> List[int]:
        datatypes = self.metadata.datatypes
        return [idx for idx, dt in enumerate(datatypes)]
    
    @property
    def data(self) -> MetaData:
        return self.metadata
    
    
class ModisSwathMeta(Meta):
    """
    Terra/Aqua MODIS meta information child class tailored to the 
    sensor-specific data processing
    """
    def get_var_input_file_index(self, var: str) -> int:
        #for MODIS the input data stems from two different swath files
        #making it necessary to open both, but both feature variable names
        indices = {'lat': 0,
                   'lon': 0,
                   'sat_zen': 0,
                   'sat_azi': 0,
                   'sol_zen': 0,
                   'sol_azi': 0,
                   'ch01': 1,
                   'ch02': 1,
                   'ch03': 1,
                   'ch04': 1,
                   'ch05': 1,
                   'ch06': 1,
                   'ch07': 1,
                   'ch17': 1,
                   'ch18': 1,
                   'ch19': 1,
                   'ch26': 1,
                   'ch20': 1,
                   'ch21': 1,
                   'ch22': 1,
                   'ch23': 1,
                   'ch24': 1,
                   'ch25': 1,
                   'ch27': 1,
                   'ch28': 1,
                   'ch29': 1,
                   'ch31': 1,
                   'ch32': 1,
                   'ch33': 1,
                   'ch34': 1,
                   'ch35': 1,
                   'ch36': 1,
                   }
        return indices[var]
    
    @property
    def data_prefix(self) -> str:
        data_prefixes = {'terra': 'MOD',
                         'aqua': 'MYD',
                         }
        return data_prefixes[self.carrier]

    def update_input_parameter(self, swaths: tuple) -> None:
        for var in self.metadata:
            VARNAME = var.name
            idx = self.get_var_input_file_index(VARNAME)
            var.input_parameter['file'] = swaths[idx]

    
class SlstrSwathMeta(Meta):
    """
    Sentinel3-A/B SLSTR meta information child class tailored to the 
    sensor-specific data processing
    """      
    def update_input_parameter(self, zippath: str) -> None:
        #loop through meta data and update the paths
        for var in self.metadata:
            FILENAME = var.input_file
            FILENAME = FILENAME.split('/')[-1]
            FILENAME = os.path.join(zippath, FILENAME)
            var.input_parameter['file'] = FILENAME


class OlciSwathMeta(Meta):
    """
    Sentinel3-A/B OLCI meta information child class tailored to the 
    sensor-specific data processing
    """      
    def update_input_parameter(self, zippath: str) -> None:
        #loop through meta data and update the paths
        for var in self.metadata:
            FILENAME = var.input_file
            FILENAME = FILENAME.split('/')[-1]
            FILENAME = os.path.join(zippath, FILENAME)
            var.input_parameter['file'] = FILENAME
