# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod

import os
import yaml


# In[]

"""
Meta Information on Sensor/Carrier
"""

class Meta(ABC):
    """
    Abstract base class for all meta information regarding the supported 
    sensor/carrier combinations
    """    
    def __init__(self, sensor: str, version: str):
        #loads the yaml file and reads it content
        fn = f'{sensor}_{version}.yaml'
        with open(os.path.join(os.getcwd(), 'meta', fn)) as f:
            self.meta = yaml.safe_load(f)
    
    def set_carrier(self, carrier: str) -> None:
        #sets the current carrier
        self.carrier = carrier
    
    def get_carrier(self) -> str:
        #returns the current carrier
        return self.carrier
            
    def get_urls(self) -> dict:
        #returns the specified variables for the current sensor
        CARRIER = self.get_carrier()
        return self.meta['urls'][CARRIER]
            
    def get_variables(self) -> list:
        #returns the variable tags to be used by all other instances
        return self.meta['variables']

    def get_grp_data(self, grp: str) -> dict:
        return self.meta[grp]
    
    def get_var_data(self, grp: str, var: str) -> list:
        return self.meta[grp][var]
    
    def get_resample_variables(self) -> list:
        #returns a subset of variables that have a grid definition
        grp = 'grid_specs'
        return self.get_grp_data(grp).keys()
            
    def get_var_input_specs(self, var: str) -> list:
        grp = 'input_specs'
        return self.get_var_data(grp, var)
    
    def get_var_grid_specs(self, var: str) -> list:
        grp = 'grid_specs'
        return self.get_var_data(grp, var)
    
    def get_var_channel_specs(self, var: str) -> list:
        grp = 'channel_specs'
        return self.get_var_data(grp, var)
    
    def get_var_output_specs(self, var: str) -> list:
        grp = 'output_specs'
        return self.get_var_data(grp, var)

    
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

    def update_input_specs(self, swaths: tuple) -> None:
        input_specs = self.get_grp_data('input_specs')
        variables = self.get_variables()
        for var in variables:
            idx = self.get_var_input_file_index(var)
            input_specs[var][0] = swaths[idx]

    
class SlstrSwathMeta(Meta):
    """
    Sentinel3-A/B SLSTR meta information child class tailored to the 
    sensor-specific data processing
    """      
    pass