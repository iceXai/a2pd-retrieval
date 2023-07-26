# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 

import pandas as pd
import numpy as np


# In[]


class ListingData(object):
    """
    Contaienr class for all file-listing-related data and its handling through
    other classes using pandas dataframes
    """
    def __init__(self):
        self.lst = pd.DataFrame()
        
    def add_to_listing(self, df: pd.DataFrame) -> None:
        #store current addition
        self.set_current_listing(df)
        #add to overall listing
        self.lst = pd.concat([self.lst, df])
        self.lst = self.lst.reset_index().drop('index', axis=1)
        
    def set_current_listing(self, df: pd.DataFrame) -> None:
        self.tmp = df

    def get_listing(self) -> pd.DataFrame():
        return self.lst
    
    def get_current_listing(self) -> pd.DataFrame():
        return self.tmp
    


class SwathData(object):
    """
    Container for all data-related tasks and its handling through other 
    classes, i.e., swath data, resampled data
    """
    def __init__(self):
        #will store all loaded data from the swaths
        self.data = {}
        #will store the finalized data per specified aoi
        self.resampled_data = None
        
    def add_to_data(self, var_key: str, var: np.array) -> None:
        self.data[var_key] = var 
        
    def get_data(self, var_key: str):
        return self.data[var_key]
        
    def add_to_resampled_data(self, resampled_data: dict) -> None:
        self.resampled_data = resampled_data 
        
    def get_aois(self) -> list:
        if self.resampled_data is not None:
            return self.resampled_data.keys()
        else: 
            return None
    
    def get_resampled_data(self, aoi_key: str, var_key: str) -> dict:
        return self.resampled_data[aoi_key][var_key]
    
    