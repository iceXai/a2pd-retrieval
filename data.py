# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 

import pandas as pd


# In[]


class ListingData(object):
    """
    Contaienr class for all file-listing-related data and its handling through
    other classes using pandas dataframes
    """
    def __init__(self):
        self.lst = pd.DataFrame()
        
    def add_to_listing(self, df: pd.DataFrame) -> None:
        self.lst = pd.concat([self.lst, df])
        self.lst = self.lst.reset_index().drop('index', axis=1)

    def get_listing(self) -> pd.DataFrame():
        return self.lst
    
    


class Data(object):
    """
    Handles all data-related tasks and stores it in this centralized place, 
    i.e., swaths, swath data, resampled data
    """
    def __init__(self):
        #will store the list of swaths to be downloaded
        self.swaths_to_download = []
        #will store all loaded data from the swaths
        self.data_dict = {}
        #will store the finalized data per specified aoi
        self.finalized_aoi_dict = {}
        
    def add_to_data_dict(self, var_key: str, var) -> None:
        self.data_dict[var_key] = var 
        
    def get_from_data_dict(self, var_key: str):
        return self.data_dict[var_key]
        
    def add_swaths(self, swaths_list: list) -> None:
        self.swaths_to_download.extend(swaths_list)
        
    def get_swaths_to_download(self) -> list:
        return self.swaths_to_download
    
    