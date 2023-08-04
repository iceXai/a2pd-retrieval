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