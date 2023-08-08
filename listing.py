# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from loguru import logger

from proc import ModisListingProcessor
from proc import SlstrListingProcessor

import os
import sys
import requests

import numpy as np
import pandas as pd


# In[]

class Listing(ABC):
    """
    Abstract base class that handles the suitable (a.k.a., inside a specified 
    AOI) file identification process and the compilation of a list with 
    corresponding file URL's
    """        
    @abstractmethod
    def setup_listing_processor(self) -> None:
        pass
    
    @abstractmethod
    def compile_file_listing(self) -> pd.DataFrame:
        """
        compiles the listing of all suitable swath files per sensor that needs 
        to be retrieved from the server and returns it
        """
        pass
    
    def set_cfg(self, cfg: object) -> None:
        """
        Parameters
        ----------
        cfg : object
            Configuration module loading/handling the config file

        Returns
        -------
        None
            Used to pass the configuration down to the listing module and all 
            subsequent instances and modules
        """
        self.cfg = cfg     
        
    def get_date_strings(self) -> list:
        """
        handles the splitting up of the datetime objects into single strings
        independent of the used carrier/sensor
        """
        #get start/stop dates
        START = self.cfg.get_start_date()
        STOP = self.cfg.get_stop_date()
        # generate year (yy) and day-of-year (jj) strings for covered range
        dates = [START + timedelta(days = day_diff) \
                 for day_diff in range(0, (STOP - START).days+1)]
        
        date_str = [(date.strftime('%Y'), date.strftime('%j'))
                    for date in dates]
        
        return date_str

# In[]    
    
class ModisListing(Listing):
    """
    Terra/Aqua MODIS listing child class tailored to the 
    sensor-specific processing
    """
    
    def setup_listing_processor(self) -> None:
        #status
        logger.info(f'Setup listing processor...')
        #set processor
        self.proc = ModisListingProcessor()
        self.proc.set_cfg(self.cfg)
        self.proc.initialize_processor()

    
    def compile_file_listing(self) -> pd.DataFrame:
        #status
        logger.info(f'Compile file listing...')        
        #retrieve date strings for specified processing period
        date_str = self.get_date_strings()
        
        #loop over all dates
        for yy, jj in date_str:
            #status
            logger.info(f'Retrieving file listing for {jj}/{yy}...')

            #set current urls/listing file names
            self.proc.set_current_url(yy, jj)
            self.proc.set_current_lfn(yy, jj)

            #check whether listing for specified date already exists
            LISTING_EXISTS = self.proc.check_for_existing_listing()
            if LISTING_EXISTS:
                logger.info(f'File listing does already exist!')
                self.proc.load_listing()
                continue
            
            """ geoMeta/MXD03: identify swaths in AOI's """                
            #get mxd03 listing file
            DOWNLOAD_COMPLETED = self.proc.get_geometa_file()

            #continue with next date in case something went wrong
            if not DOWNLOAD_COMPLETED:
                continue
            
            #process listing
            self.proc.process_geometa_file()

            """ Compile list of matched MXD02 swaths """
            #get the mxd02 listign file
            DOWNLOAD_COMPLETED = self.proc.get_mxd02_file()

            #continue with next date in case something went wrong
            if not DOWNLOAD_COMPLETED:
                continue
            
            #further process listing; adding MXD02 file names by matching
            self.proc.process_mxd02_listing_file()
            
            #output listing csv file
            self.proc.save_listing()
            
        #returns the completed listing to the caller
        return self.proc.get_listing()
            

# In[]

class SlstrListing(Listing):
    """
    Sentinel3-A/B SLSTR listing child class tailored to the 
    sensor-specific processing
    """

    def setup_listing_processor(self) -> None:
        #status
        logger.info(f'Setup listing processor...')
        #set processor
        self.proc = SlstrListingProcessor()
        self.proc.set_cfg(self.cfg)
        self.proc.initialize_processor()
    
    
    def compile_file_listing(self):
        #status
        logger.info(f'Compile file listing...')        
        #retrieve date strings for specified processing period
        date_str = self.get_date_strings()
        
        #loop over all dates
        for yy, jj in date_str:
            #status
            logger.info(f'Retrieving file listing for {jj}/{yy}...')

            #set current urls/listing file names
            self.proc.set_current_url(yy, jj)
            self.proc.set_current_lfn(yy, jj)

            #check whether listing for specified date already exists
            LISTING_EXISTS = self.proc.check_for_existing_listing()
            if LISTING_EXISTS:
                logger.info(f'File listing does already exist!')
                self.proc.load_listing()
                continue
            
            #get geoMeta listing file
            DOWNLOAD_COMPLETED = self.proc.get_geometa_file()

            #continue with next date in case something went wrong
            if not DOWNLOAD_COMPLETED:
                continue
            
            #process listing
            self.proc.process_geometa_file() 
            
            #output listing csv file
            self.proc.save_listing()
            
        #returns the completed listing to the caller
        return self.proc.get_listing()
 
    

# In[]

class OlciListing(Listing):
    """
    Sentinel3-A/B OLCI listing child class tailored to the 
    sensor-specific processing
    """
    
    def setup_listing_processor(self) -> None:
        pass
       
    def compile_file_listing(self):
        pass

    

# In[]

class ViirsListing(Listing):
    """
    Suomi-NPP/NOOA20 VIIRS listing child class tailored to the 
    sensor-specific processing
    """
            
    def setup_listing_processor(self) -> None:
        pass
    
    
    def compile_file_listing(self):
        pass

         
 