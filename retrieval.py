# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from loguru import logger

import os
import sys

import pandas as pd


# In[]

class Retrieval(object):
    """
    Framework class for the Retrieval Process, setting up the processor class
    and defining the the general retrieval/resampling process
    """
    def __init__(self, processor: object):
        """
        Parameters
        ----------
        processor : object
            The sensor-specific initialized RetrievalProcessor() class
        """
        #status
        logger.info(f'Setup retrieval processor...')
        self.proc = processor
    
    """ API for setup """    
    def set_listing(self, listing: pd.DataFrame) -> None:
        """
        Parameters
        ----------
        listing : pd.DataFrame
            pandas DataFrame containing all swaths to download in the specific 
            format generated by the according Listing child class from the 
            listing module
        """
        self.listing = listing
        
    """ API for run """
    def retrieve_and_process(self) -> None:
        #status
        logger.info(f'Retrieve and process swaths...')
        #parse swath listing to mitigate multiple downloads of the same 
        #file due to several AOIs being specified
        self.proc.parse_swath_listing(self.listing)

        #check for previously or already downloaded and processed files
        self.proc.check_for_existing_swaths()
        
        #receive the final, cleared-up swath listing
        LISTING = self.proc.get_listing()

        #loop over all listing entries
        for _, swath in LISTING.iterrows():          
            #make processor aware of currently processed swaths
            self.proc.set_swath_id(swath)

            #download the swath files
            DOWNLOAD_COMPLETED = self.proc.get_swath_file()
            #continue with next entry in case something went wrong
            if not DOWNLOAD_COMPLETED:
                continue

            #load swath data
            self.proc.load_swath()            

            #resample swath data if specified
            APPLY_RESAMPLING = self.proc.cfg.apply_resampling
            if APPLY_RESAMPLING:
                #id aoi's for current swath
                self.proc.identify_resample_aois()
                #resample
                self.proc.resample_swath()

            #save swath data to h5 format
            self.proc.save_swath()

            #clean-up afterwards
            self.proc.cleanup()  

