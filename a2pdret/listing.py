# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod
from loguru import logger

from proc import ModisListingProcessor
from proc import SlstrListingProcessor


# In[]

class Listing(ABC):
    """
    Abstract base class that handles the suitable (a.k.a., inside a specified 
    AOI) file identification process and the compilation of a list with 
    corresponding file URL's
    """
    def __init__(self, cfg: object):
        """
        Parameters
        ----------
        cfg : object
            Configuration module loading/handling the config file
        """
        self.cfg = cfg
        #set processor
        self._initialize_listing_processor()
    
    """ Internals """
    @abstractmethod
    def _initialize_listing_processor(self) -> None:
        pass
    
    """ API for run """
    def compile_file_listing(self) -> None:
        #status
        logger.info(f'Compile file listing...')        
        #retrieve date strings for specified processing period
        date_str = self.proc.get_dates()
        
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
    
class ModisListing(Listing):
    """
    Terra/Aqua MODIS listing child class tailored to the 
    sensor-specific processing
    """
    def _initialize_listing_processor(self) -> None:
        #status
        logger.info(f'Setup retrieval processor...')
        self.proc = ModisListingProcessor(self.cfg) 
            

# In[]

class SlstrListing(Listing):
    """
    Sentinel3-A/B SLSTR listing child class tailored to the 
    sensor-specific processing
    """
    def _initialize_listing_processor(self) -> None:
        #status
        logger.info(f'Setup retrieval processor...')
        self.proc = SlstrListingProcessor(self.cfg) 
    

# In[]

class OlciListing(Listing):
    """
    Sentinel3-A/B OLCI listing child class tailored to the 
    sensor-specific processing
    """
    def _initialize_listing_processor(self) -> None:
        pass
 

# In[]

class ViirsListing(Listing):
    """
    Suomi-NPP/NOOA20 VIIRS listing child class tailored to the 
    sensor-specific processing
    """
    def _initialize_listing_processor(self) -> None:
        pass


         
 