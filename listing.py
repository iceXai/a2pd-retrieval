# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from loguru import logger


# In[]

class Listing(object):
    """
    Framework class for the Listing Process, setting up the processor class
    and defining the the general listing process
    """
    def __init__(self, processor: object):
        """
        Parameters
        ----------
        processor : object
            The sensor-specific initialized ListingProcessor() class
        """
        #status
        logger.info(f'Setup listing processor...')
        self.proc = processor
    
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
         
 