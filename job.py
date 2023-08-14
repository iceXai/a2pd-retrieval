#!/usr/bin/env python3
#coding: utf-8
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
"""
Modules
"""
from loguru import logger

import cfg


# In[]

"""
Job setup
"""

class RetrievalJob(object):
    def __init__(self, args) -> None:
        """
        Parses all user input arguments into the job processor

        Returns
        -------
        None.

        """        
        #calls and initiates the configuration
        self.cfg = cfg.Configuration()
    
    def validate(self) -> None:
        pass
    
    def setup(self) -> None:
        """
        Puts together the respective components for the current job,
        i.e., resample module, listing module, download module, 
        process module, configuration setup module, I/O module, etc.

        Returns
        -------
        None.

        """
        #status
        logger.info('Setting-up job processor...')
        
        #initialize the correct listing module/processor
        self.lst = self.cfg.get_listing_module()
        
        #passes the configuration along
        self.lst.set_cfg(self.cfg)
        
        #sets up the listing processor 
        self.lst.setup_listing_processor()
        
        if self.cfg.do_swath_download():
            #initialize the correct retrieval module/processor
            self.ret = self.cfg.get_retrieval_module(self.cfg)            

        
    def run(self) -> None:
        """
        Executes the built retrieval job

        Returns
        -------
        None.

        """
        #status
        logger.info('Executing job processor...')
        
        #run the listing processor to compile the file listing
        listing = self.lst.compile_file_listing()
        
        #pass along listing information to retrieval processor
        self.ret.set_listing(listing)
        
        #run the retrieval processor
        self.ret.run()
        
        #status
        logger.info('Job complete! :)')
        
        

    
