#!/usr/bin/env python3
#coding: utf-8
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
"""
Modules
"""
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
        #adds all necessary components to the current job
        self.lst  = self.cfg.set_listing()
        self.ret  = self.cfg.set_retrieval()
        
        #sets aoi in listing process
        self.lst.set_aoi(self.cfg.set_aoi())
        
        
    def run(self):
        """
        Executes the built retrieval job

        Returns
        -------
        None.

        """
        #setup and run the listing processor to compile the file listing
        self.lst.setup_listing_processor()
        listing = self.lst.compile_file_listing()
        
        #pass along listing information to retrieval processor
        self.ret.set_listing(listing)
        
        #setup and run the retrieval processor
        self.ret.setup_retrieval_processor()
        self.ret.download_and_process_swaths()
        
        

    
