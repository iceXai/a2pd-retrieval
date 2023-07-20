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
        #initialize the correct listing module/processor
        self.lst = self.cfg.set_listing()
        
        #sets aoi in listing process
        self.lst.set_aoi(self.cfg.compile_aoi_info())
        
        #sets up the listing processor 
        self.lst.setup_listing_processor()
        
        if self.cfg.do_swath_download():
            #initialize the correct retrieval module/processor
            self.ret = self.cfg.set_retrieval()
            
            #sets aoi in retrieval process
            self.ret.set_aoi(self.cfg.compile_aoi_info())
            
            #setup the retrieval processor
            self.ret.setup_retrieval_processor()
        
        if self.cfg.do_resampling():
            #add the resampling module to the retrieval and processor module
            self.ret.apply_resampling()
            

        
    def run(self) -> None:
        """
        Executes the built retrieval job

        Returns
        -------
        None.

        """
        #run the listing processor to compile the file listing
        listing = self.lst.compile_file_listing()
        
        #pass along listing information to retrieval processor
        self.ret.set_listing(listing)
        
        #run the retrieval processor
        self.ret.download_and_process_swaths()
        
        

    
