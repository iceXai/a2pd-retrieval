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
        #change the current working directory to the output directory
        self.cfg.set_cwd()
        
        #adds all necessary components to the current job
        self.io   = self.cfg.set_io()
        self.aoi  = self.cfg.set_aoi()
        self.meta = self.cfg.set_meta()
        self.lst  = self.cfg.set_listing()
        
        #sets aoi in lsitign process
        self.lst.set_aoi(self.aoi)
        
        
    def run(self):
        pass

    
