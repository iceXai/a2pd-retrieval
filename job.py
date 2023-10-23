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

import argparse
import os
import sys

from cfg import Configuration
from cfg import CFGValidator


# In[]

"""
Retrieval Job setup and execution
"""

class RetrievalJob(object):
    def __init__(self, args: dict) -> None:
        """
        Parses all user input arguments into the job processor

        Returns
        -------
        None.

        """
        #status
        logger.info('Initialize job processor...')
        #parse arguments
        self.args = args
        #status
        logger.info('Complete.')

    
    def validate(self) -> None:
        """
        Validates the user input, for now, the provided configuration file, 
        it's existence and its setup
        
        Returns
        -------
        None.
        
        """
        #status
        logger.info('Validate user arguments and setting files...')
        
        #validate configuration file existence
        CFG_FILE = self.args['cfg']
        CFG_PATH = os.path.join(os.getcwd(), 'cfg', CFG_FILE)
        if not os.path.isfile(CFG_PATH):
            logger.critical('No configuration file found!')
            sys.exit()
        #after initial validation of file existance initiate the configuration
        self.cfg = Configuration(CFG_FILE)
        #initiate Validator
        validator = CFGValidator(self.cfg)
        #make initial sanity checks
        validator.validate()
        #status
        logger.info('Complete.')
    
    def setup(self) -> None:
        """
        Puts together the respective components for the current job,
        i.e., listing module and retrieval module as well as setting 
        up the configuration

        Returns
        -------
        None.

        """
        #status
        logger.info('Setting-up job processor...')
        
        #initialize the correct listing module/processor
        self.lst = self.cfg.setup_listing_module()
        
        APPLY_RETRIEVAL = self.cfg.apply_swath_download
        if APPLY_RETRIEVAL:
            #initialize the correct retrieval module/processor
            self.ret = self.cfg.setup_retrieval_module()
        #status
        logger.info('Complete.')
        
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
        
        APPLY_RETRIEVAL = self.cfg.apply_swath_download
        if APPLY_RETRIEVAL:
            #pass along listing information to retrieval processor
            self.ret.set_listing(listing)
            
            #run the retrieval processor to retrieve and process all swaths
            self.ret.retrieve_and_process()
        
        #status
        logger.info('Job complete! :)')
        
        

    
