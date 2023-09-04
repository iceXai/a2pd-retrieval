# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[]
from aoi import AoiData

from loguru import logger

import yaml
import datetime
import os
import importlib


# In[]
"""
Configuration
"""

class Configuration(object):
    def __init__(self, cfg_file_name: str):
        #status
        logger.info('Load configuration file...')
        #loads the yaml file and reads it content
        with open(os.path.join(os.getcwd(), 'cfg', cfg_file_name)) as f:
            self.config = yaml.safe_load(f)
        #configure the logger
        self.configure_logger()
            
    """ Logger Setup """
    def configure_logger(self) -> None:
        path = self.output_path
        LOG_PATH = os.path.join(path,'log')
        if not os.path.isdir(LOG_PATH):
            os.makedirs(LOG_PATH)
        CARRIER = self.carrier
        SENSOR = self.sensor
        VERSION = self.version
        START = self.start_date.strftime('%Y%j')
        END = self.stop_date.strftime('%Y%j')
        NAME = f'{CARRIER}_{SENSOR}_{VERSION}_{START}-{END}.log'
        logger.add(f'{LOG_PATH}/{NAME}')
        
            
    """Helper Functions"""
    def get_class(self, module_name: str, class_name: str) -> object:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    
    """ Configfile::Authentication """
    @property
    def token(self) -> str:
        #returns LAADS token for download authentication
        return self.config['authentication']['token']
    
    
    """ Configfile::Metadata """
    @property
    def sensor(self) -> str:
        #returns the specified sensor
        return self.config['meta']['sensor'].lower()
    
    @property
    def carrier(self) -> str:
        #returns the carrier of the sensor
        return self.config['meta']['carrier'].lower()
    
    @property
    def version(self) -> str:
        #returns the version needed for the meta class
        return self.config['meta']['version'].lower()
    
    
    """ Configfile::Output """
    @property
    def output_path(self) -> str:
        #get output path
        path = self.config['output']['path']
        #check for existence and create otherwise
        if not os.path.isdir(path):
            os.makedirs(path)
        #returns the specified output path
        return path
    
    
    """ Configfile::Date """
    @property
    def start_date(self) -> datetime.date:
        #returns the start date/time as datetime.date object
        return self.config['date']['start']
    
    @property
    def stop_date(self) -> datetime.date:
        #returns the end date/time as datetime.date object
        return self.config['date']['stop']
    

    """ Configfile::AOI """
    @property
    def user_aois(self) -> list:
        #returns list of aois to be used on the current job
        return self.config['meta']['aoi']
    
    def compile_aoi_data(self) -> None:
        #returns the user specified aoi's
        USER_AOIS = self.user_aois
        #initiates and populates the AOI Data Handler
        AOI = AoiData(USER_AOIS)
        #returns to caller
        return AOI
    
    
    """ Configfile::Processing Modules """
    @property
    def apply_resampling(self) -> bool:
        #returns the status whether to resample the data or not
        return self.config['resampling']['apply']
    
    @property
    def apply_swath_download(self) -> bool:
        #returns the status of the actual file retrieval
        return self.config['retrieval']['apply']
    
    def get_listing_class(self) -> object:
        module_name = 'listing'
        class_name = 'Listing'
        return self.get_class(module_name, class_name)
    
    def get_listingprocessor_class(self) -> object:
        module_name = 'proc'
        class_name = 'ListingProcessor'
        return self.get_class(module_name, class_name)

    def get_retrieval_class(self) -> object:
        module_name = 'retrieval'
        class_name = 'Retrieval'
        return self.get_class(module_name, class_name)
    
    def get_retrievalprocessor_class(self) -> object:
        module_name = 'proc'
        class_name = 'RetrievalProcessor'
        return self.get_class(module_name, class_name)
    
    def get_meta_class(self) -> object:
        #returns the appropriate meta class
        sensor = self.sensor.capitalize()
        class_name = f'{sensor}SwathMeta'
        module_name = 'meta'
        #status
        logger.info(f'Set meta class {class_name}...')
        return self.get_class(module_name, class_name)
    
    """ Job::Listing """
    def setup_listing_module(self) -> object:
        listing_modules = self.config['listing']['modules']
        for key in listing_modules:
            class_name = listing_modules[key]
            listing_modules[key] = self.get_class('proc', class_name)
        #status
        logger.info(f'Initiate listing class...')
        #sets the correct listing class/processor corresponding to the 
        #sensor/carrier
        listing_proc = self.get_listingprocessor_class()
        listing_proc = listing_proc(self, **listing_modules)
        return self.get_listing_class()(listing_proc)
    
    """ Job::Retrieval """
    def setup_retrieval_module(self) -> object:
        retrieval_modules = self.config['retrieval']['modules']
        for key in retrieval_modules:
            class_name = retrieval_modules[key]
            retrieval_modules[key] = self.get_class('proc', class_name)
        #status
        logger.info(f'Initiate retrieval class...')
        #sets the correct retrieval processor corresponding to the
        #sensor/carrier
        retrieval_proc = self.get_retrievalprocessor_class()
        retrieval_proc = retrieval_proc(self, **retrieval_modules)
        return self.get_retrieval_class()(retrieval_proc)

    """ Proc::MetaData """
    def get_meta_module(self) -> object:
        #sets the correct meta data class corresponding to the
        #sensor/carrier
        SENSOR = self.sensor
        VERSION = self.version
        return self.get_meta_class()(SENSOR, VERSION)
