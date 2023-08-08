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
    def __init__(self):
        #status
        logger.info('Load configuration file...')
        #loads the yaml file and reads it content
        with open(os.path.join(os.getcwd(), 'cfg', 'config.yaml')) as f:
            self.config = yaml.safe_load(f)
        #configure the logger
        self.configure_logger()
            
    """ Logger Setup """
    def configure_logger(self) -> None:
        path = self.get_output_path()
        LOG_PATH = os.path.join(path,'log')
        if not os.path.isdir(LOG_PATH):
            os.makedirs(LOG_PATH)
        CARRIER = self.get_carrier()
        SENSOR = self.get_sensor()
        VERSION = self.get_version()
        START = self.get_start_date().strftime('%Y%j')
        END = self.get_stop_date().strftime('%Y%j')
        NAME = f'{CARRIER}_{SENSOR}_{VERSION}_{START}-{END}.log'
        logger.add(f'{LOG_PATH}/{NAME}')
        
            
    """Helper Functions"""
    def get_class(self, module_name: str, class_name: str) -> object:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    
    """ Configfile::Authentication """
    def get_token(self) -> str:
        #returns LAADS token for download authentication
        return self.config['authentication']['token']
    
    
    """ Configfile::Metadata """
    def get_sensor(self) -> str:
        #returns the specified sensor
        return self.config['meta']['sensor'].lower()
    
    def get_carrier(self) -> str:
        #returns the carrier of the sensor
        return self.config['meta']['carrier'].lower()
    
    def get_version(self) -> str:
        #returns the version needed for the meta class
        return self.config['meta']['version'].lower()
    
    
    """ Configfile::Output """
    def get_output_path(self) -> str:
        #get output path
        path = self.config['output']['path']
        #check for existence and create otherwise
        if not os.path.isdir(path):
            os.makedirs(path)
        #returns the specified output path
        return path
    
    
    """ Configfile::Date """
    def get_start_date(self) -> datetime.date:
        #returns the start date/time as datetime.date object
        return self.config['date']['start']
        
    def get_stop_date(self) -> datetime.date:
        #returns the end date/time as datetime.date object
        return self.config['date']['stop']
    

    """ Configfile::AOI """    
    def get_aois(self) -> list:
        #returns list of aois to be used on the current job
        return self.config['meta']['aoi']
    
    def compile_aoi_data(self) -> None:
        #returns the user specified aoi's
        user_aois = self.get_aois()
        #initiates and populates the AOI Data Handler
        aoi = AoiData(user_aois)
        #returns to caller
        return aoi
    
    
    """ Configfile::Processing Modules """    
    def do_resampling(self) -> bool:
        #returns the status whether to resample the data or not
        return self.config['processing']['resampling']['apply']
    
    def do_swath_download(self) -> bool:
        #returns the status of the actual file retrieval
        return self.config['processing']['retrieval']['apply']
        
    def get_listing_class(self) -> object:
        #returns the specified listing class
        class_name = self.config['processing']['listing']['name']
        module_name = 'listing'
        #status
        logger.info(f'Set listing class {class_name}...')
        return self.get_class(module_name, class_name)
    
    def get_retrieval_class(self) -> object:
        #returns the specified download class
        class_name = self.config['processing']['retrieval']['name']
        module_name = 'retrieval'
        #status
        logger.info(f'Set retrieval class {class_name}...')
        return self.get_class(module_name, class_name)
    
    def get_meta_class(self) -> object:
        #returns the appropriate meta class
        sensor = self.get_sensor().capitalize()
        class_name = f'{sensor}SwathMeta'
        module_name = 'meta'
        #status
        logger.info(f'Set meta class {class_name}...')
        return self.get_class(module_name, class_name)
    
    """ Job::Listing """
    def get_listing_module(self) -> object:
        #sets the correct listing processor corresponding to the 
        #sensor/carrier
        return self.get_listing_class()()
    
    """ Job::Retrieval """
    def get_retrieval_module(self) -> object:
        #sets the correct retrieval processor corresponding to the
        #sensor/carrier
        return self.get_retrieval_class()()

    """ Job::MetaData """
    def get_meta_module(self) -> object:
        #sets the correct meta data class corresponding to the
        #sensor/carrier
        return self.get_meta_class()()



    #[...] more necessary getters/setters