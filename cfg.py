# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[]
from aoi import AOI

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
        #loads the yaml file and reads it content
        with open(os.path.join(os.getcwd(), 'cfg', 'config.yaml')) as f:
            self.config = yaml.load(f,Loader=yaml.FullLoader)
            
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

    def get_hemisphere(self) -> str:
        #returns specified hemisphere
        return self.config['meta']['hemisphere'].lower()
    
    
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
    def get_aoi(self) -> list:
        #returns list of aois to be used on the current job
        return self.config['meta']['aoi']
    
    def compile_aoi_info(self) -> None:
        #sets the aoi definitions based on the specified aoi's
        aoi_dict = {}
        aoi_specified = self.get_aoi()
        hemisphere = self.get_hemisphere()
        
        for aoi in aoi_specified:
            aoi = aoi.lower()
            aoi_dict[aoi] = AOI(aoi = aoi, 
                                hemisphere = hemisphere)
        return aoi_dict
    
    
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
        return self.get_class(module_name, class_name)
    
    def get_retrieval_class(self) -> object:
        #returns the specified download class
        class_name = self.config['processing']['retrieval']['name']
        module_name = 'retrieval'
        return self.get_class(module_name, class_name)
    
    
    """ Job::Listing """
    def set_listing(self) -> None:
        #sets the correct listing processor corresponding to the 
        #sensor/carrier
        token = self.get_token()
        carrier = self.get_carrier()
        start = self.get_start_date()
        stop = self.get_stop_date()
        out = self.get_output_path()
        return self.get_listing_class()(token, carrier, start, stop, out)
    
    """ Job::Retrieval """
    def set_retrieval(self) -> None:
        #sets the correct retrieval processor corresponding to the
        #sensor/carrier
        token = self.get_token()
        out = self.get_output_path()
        return self.get_retrieval_class()(token, out)



    #[...] more necessary getters/setters