# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[]
from iotools import ModisIO, SlstrIO
from meta import ModisMeta, SlstrMeta
from listing import ModisListing, SlstrListing
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
    
    def get_ncores(self) -> int:
        #returns number of cores to be used for resampling
        return self.config['meta']['n_cores']
    
    
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
    
    def set_aoi(self) -> None:
        #sets the aoi definitions based on the specified aoi's
        aoi_dict = {}
        aoi_specified = self.get_aoi()
        n_cores = self.get_ncores()
        hemisphere = self.get_hemisphere()
        
        for aoi in aoi_specified:
            aoi = aoi.lower()
            aoi_dict[aoi] = AOI(aoi = aoi, 
                                n_cores = n_cores, 
                                hemisphere = hemisphere)
        return aoi_dict
    
    
    """ Configfile::Processing Modules """    
    def do_resampling(self) -> bool:
        #returns the status whether to resample the data or not
        return self.config['processing']['resampling']['apply']
    
    def do_file_listing(self) -> bool:
        #returns the status of the file listing retrieval
        return self.config['processing']['listing']['apply']
    
    def do_swath_download(self) -> bool:
        #returns the status of the actual file retrieval
        return self.config['processing']['download']['apply']

    def get_resampling_class(self) -> object:
        #returns the specified resampling class
        class_name = self.config['processing']['resampling']['name']
    
    def get_listing_class(self) -> object:
        #returns the specified listing class
        class_name = self.config['processing']['listing']['name']
        module_name = 'listing'
        return self.get_class(module_name, class_name)
    
    def get_download_class(self) -> object:
        #returns the specified download class
        class_name = self.config['processing']['download']['name']
    
    
    """ Job::I/O """
    def set_io(self) -> None:
        #sets the io tools correponding to the sensor/carrier
        class_name = self.get_sensor().capitalize() + 'IO'
        module_name = 'iotools'
        return self.get_class(module_name, class_name)
    
    """ Job::Metadata """
    def set_meta(self) -> None:
        #sets the meta information correponding to the sensor/carrier
        class_name = self.get_sensor().capitalize() + 'Meta'
        module_name = 'meta'
        return self.get_class(module_name, class_name)
    
    """ Job::Listing """
    def set_listing(self) -> None:
        #sets the correct listing processor corresponding to the 
        #sensor/carrier
        token = self.get_token()
        carrier = self.get_carrier()
        start = self.get_start_date()
        stop = self.get_stop_date()
        return self.get_listing_class()(token, carrier, start, stop)

    """ Job::CWD """
    def set_cwd(self) -> None:
        #sets the io tools correponding to the sensor/carrier
        os.chdir(self.get_output_path())
      
    #[...] more necessary getters/setters