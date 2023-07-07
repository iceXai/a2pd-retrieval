# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

#from iotools import ModisListingIO
from proc import ModisListingProcessor
from data import ListingData

import os
import sys
import requests

import numpy as np
import pandas as pd


# In[]


class Listing(ABC):
    """
    Abstract base class that handles the suitable (a.k.a., inside a specified 
    AOI) file identification process and the compilation of a list with 
    corresponding file URL's
    """
    def __init__(self,
                 token: str,
                 carrier: str,
                 start: datetime.date,
                 stop: datetime.date,
                 out: str
                 ):
        """
        Parameters
        ----------
        token : str
            DESCRIPTION.
        carrier : str
            DESCRIPTION.
        start : datetime.date
            DESCRIPTION.
        stop : datetime.date
            DESCRIPTION.
        out : str
            DESCRIPTION.
        """
        
        # #common dict of url's
        # self.url = {'terra':{'mxd02':'https://ladsweb.modaps.eosdis.'+\
        #                              'nasa.gov/archive/allData/61/MOD021KM/',
        #                      'mxd03':'https://ladsweb.modaps.eosdis.'+\
        #                              'nasa.gov/archive/allData/61/MOD03/',
        #                      'meta':'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/geoMeta/61/TERRA/'
        #                             },
        #             'aqua':{'mxd02':'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/allData/61/MYD021KM/',
        #                     'mxd03':'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/allData/61/MYD03/',
        #                     'meta':'https://ladsweb.modaps.eosdis.'+\
        #                            'nasa.gov/archive/geoMeta/61/AQUA/'
        #                            },
        #             's3a':{'slstr': 'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/allData/450/'+\
        #                             'S3A_SL_1_RBT/',
        #                    'olci': 'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/allData/450/'+\
        #                             'S3A_OL_1_EFR/',
        #                    'meta': 'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/geoMetaSentinel3/450/'
        #                         },
        #             's3b':{'slstr': 'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/allData/450/'+\
        #                             'S3B_SL_1_RBT/',
        #                    'olci': 'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/allData/450/'+\
        #                             'S3B_OL_1_EFR/',
        #                    'meta': 'https://ladsweb.modaps.eosdis.'+\
        #                             'nasa.gov/archive/geoMetaSentinel3/450/'
        #                         }
        #                   }
        
        # #common dict of prefixes
        # self.prefix = {'terra': 'MOD',
        #                'aqua': 'MYD'
        #                }
        
        #store arguments
        self.token = token
        self.carrier = carrier
        self.start = start
        self.stop = stop
        self.out = out

    
    """ High-level (abstract) functions """
    
    @abstractmethod
    def compile_file_listing(self):
        """
        compiles the listing of all suitable swath files per sensor that needs 
        to be retrieved from the server
        """
        pass
    
    @abstractmethod
    def skip_existing_files(self):
        """
        handles the skipping of already downloaded files in a 
        restarted/continued job setup
        """
        pass


    def set_aoi(self, aoi: dict) -> None:
        """
        allows for setting the user specified AOIs for the listing process
        """
        self.aoi = aoi
        
        
    """ Low-level functions """
        
    def _get_date_strings(self) -> list:
        """
        handles the splitting up of the datetime objects into single strings
        independent of the used carrier/sensor
        """
        # generate year (yy) and day-of-year (jj) strings for covered range
        dates = [self.start + timedelta(days = day_diff) \
                 for day_diff in range(0, (self.stop - self.start).days+1)]
        
        date_str = [(date.strftime('%Y'), date.strftime('%j'))
                    for date in dates]
        
        return date_str

# In[]    
    
class ModisListing(Listing):
    """
    Terra/Aqua MODIS listing process child class tailored to the 
    sensor-specific processing
    """
    
    def compile_file_listing(self):
        #set processor
        processor = ModisListingProcessor()
        processor.set_carrier(self.carrier)
        processor.set_token(self.token)
        processor.set_aoi(self.aoi)
        processor.set_output_path(self.out)
        processor.set_url()
        
        #set data container
        listing = ListingData()
        
        #set listing io handler
        #lstio = ModisListingIO()
        
        #retrieve date strings for specified processing period
        date_str = self._get_date_strings()
        
        #loop over all dates
        for yy, jj in date_str:            
            """ geoMeta/MXD03: identify swaths in AOI's """
            #set current urls/listing file names
            processor.set_current_url(yy, jj)
            processor.set_current_lfn(yy, jj)
                
            #get mxd03 listing file
            download_complete = processor.get_listing_file('meta', 'mxd03')
            
            #continue with next date in case no file can be found or 
            #it already exists
            if not download_complete:
                ##TODO
                #log failures!
                continue
            
            #process listing
            lst = processor.process_mxd03_listing_file()
            
            """ Compile list of matched MXD02 swaths """
            #get the mxd02 listign file
            download_complete = processor.get_listing_file('mxd02', 'mxd02')

            #continue with next date in case no file can be found or 
            #it already exists
            if not download_complete:
                ##TODO
                #log failures!
                continue
            
            #further process listing; adding MXD02 file names by matching
            lst = processor.process_mxd02_listing_file(lst)
            
            #add to listing data
            listing.add_to_listing(lst)
            
            #TODO rethink the process of how to handle already exisiting files
            # as well ass the download failures... store csv's per day via 
            # iotools ListingIO() with specific Modis Version by yy/jj if it 
            # already exists and in that case load it and add it to the listing
            # otherwise dow the normal download stuff and processign to add it 
            # to the listing AND REMOVE THE TXT FILES afterwards?!?
            
        
    def skip_existing_files(self):
        #handling only the completely processed h5 data!!!!
        #TODO likely move this also to the ABC and rework the way the listign 
        # is stored to something like pandas or polars instead of lists?
        # [date_tag, url (mxd02/mxd03), aoi, covfrac]
        # and store as csv?
        pass

# In[]

class SlstrListing(Listing):
    """
    Sentinel3-A/B SLSTR listing process child class tailored to the 
    sensor-specific processing
    """
            
    def compile_file_listing(self):
        pass
        
    
    def skip_existing_files(self):
        pass           
 
    

# In[]

class OlciListing(Listing):
    """
    Sentinel3-A/B OLCI listing process child class tailored to the 
    sensor-specific processing
    """
            
    def compile_file_listing(self):
        pass


    def skip_existing_files(self):
        pass           
 