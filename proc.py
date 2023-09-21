# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[] 
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from loguru import logger

from iotools import ListingIO
from data import ListingData
from data import SwathData
from resampling import ResampleHandler

import pandas as pd
import numpy as np

import requests
import os
import sys
import zipfile


# In[] 


class ListingProcessor(object):
    """
    Listing processor class handling the actual file listing for all sensors 
    with high-level API's for the Listing class
    """
    def __init__(self, config: object, process: object, retrieval: object):
        #pass configuration
        self.cfg = config
        
        #pass listing process/retrieval handler classes
        self.listing = retrieval(self)
        self.process = process(self)
       
        #initialize base/sensor-specific modules
        self.initialize_base_modules()
        
    """ Initializations """
    def initialize_base_modules(self) -> None:
        #basics
        self._set_carrier()
        self._set_token()
        self._set_url()
        self._set_output_path()
        #modules
        self._set_aoi_handler()
        self._set_listing_data()
        self._set_listing_io()
        self._set_error_handler()
        
    """ Internal Getters/Setters for Processor Setup """        
    def _set_carrier(self) -> None:
        self.carrier = self.cfg.carrier
        
    def _set_token(self) -> None:
        self.token = self.cfg.token
        
    def _set_url(self) -> None:
        #initiate meta data handler
        META = self.cfg.get_meta_module()
        META.set_carrier(self.carrier)
        #set url's
        self.url = META.get_urls()  
        
    def _set_output_path(self) -> None:
        #output directory
        LISTING_FOLDER = 'listing'
        
        #create listing directory if necessary
        OUTPATH = os.path.join(self.cfg.output_path, LISTING_FOLDER)
        if not os.path.isdir(OUTPATH):
            os.makedirs(OUTPATH) 
        #status
        logger.info(f'Set listing output directory: {OUTPATH}')
        self.lstout = OUTPATH

    def _set_aoi_handler(self) -> None:
        #initiate aoi_handler
        self.aoi = self.cfg.aoi_data
        
    def _set_listing_data(self) -> None:
        #initiate listing data container
        self.data = ListingData()
        
    def _set_listing_io(self) -> None:
        #initiate i/o handler
        self.io = ListingIO(self.lstout)
        
    def _set_error_handler(self) -> None:
        #initiate download error handler
        self.error = DownloadErrorHandler()

    """ High-level API's """
    def get_dates(self) -> list:
        """
        API function that handles the splitting-up of the datetime objects 
        into single strings independent of the used carrier/sensor
        """
        #get start/stop dates
        START = self.cfg.start_date
        STOP = self.cfg.stop_date
        # generate year (yy) and day-of-year (jj) strings for covered range
        DATES = [START + timedelta(days = day_diff) \
                 for day_diff in range(0, (STOP - START).days+1)]
        
        DATES_LIST = [(DATE.strftime('%Y'), DATE.strftime('%j'))
                      for DATE in DATES]
        #return to caller
        return DATES_LIST
    
    def set_current_url(self, yy: str, jj: str) -> None:
        """
        Parameters
        ----------
        yy : str
            Current year in yyyy format
        jj : str
            Current julian day of year

        Returns
        -------
        None
            API function to store the current url's for the listing file 
            retrievals after compiling the geoMeta file name 
            (with current date) as well as the year and julian day of year 
            extensions to the base url's'
        """
        self.process.set_current_url(yy, jj)

    def set_current_lfn(self, yy: str, jj: str) -> None:
        """
        Parameters
        ----------
        yy : str
            Current year in yyyy format
        jj : str
            Current julian day of year

        Returns
        -------
        None
            API function to store the current listing file name (lfn) with 
            the format:
            "[CARRIER]_[SENSOR]_listing_[yyyy]_[jj].csv"
        """
        self.process.set_current_lfn(yy, jj)
        
    def check_for_override_listing(self) -> bool:
        """
        Returns
        -------
        bool
            API function returning the status of overriding existing 
            listing files 
        """
        return self.cfg.override_listing
        
    
    def check_for_existing_listing(self) -> bool:
        """
        Returns
        -------
        bool
            API function returning the status of whether a current listing 
            file does already exist or not
        """
        #compile path to potential listing
        path = os.path.join(self.lstout, self.process.get_current_lfn())
        #check for existance and return to caller
        return os.path.isfile(path)
    
    def load_listing(self) -> None:
        """
        API function dealing with the loading of an existing listing file 
        into the data container
        """
        self.process.load_listing()
    
    def save_listing(self) -> None:
        """
        API function dealing with the saving of an retrieved loading into 
        the data container
        """
        self.process.save_listing()
    
    def get_listing(self) -> pd.DataFrame:
        """
        API function dealing with the returning of a finished file listing
        from the data container
        """
        return self.process.get_listing()
        
    def get_geometa_file(self) -> bool:
        """
        Returns
        -------
        bool
            API function to download/retrieve the respective geoMeta file
            from LAADS; returning the status on it's success calling the 
            respective ListingHandler() class
        """
        return self.listing.get_geometa_file()

    def process_geometa_file(self) -> None:
        """
        API function to process the retrieved respective geoMeta file from 
        LAADS, identify AOI overlaps, and compile retrieval URL's to be used 
        by the Retrieval() classes
        """
        self.listing.process_geometa_file()
    

# In[]
# In[]
# In[]


""" Process Handling """
class ListingProcessHandler(ABC):
    def __init__(self, host_class: object):
        #keep instance of the host class to use this as nestes class
        self.ref = host_class
    
    @abstractmethod
    def set_current_url(self, yy: str, jj: str) -> None:
        #compile day and month of current date
        dd, mm = self._compile_ddmm_from_yyjj(yy, jj)
        #compile geometa filename
        GEOMETA = self._compile_geometa_filename(yy, mm, dd)
        #compile url's
        meta_url = f'{self.ref.url["meta"]}{yy}/{GEOMETA}'
        data_url = f'{self.ref.url["data"]}{yy}/{jj}/' 
        #set current urls
        self.current_url = {'meta': meta_url,
                            'data': data_url,
                            }
    
    def _compile_ddmm_from_yyjj(self, yy: str, jj: str) -> tuple:
        d = datetime.strptime(f'{yy}-{jj}', "%Y-%j")
        dd = d.strftime('%d')
        mm = d.strftime('%m')
        return dd, mm
    
    @abstractmethod
    def _compile_geometa_filename(self, yy: str, mm: str, dd: str) -> str:
        """
        Parameters
        ----------
        yy : str
            Current year in yyyy format
        mm : str
            Current month in mm format
        dd : str
            Current day in dd format

        Returns
        -------
        str
            Compiles and returns the current GeoMeta filename in its sensor 
            specific format (https://ladsweb.modaps.eosdis.nasa.gov/archive/)
        """
        pass
            
    def set_current_lfn(self, yy: str, jj: str) -> None:
        CARRIER = self.ref.cfg.carrier.lower()
        SENSOR = self.ref.cfg.sensor.lower()
        self.current_lfn = f'{CARRIER}_{SENSOR}_listing_{yy}_{jj}.csv'
    
    def get_current_url(self, url_type: str) -> str:
        return self.current_url[url_type]

    def get_current_lfn(self) -> str:
        return self.current_lfn
    
    def save_listing(self) -> None:
        N = self.ref.data.get_number_of_entries()
        if N > 0:
            LISTING_FILE = self.get_current_lfn()
            self.ref.io.set_listing_file_name(LISTING_FILE)
            LISTING_DATA = self.ref.data.get_listing()
            self.ref.io.to_csv(LISTING_DATA)
        else:
            logger.critical(f'No listing available/retrievable!')
            sys.exit()       
        
    def load_listing(self) -> None:
        LISTING_FILE = self.get_current_lfn()
        self.ref.io.set_listing_file_name(LISTING_FILE)
        LOADED_LISTING_FILE = self.ref.io.from_csv()
        self.ref.data.add_to_listing(LOADED_LISTING_FILE)
        
    def get_listing(self) -> pd.DataFrame:
        return self.ref.data.get_listing()
    
    
class ModisListingProcessHandler(ListingProcessHandler):
    def set_current_url(self, yy: str, jj: str) -> None:
        #compile day and month of current date
        dd, mm = self._compile_ddmm_from_yyjj(yy, jj)
        #build url using the GeoMeta part and file name
        GEOMETA = self._compile_geometa_filename(yy, mm, dd)
        meta_url = f'{self.ref.url["meta"]}{yy}/{GEOMETA}'
        mxd3_url = f'{self.ref.url["mxd03"]}{yy}/{jj}/' 
        mxd2_url = f'{self.ref.url["mxd02"]}{yy}/{jj}/' 
        
        #set current urls
        self.current_url = {'meta': meta_url,
                            'mxd03': mxd3_url,
                            'mxd02': mxd2_url
                            }
    def _compile_geometa_filename(self, yy: str, mm: str, dd: str) -> str:
        #initiate meta data handler
        META = self.ref.cfg.get_meta_module()
        META.set_carrier(self.ref.carrier)
        #get prefix
        PREFIX = META.get_data_prefix().upper()
        return f'{PREFIX}03_{yy}-{mm}-{dd}.txt'


class SlstrListingProcessHandler(ListingProcessHandler):
    def set_current_url(self, yy: str, jj: str) -> None:
        super().set_current_url(yy, jj)

    def _compile_geometa_filename(self, yy: str, mm: str, dd: str) -> str:
        CARRIER = self.ref.carrier.upper()
        return f'{CARRIER}_SL_1_RBT_{yy}-{mm}-{dd}.txt'
    
    
class OlciListingProcessHandler(ListingProcessHandler):
    def set_current_url(self, yy: str, jj: str) -> None:
        super().set_current_url(yy, jj)

    def _compile_geometa_filename(self, yy: str, mm: str, dd: str) -> str:
        CARRIER = self.ref.carrier.upper()
        return f'{CARRIER}_OL_1_EFR_{yy}-{mm}-{dd}.txt'
    
       
""" Listing Retrieval """
class ListingRetrievalHandler(ABC):
    def __init__(self, host_class: object):
        #keep instance of the host class to use this as nestes class
        self.ref = host_class
        #allocate dict for temporary listing
        self.temporary_listing = {}
    
    @abstractmethod
    def get_geometa_file(self) -> bool:
        #set current url type to use
        self._set_current_url_type('meta')
        #call retrieval function and return its status
        return self.get_listing_file()
    
    def _set_current_url_type(self, url_type) -> None:
        self.current_url_type = url_type
        
    def _get_current_url_type(self) -> str:
        return self.current_url_type
    
    def get_listing_file(self) -> bool:
        """
        Returns
        -------
        bool
            True/False on the retrieval process completion.
        """
        URL_TYPE = self._get_current_url_type()
        #get url to download from
        URL = self.ref.process.get_current_url(URL_TYPE)
        
        #call download function
        REQUEST_OBJ = self.download_listing(URL)
        
        if REQUEST_OBJ.status_code == 200:
            LISTING = self._parse_byte_listing(REQUEST_OBJ.content)
            self.temporary_listing[URL_TYPE] = LISTING
            #status
            logger.info(f'Retrieval complete!')
            #reset counter
            self.ref.error.reset_crit_counter()
            return True
        else:
            #status
            logger.info(f'Retrieval incomplete!')
            logger.error(f'Error with file listing retrieval!')
            #increase coutner
            self.ref.error.increase_crit_counter()
            return False
        
    def download_listing(self, url: str) -> object:
        """
        Parameters
        ----------
        url : str
            sensor/carrier specific download url

        Returns
        -------
        Bool :
            download successful? True/False
        """     
        #status
        logger.info(f'Retrieving listing file...')
        
        #requests call
        headers = {'Authorization': "Bearer {}".format(self.ref.token)}
        r = requests.get(url, headers=headers)

        #return request object
        return r
        
    def _parse_byte_listing(self, byte_listing) -> list:
        return byte_listing.decode('UTF-8').split('\n')
    
    @abstractmethod
    def process_geometa_file(self) -> None:
        """
        Processes the downloaded geoMeta file from LAADS by identifying 
        matches with the predefined AOI's and neatly compiling all necessary
        information for the retrieval process
        """
        #parse listing
        self._parse_geometa_listing()
        
        #allocate dataframe for listing storage
        df = pd.DataFrame()
        #loop over entries
        for lst_entry in self.geometa:
            #check for overlap with specified aoi's
            aois, frac = self._validate_aoi_overlap(lst_entry)
            
            #process in case of overlap
            if len(aois) > 0:
                #get swath name from listing entry
                SWATH = lst_entry[0]
                #compile lists of aois/overlap fractions
                AOI = [aoi for aoi in aois]
                FRC = [frc for frc in frac]
                #compose dict
                d = {'url': self.ref.process.get_current_url('data'),
                     'file': SWATH,
                     'aoi': AOI,
                     'frac': FRC,
                     }
                #append to df
                df = pd.concat([df, pd.DataFrame(d)])
        #update listing
        df = df.reset_index().drop('index', axis=1)
        #store in data container
        self.ref.data.add_to_listing(df)
    
    def _parse_geometa_listing(self) -> None:         
        #listing file consists of various meta information w/ 
        #line[0]:=file_name; line[9:12]:=RingLON; 
        #line[13:16]:=RingLAT       
        lst = [[s[0], s[9], s[10], s[11], s[12], s[13], s[14], s[15], s[16]]
               for s in [e.split(',') 
                         for e in self.temporary_listing['meta'][3:-1]]]
        
        #update temporary listing
        self.geometa = lst
        
    def _validate_aoi_overlap(self, lst_entry: list) -> (list, list):
        #check for overlap to exclude non-matching links from listing
        AOIS = []
        FRAC = []
        
        #get aois to process
        aois_to_check = self.ref.aoi.get_aois()
        
        #check for overlap of bounding box with predefined aoi polygons
        for aoi in aois_to_check:    
            #check for overlap
            grid = self.ref.aoi.get_aoi(aoi)
            overlap, frac = grid.check_overlap_with_aoi(lst_entry)
            if overlap and frac >= 5.0:
                AOIS.append(aoi)
                FRAC.append(frac)
        
        #return
        return AOIS, FRAC


class SlstrListingRetrievalHandler(ListingRetrievalHandler):
    def get_geometa_file(self) -> bool:
        return super().get_geometa_file()
    
    def process_geometa_file(self) -> None:
        super().process_geometa_file()


class OlciListingRetrievalHandler(ListingRetrievalHandler):
    def get_geometa_file(self) -> bool:
        return super().get_geometa_file()
    
    def process_geometa_file(self) -> None:
        super().process_geometa_file()
        

class ModisListingRetrievalHandler(ListingRetrievalHandler):
    def get_geometa_file(self) -> bool:
        """
        Due to the duality of needed data/sources for MODIS, within the 
        geoMeta API, two retrievals are initiated to also lok-up all 
        available MXD021KM files on LAADS for the given day to match
        with the geoMeta files
        """
        #set current url type to use
        self._set_current_url_type('meta')
        #call retrieval function and return its status
        STATUS_GEOMETA = self.get_listing_file()
        #set current url type to use
        self._set_current_url_type('mxd02')
        #call retrieval function and return its status
        STATUS_MXD02 = self.get_listing_file()
        #return status
        return (STATUS_GEOMETA, STATUS_MXD02)
    
    def process_geometa_file(self) -> None:
        """
        Sensor-specific adaptation of the base function, primarily allowing 
        for a better matchmaking with the MXD021KM files through the use of 
        datetags stored in the listing dataframe
        """
        #parse listing
        self._parse_geometa_listing()
        
        #allocate dataframe for listing storage
        df = pd.DataFrame()
        
        #loop over entries
        for lst_entry in self.geometa:
            #check for overlap with specified aoi's
            aois, frac = self._validate_aoi_overlap(lst_entry)
            
            #process in case of overlap
            if len(aois) > 0:
                #compile filename and date tag
                HDF_FILE = lst_entry[0]
                HDF_PARTS = HDF_FILE.split('.')
                HDF_TAG = '.'.join(HDF_PARTS[1:4])
                #compile lists of aois/overlap fractions
                AOI = [aoi for aoi in aois]
                FRC = [frc for frc in frac]
                #get url's
                URLS = self.ref.process.get_current_url('mxd03')
                #compose dict
                d = {'tag': HDF_TAG,
                     'url_mxd03': URLS,
                     'mxd03': HDF_FILE,
                     'aoi': AOI,
                     'frac': FRC,
                     }
                #append to df
                df = pd.concat([df, pd.DataFrame(d)])
            
        #update listing
        df_geometa = df.reset_index().drop('index', axis=1)
        
        #parse mxd02 listing
        self._parse_mxd02_listing()
        
        #create search tags for mxd02
        TAGS = ['.'.join(lst_entry.split('.')[1:4])
                for lst_entry in self.mxd02]
        
        #get url's
        URLS = self.ref.process.get_current_url('mxd02')
        
        #compile df
        df_mxd02 = pd.DataFrame({'tag': TAGS,
                                 'url_mxd02': URLS,
                                 'mxd02': self.mxd02})
        
        #join them on tags and rearrange them
        df = df_geometa.join(df_mxd02.set_index('tag'), on='tag')
        df.drop('tag', axis=1, inplace=True)
        df = df[['url_mxd03', 'mxd03', 'url_mxd02', 'mxd02', 'aoi', 'frac']]
        #store in data container
        self.ref.data.add_to_listing(df)

    def _parse_mxd02_listing(self) -> None:
        #create list of file links 
        SEARCH_STR = '<a class="btn btn-default" href="/archive/allData/'
        LST = [line.split('"')[3].split('/')[-1] \
               for line in self.temporary_listing['mxd02'] 
               if SEARCH_STR in line]
        #update temporary listing
        self.mxd02 = LST



# In[]
# In[]
# In[]



"""
Processing::Swath Download
"""

class RetrievalProcessor(object):
    """
    Retrieval processor class handling the actual file/swath retrieval for all 
    sensors with high-level API's for the Retrieval class
    """
    def __init__(self, config: object, swath: object, retrieval: object):
        #pass configuration
        self.cfg = config
        
        #pass listing/retrieval handler classes
        self.swath = swath(self)
        self.retrieval = retrieval(self)
        
        #initialize base/sensor-specific modules
        self.initialize_base_modules()
        
        #initialize resampling if necessary
        APPLY_RESAMPLING = self.cfg.apply_resampling
        if APPLY_RESAMPLING:
            self.initialize_resample_module()
        
        #global variables
        self.overlapping_aois = None
        
    """ Initializations """
    def initialize_base_modules(self) -> None:
        #basics
        self._set_carrier()
        self._set_token()
        self._set_output_path()
        #modules
        self._set_aoi_handler()
        self._set_swath_meta()
        self._set_swath_data()
        self._set_swath_io()
        self._set_error_handler()
        self._set_zip_handler()
        
    def initialize_resample_module(self) -> None:
        self.resampling = ResampleHandler(self)
        
    """ Internal Getters/Setters for Processor Setup """        
    def _set_carrier(self) -> None:
        self.carrier = self.cfg.carrier
        
    def _set_token(self) -> None:
        self.token = self.cfg.token
        
    def _set_output_path(self) -> None:
        #general output path
        OUTPATH = self.cfg.output_path
        #raw output directory
        RAW_FOLDER = 'tmp'
        #status
        logger.info(f'Set processed output directory: {OUTPATH}')
        #set output directory
        self.out = OUTPATH
        
        #create raw output temporary directory if necessary
        RAWPATH = os.path.join(OUTPATH, RAW_FOLDER)
        if not os.path.isdir(RAWPATH):
            os.makedirs(RAWPATH)   
        #status
        logger.info(f'Set temporary output directory: {RAWPATH}')
        self.rawout = RAWPATH

    def _set_aoi_handler(self) -> None:
        #initiate aoi_handler
        self.aoi = self.cfg.aoi_data
        
    def _set_swath_meta(self) -> None:
        #initiate meta data handler
        self.meta = self.cfg.get_meta_module()
        self.meta.set_carrier(self.carrier)      
        
    def _set_swath_data(self) -> None:
        #initiate swath data container
        self.data = SwathData()
        
    def _set_swath_io(self) -> None:
        #initiate i/o handler
        self.io = self.cfg.setup_swath_io()
        # SENSOR = self.cfg.sensor.capitalize()
        # IO_CLASS = self.cfg.get_class('iotools', f'{SENSOR}SwathIO') 
        # self.io = IO_CLASS(self.out)
        
    def _set_error_handler(self) -> None:
        #initiate download error handler
        self.error = DownloadErrorHandler()
        
    def _set_zip_handler(self) -> None:
        self.zip = ZipFileHandler(self.rawout)
        
    """ High-level API's """
    def set_swath_id(self, entry: pd.Series) -> None:
        """
        Parameters
        ----------
        entry : pd.Series
            Swath name or id to be used for identification and processing

        Returns
        -------
        None
            API function to be called by the Retrieval() Class to set the 
            swath id for later usage.
            Intention is to either implement this base version by a call 
            to super().set_swath_id() or to override it in the sensor-specific 
            RetrievalProcessor() child class
        """
        self.swath.set_swath_id(entry)
     
    def get_swath_id(self, swath_only: bool) -> tuple:
        """
        Parameters
        ----------
        swath_only : bool
            Should only the swath name be returned (True) or the 
            full path (False)

        Returns
        -------
        None
            API function to be called by the Retrieval() Class to get the 
            swath id. 
            Intention is to either implement this base version by a call 
            to super().get_swath_id() or to override it in the sensor-specific 
            RetrievalProcessor() child class
        """
        SWATH = self.swath.get_swath_id(swath_only)
        return SWATH
      
    def parse_swath_listing(self, df: pd.DataFrame) -> None:
        """
        Parameters
        ----------
        df : pd.DataFrame
            The swath listing as provided by the Listing() class or its 
            sensor-specific child class version as pandas DataFrame with 
            potentially multiple entries for swaths that are supposed 
            to be resampled to several different AOI's

        Returns
        -------
        None
            API function to be called by the Retrieval() Class to parse and 
            store the reduced listing without double entries per swath file
        """
        #receive raw listing from Listing class, parse, and store it
        self.raw_listing = df
        self.listing = self.retrieval.parse_swath_listing(df)
        
    def check_for_existing_swaths(self) -> None:
        """
        Returns
        -------
        None
            API function to be called by the Retrieval() Class to check for
            already existing (fully processed .h5) swaths within the parsed 
            listing and remove these from the upcoming retrieval process
        """
        #check parsed listing for existing (fully processed) swaths and store
        #potentially reduced listing
        self.listing = self.retrieval.check_for_existing_swaths(self.listing)
        
    def get_listing(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame
            API function to return the cleared-up final listing for the 
            retrieval process as pandas DataFrame
        """
        return self.listing
    
    def get_swath_file(self) -> bool:
        """
        Returns
        -------
        bool
            API function to return the status of the swath retrieval process
        """
        STATUS = self.retrieval.get_swath_file()
        return STATUS

    def load_swath(self) -> None:
        """
        API function to handle the swath loading by getting all available 
        variables from the meta file, looping over them, opening the 
        corresponding files and storing the data within the data container
        """    
        #loop through all downloaded variables and import corresponding 
        #files/data
        VARIABLES_TO_PROCESS = self.meta.get_input_variables()
        
        for VAR in VARIABLES_TO_PROCESS:
            #open file link
            self.swath.open_swath(VAR)
            
            #retrieve content from group/variable
            self.swath.load_variable(VAR)
            
            #close file handle
            self.swath.close_swath()
            
    def save_swath(self) -> None:
        """
        API function to handle the swath saving to h5 format by separating 
        between resampling and non-resampling mode and putting variable by 
        variable into the new file using the respective SwathHandler class
        """
        #creating the h5 output file with base global attributes 
        VARIABLES_TO_PROCESS = self.meta.get_output_variables()
        
        #decide on resampling or not
        RESAMPLING_APPLIED = self.cfg.apply_resampling
        if RESAMPLING_APPLIED:
            for aoi in self.overlapping_aois:
                self.swath.create_swath(aoi)
                #set variables into the new file
                for VAR in VARIABLES_TO_PROCESS:
                    self.swath.set_variable(VAR, aoi)
        else:
            self.swath.create_swath()
            #set variables into the new file
            for VAR in VARIABLES_TO_PROCESS:
                self.swath.set_variable(VAR)

        #close file connection
        self.swath.close_swath()
        
    def cleanup(self) -> None:
        """
        API function to handle the clean-up of all downloaded files after 
        processing to h5
        """
        self.swath.cleanup()
        
    def identify_resample_aois(self) -> None:
        """
        API function to identify the current swath-specific AOI's that it 
        covers and should be resampled to using the 'raw' listing, i.e., the
        one with potentially multiple entries per swath as it may cover 
        several AOI's. Stores an list of AOI's.
        """    
        self.swath.identify_resample_aois()
        
    def resample_swath(self) -> None:
        """
        API function to handle the resample call by checking the meta data, 
        grouping the data, and sending to grouped data to the ResampleHandler 
        class for resampling with the data to be stored in the data container
        """
        #loop through all variables in the data and send it to the 
        #resample procedure
        VARIABLES_TO_PROCESS = self.meta.get_resample_variables()

        for VAR in VARIABLES_TO_PROCESS: 
            #get resample information from meta data
            LON, LAT = self.meta.get_var_grid_specs(VAR)
            #regroup/shuffle the data by their used lon/lat information
            self.resampling.add_data_to_group(VAR, LON, LAT)

        #stack the groups if necessary
        self.resampling.add_groups_to_resample_stack()
        #resample the data
        for aoi in self.overlapping_aois:
            #status
            logger.info(f'Resampling to grid: {aoi}')
            self.resampling.resample(aoi)
        #add to data container
        RESAMPLED_DATA = self.resampling.get_resampled_data()
        self.data.add_to_resampled_data(RESAMPLED_DATA)
        


# In[]
# In[]
# In[]

""" Download Error's """
class DownloadErrorHandler(object):
    """
    Convenience class to handle the download error management for swath and 
    listing retrieval to reduce boilerplate code
    """
    def __init__(self, crit: int = 5):
        #counters for download failures
        self.critical_download_failures = crit
        self.current_download_failures = 0  
        
    def increase_crit_counter(self) -> None:
        self.current_download_failures += 1
        if self.current_download_failures == self.critical_download_failures:
            msg = f'Critical value ({self.critical_download_failures}) of '\
                f'download failures reached!'
            logger.critical(msg)
            sys.exit()
            
    def reset_crit_counter(self) -> None:
        self.current_download_failures = 0

 
""" Zip File Handling """       
class ZipFileHandler(object):
    """
    Convenience class to handle the management of swath's downloaded as ZIP
    files to reduce boilerplate code
    """
    def __init__(self, outpath: str):
        self.outpath = outpath
    
    def set_zip_path(self, swath: str) -> None:
        #save zip file location and output path
        self.zippath = os.path.join(self.outpath, swath)
        
    def get_zip_path(self) -> str:
        return self.zippath
        
    def load_zip_file(self) -> None:
        #open zipfile and stores content pathing/folder structure
        ZIPPATH = self.get_zip_path()
        self.zipfile = zipfile.ZipFile(ZIPPATH, 'r')
        ZIPLIST = self.zipfile.namelist()
        self.ziplist = [os.path.join(z[0],z[1]) 
                        for z in [f.split('/') for f in ZIPLIST[1:]]]
        self.zipdir = ZIPLIST[0]
        
    def extract_zip_file(self) -> None:
        #extract file content and store folder location w/ zip folder
        self.zipfile.extractall(self.outpath)
        self.extpath = os.path.join(self.outpath, self.zipdir)
        
    def get_unzip_path(self) -> str:
        return self.extpath
        
    def close_zip_file(self) -> None:
        self.zipfile.close()
        
    def remove_extracted_content(self) -> None:
        logger.info(f'Removing extracted zip-file content...')
        RMLIST = [os.path.join(self.outpath, f) for f in self.ziplist]
        for f in RMLIST:
            os.remove(f)
        logger.info(f'Removing extracted zip-file folder...')
        os.rmdir(self.extpath)

   
""" Swath Handling """
class SwathHandler(ABC):
    def __init__(self, host_class: object):
        #keep instance of the host class to use this as nestes class
        self.ref = host_class
     
    @abstractmethod
    def set_swath_id(self, entry: pd.Series) -> None:
        SWATH = entry[0]
        self.ref.data.set_swath_id(SWATH)

    @abstractmethod
    def get_swath_id(self, swath_only: bool) -> str:
        SWATH = self.ref.data.get_swath_id()
        if swath_only:
            SWATH = SWATH.split('/')[-1]
        return SWATH
    
    @abstractmethod
    def open_swath(self, var: str) -> None:
        SPECS = self.ref.meta.get_var_input_specs(var)
        FILENAME = SPECS['file']
        FILEPATH = os.path.join(self.ref.rawout, FILENAME)
        self.ref.io.open_input_swath(FILEPATH)
        
    @abstractmethod
    def load_variable(self, var: str) -> None:
        SPECS = self.ref.meta.get_var_input_specs(var)
        #retrieve the actual variable data from the swath
        self.ref.io.get_variable(**SPECS)
        #check for available channel specs to be applied
        CHSPECS_KEYS = self.ref.meta.get_chspecs_variables()
        if var in CHSPECS_KEYS:
            #retrieve corresponding channel specifications
            CHSPECS = self.ref.meta.get_var_channel_specs(var)
            #apply
            DATA = self.ref.io.process_variable(**CHSPECS)
        else:
            DATA = self.ref.io.process_variable()
        #store it to the data container using the same variable key handle
        self.ref.data.add_to_data(var, DATA)
     
    def close_swath(self) -> None:
        self.ref.io.close() 
        
    def create_swath(self, aoi: str = None) -> None:
        FILENAME = self._compile_output_swath_name(aoi)
        FILEPATH = os.path.join(self.ref.out, FILENAME)
        #status
        logger.info(f'Saving to file: {FILENAME}')
        self.ref.io.save(FILEPATH)

    def set_variable(self, var: str, aoi: str = None) -> None:
        #get variable specific output specifications
        GRP, VAR, ATTR = self.ref.meta.get_var_output_specs(var)
        #compile in-file specific group/variable path
        INPATH = f'{GRP}/{VAR}'
        #pick dataset
        if aoi is None:
            DS = self.ref.data.get_data(var)
        else:
            DS = self.ref.data.get_resampled_data(aoi, var)
        #pass data to io
        self.ref.io.set_var(INPATH, DS, ATTR)    
        
    def _compile_output_swath_name(self, aoi: str = None) -> str:
        #correct processing state extension
        if aoi is None:
            EXT = 'raw'
        else:
            EXT = aoi
        pass
        #set file-name parts
        DATE = self._get_date_from_swath_file()
        CARRIER = self.ref.cfg.carrier.lower()[0:3]
        SENSOR = self.ref.cfg.sensor.lower()
        VERSION = self.ref.cfg.version.lower()
        #compile and return
        return f'{CARRIER}_{SENSOR}_{DATE}_{VERSION}_{EXT}.h5'
    
    @abstractmethod
    def _get_date_from_swath_file(self) -> str:
        pass
    
    @abstractmethod
    def cleanup(self) -> None:
        #get swath id
        SWATH = self.get_swath_id(swath_only=True)
        #remove swath
        logger.info(f'Removing downloaded file: {SWATH}')
        SUCCESS = self._remove_swath(SWATH)
        if SUCCESS:
            logger.info(f'Removal successful!')
        else:
            logger.error(f'Removal of {SWATH} failed!')
        #clear data container
        self._cleanup_data_container()
        
    def _remove_swath(self, swath: str) -> bool:
        FILENAME = swath
        FILEPATH = os.path.join(self.ref.rawout, FILENAME)
        try:
            self.ref.io.cleanup(FILEPATH)
            return True
        except:
            return False
        
    def _cleanup_data_container(self) -> None:
        self.ref.data.cleanup()      
            
    @abstractmethod
    def identify_resample_aois(self) -> None:
        SWATH = self.get_swath_id(swath_only=True)
        LISTING = self.ref.raw_listing
        AOI_LIST = LISTING['aoi'].loc[LISTING['file']==SWATH].tolist()
        self.ref.overlapping_aois = AOI_LIST
        

class SlstrSwathHandler(SwathHandler):
    def set_swath_id(self, entry: pd.Series) -> None:
        super().set_swath_id(entry)
        
    def get_swath_id(self, swath_only: bool) -> str:
        return super().get_swath_id(swath_only)
    
    def open_swath(self, var: str) -> None:
        FILENAME = self.ref.meta.get_var_input_specs(var)[0]
        UNZIPPATH = self.ref.zip.get_unzip_path()
        FILEPATH = os.path.join(UNZIPPATH, FILENAME)
        self.ref.io.load(FILEPATH)
        
    def load_variable(self, var: str) -> None:
        super().load_variable(var)
        
    def _get_date_from_swath_file(self) -> str:
        #get swath id
        SWATH = self.get_swath_id(swath_only=True)
        #take raw date from swath file name and convert it to datetime object
        raw_date = SWATH.split('_')[7]
        raw_date = datetime.strptime(raw_date,'%Y%m%dT%H%M%S')
        #transform it
        yyjj = raw_date.strftime('%Y%j')
        hhmm = raw_date.strftime('%H%M%S')
        #return
        return f'{yyjj}_{hhmm}'
    
    def cleanup(self) -> None:
        #remove extracted zipfile content
        self.ref.zip.remove_extracted_content()
        #remove the zip file that was downloaded
        super().cleanup()
        
    def identify_resample_aois(self) -> None:
        super().identify_resample_aois()


class OlciSwathHandler(SwathHandler):
    def set_swath_id(self, entry: pd.Series) -> None:
        super().set_swath_id(entry)
        
    def get_swath_id(self, swath_only: bool) -> str:
        return super().get_swath_id(swath_only)
    
    def open_swath(self, var: str) -> None:
        FILENAME = self.ref.meta.get_var_input_specs(var)[0]
        UNZIPPATH = self.ref.zip.get_unzip_path()
        FILEPATH = os.path.join(UNZIPPATH, FILENAME)
        self.ref.io.load(FILEPATH)
        
    def load_variable(self, var: str) -> None:
        super().load_variable(var)
        
    def _get_date_from_swath_file(self) -> str:
        #get swath id
        SWATH = self.get_swath_id(swath_only=True)
        #take raw date from swath file name and convert it to datetime object
        raw_date = SWATH.split('_')[7]
        raw_date = datetime.strptime(raw_date,'%Y%m%dT%H%M%S')
        #transform it
        yyjj = raw_date.strftime('%Y%j')
        hhmm = raw_date.strftime('%H%M%S')
        #return
        return f'{yyjj}_{hhmm}'
    
    def cleanup(self) -> None:
        #remove extracted zipfile content
        self.ref.zip.remove_extracted_content()
        #remove the zip file that was downloaded
        super().cleanup()
        
    def identify_resample_aois(self) -> None:
        super().identify_resample_aois()
        
        
class ModisSwathHandler(SwathHandler):
    def set_swath_id(self, entry: pd.Series) -> None:
        SWATHS = {'mxd03': entry[0],'mxd02': entry[1]}
        self.ref.data.set_swath_id(SWATHS)
    
    def get_swath_id(self, swath_only: bool) -> tuple:
        URLS = self.ref.data.get_swath_id()
        if swath_only:
            SWATHS = {}
            SWATHS['mxd03'] = URLS['mxd03'].split('/')[-1]
            SWATHS['mxd02'] = URLS['mxd02'].split('/')[-1]
            return SWATHS
        return URLS
    
    def open_swath(self, var: str) -> None:
        #update meta info on variables for MODIS
        self._update_meta_info()
        #call base class function
        super().open_swath(var)
        
    def load_variable(self, var: str) -> None:
        super().load_variable(var)
        
    def _update_meta_info(self) -> None:
        """
        Returns
        -------
        None
            Updates the MODIS specific meta data information on the to be 
            used files (mxd03 or mxd02) for the variable processing
        """
        SWATHS = self.get_swath_id(swath_only=True)
        MXD03 = SWATHS['mxd03']
        MXD02 = SWATHS['mxd02']
        self.ref.meta.update_input_specs((MXD03, MXD02))
        
    def _get_date_from_swath_file(self) -> str:
        #get swath id
        SWATH = self.get_swath_id(swath_only=True)['mxd02']
        #take raw date from swath id and convert it to datetime object
        raw_yyjj = SWATH.split('.')[1]
        raw_hhmm = SWATH.split('.')[2]
        raw_date = datetime.strptime(raw_yyjj+raw_hhmm,'A%Y%j%H%M')
        #transform it
        yyjj = raw_date.strftime('%Y%j')
        hhmm = raw_date.strftime('%H%M%S')
        #return
        return f'{yyjj}_{hhmm}'
    
    def cleanup(self) -> None:
        #get swath id
        SWATHS = self.get_swath_id(swath_only=True)
        #remove swaths
        MXD03 = SWATHS['mxd03']
        logger.info(f'Removing downloaded file: {MXD03}')
        self._remove_swath(MXD03)
        MXD02 = SWATHS['mxd02']
        logger.info(f'Removing downloaded file: {MXD02}')
        self._remove_swath(MXD02)
        #clear data container
        self._cleanup_data_container()
        
    def identify_resample_aois(self) -> None:
        SWATH = self.get_swath_id(swath_only=True)['mxd03']
        LISTING = self.ref.raw_listing
        AOI_LIST = LISTING['aoi'].loc[LISTING['mxd03']==SWATH].tolist()
        self.ref.overlapping_aois = AOI_LIST
    

""" Retrieval procedure """
class RetrievalHandler(ABC):
    def __init__(self, host_class: object):
        #keep instance of the host class to use this as nestes class
        self.ref = host_class
    
    @abstractmethod
    def parse_swath_listing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parameters
        ----------
        df : pd.DataFrame
            The raw listing dataframe with potentially multiple entries for 
            swaths that are supposed to be resampled to several different 
            AOI's

        Returns
        -------
        pd.DataFrame
            Reduced dataframe with only unique swaths
        """
        swaths = df['url'].astype(str) + df['file'].astype(str)
        return pd.DataFrame({'swaths': swaths.unique()})

    @abstractmethod
    def check_for_existing_swaths(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parameters
        ----------
        df : pd.DataFrame
            Reduced swath listing after parsing to be handeled by the 
            processor to check for already fully-processed h5 files
        
        Returns
        -------
        pd.DataFrame :
            shortened listing in case of existing files
        """
        #retrieve processed files
        processed_files = [f.name.split('_')[:4] 
                           for f in os.scandir(self.ref.out) 
                           if f.is_file()]
        
        for idx, swath in df.iterrows():
            #temporarily set file id
            self.ref.swath.set_swath_id(swath)
            #compile output swath-file name
            sname = self.ref.swath._compile_output_swath_name()
            #check for existance
            if sname.split('_')[:4] not in processed_files:
                break

        #return updated listing
        return df.iloc[idx:,:]

    @abstractmethod
    def get_swath_file(self) -> bool:
        """
        Wrapper function to handle the check for already downloaded but 
        unprocessed swath files as well as the actual swath retrieval in 
        case it is necessary
        """
        #retieve list of currently temporarily stored/downloaded files
        DOWNLOADED_FILES = [f.name for f in os.scandir(self.ref.rawout) 
                            if f.is_file()]
        #retrieve current swath id's
        SWATH = self.ref.swath.get_swath_id(swath_only=True)
        #only download in case do not already exist
        SWATH_EXISTS = SWATH in DOWNLOADED_FILES
        URL = self.ref.swath.get_swath_id(swath_only=False)
        if not SWATH_EXISTS:
            status = self.download_swath(URL)
        else:
            status = True
        
        #return
        return status

    def download_swath(self, url: str) -> bool:
        """
        Parameters
        ----------
        url : str
            sensor/carrier specific download url
        
        Returns
        -------
        Bool :
            download successful? True/False
        """
        #solely the swath file name
        swath = url.split('/')[-1]
        
        #status
        logger.info(f'Retrieving swath file: {swath}')

        #requests call
        headers = {'Authorization': "Bearer {}".format(self.ref.token)}
        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            #store downloaded swath
            with open(os.path.join(self.ref.rawout, swath), "wb") as f:
                f.write(r.content)
            #status
            logger.info(f'Retrieval complete!')
            self.ref.error.reset_crit_counter()
            return True
        else:
            #status
            logger.info(f'Retrieval incomplete!')
            logger.error(f'Error with swath retrieval!')
            self.ref.error.increase_crit_counter()
            return False


class SlstrRetrievalHandler(RetrievalHandler): 
    def parse_swath_listing(self, df: pd.DataFrame) -> pd.DataFrame:
        return super().parse_swath_listing(df)
    
    def check_for_existing_swaths(self, df: pd.DataFrame) -> pd.DataFrame:
        return super().check_for_existing_swaths(df)
    
    def get_swath_file(self) -> bool:
        #get swath file
        STATUS =  super().get_swath_file()
        #get swath id to initialize zip handler
        SWATH = self.ref.swath.get_swath_id(swath_only=True)
        self.ref.zip.set_zip_path(SWATH)
        #load zip file
        #TODO exception handler for BadZipFile exception! return bad STATUS
        self.ref.zip.load_zip_file()
        #extract zip file
        self.ref.zip.extract_zip_file()
        #close zip file connection
        self.ref.zip.close_zip_file()
        return STATUS
    
    
class OlciRetrievalHandler(RetrievalHandler): 
    def parse_swath_listing(self, df: pd.DataFrame) -> pd.DataFrame:
        return super().parse_swath_listing(df)
    
    def check_for_existing_swaths(self, df: pd.DataFrame) -> pd.DataFrame:
        return super().check_for_existing_swaths(df)
    
    def get_swath_file(self) -> bool:
        #get swath file
        STATUS =  super().get_swath_file()
        #get swath id to initialize zip handler
        SWATH = self.ref.swath.get_swath_id(swath_only=True)
        self.ref.zip.set_zip_path(SWATH)
        #load zip file
        #TODO exception handler for BadZipFile exception! return bad STATUS
        self.ref.zip.load_zip_file()
        #extract zip file
        self.ref.zip.extract_zip_file()
        #close zip file connection
        self.ref.zip.close_zip_file()
        return STATUS


class ModisRetrievalHandler(RetrievalHandler):      
    def parse_swath_listing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        MODIS specific function dealing with the file duality of MXD03 and 
        MXD02 necessary to get the full dataset
        """
        mxd03 = df['url_mxd03'].astype(str) + df['mxd03'].astype(str)
        mxd02 = df['url_mxd02'].astype(str) + df['mxd02'].astype(str)
        return pd.DataFrame({'mxd03': mxd03.unique(),
                             'mxd02': mxd02.unique()})

    def check_for_existing_swaths(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        MODIS specific function dealing with the file duality of MXD03 and 
        MXD02 necessary to get the full dataset
        """
        #retrieve processed files
        processed_files = [f.name.split('_')[:4] for f in os.scandir(self.ref.out) 
                           if f.is_file()]
        
        for idx, swath in df.iterrows():
            #temporarily set file id
            self.ref.swath.set_swath_id(swath)
            #compile output swath-file name
            sname = self.ref.swath._compile_output_swath_name()
            #check for existance
            if sname.split('_')[:4] not in processed_files:
                break

        #return updated listing
        return df.iloc[idx:,:]

    def get_swath_file(self) -> bool:
        """
        MODIS specific function dealing with the file duality of MXD03 and 
        MXD02 necessary to get the full dataset
        """
        #retieve list of currently temporarily stored/downloaded files
        downloaded_files = [f.name for f in os.scandir(self.ref.rawout) 
                            if f.is_file()]

        #retrieve current swath id's
        SWATHS = self.ref.swath.get_swath_id(swath_only=True)
        URLS = self.ref.swath.get_swath_id(swath_only=False)

        #only download in case do not already exist
        MXD03_EXISTS = SWATHS['mxd03'] in downloaded_files
        if not MXD03_EXISTS:
            status_mxd03 = self.download_swath(URLS['mxd03'])
        else:
            status_mxd03 = True
        MXD02_EXISTS = SWATHS['mxd02'] in downloaded_files
        if not MXD02_EXISTS:
            status_mxd02 = self.download_swath(URLS['mxd02'])
        else:
            status_mxd02 = True
            
        return all((status_mxd03, status_mxd02))


