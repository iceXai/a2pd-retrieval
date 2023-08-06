# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[] 
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from loguru import logger

from iotools import ListingIO, ModisSwathIO
from data import ListingData, SwathData
from meta import ModisSwathMeta
from meta import SlstrSwathMeta
from resampling import Resample

import pandas as pd
import numpy as np

import requests
import os
import sys


# In[] 

class ListingProcessor(ABC):
    """
    Listing base class which all sensor-specific listing classes are supposed
    to inherit/be a child class from
    """
    def __init__(self):    
        #download error management
        self.error = DownloadErrorManager()
    
    """ High-level (abstract) functions """
    
    """ Getters/Setters for Processor Setup """
    def set_carrier(self, carrier: str) -> None:
        self.carrier = carrier
  
        
    def set_token(self, token: str) -> None:
        self.token = token

        
    def set_aoi(self, aoi: object) -> None:
        self.aoi = aoi
        
        
    def set_meta(self, meta: object) -> None:
        self.meta = meta
        self.meta.set_carrier(self.carrier)

        
    def set_url(self) -> None:
        self.url = self.meta.get_urls()        
        
    def set_output_path(self, path: str) -> None:
        #output directory
        LISTING_FOLDER = 'listing'
        
        #create listing directory if necessary
        path = os.path.join(path, LISTING_FOLDER)
        if not os.path.isdir(path):
            os.makedirs(path) 
        #status
        logger.info(f'Set listing output directory: {path}')
        self.lstout = path
        
        
    def initialize_listing_data(self) -> None:
        #initiate listing data container
        self.listing = ListingData()
        
        
    def initialize_listing_io(self) -> None:
        #initiate i/o handler
        self.io = ListingIO(self.lstout)
        
        
    """ URL/Listing File Name Management """
    @abstractmethod
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
            Stores the current url's for the listing file retrievals after 
            compiling the geoMeta file name (with current date) as well as 
            the year and julian day of year extensions to the base url's'
        """
        pass
    
    def compile_ddmm_from_yyjj(self, yy: str, jj: str) -> tuple:
        d = datetime.strptime(f'{yy}-{jj}', "%Y-%j")
        dd = d.strftime('%d')
        mm = d.strftime('%m')
        return dd, mm
    
    def get_current_url(self, url_type: str) -> str:
        return self.current_url[url_type]
    
    @abstractmethod
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
            Stores the current listing file name (lfn) with the format:
            "[CARRIER]_[SENSOR]_listing_[yyyy]_[jj].csv" in self.current_lfn
        """
        pass
      
    def get_current_lfn(self) -> str:
        return self.current_lfn

    
    """ Listing Procedure """
    def check_for_existing_listing(self) -> bool:
        path = os.path.join(self.lstout, self.get_current_lfn())
        return os.path.isfile(path)
    
    
    def get_geometa_file(self) -> bool:
        return self.get_listing_file('meta')

    def get_listing_file(self, url_type: str) -> bool:
        """
        Parameters
        ----------
        url_type : str
            Type of url to be retrieved; according to the url dict, i.e.,
            'meta', 'data', or other sensor-specific ones like 'mxd03', 
            and 'mxd02'

        Returns
        -------
        bool
            True/False on the retrieval process completion.
        """
        
        #get url to download from
        download_url = self.get_current_url(url_type)
        
        #call download function
        status = self.download_listing(download_url)
                
        #return status
        return status
    
    def download_listing(self, url: str) -> bool:
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
        headers = {'Authorization': "Bearer {}".format(self.token)}
        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            self.temporary_listing = self.parse_byte_listing(r.content)
            #status
            logger.info(f'Retrieval complete!')
            #reset counter
            self.error.reset_crit_counter()
            return True
        else:
            #status
            logger.info(f'Retrieval incomplete!')
            logger.error(f'Error with file listing retrieval!')
            #increase coutner
            self.error.increase_crit_counter()
            return False
    
    
    

class SlstrListingProcessor(ListingProcessor):
    """ High-level functions """

    """ URL/Listing File Name Management """
    def set_current_url(self, yy: str, jj: str) -> None:
        #compile day and month of current date
        dd, mm = self.compile_ddmm_from_yyjj(yy, jj)
        
        #build url using the GeoMeta part and file name
        CARRIER = self.carrier.upper()
        geometa_file_name = f'{CARRIER}_SL_1_RBT_{yy}-{mm}-{dd}.txt'
        meta_url = f'{self.url["meta"]}{yy}/{geometa_file_name}'
        data_url = f'{self.url["data"]}{yy}/{jj}/' 
        
        #set current urls
        self.current_url = {'meta': meta_url,
                            'data': data_url,
                            }


    def set_current_lfn(self, yy: str, jj: str) -> None:
        #set curreent listing file name
        self.current_lfn = f'{self.carrier}_slstr_listing_{yy}_{jj}.csv'


    """ Listing Procedure """
    
    

class ModisListingProcessor(ListingProcessor):
    """ High-level functions """
    
    """ Getters/Setters for Processor Setup """
    def set_prefix(self) -> None:
        """
        Additional function dealing with the carrier-dependent file prefix:
        Aqua -> MYD, Terra -> MOD!
        """
        self.prefix = self.meta.get_data_prefix() 


    """ URL/Listing File Name Management """
    def set_current_url(self, yy: str, jj: str) -> None:
        #compile day and month of current date
        dd, mm = self.compile_ddmm_from_yyjj(yy, jj)
        
        #build url using the GeoMeta part and file name
        geometa_file_name = f'{self.prefix}03_{yy}-{mm}-{dd}.txt'
        meta_url = f'{self.url["meta"]}{yy}/{geometa_file_name}'
        mxd3_url = f'{self.url["mxd03"]}{yy}/{jj}/' 
        mxd2_url = f'{self.url["mxd02"]}{yy}/{jj}/' 
        
        #set current urls
        self.current_url = {'meta': meta_url,
                            'mxd03': mxd3_url,
                            'mxd02': mxd2_url
                            }
    
    
    def set_current_lfn(self, yy: str, jj: str) -> None:
        #set curreent listing file name
        self.current_lfn = f'{self.carrier}_modis_listing_{yy}_{jj}.csv'

    
    """ Listing Procedure """
    def get_mxd02_file(self) -> bool:
        return self.get_listing_file('mxd02')

    
    def process_mxd03_listing_file(self) -> None:
        #parse listing
        df = pd.DataFrame()
        self._parse_mxd03_listing()
        
        #loop over entries
        for lst_entry in self.mxd03:
            #check for overlap with specified aoi's
            valid_aois, overlap_aois = self._validate_aoi_overlap(lst_entry)
            
            #process in case of overlap
            if valid_aois:
                tgs, mxd, mt = self._get_processed_mxd03_listing(lst_entry,
                                                                 overlap_aois)
                mxd03_dict = {'tag': tgs,
                              'url_mxd03': self.get_current_url('mxd03'),
                              'mxd03': mxd,
                              'aoi': mt[0],
                              'frac': mt[1]}
                #append to df
                df = pd.concat([df, pd.DataFrame(mxd03_dict)])
            
        #update listing
        self.mxd03 = df.reset_index().drop('index', axis=1)
    

    def process_mxd02_listing_file(self) -> None:
        #parse listing
        self._parse_mxd02_listing()
        
        #match it with mxd03 list
        self._get_processed_mxd02_listing()
    
        
    """ I/O Management """    
    def save_listing(self) -> None:
        self.io.set_listing_file_name(self.get_current_lfn())
        self.io.to_csv(self.listing.get_listing())
        
    
    def load_listing(self) -> None:
        self.io.set_listing_file_name(self.get_current_lfn())
        self.listing.add_to_listing(self.io.from_csv())
        
        
    def get_listing(self) -> pd.DataFrame:
        return self.listing.get_listing()
    
    
    def parse_byte_listing(self, byte_listing) -> list:
        return byte_listing.decode('UTF-8').split('\n')
    

    """ Low-level functions """  
    def _parse_mxd03_listing(self) -> None:         
        #listing file consists of various meta information w/ 
        #data[0]:=hdf_file_name; data[9:12]:=RingLON; 
        #data[13:16]:=RingLAT
        lst = [[line.split(',')[0],line.split(',')[9],
                line.split(',')[10],line.split(',')[11],
                line.split(',')[12],line.split(',')[13],
                line.split(',')[14],line.split(',')[15],
                line.split(',')[16]] for line in self.temporary_listing[3:-1]]
        
        #update temporary listing
        self.mxd03 = lst
    
    def _validate_aoi_overlap(self, lst_entry: list) -> (bool, list):
        #check for overlap to exclude non-matching links from listing
        overlap_aois = []
        
        #get aois to process
        aois_to_check = self.aoi.get_aois()
        
        #check for overlap of bounding box with predefined aoi polygons
        for aoi in aois_to_check:    
            #check for overlap
            grid = self.aoi.get_aoi(aoi)
            overlap, frac = grid.check_overlap_with_aoi(lst_entry)
            if overlap and frac >= 5.0:
                overlap_aois.append([aoi,frac])
        
            #check status
        if len(overlap_aois)>0:
            valid = True
        else:
            valid = False
        
        #return
        return valid, overlap_aois
    
    def _get_processed_mxd03_listing(self, 
                                     lst_entry: list, 
                                     overlap_aois: list) -> (str, str, list):
        #compile download link and append to swath/search list
        hdf_file = lst_entry[0]
        hdf_split = hdf_file.split('.')
        
        #compile meta output
        aoi_list = [aoi for aoi,frc in overlap_aois]
        frc_list = [frc for aoi,frc in overlap_aois]
        
        #return
        return '.'.join(hdf_split[1:4]), hdf_file , [aoi_list,frc_list]
        
                
    def _parse_mxd02_listing(self) -> None:
        #create list of file links 
        SEARCH_STR = '<a class="btn btn-default" href="/archive/allData/'
        lst = [line.split('"')[3].split('/')[-1] \
               for line in self.temporary_listing if SEARCH_STR in line]

        #update temporary listing
        self.mxd02 = lst       
     
    
    def _get_processed_mxd02_listing(self) -> None:
        #create search tags for mxd02
        tags = ['.'.join(lst_entry.split('.')[1:4])
                for lst_entry in self.mxd02]
        
        #compile df
        df_to_join = pd.DataFrame({'tag': tags,
                                   'url_mxd02': self.get_current_url('mxd02'),
                                   'mxd02': self.mxd02})
        
        #join them on tags and rearrange them
        df = self.mxd03.join(df_to_join.set_index('tag'), on='tag')
        df.drop('tag', axis=1, inplace=True)
        df = df[['url_mxd03', 'mxd03', 'url_mxd02', 'mxd02', 'aoi', 'frac']]
        #store in data container
        self.listing.add_to_listing(df)
       

# In[]
# In[]
# In[]


"""
Processing::Swath Download
"""

class RetrievalProcessor(ABC):
    pass


class ModisRetrievalProcessor(RetrievalProcessor):
    """
    Handles the actual download process of the identified swaths from the 
    file listing process
    """
    def __init__(self):
        #aoi
        self.overlapping_aois = None
        
        #download error management
        self.error = DownloadErrorManager()
    
    """ High-level functions """
    
    """ Getters/Setters for Processor Setup """
    def set_token(self, token: str) -> None:
        self.token = token
        
    
    def set_carrier(self, carrier: str) -> None:
        self.carrier = carrier
        
        
    def set_output_path(self, path: str) -> None:
        #output directory
        RAW_FOLDER = 'tmp'
        
        #status
        logger.info(f'Set processed output directory: {path}')
        #set output directory
        self.out = path
        
        #create raw output temporary directory if necessary
        path = os.path.join(path, RAW_FOLDER)
        if not os.path.isdir(path):
            os.makedirs(path)   
        #status
        logger.info(f'Set temporary output directory: {path}')
        self.rawout = path
        
        
    def set_aoi(self, aoi: dict) -> None:
        self.aoi = aoi
        
    def set_meta(self, meta: object) -> None:
        self.meta = meta
        

    def initialize_swath_data(self) -> None:
        #initiate swath data container
        self.swath = SwathData()
        
        
    def initialize_swath_io(self) -> None:
        #initiate i/o handler
        self.io = ModisSwathIO(self.out)
        
        
    def initialize_resampling(self) -> None:
        self.resampling = Resample()
        self.resampling.set_aoi(self.aoi)
        self.resampling.set_data(self.swath)
        
    
    def set_swath_id(self, swaths: str) -> None:
        SWATHS = {'mxd03': swaths[0],'mxd02': swaths[1]}
        self.swath.set_swath_id(SWATHS)
        
    
    def get_swath_id(self, short: bool = True) -> tuple:
        SWATHS = self.swath.get_swath_id()
        if short:
            SWATHS['mxd03'] = SWATHS['mxd03'].split('/')[-1]
            SWATHS['mxd02'] = SWATHS['mxd02'].split('/')[-1]
        return SWATHS
    
        
    """ Retrieval procedure """
    def parse_swath_listing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parameters
        ----------
        df : pd.DataFrame
            The raw listing data frame with potentially multiple entries for 
            swaths that are supposed to be resampled to several different 
            AOI's

        Returns
        -------
        pd.DataFrame
            Reduced dataframe with only unique swaths
        """
        mxd03 = df['url_mxd03'].astype(str) + df['mxd03'].astype(str)
        mxd02 = df['url_mxd02'].astype(str) + df['mxd02'].astype(str)
        return pd.DataFrame({'mxd03': mxd03.unique(),
                             'mxd02': mxd02.unique()})

    
    def check_for_existing_swaths(self, lst: pd.DataFrame) -> pd.DataFrame:
        """
        Parameters
        ----------
        lst : pd.DataFrame
            swath listing to be handeled by the processor
        
        Returns
        -------
        pd.DataFrame :
            shortened listing in case o existing files
        """
        #retrieve processed files
        processed_files = [f.name.split('_')[:4] for f in os.scandir(self.out) 
                           if f.is_file()]
        
        for idx, mxd02, mxd03 in lst.itertuples():
            #temporarily set file id
            self.set_swath_id((mxd03, mxd02))
            #compile output swath-file name
            sname = self.compile_output_swath_name()
            #check for existance
            if sname.split('_')[:4] not in processed_files:
                break

        #return updated listing
        return lst.iloc[idx:,:]
    

    def get_swath_files(self) -> None:       
        #retieve list of currently temporarily stored/downloaded files
        downloaded_files = [f.name for f in os.scandir(self.rawout) 
                            if f.is_file()]
        #retrieve current swath id's
        GET_SWATH_NAME_ONLY = False
        SWATHS = self.get_swath_id(GET_SWATH_NAME_ONLY)
        
        #only download in case do not already exist
        MXD03_EXISTS = SWATHS['mxd03'].split('/')[-1] in downloaded_files
        if not MXD03_EXISTS:
            status_mxd03 = self.download_swath(SWATHS['mxd03'])
        else:
            status_mxd03 = True
        MXD02_EXISTS = SWATHS['mxd02'].split('/')[-1] in downloaded_files
        if not MXD02_EXISTS:
            status_mxd02 = self.download_swath(SWATHS['mxd02'])
        else:
            status_mxd02 = True
            
        return status_mxd03, status_mxd02
    
    
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
        headers = {'Authorization': "Bearer {}".format(self.token)}
        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            #store downloaded swath
            with open(os.path.join(self.rawout, swath), "wb") as f:
                f.write(r.content)
            #status
            logger.info(f'Retrieval complete!')
            self.error.reset_crit_counter()
            return True
        else:
            #status
            logger.info(f'Retrieval incomplete!')
            logger.error(f'Error with swath retrieval!')
            self.error.increase_crit_counter()
            return False
        
    def update_meta_info(self, swaths: tuple) -> None:
        """
        Parameters
        ----------
        swaths : tuple(str,str)
            Tuple of current swath urls by order (mxd03, mxd02) that will be 
            reduced to swath names

        Returns
        -------
        None
            Updates the meta data information on the to be used files 
            (mxd03 or mxd02) for the variable processing
        """
        mxd03 = swaths[0].split('/')[-1]
        mxd02 = swaths[1].split('/')[-1]
        self.meta.update_input_specs((mxd03, mxd02))

    def get_variables(self) -> list:
        return self.meta.get_variables()
    
    def open_swath(self, var: str) -> None:
        FILENAME = self.meta.get_var_input_specs(var)[0]
        FILEPATH = os.path.join(self.rawout, FILENAME)
        self.io.load(FILEPATH)
        
    def load_variable(self, var: str) -> None:
        #get current variable/group name info
        GRP, VAR = self.meta.get_var_input_specs(var)[1:]
        #retrieve corresponding channel specifications
        CHSPECS = self.meta.get_var_channel_specs(var)
        #retrieve the actual variable data from the swath
        variable = self.io.get_var(VAR, GRP, CHSPECS)
        #store it to the data container using the same variable key handle
        self.swath.add_to_data(var, variable)
        
    def close_swath(self) -> None:
        self.io.close()
        
        
    """ Resample procedure """
    def identify_resample_aois(self, df: pd.DataFrame, swath: str) -> list:
        """
        Parameters
        ----------
        df : pd.DataFrame
            The raw listing data frame with potentially multiple entries for 
            swaths that are supposed to be resampled to several different 
            AOI's
        swath : str
            Name of current swath to identify applicable AOI's for resampling

        Returns
        -------
        list :
            List of AOI's for this swath
        """
        swath = swath.split('/')[-1]
        self.overlapping_aois = df['aoi'].loc[df['mxd03']==swath].tolist()
    
    def get_resample_variables(self) -> list:
        return self.meta.get_resample_variables()
    
    def group_data_to_resample(self, var: str) -> None:
        #get resample information from meta data
        lon, lat = self.meta.get_var_grid_specs(var)
        #regroup/shuffle the data by their used lon/lat information
        self.resampling.add_data_to_group(var, lon, lat)
    
    def resample_swath(self) -> None:
        #stack the groups if necessary
        self.resampling.add_groups_to_resample_stack()
        #resample the data
        for aoi in self.overlapping_aois:
            #status
            logger.info(f'Resampling to grid: {aoi}')
            self.resampling.resample(aoi)
        #add to data container
        resampled_data = self.resampling.get_resampled_data()
        self.swath.add_to_resampled_data(resampled_data)
        
        
    """ Output """
    def save_swath(self, aoi: str = None) -> None:
        #wrapper function to handle the h5 output file-creation and data- 
        #storage process
        vars_to_process = self.get_variables()
        self.create_swath(aoi)
        for var in vars_to_process:
            self.set_variable(var)
            
    
    def save_resampled_swath(self) -> None:
        #wrapper function to handle the h5 output file-creation and data- 
        #storage process of the resampled data
        for aoi in self.overlapping_aois:
            self.save_swath(aoi)
        
        
    def create_swath(self, aoi: str = None) -> None:
        FILENAME = self.compile_output_swath_name(aoi)
        FILEPATH = os.path.join(self.out, FILENAME)
        #status
        logger.info(f'Saving to file: {FILENAME}')
        self.io.save(FILEPATH)
    
    
    def compile_output_swath_name(self, aoi: str = None) -> str:
        #correct processing state extension
        if aoi is None:
            EXT = 'raw'
        else:
            EXT = aoi
        pass
        #set file-name parts
        DATE = self.get_date_from_swath_file()
        CARRIER = self.carrier.lower()[0:3]
        SENSOR = 'modis'
        #compile and return
        return f'{CARRIER}_{SENSOR}_{DATE}_{EXT}.h5'
    
    
    def get_date_from_swath_file(self):
        #get swath id
        swath = self.get_swath_id()['mxd02']
        #take raw date from swath id and convert it to datetime object
        raw_yyjj = swath.split('.')[1]
        raw_hhmm = swath.split('.')[2]
        raw_date = datetime.strptime(raw_yyjj+raw_hhmm,'A%Y%j%H%M')
        #transform it
        yyjj = raw_date.strftime('%Y%j')
        hhmm = raw_date.strftime('%H%M%S')
        #return
        return f'{yyjj}_{hhmm}'
    
    
    def set_variable(self, var: str, aoi: str = None) -> None:
        #get variable specific output specifications
        GRP, VAR, ATTR = self.meta.get_var_output_specs(var)
        #compile in-file specific group/variable path
        INPATH = f'{GRP}/{VAR}'
        #pick dataset
        if aoi is None:
            DS = self.swath.get_data(var)
        else:
            DS = self.swath.get_resampled_data(aoi, var)
        #pass data to io
        self.io.set_var(INPATH, DS, ATTR)


    """ Cleanup """
    def cleanup(self):
        #get swath id
        swaths = self.get_swath_id()
        #remove swaths
        mxd03 = swaths['mxd03']
        logger.info(f'Removing downloaded file: {mxd03}')
        self._remove_swath(mxd03)
        mxd02 = swaths['mxd02']
        logger.info(f'Removing downloaded file: {mxd02}')
        self._remove_swath(mxd02)
        
    def _remove_swath(self, swath: str) -> None:
        FILENAME = swath
        FILEPATH = os.path.join(self.rawout, FILENAME)
        try:
            self.io.cleanup(FILEPATH)
            logger.info(f'Removal successful!')
        except:
            logger.error(f'Removal of {FILENAME} failed!')



# In[]
# In[]
# In[]

"""
Misc: Common Functions/Classes
"""

class DownloadErrorManager(object):
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