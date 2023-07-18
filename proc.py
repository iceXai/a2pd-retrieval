# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[] 

from datetime import datetime, timedelta

from iotools import ListingIO
from data import ListingData

import pandas as pd
import numpy as np

import requests
import os
import sys


# In[] 

#TODO with implementation of more sensors this likely has some potential to 
#     outsource common functions to a ListingProcessor() ABC
class ModisListingProcessor(object):
    def __init__(self):
        #define sensor specific download url's
        self.url = {'terra':{'mxd02':'https://ladsweb.modaps.eosdis.'+\
                                     'nasa.gov/archive/allData/61/MOD021KM/',
                             'mxd03':'https://ladsweb.modaps.eosdis.'+\
                                     'nasa.gov/archive/allData/61/MOD03/',
                             'meta':'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/geoMeta/61/TERRA/'
                                    },
                    'aqua':{'mxd02':'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/allData/61/MYD021KM/',
                            'mxd03':'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/allData/61/MYD03/',
                            'meta':'https://ladsweb.modaps.eosdis.'+\
                                   'nasa.gov/archive/geoMeta/61/AQUA/'
                                   }
                        }
        
        #file prefixes
        self.prefix = {'terra': 'MOD',
                       'aqua': 'MYD'
                       }
        
        
    """ High-level functions """
    
    """ Getters/Setters for Processor Setup """
    def set_carrier(self, carrier: str) -> None:
        self.carrier = carrier
  
        
    def set_token(self, token: str) -> None:
        self.token = token

        
    def set_aoi(self, aoi: dict) -> None:
        self.aoi = aoi

        
    def set_url(self) -> None:
        self.url = self.url[self.carrier]
        self.prefix = self.prefix[self.carrier]
        
        
    def set_output_path(self, path: str) -> None:
        #output directory
        LISTING_FOLDER = 'listing'
        
        #create listing directory if necessary
        path = os.path.join(path, LISTING_FOLDER)
        if not os.path.isdir(path):
            os.makedirs(path)   
        self.lstout = path
        
        
    def initialize_listing_data(self) -> None:
        #initiate listing data container
        self.listing = ListingData()
        
        
    def initialize_listing_io(self) -> None:
        #initiate i/o handler
        self.io = ListingIO(self.lstout)


    """ URL/Listing File Name Management """
    def set_current_url(self, yy: str, jj: str) -> None:
        #compile day and month of current date
        d = datetime.strptime(f'{yy}-{jj}', "%Y-%j")
        dd = d.strftime('%d')
        mm = d.strftime('%m')
        
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
        
    def get_current_url(self, url_type: str) -> str:
        return self.current_url[url_type]
    
    
    def set_current_lfn(self, yy: str, jj: str) -> None:
        #set curreent listing file name
        self.current_lfn = f'{self.carrier}_modis_listing_{yy}_{jj}.csv'
    
        
    def get_current_lfn(self) -> str:
        return self.current_lfn
    
    """ Listing Procedure """
    def check_for_existing_listing(self) -> bool:
        path = os.path.join(self.lstout, self.get_current_lfn())
        return os.path.isfile(path)
    
    
    def get_listing_file(self, url_type: str) -> bool:
        """
        Parameters
        ----------
        url_type : str
            Type of url to be retrieved; according to the url dict, i.e.,
            'meta', 'mxd03', and 'mxd02'

        Returns
        -------
        bool
            True/False on the retrieval process completion.
        """
        
        #get url to download from
        download_url = self.get_current_url(url_type)
        
        try:
            #call download function
            status = self.download_listing(download_url)
                    
            #return status
            return status
        except:
            #TODO needs improvemenet
            return False


    
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

        #TODO put this function into the ABC ?!
     
        print(f'['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
               f'] - Retrieving listing file...')

        #requests call
        headers = {'Authorization': "Bearer {}".format(self.token)}
        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                  '] - Retrieval of file listing complete!')
            
            self.temporary_listing = self._parse_byte_listing(r.content)
            
            return True
        else:
            print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                  '] - Error with file listing retrieval!')
        
            return False
        
    """ I/O Management """    
    def save_listing(self) -> None:
        self.io.set_listing_file_name(self.get_current_lfn())
        self.io.to_csv(self.listing.get_listing())
        
        
    def load_listing(self) -> None:
        self.io.set_listing_file_name(self.get_current_lfn())
        self.listing.add_to_listing(self.io.from_csv())
        
    def get_listing(self) -> pd.DataFrame:
        return self.listing.get_listing()
    

    """ Low-level functions """
    def _parse_byte_listing(self, byte_listing) -> list:
        return byte_listing.decode('UTF-8').split('\n')
    
    
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
        
        #create polygon/bounding box from swath coordinates
        lon = np.array([float(lst_entry[1]),float(lst_entry[2]),
                        float(lst_entry[3]),float(lst_entry[4])])
        lat = np.array([float(lst_entry[5]),float(lst_entry[6]),
                        float(lst_entry[7]),float(lst_entry[8])])
            
        #create swath polyon
        polygon_coordinates = [[lat[0],lon[0]],
                               [lat[3],lon[3]],
                               [lat[2],lon[2]],
                               [lat[1],lon[1]],
                               [lat[0],lon[0]]]
        
        #check for overlap of bounding box with predefined aoi polygons
        for aoi in self.aoi:    
            #set swath polygon
            self.aoi[aoi].set_swath_polygon(polygon_coordinates)
            
            #check for overlap
            overlap, frac = self.aoi[aoi].check_overlap_with_aoi()
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
class ModisRetrievalProcessor(object):
    """
    Handles the actual download process of the identified swaths from the 
    file listing process
    """
    def __init__(self):
        pass
    
    
    
# In[]
# In[]
# In[]

"""
Processing:Swath Handling
"""
class Swath(object):
    """
    Handles the downloaded swath 
    """
    def __init__(self):
        pass

    
# In[]
# In[]
# In[]
    
"""
Processing::Resampling to Grid
"""
class Resample(object):
    """
    Handles all the resample process of the current tobe processed swath data
    """
    def __init__(self):
        pass
    
    


