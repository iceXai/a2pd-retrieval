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
        #set curreent listing file names
        lfn_mxd03 = f'{self.carrier}_mxd03_listing_{yy}_{jj}.txt'
        lfn_mxd02 = f'{self.carrier}_mxd02_listing_{yy}_{jj}.txt'
        lfn_final = f'{self.carrier}_modis_listing_{yy}_{jj}.txt'
        self.current_lfn = {'mxd03': lfn_mxd03,
                            'mxd02': lfn_mxd02,
                            'final': lfn_final
                            }
        
    def get_current_lfn(self, lfn_type: str) -> str:
        return self.current_lfn[lfn_type]

    
    def get_listing_file(self, url_type: str, lfn_type: str) -> bool:
        """
        Parameters
        ----------
        url_type : str
            Type of url to be retrieved; according to the url dict, i.e.,
            'meta', 'mxd03', and 'mxd02'
        lfn_type : str
            Type of listing file name to be set for the file retrieval, i.e.,
            'meta', 'mxd03', and 'mxd02'

        Returns
        -------
        bool
            True/False on the retrieval process completion.
        """
        
        #get url to download from
        download_url = self.get_current_url(url_type)
        
        #set name for listing file
        listing_file_name = self.get_current_lfn(lfn_type)
        
        #check for file availability
        if os.path.isfile(os.path.join(self.lstout,
                                       listing_file_name)):
            ##TODO
            #modify logging and status messages
            print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                    f'] - Skipped: {lfn_type.upper()} Listing file already exists!')
                
            #set status to False
            process_complete = False
        else:
            #call download function
            process_complete = self.download_listing(download_url,
                                                     listing_file_name)
        #return status
        return process_complete

    
    def process_mxd03_listing_file(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame
            pandas dataframe with search tags, MXD03 url/swath information
            as well as meta data on the AOI and overlaps fraction
        """
        
        df = pd.DataFrame()
        
        #read-in the listing file
        lst = self._read_mxd03_listing_file(self.get_current_lfn('mxd03'))
        
        #loop over entries
        for lst_entry in lst:
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
            
        #return processed listing info
        return df.reset_index().drop('index', axis=1)
    

    def process_mxd02_listing_file(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parameters
        ----------
        df : pd.DataFrame
            pandas DataFrame containing the already retrieved MXD03 
            geolocation informartion

        Returns
        -------
        pd.DataFrame :
            Updated pandas DataFrame containing additional MXD02 information
        """
        
        
        #read-in the listing file
        lst = self._read_mxd02_listing_file(self.get_current_lfn('mxd02'))
        
        #match it with mxd03 list and return
        return self._get_processed_mxd02_listing(lst, df)


    def download_listing(self, url: str, lfn: str) -> bool:
        """
        Parameters
        ----------
        url : str
            sensor/carrier specific download url
        lfn : str
            listing_file_name

        Returns
        -------
        Bool :
            download successful? True/False
        """
        #set counter and times [in seconds] for ftp/https retry in case 
        #download fails
        retry_count = 10
        retry_sleep = 5

        #TODO get rid of this stype separation and put this function into the ABC
        #get sensor type
        stype = lfn.split('_')[1]
        for retry in range(retry_count+1):
            try:        
                print(f'['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                       f'] - Retrieving {stype} listing file...')

                #requests call
                headers = {'Authorization': "Bearer {}".format(self.token)}
                r = requests.get(url, headers=headers)

                if r.status_code == 200:
                    print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                            '] - Retrieval of file listing complete!')
                    break
                else:
                    print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                            '] - Error with file listing retrieval... going to retry')
                    raise
            except:
                #exception handling
                if retry != range(retry_count+1)[-1]:
                    #wait a moment
                    time.sleep(retry_sleep)
                    print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                            '] - Retry of file listing retrieval...')
                    continue
                else:
                    print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                            '] ! Retrieval finally failed!')
                    sys.exit()
        
        #save file conent
        with open(os.path.join(self.lstout, lfn), 'wb+') as f:
            f.write(r.content)
        
        #return status            
        return True
    

    """ Low-level functions """
    def _read_mxd03_listing_file(self, lfn: str) -> list:
        #open listing file
        with open(os.path.join(self.lstout, lfn), 'rt') as f:
            data = f.readlines()
         
        #listing file consists of various meta information w/ 
        #data[0]:=hdf_file_name; data[9:12]:=RingLON; 
        #data[13:16]:=RingLAT
        lst = [[line.split(',')[0],line.split(',')[9],
                line.split(',')[10],line.split(',')[11],
                line.split(',')[12],line.split(',')[13],
                line.split(',')[14],line.split(',')[15],
                line.split(',')[16]] for line in data[3:]]
        
        #return to caller
        return lst
    
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
        
                
    def _read_mxd02_listing_file(self, lfn: str) -> list:
        #open listing file
        with open(os.path.join(self.lstout, lfn), 'rt') as f:
            data = f.readlines()

        #create list of file links 
        SEARCH_STR = '<a class="btn btn-default" href="/archive/allData/'
        lst = [line.split('"')[3].split('/')[-1] \
               for line in data if SEARCH_STR in line]

        #return to caller
        return lst        
     
    
    def _get_processed_mxd02_listing(self,
                                     lst: list,
                                     df: pd.DataFrame) -> pd.DataFrame:
                #create search tags for mxd02
        tags = ['.'.join(lst_entry.split('.')[1:4]) for lst_entry in lst]
        
        #compile df
        df_to_join = pd.DataFrame({'tag': tags,
                                   'url_mxd02': self.get_current_url('mxd02'),
                                   'mxd02': lst})
        
        #join them on tags and rearrange them
        df = df.join(df_to_join.set_index('tag'), on='tag')
        df.drop('tag', axis=1, inplace=True)
        return df[['url_mxd03', 'mxd03', 'url_mxd02', 'mxd02', 'aoi', 'frac']]
       

# In[]
# In[]
# In[]


"""
Processing::FileDownload
"""
class Download(object):
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
Processing::Resampling to Grid
"""
class Resample(object):
    """
    Handles all the resample process of the current tobe processed swath data
    """
    def __init__(self):
        pass
    
    


