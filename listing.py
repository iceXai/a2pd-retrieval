# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

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
                 stop: datetime.date
                 ):
        
        #common dict of url's
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
                                   },
                    's3a':{'slstr': 'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/allData/450/'+\
                                    'S3A_SL_1_RBT/',
                           'olci': 'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/allData/450/'+\
                                    'S3A_OL_1_EFR/',
                           'meta': 'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/geoMetaSentinel3/450/'
                                },
                    's3b':{'slstr': 'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/allData/450/'+\
                                    'S3B_SL_1_RBT/',
                           'olci': 'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/allData/450/'+\
                                    'S3B_OL_1_EFR/',
                           'meta': 'https://ladsweb.modaps.eosdis.'+\
                                    'nasa.gov/archive/geoMetaSentinel3/450/'
                                }
                          }
        
        #common dict of prefixes
        self.prefix = {'terra': 'MOD',
                       'aqua': 'MYD'
                       }
        
        #store arguments
        self.token = token
        self.carrier = carrier
        self.start = start
        self.stop = stop
        
        #set standard output
        self.lstout = 'listing'
        
        #set correct pathing
        self._initialize()
    
    """ High-level (abstract) functions """
    
    @abstractmethod
    def compile_file_listing(self):
        """
        compiles the listing of all suitable swath files per sensor that needs 
        to be retrieved from the server
        """
        pass
    
    @abstractmethod
    def download_listing(self):
        """
        subfunction to the list compilation function to retrieve swath listing 
        information from the server, doing the actual system's call to wget
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
        
        
    """ Low-level (abstract) functions """
    
    @abstractmethod
    def _set_current_url(self) -> None:
        """
        internal method to handle the setting of current urls to be used by 
        various functions
        """
        pass
    
    @abstractmethod
    def _get_current_url(self) -> None:
        """
        internal function to handle the retrieval of current urls
        """
        pass
    
    @abstractmethod
    def _set_current_lfn(self) -> None:
        """
        internal method to handle the setting of the current listing file name
        to be used by various functions
        """
        pass
    
    @abstractmethod
    def _get_current_lfn(self) -> None:
        """
        internal function to handle the retrieval of the current listing file
        name
        """
        pass

    def _initialize(self) -> None:
        """
        specific initialization method to be called to set the corresponding 
        urls and prefixes correctly
        """
        #set-up download url's
        self.url = self.url[self.carrier]
        #set-up sensor data prefix (MOD/MYD)
        self.prefix = self.prefix[self.carrier]
        #create listing directory if necessary
        path = os.path.join(os.getcwd(), self.lstout)
        if not os.path.isdir(path):
            os.makedirs(path)
        
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
    
    def test(self):
        #set processor
        processor = ModisListingProcessor()
        processor.set_carrier(self.carrier)
        processor.set_token(self.token)
        processor.set_aoi(self.aoi)
        processor.set_url()
        
        #set data container
        listing = ListingData()
        
        #retrieve date strings for specified processing period
        date_str = self._get_date_strings()
        
        #loop over all dates
        for yy, jj in date_str:            
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
            
            #add to listing data
            listing.add_to_listing(lst)
    
    
    def compile_file_listing(self) -> None: 
        """
        Run through all specified days and retrieve file download url's
        """
        #alloacte list for collecting
        swaths = []
        
        #retrieve date strings for specified processing period
        date_str = self._get_date_strings()
        
        #loop over all dates
        for yy, jj in date_str:
            """
            (1) geoMeta/MXD03: identify swaths in AOI's
            """
            
            #set current urls/listinf file names
            self._set_current_url(yy, jj)
            self._set_current_lfn(yy, jj)
            
            #download individual mxd03 listing file
            complete = self.get_mxd03_listing_file()

            #continue in loop if no file can be found or it already exists
            if not complete:
                ##TODO
                #log failures!
                continue

            #process listing
            tags, mxd03, meta = self.process_mxd03_listing_file()
            
            """
            (2) MXD02: retrieve correct file links to these swaths
            """
              
            #allocate list for MXD03 swath links
            mxd02_swaths = []
        
            #download individual mxd02 listing file
            complete = self.get_mxd02_listing_file()
            
            #continue in loop if no file can be found or it already exists
            if not complete:
                ##TODO
                #log failures!
                continue
            
            #process listing
            mxd02 = self.process_mxd02_listing_file(tags)
            
            """
            (3) Compile combined/matched list of MXD03/MXD02 files to download
            """

            #combine and match MXD03 and MXD02 file urls
            swath_combo = self.retrieve_swaths_to_download(mxd03,mxd02)
    
            #append to global list
            for k in range(len(meta)):
                swaths.append([swath_combo[k],meta[k]])
                
            #return
            return swaths
        
            
    def _set_current_url(self, yy: str, jj: str) -> None:
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
        
    def _get_current_url(self, url_type: str) -> str:
        return self.current_url[url_type]
    
    
    def _set_current_lfn(self, yy: str, jj: str) -> None:
        #set curreent listing file names
        lfn_mxd03 = f'{self.carrier}_mxd03_listing_{yy}_{jj}.txt'
        lfn_mxd02 = f'{self.carrier}_mxd02_listing_{yy}_{jj}.txt'
        self.current_lfn = {'mxd03': lfn_mxd03,
                            'mxd02': lfn_mxd02
                            }
        
    def _get_current_lfn(self, lfn_type: str) -> str:
        return self.current_lfn[lfn_type]
        
                
    def get_mxd03_listing_file(self) -> bool:
        #get url to download from
        download_url = self._get_current_url('meta')
        
        #set name for listing file
        listing_file_name = self._get_current_lfn('mxd03')
        
        #check for file availability
        if os.path.isfile(os.path.join(os.getcwd(),
                                       self.lstout,
                                       listing_file_name)):
            ##TODO
            #modify logging and status messages
            print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                    '] - Skipped: MXD03 Listing file already exists!')
                
            #set status to False
            process_complete = False
        else:
            #call download function
            process_complete = self.download_listing(download_url,
                                                     listing_file_name)
        #return status
        return process_complete
    
    def get_mxd02_listing_file(self) -> bool:
        #get url to download from
        download_url = self._get_current_url('mxd02')      
        
        #set name for listing file
        listing_file_name = self._get_current_lfn('mxd02')
        
        #check for file availability
        if os.path.isfile(os.path.join(os.getcwd(),
                                       self.lstout,
                                       listing_file_name)):
            ##TODO
            #modify logging and status messages
            print('['+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                    '] - Skipped: MXD02 Listing file already exists!')
            
            #set status to False
            process_complete = False
        else:
            #call download function
            process_complete = self.download_listing(download_url,
                                                     listing_file_name)
        #return status
        return process_complete
    
    def process_mxd03_listing_file(self) -> (list, list, list):
        """
        Returns
        -------
        list, list, list
            lists on search tags, MXD03 swath information as well as meta data
            on the AOI overlaps.
        """
        df = pd.DataFrame()
        
        #allocate search tags list to identify MXD02 swaths from MXD03 data
        tags = []
        #allocate list for MXD03 swath links
        mxd03 = []
        #allocate list for swath meta information
        meta = []
        
        #read-in the listing file
        lst = self._read_mxd03_listing_file(self._get_current_lfn('mxd03'))
        
        #loop over entries
        for lst_entry in lst:
            #check for overlap with specified aoi's
            valid_aois, overlap_aois = self._validate_aoi_overlap(lst_entry)
            
            #process in case of overlap
            if valid_aois:
                tgs, mxd, mt = self._get_processed_mxd03_listing(lst_entry,
                                                                 overlap_aois)
                
                #append to lists
                tags.append(tgs)
                mxd03.append(mxd)
                meta.append(mt)
        
        
        
        #return processed listing info
        return tags, mxd03, meta
    
    def _read_mxd03_listing_file(self, lfn: str) -> list:
        #open listing file
        with open(os.path.join(os.getcwd(), self.lstout, lfn), 'rt') as f:
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
        import pdb; pdb.set_trace()
        #return
        return '.'.join(hdf_split[1:4]), hdf_file , [aoi_list,frc_list]
        
                
    def process_mxd02_listing_file(self, tags: list) -> list:
        """
        Parameters
        ----------
        tags : list
            List of unique id tags per identified mxd03 file

        Returns
        -------
        list
            List of MXD02 swath download URL's
        """
        
        #read-in the listing file
        lst = self._read_mxd02_listing_file(self._get_current_lfn('mxd02'))
        
        #match it with mxd03 list
        mxd02 = self._get_processed_mxd02_listing(lst, tags)

        #return
        return mxd02

        
    def _read_mxd02_listing_file(self, lfn: str) -> list:
        #open listing file
        with open(os.path.join(os.getcwd(), self.lstout, lfn), 'rt') as f:
            data = f.readlines()
        
        #create list of file links    
        lst = [line.split('"')[3].split('/')[-1] \
                   for line in data \
                       if '<a class="btn btn-default" href="' in line]
        
        #return to caller
        return lst        
     
    
    def _get_processed_mxd02_listing(self, lst: list, tags: list) -> list:
        #only pick search-tag corresponding file links
        return [lst_entry for lst_entry in lst \
                if '.'.join(lst_entry.split('.')[1:4]) in tags]
                    
     
    def retrieve_swaths_to_download(self, mxd03: list, mxd02: list) -> list:
        """
        Parameters
        ----------
        mxd03 : list
            List of MXD02 swath download URL's.
        mxd02 : list
            List of MXD02 swath download URL's.

        Returns
        -------
        list
            Combined and matched list of MXD03/MXD02 download link pairs.
        """
        #create combined list [[mxd03,mxd02],[mxd03,mxd02],...]
        combined_swath_urls = []
        
        #loop over all mxd03 swaths
        for current_mxd03 in mxd03:
            #loop over all mxd03 swaths that were identified
            date_mxd03 = '.'.join(current_mxd03.split('.')[1:4])
            
            #find correct index in swaths based on MXD03
            found_match = False
            for current_mxd02 in mxd02:
                date_mxd02 = '.'.join(current_mxd02.split('.')[1:4])
                if date_mxd02 == date_mxd03:
                    found_match = True
                    break
                
            #assign correct one and continue
            if found_match:
                current_mxd03 = self._get_current_url('mxd03') + current_mxd03
                current_mxd02 = self._get_current_url('mxd02') + current_mxd02
                combined_swath_urls.append([current_mxd03, current_mxd02])
            else: 
                continue
            
        #return to caller
        return combined_swath_urls
        
                
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
        with open(os.path.join(os.getcwd(), self.lstout, lfn), 'wb+') as f:
            f.write(r.content)
        
        #return status            
        return True
        
    def skip_existing_files(self):
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
        
    def download_listing(self):
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
        
    def download_listing(self):
        pass
    
    def skip_existing_files(self):
        pass           
 