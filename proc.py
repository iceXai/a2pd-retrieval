# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod

import datetime


# In[]

"""
Processing::FileListing
"""
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
                    's3a':'https://ladsweb.modaps.eosdis.nasa.gov/archive/'+\
                          'allData/450/S3A_SL_1_RBT/',
                    's3b':'https://ladsweb.modaps.eosdis.nasa.gov/archive/'+\
                          'allData/450/S3B_SL_1_RBT/'
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
        
        #set correct pathing
        self.initialize()
    
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
    
    def initialize(self) -> None:
        """
        specific initialization method to be called to set the corresponding 
        urls and prefixes correctly
        """
        #set-up download url's
        self.url = self.url[self.carrier]
        #set-up sensor data prefix (MOD/MYD)
        self.prefix = self.prefix[self.carrier]
        
    def get_date_strings(self) -> list:
        """
        handles the splitting up of the datetime objects into single strings
        independent of the used carrier/sensor
        """
        # generate year (yy), day (dd), month(mm), and day-of-year (jj) 
        # strings for the covered range
        dates = [self.start + datetime.timedelta(days = day_diff) \
                 for day_diff in range(0, (self.stop - self.start).days+1)]
        
        yy_str = [date.strftime('%Y') for date in dates]
        jj_str = [date.strftime('%j') for date in dates]
        dd_str = [date.strftime('%d') for date in dates]
        mm_str = [date.strftime('%m') for date in dates]
        
        return [yy_str, jj_str, dd_str, mm_str]
        
    
class ModisListing(Listing):
    """
    Terra/Aqua MODIS listing process child class tailored to the 
    sensor-specific processing
    """
        
    def compile_file_listing(self) -> None: 
        """
        Run through all specified days and retrieve files first on geoMeta 
        then on MXD02 server
        """
        #retrieve date strings for specified processing period
        date_str = self.get_date_strings()
        
        #loop over all dates
        for yy, jj, dd, mm in date_str:
            """
            (1) geoMeta: identify swaths in AOI's
            """
            
            #allocate list for search tags to identify MXD02 swaths from 
            #MXD03 data
            swath_search_tags = []
            #allocate list for MXD03 swath links
            mxd03_swaths = []
            #allocate list for swath meta information
            meta_swaths = []
            
            #download individual mxd03 listing file
            lfn, complete = self.get_mxd03_listing_file(yy, jj, dd, mm)

            #continue in loop if no file can be found
            if not complete:
                ##TODO
                #log failures!
                continue

            #process listing
            tags, mxd03, meta = self.process_listing_file(lfn)
            #append to lists
            swath_search_tags.append(tags)
            mxd03_swaths.append(mxd03)
            meta_swaths.append(meta)
            
            """
            (2) MXD02: retrieve correct file links to these swaths
            """
              
            #allocate list for MXD03 swath links
            mxd02_swaths = []
        
            #download individual mxd02 listing file
            lfn, complete = self.get_mxd02_listing_file(yy, jj, dd, mm)
            
            #continue in loop if no file can be found
            if not complete:
                ##TODO
                #log failures!
                continue
            
######
            
            #process listing file if available  
            if os.path.isfile(os.path.join(self.lstout,listing_file_name)):
                #open temporary file
                f = open(os.path.join(self.lstout,listing_file_name), 'rt')
                data = f.readlines()

                #create list of file links
#                current_file_links = [url_to_download_from+line.split('"')[3].split('/')[-1] \
#                                      for line in data if '<tr><td class="product-cell"><a href=' in line]
#                current_file_links = [url_to_download_from+line.split('"')[3].split('/')[-1] \
#                                      for line in data if '<a class="btn btn-default hide" href="' in line]
                current_file_links = [url_to_download_from+line.split('"')[3].split('/')[-1] \
                                      for line in data if '<a class="btn btn-default" href="' in line]

                #only pick search-tag corresponding file links
                mxd02_swaths = [link for link in current_file_links \
                                if '.'.join(link.split('.')[5:8]) in swath_search_tags]

        
        ### TO BE PUT IN A THIRD FUNCTION THAT RETURNS THE OUTPUT SWATH LIST TO CALLER FUNCTION
            #create combined list [[mxd03,mxd02],[mxd03,mxd02],...]
            combined_swath_links = []
            for current_mxd03 in mxd03_swaths:
                #loop over all mxd03 swaths that were identified
                date_mxd03 = '.'.join(current_mxd03.split('.')[5:7])
                #find correct index in swaths based on MXD03
                found_match = False
                for current_mxd02 in mxd02_swaths:
                    date_mxd02 = '.'.join(current_mxd02.split('.')[5:7])
                    if date_mxd02 == date_mxd03:
                        found_match = True
                        break
                #assign correct one and continue
                if found_match:
                    combined_swath_links.append([current_mxd03, current_mxd02])
                else: 
                    continue

            #append to global list and close file connection
            for k in range(len(meta_swaths)):
                self.swaths_to_download.append([combined_swath_links[k],
                                                meta_swaths[k]])   
            # self.swaths_to_download.extend(combined_swath_links)
            f.close()
                
                
        
                
    def get_mxd03_listing_file(self, yy: str, jj: str,
                               dd: str, mm: str) -> str,bool:
        #build url using the GeoMeta part and file name
        geometa_file_name = f'{self.prefix}03_{yy}-{mm}-{dd}.txt'
        download_url = f'{self.url["meta"]}{yy}/{geometa_file_name}'
        
        #set name for listing file
        listing_file_name = f'{self.carrier}_mxd03_listing_{yy}_{jj}.txt'
        
        #check for file availability
        if os.path.isfile(os.path.join('./listing',listing_file_name)):
            ##TODO
            #modify logging and status messages
            print('['+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                    '] - Skipped: MXD03 Listing file already exists!')
        else:
            #call download function
            process_complete = self.download_listing(download_url,
                                                     listing_file_name,
                                                     'MXD03'
                                                     )
        #return status
        return listing_file_name, process_complete
    
    def get_mxd02_listing_file(self, yy: str, jj: str,
                               dd: str, mm: str) -> str,bool:
        #build url
        download_url = f'{self.url["mxd02"]}{yy}/{jj}/      
        
        #set name for listing file
        listing_file_name = f'{self.carrier}_mxd02_listing_{yy}_{jj}.txt'
        
        #check for file availability
        if os.path.isfile(os.path.join('./listing',listing_file_name)):
            ##TODO
            #modify logging and status messages
            print('['+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+\
                    '] - Skipped: MXD02 Listing file already exists!')
        else:
            #call download function
            process_complete = self.download_listing(download_url,
                                                     listing_file_name,
                                                     'MXD02'
                                                     )
        #return status
        return listing_file_name, process_complete
    
    def process_mxd03_listing_file(self, lfn: str) -> list, list, list:
        """
        Parameters
        ----------
        lfn : str
            listing_file_name

        Returns
        -------
        list, list, list
            lists on search tags, MXD03 swath information as well as meta data
            on the AOI overlaps.

        """
        #allocate search tags list to identify MXD02 swaths from MXD03 data
        tags = []
        #allocate list for MXD03 swath links
        mxd03 = []
        #allocate list for swath meta information
        meta = []
            
        #open temporary file
        f = open(os.path.join('./listing',listing_file_name), 'rt')
        data = f.readlines()
        
        #listing file consists of various meta information w/ 
        #data[0]:=hdf_file_name; data[9:12]:=RingLON; 
        #data[13:16]:=RingLAT
        current_file_links = [[line.split(',')[0],line.split(',')[9],
                                line.split(',')[10],line.split(',')[11],
                                line.split(',')[12],line.split(',')[13],
                                line.split(',')[14],line.split(',')[15],
                                line.split(',')[16]] for line in data[3:]]    
        
        #loop over entries and check for overlap with specified aoi's
        for coordinates in current_file_links:
            #check for overlap to exclude non-matching links from listing
            overlap_aois = []
            
            #create polygon/bounding box from swath coordinates
            lon = np.array([float(coordinates[1]),float(coordinates[2]),
                            float(coordinates[3]),float(coordinates[4])])
            lat = np.array([float(coordinates[5]),float(coordinates[6]),
                            float(coordinates[7]),float(coordinates[8])])
                
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
            
            if len(overlap_aois)>0:
                #compile download link and append to swath/search list
                hdf_file = coordinates[0]
                hdf_split = hdf_file.split('.')
                geoloc_file_url = f'{self.url["mxd03"]}{yy}/{jj}/{hdf_file}'
                #compile meta output
                aoi_list = [aoi for aoi,frc in overlap_aois]
                frc_list = [frc for aoi,frc in overlap_aois]
                #append to global lists
                tags.append('.'.join(hdf_split[1:4]))
                mxd03.append(geoloc_file_url)
                meta.append([aoi_list,frc_list])
        
        #close file connection
        f.close()
        
        #return processed listing info
        return tags, mxd03, meta
                
    def process_mxd02_listing_file(self,lfn: str) -> list:
        """
        Parameters
        ----------
        lfn : str
            listing_file_name

        Returns
        -------
        list
            List of MXD02 swath download URL's

        """
        pass
                
    def download_listing(self):
        pass
    
    def skip_existing_files(self):
        pass
    
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
    
    
"""
Processing::Resampling to Grid
"""
class Resample(object):
    """
    Handles all the resample process of the current tobe processed swath data
    """
    def __init__(self):
        pass
    
    


