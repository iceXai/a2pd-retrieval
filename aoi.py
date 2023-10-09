#!/usr/bin/env python3
# coding: utf-8
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[] 
import pyresample as pr
import numpy as np

import os
import yaml

from pyproj import CRS
from pyproj import Transformer
from shapely.geometry import Polygon
from shapely.ops import transform

from loguru import logger



# In[]
class AoiGrid(object):
    def __init__(self, grid_file: str, scl: float):
        #loads the yaml reference file
        with open(os.path.join(os.getcwd(), 'aoi', grid_file)) as f:
            self.grid_def = yaml.safe_load(f)

        #set the aoi grid
        self.set_grid(scl)
        #set transformer for CRS transformation
        self.set_transformer()
        #transform AOI
        self.set_aoi_poly()
        
    def get_meta_spec(self, specification: str) -> str:
        return self.grid_def['meta'][specification]
            
    def get_grid_spec(self, specification: str) -> str:
        return self.grid_def['grid'][specification]
    
    @property
    def hemisphere(self) -> str:
        return self.get_meta_spec('hemisphere').lower()
    
    @property
    def area_id(self) -> str:
        return self.get_grid_spec('area_id')
    
    @property
    def description(self) -> str:
        return self.get_grid_spec('description')
    
    @property
    def proj_id(self) -> str:
        return self.get_grid_spec('proj_id')
    
    @property
    def projection(self) -> str:
        return self.get_grid_spec('projection')
    
    @property
    def width(self) -> int:
        return self.get_grid_spec('width')
    
    @property
    def height(self) -> int:
        return self.get_grid_spec('height')
    
    @property
    def area_extent(self) -> tuple:
        EXTENT = self.get_grid_spec('area_extent')
        return tuple([float(e) for e in EXTENT[1:-1].split(',')])
        
    def set_grid(self, scale_factor: float) -> None:
        ID       = self.area_id
        LONGNAME = self.description
        PROJID   = self.proj_id
        PROJ     = self.projection
        WIDTH    = self.width * scale_factor
        HEIGHT   = self.height * scale_factor
        EXTENT   = self.area_extent
        #pyresample area definition
        self.grid = pr.geometry.AreaDefinition(ID, LONGNAME, PROJID, PROJ,
                                               WIDTH, HEIGHT, EXTENT)
        
    def get_grid(self) -> object:
        return self.grid
    
    @property
    def target_epsg(self) -> str:
        HEMISPHERE = self.hemisphere
        if HEMISPHERE == 'south':
            TARGET_EPSG = 'EPSG:6932'
        else:
            TARGET_EPSG = 'EPSG:6931'
        return TARGET_EPSG
    
    def set_transformer(self) -> None:
        SOURCE = CRS(self.projection)
        TARGET = CRS(self.target_epsg)
        TRANSFORMER = Transformer.from_crs(SOURCE, TARGET, always_xy=True)
        self.transformer = TRANSFORMER.transform
        
    def get_transformer(self) -> object:
        return self.transformer

    def set_aoi_poly(self) -> None:
        #get lat/lon from grid definition
        lon, lat = self.grid.get_lonlats()
        #create shapely polygon
        AOI_POLY = Polygon([[lon[ 0, 0],lat[ 0, 0]],
                            [lon[-1, 0],lat[-1, 0]],
                            [lon[-1,-1],lat[-1,-1]],
                            [lon[ 0,-1],lat[ 0,-1]]])
        TRANSFORMER = self.get_transformer()
        self.aoi_poly = transform(TRANSFORMER, AOI_POLY)
        
    def get_aoi_poly(self) -> object:
        return self.aoi_poly            

    def check_overlap_with_aoi(self, listing_entry: list) -> (bool, list):
        """
        Parameters
        ----------
        listing_entry : list
            Entry of the current retrieved listing with among other parameters 
            the swath bouding ring coordinates

        Returns
        -------
        (bool, list)
            Returns wether the swath lies within this AOI and states in that 
            case also the fractional coverage
            
        This is the primary high-level function to be called to check wether 
        any given swath is within the bounds of the specified AOI by first 
        creating a swath polygon and then checking its intersection with the
        already computed AOI polygon
        """
        
        """ Create Swath Polygon """
        #create polygon/bounding box from swath coordinates in listing entry
        lon = np.array([float(listing_entry[1]),float(listing_entry[2]),
                        float(listing_entry[3]),float(listing_entry[4])])
        lat = np.array([float(listing_entry[5]),float(listing_entry[6]),
                        float(listing_entry[7]),float(listing_entry[8])])
                    
        #create shapely polygon
        SWATH_POLY = Polygon([[lon[0],lat[0]],
                              [lon[3],lat[3]],
                              [lon[2],lat[2]],
                              [lon[1],lat[1]]])
        #validate swath being in the correct hemisphere 
        HEMISPHERE = self.hemisphere
        ENVELOPE = SWATH_POLY.bounds
        if HEMISPHERE == 'south':
            IN_HEMISPHERE = ENVELOPE[3] < (-30.0)
        else:
            IN_HEMISPHERE = ENVELOPE[1] > (30.0)
            
        """ Check for Overlap with AOI Polygon"""
        #transform swath polygons
        AOI_POLY = self.get_aoi_poly()
        TRANSFORMER = self.get_transformer()
        SWATH_POLY = transform(TRANSFORMER, SWATH_POLY)

        #check for overlap
        SWATH_INTERSECTS = SWATH_POLY.intersects(AOI_POLY)
        if SWATH_INTERSECTS and IN_HEMISPHERE:
            OVERLAP = True
            AOI_AREA = AOI_POLY.area
            INTERSECTION_AREA = AOI_POLY.intersection(SWATH_POLY).area
            FRACTION = np.round((INTERSECTION_AREA / AOI_AREA)*100,2)
        else:
            OVERLAP = False
            FRACTION = 0.0
        
        #return to caller
        return OVERLAP, FRACTION


# In[]
class AoiData(object):
    def __init__(self, aois: list, scale_factor: float):
        #status
        logger.info('Compile AOI grid specifications...')
        #loads the yaml reference file
        with open(os.path.join(os.getcwd(), 'aoi', 'list_of_aois.yaml')) as f:
            refaois = yaml.safe_load(f)
        self.refs = refaois['aois']
        #allocate container
        self.aoi_dict = {}
        #loop over all user specified aoi's
        for aoi in aois:
            #status
            logger.info(f'Initiate AOI: {aoi}')
            #get file name of grid file
            aoi = aoi.lower()
            fn = self.get_aoi_grid_file(aoi)
            #get aoi grid
            grid = self.initiate_aoi_grid(fn, scale_factor)
            #store it
            self.aoi_dict[aoi] = grid
        
    def get_aoi_grid_file(self, aoi: str) -> str:
        return self.refs[aoi]
    
    def initiate_aoi_grid(self, grid_file: str, scl: float) -> AoiGrid:
        return AoiGrid(grid_file, scl)
    
    def get_aois(self) -> list:
        return self.aoi_dict.keys()
    
    def get_aoi(self, aoi: str) -> AoiGrid:
        return self.aoi_dict[aoi]
