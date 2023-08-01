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

from osgeo import ogr
from osgeo import osr
from osgeo import gdal
from loguru import logger


# In[]
class AoiData(object):
    def __init__(self, aois: list):
        #status
        logger.info('Compile AOI grid specifications...')
        #loads the yaml reference file
        with open(os.path.join(os.getcwd(), 'aoi', 'list_of_aois.yaml')) as f:
            refaois = yaml.safe_load(f)
        self.refs = refaois['aois']
        #allocate container
        aoi_dict = {}
        #loop over all user specified aoi's
        for aoi in aois:
            #status
            logger.info(f'Initiate AOI: {aoi}')
            #get file name of grid file
            aoi = aoi.lower()
            fn = self.get_aoi_grid_file(aoi)
            #get aoi grid
            grid = self.initiate_aoi_grid(fn)
            #store it
            aoi_dict[aoi] = grid
        
    def get_aoi_grid_file(self, aoi: str) -> str:
        return self.refs[aoi]
    
    def initiate_aoi_grid(self, grid_file: str) -> object:
        return AoiGrid(grid_file)
    
    def get_aois(self) -> list:
        return aoi_dict.keys()
    
    def get_aoi(self, aoi: str) -> object:
        return aoi_dict[aoi]


# In[]
class AoiGrid(object):
    def __init__(self, grid_file: str):
        #loads the yaml reference file
        with open(os.path.join(os.getcwd(), 'aoi', grid_file)) as f:
            self.grid_def = yaml.safe_load(f)

        #set the aoi grid
        self.set_grid()
        #set derived aoi polygon
        self.set_aoi_poly()
        #set the target EPSG code for any projection needed in the 
        #overlap check
        self.set_target_epsg()
        
    def get_gdal_version(self) -> int:
        GDAL_VERSION = gdal.__version__[0]
        return int(GDAL_VERSION)
        
    def get_meta_spec(self, specification: str) -> str:
        return self.grid_def['meta'][specification]

    def get_hemisphere(self) -> str:
        return self.get_meta_spec('hemisphere').lower()
        
    def get_grid_spec(self, specification: str) -> str:
        return self.grid_def['grid'][specification]
    
    def get_area_id(self) -> str:
        return self.get_grid_spec('area_id')
    
    def get_description(self) -> str:
        return self.get_grid_spec('description')
    
    def get_proj_id(self) -> str:
        return self.get_grid_spec('proj_id')
    
    def get_projection(self) -> str:
        return self.get_grid_spec('projection')
    
    def get_width(self) -> int:
        return self.get_grid_spec('width')
    
    def get_height(self) -> int:
        return self.get_grid_spec('height')
    
    def get_area_extent(self) -> tuple:
        EXTENT = self.get_grid_spec('area_extent')
        return tuple([float(e) for e in EXTENT[1:-1].split(',')])
        
    def set_grid(self) -> None:
        ID       = self.get_area_id()
        LONGNAME = self.get_description()
        PROJID   = self.get_proj_id()
        PROJ     = self.get_projection()
        WIDTH    = self.get_width()
        HEIGHT   = self.get_height()
        EXTENT   = self.get_area_extent()
        #pyresample area definition
        self.grid = pr.geometry.AreaDefinition(ID, LONGNAME, PROJID, PROJ,
                                               WIDTH, HEIGHT, EXTENT)
        
    def get_grid(self) -> object:
        return self.grid

    def set_aoi_poly(self) -> None:
        #get lat/lon from grid definition
        lon, lat = self.grid.get_lonlats()
        
        #create ring
        ring = ogr.Geometry(ogr.wkbLinearRing)
        GDAL_VERSION = self.get_gdal_version()
        if GDAL_VERSION >= 3:
            ring.AddPoint(lat[ 0, 0],lon[ 0, 0])
            ring.AddPoint(lat[-1, 0],lon[-1, 0])
            ring.AddPoint(lat[-1,-1],lon[-1,-1])
            ring.AddPoint(lat[ 0,-1],lon[ 0,-1])
            ring.AddPoint(lat[ 0, 0],lon[ 0, 0])
        else:
            ring.AddPoint(lon[ 0, 0],lat[ 0, 0])
            ring.AddPoint(lon[-1, 0],lat[-1, 0])
            ring.AddPoint(lon[-1,-1],lat[-1,-1])
            ring.AddPoint(lon[ 0,-1],lat[ 0,-1])
            ring.AddPoint(lon[ 0, 0],lat[ 0, 0])
            
        #create polygon from ring
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring) 
        #set ogr polygon
        self.aoi_poly = poly
        
    def get_aoi_poly(self) -> object:
        return self.aoi_poly
    
    def set_target_epsg(self) -> None:
        HEMISPHERE = self.get_hemisphere()
        if HEMISPHERE == 'south':
            self.target_epsg = 6932
        else:
            self.target_epsg = 6931
            
    def get_target_epsg(self) -> int:
        return self.target_epsg
    
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
            
        #compile swath polyon
        coordinates = [[lat[0],lon[0]],
                       [lat[3],lon[3]],
                       [lat[2],lon[2]],
                       [lat[1],lon[1]],
                       [lat[0],lon[0]]]
        #create ring
        ring = ogr.Geometry(ogr.wkbLinearRing)
        
        #fill it
        GDAL_VERSION = self.get_gdal_version()
        if GDAL_VERSION >= 3:
            for coord in coordinates:
                ring.AddPoint(float(coord[0]),float(coord[1]))
        else:
            for coord in coordinates:
                ring.AddPoint(float(coord[1]),float(coord[0]))
            
        #create polygon from ring
        SWATH_POLY = ogr.Geometry(ogr.wkbPolygon)
        SWATH_POLY.AddGeometry(ring)

        #validate swath being in the correct hemisphere 
        ENVELOPE = SWATH_POLY.GetEnvelope()
        HEMISPHERE = self.get_hemisphere()
        if HEMISPHERE == 'south':
            if GDAL_VERSION >= 3:
                inHemisphere = ENVELOPE[0] < (-30.0)
            else:
                inHemisphere = ENVELOPE[2] < (-30.0)
        else:
            if GDAL_VERSION >= 3:
                inHemisphere = ENVELOPE[0] > (30.0)
            else:
                inHemisphere = ENVELOPE[2] > (30.0)
            
        """ Check for Overlap with AOI Polygon"""
        #define soure/target spatial reference
        SOURCE_EPSG = self.get_projection()
        TARGET_EPSG = self.get_target_epsg()
        insrs = osr.SpatialReference()
        insrs.ImportFromEPSG(SOURCE_EPSG)
        outsrs = osr.SpatialReference()
        outsrs.ImportFromEPSG(TARGET_EPSG)
        #set up transformation
        crs_transformation = osr.CoordinateTransformation(insrs,outsrs)
        #transform aoi/swath polygons
        AOI_POLY = self.get_aoi_poly()
        AOI_POLY.Transform(crs_transformation)
        SWATH_POLY.Transform(crs_transformation)

        #check for overlap
        INTERSECT_GEOMETRY = AOI_POLY.Intersection(SWATH_POLY.Buffer(0))
        if INTERSECT_GEOMETRY is not None \
            and INTERSECT_GEOMETRY.Area()>0 \
            and inHemisphere:
            OVERLAP = True
            FRACTION = np.round(INTERSECT_GEOMETRY.Area()/
                                AOI_POLY.Area()*100.,2)
        else:
            OVERLAP = False
            FRACTION = 0.0
        
        #TODO is this still necessary?!
        #reverse transformation
        crs_transformation = osr.CoordinateTransformation(outsrs,insrs)
        #transform aoi/swath polygons
        AOI_POLY.Transform(crs_transformation)
        SWATH_POLY.Transform(crs_transformation)
        
        #return to caller
        return OVERLAP, FRACTION
