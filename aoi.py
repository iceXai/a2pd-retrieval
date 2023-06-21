#!/usr/bin/env python3
# coding: utf-8
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[] 
import pyresample as pr
import numpy as np

from osgeo import ogr
from osgeo import osr
from osgeo import gdal


# In[]
class AOI(object):
    """
    class to keep all necessary aoi specifications
    
    TODO: this could probably use some refactoring and clean-up as well but 
          for now keep as is
    """
    def __init__(self,aoi,n_cores,hemisphere):
        #store cores to use
        self.n_cores         = n_cores
        #store hemisphere to use
        self.hemisphere      = hemisphere
        #transformations/projections for overlap check
        self.prj_epsg_source = 4326
        #store gdal version
        gdal_version         = gdal.__version__
        self.gdal            = int(gdal_version[0])
        #placeholder for swath and overlap check
        self.swath           = None
        #set the specified aoi
        self.set_aoi(aoi)

    def get_aoi_grid(self):
        return self.grid
        
    def get_aoi_polygon(self):
        return self.polygon
    
    def set_swath_polygon(self,swath_coordinates):     
        self.swath = self.create_swath_poly(swath_coordinates)
        
    def get_swath_polygon(self):
        return self.swath
    
    def check_overlap_with_aoi(self):
        #additional check in case of wrong hemisphere due to projection problems 
        swath_env = self.swath.GetEnvelope()
        if self.hemisphere == 'south':
            if self.gdal >= 3:
                inHemisphere = swath_env[0] < (-30.0)
            else:
                inHemisphere = swath_env[2] < (-30.0)
        else:
            if self.gdal >= 3:
                inHemisphere = swath_env[0] > (30.0)
            else:
                inHemisphere = swath_env[2] > (30.0)
            
        #check for overlap
        if self.swath != None:
            #define soure/target spatial reference
            insrs = osr.SpatialReference()
            insrs.ImportFromEPSG(self.prj_epsg_source)
            outsrs = osr.SpatialReference()
            outsrs.ImportFromEPSG(self.prj_epsg_target)
            #set up transformation
            crs_transformation = osr.CoordinateTransformation(insrs,outsrs)
            #transform aoi/swath polygons
            self.polygon.Transform(crs_transformation)
            self.swath.Transform(crs_transformation)

            #check for overlap
            intersect_geom = self.polygon.Intersection(self.swath.Buffer(0))
            if intersect_geom is not None and intersect_geom.Area()>0 \
                and inHemisphere:
                overlap = True
                frac = np.round(intersect_geom.Area()/
                                self.polygon.Area()*100.,2)
            else:
                overlap = False
                frac = 0.0
            
            #reverse transformation
            crs_transformation = osr.CoordinateTransformation(outsrs,insrs)
            #transform aoi/swath polygons
            self.polygon.Transform(crs_transformation)
            self.swath.Transform(crs_transformation)
            
            #return
            return overlap, frac
    
    def set_aoi(self,aoi):
        #choose grid/polygon definitions
        if aoi == 'atka':
            area_id     = 'atkabay'
            description = 'Antarctic Atka-Bay Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 222
            height      = 111
            area_extent = (-11.0,-71.0,-5.0,-70.0)
        elif aoi == 'berkner':
            area_id     = 'berkner'
            description = 'Antarctic Berkner Island Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 510
            height      = 390
            area_extent = (-50.0,-78.5,-30.0,-75.0)
        elif aoi == 'brunt':
            area_id     = 'brunt'
            description = 'Antarctic Brunt Ice Shelf Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 460
            height      = 445
            area_extent = (-34.0,-77.0,-18.0,-73.0)
        elif aoi == 'weddell':
            area_id     = 'weddell'
            description = 'Antarctic Southern Weddell Sea Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 1290
            height      = 1060
            area_extent = (-66.0,-78.5,-18.0,-69.0)
        elif aoi == 'tnb':
            area_id     = 'terranovabay'
            description = 'Antarctic Terra Nova Bay Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 560
            height      = 300
            area_extent = (160.0,-77.5,170.0,-72.5)
        elif aoi == 'ross-west':
            area_id     = 'ross'
            description = 'Antarctic Ross Sea Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 530
            height      = 195
            area_extent = (-180.,-78.75,-158,-77.0)
        elif aoi == 'ross-east':
            area_id     = 'ross'
            description = 'Antarctic Ross Sea Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 490
            height      = 190
            area_extent = (160.,-78.2,180,-76.5)
        elif aoi == 'prydz':
            area_id     = 'prydzbay'
            description = 'Antarctic Prydz Bay Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 1300
            height      = 556
            area_extent = (60.0,-70.0,90.0,-65.0)
        elif aoi == 'darnley':
            area_id     = 'capedarnley'
            description = 'Antarctic Cape Darnley Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 435
            height      = 445
            area_extent = (65.0,-70.0,75.0,-66.0)
        elif aoi == 'barrier':
            area_id     = 'barrier'
            description = 'Antarctic West Ice Shelf (Barrier) Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 652
            height      = 556
            area_extent = (75.0,-70.0,90.0,-65.0)
        elif aoi == 'mertz':
            area_id     = 'mertz'
            description = 'Antarctic Mertz Glacier Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 435
            height      = 278
            area_extent = (140.0,-68.0,150.0,-65.5)
        elif aoi == 'vincennes':
            area_id     = 'vincennesbay'
            description = 'Antarctic Vincennes Bay Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 266
            height      = 334
            area_extent = (126.0,-68.0,132.0,-65.0)
        elif aoi == 'amundsen':
            area_id     = 'amundsen'
            description = 'Antarctic Amundsen Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 260
            height      = 334
            area_extent = (-116.0,-75.0,-108.0,-72.0)
        elif aoi == 'greater-amundsen':
            area_id     = 'greater-amundsen'
            description = 'Antarctic Greater Amundsen Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 520
            height      = 390
            area_extent = (-116.0,-75.5,-100.0,-72.0)
        elif aoi == 'getz-west':
            area_id     = 'getz-west'
            description = 'Antarctic Western Getz Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 368
            height      = 222
            area_extent = (-135.0,-75.0,-123.0,-73.0)
        elif aoi == 'getz-east':
            area_id     = 'getz-east'
            description = 'Antarctic Eastern Getz Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 306
            height      = 222
            area_extent = (-120.0,-75.0,-110.0,-73.0)
        elif aoi == 'dibble':
            area_id     = 'dibble'
            description = 'Antarctic Dibble Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 272
            height      = 222
            area_extent = (132.0,-67.0,138.0,-65.0)
        elif aoi == 'dalton':
            area_id     = 'dalton'
            description = 'Antarctic Dalton Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 716
            height      = 278
            area_extent = (112.0,-67.5,128.0,-65.0)         
        elif aoi == 'shackleton':
            area_id     = 'shackleton'
            description = 'Antarctic Shackleton Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 448
            height      = 390
            area_extent = (90.0,-67.5,100.0,-64.0)
        elif aoi == 'ronne':
            area_id     = 'ronne'
            description = 'Antarctic Ronne Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 540
            height      = 490
            area_extent = (-64.0,-78.0,-45.0,-73.6)
        elif aoi == 'thwaites':
            area_id     = 'thwaites'
            description = 'Antarctic Thwaites Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 208
            height      = 156
            area_extent = (-111.0,-75.5,-104.0,-74.1)
        elif aoi == 'pineisland':
            area_id     = 'pineisland'
            description = 'Antarctic PineIsland Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 174
            height      = 390
            area_extent = (-105.5,-75.5,-100.0,-72.0)
        elif aoi == 'larsen-b':
            area_id     = 'larsen-b'
            description = 'Antarctic Larsen-B Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 256
            height      = 156
            area_extent = (-62.5,-66.2,-57.0,-64.8)
        elif aoi == 'larsen-c':
            area_id     = 'larsen-c'
            description = 'Antarctic Larsen-C Grid'
            proj_id     = 'lonlat'
            projection  = '+proj=longlat +ellps=WGS84 +datum=WGS84'
            width       = 320
            height      = 334
            area_extent = (-65.5,-69.0,-58.0,-66.0)          
        else:
            raise Exception('Unrecognized AOI-tag specified!')
    
        #create area definition
        self.grid = pr.geometry.AreaDefinition(area_id,description,proj_id,\
                                               projection,width,height,\
                                               area_extent,\
                                               nprocs=self.n_cores)
            
        #store polygon and grid specs
        self.polygon = self.create_aoi_poly()
        
        #store target projection epsg code for overlap check
        if self.hemisphere == 'south':
            self.prj_epsg_target = 6932
        else:
            self.prj_epsg_target = 6931
        

    def create_aoi_poly(self):
        #get lat/lon from grid definition
        lon, lat = self.grid.get_lonlats()
        
        #create ring
        ring = ogr.Geometry(ogr.wkbLinearRing)
        if self.gdal >= 3:
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

        #return ogr polygon
        return poly
        
    
    def create_swath_poly(self,swath_coords):
        #create ring
        ring = ogr.Geometry(ogr.wkbLinearRing)
        #fill it
        if self.gdal >= 3:
            for coord in swath_coords:
                ring.AddPoint(float(coord[0]),float(coord[1]))
        else:
            for coord in swath_coords:
                ring.AddPoint(float(coord[1]),float(coord[0]))
            
        #create polygon from ring
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)

        #return ogr polygon
        return poly
