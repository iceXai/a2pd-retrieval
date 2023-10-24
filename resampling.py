# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[] 
from pyresample.kd_tree import get_neighbour_info
from pyresample.kd_tree import get_sample_from_neighbour_info
from dataclasses import dataclass
from typing import List, Dict

import pyresample as pr
import numpy as np

from aoi import AoiGrid
from data import SwathVariable
from data import DataVariable


# In[]

@dataclass
class ResampledVariable(SwathVariable):
    """ Databaseclass to keep track of a resampled variable """
    aoi: str
    data: np.array
    
    @property
    def shape(self) -> tuple:
        return self.data.shape

@dataclass
class ResampleStack:
    """ 
    Container class to take on a DataStack group and resample it; turning 
    all DataVariable's into ResampledVariable's
    """
    variables: List[DataVariable]
    lon: DataVariable
    lat: DataVariable
    aoi: AoiGrid
    
    def __len__(self) -> int:
        return len(self.variables)
    
    def __iter__(self):
        return iter(self.variables)
    
    def __getitem__(self, item: str) -> SwathVariable:
        if item in self.names:
            return [var for var in self.variables if item == var.name][0]
        else:
            return None
    
    @property
    def names(self) -> List[str]:
        return [var.name for var in self.variables]
    
    @property
    def datatypes(self) -> List[str]:
        return [var.datatype for var in self.variables]
    
    @property
    def size(self) -> int:
        return self.__len__()
    
    @property
    def datastack(self) -> np.array:
        stack = [var.data for var in self.variables]
        return np.stack(stack,axis=2)
    
    @property
    def metadata(self) -> List[Dict]:
        return [var.meta for var in self.variables]
    
    def resample(self):
        #stack available data
        STACK = self.datastack
        #set-up swath definition
        LON = self.lon.data
        LAT = self.lat.data
        SWATH_DEF = pr.geometry.SwathDefinition(lons = LON, lats = LAT)
        #resample using kd tree
        AOI_GRID = self.aoi
        self.stack = self._kd_tree_resample(SWATH_DEF, AOI_GRID, STACK)
        f64 = np.dtype('float64')
        f32 = np.dtype('float32')
        if self.stack.dtype == f64 or self.stack.dtype == f32:
            self.stack[np.where(self.stack == 0.0)] = np.nan
        
    def export(self) -> List[ResampledVariable]:
        NAMES = self.names
        AOI_ID = self.aoi.area_id
        DATATYPE = 'resampled'
        STACK = self.stack
        META = self.metadata
        return [ResampledVariable(name, DATATYPE, META[idx], AOI_ID, 
                                  STACK[idx]) 
                for idx, name in enumerate(NAMES)]
    
    def _kd_tree_neighbours(self, swath_def, aoi_grid) -> tuple:
        ROI = 5000
        in_idx, out_idx, idx, _ = get_neighbour_info(swath_def,
                                                     aoi_grid,
                                                     radius_of_influence=ROI,
                                                     neighbours=1)
        return in_idx, out_idx, idx
    
    def _kd_tree_resample(self, swath_def, aoi_grid, stack) -> np.array:
        in_idx, out_idx, idx = self._kd_tree_neighbours(swath_def, aoi_grid)
        
        grid_shape = aoi_grid.shape
        
        rs = get_sample_from_neighbour_info('nn', grid_shape, stack, 
                                            in_idx, out_idx, idx)
        
        return np.transpose(rs, (2,0,1))
        