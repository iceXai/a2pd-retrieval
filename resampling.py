# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[] 
from pyresample.kd_tree import get_neighbour_info
from pyresample.kd_tree import get_sample_from_neighbour_info
import pyresample as pr
import numpy as np


# In[]


"""
Resampling to Grid
"""

class Resample(object):
    """
    Handles all the resample process of the current tobe processed swath data
    """
    def __init__(self):
        #for the grouping/stacking process
        self.coord = {}
        self.stack = {}
        self.names = {}
        #for the overall resampled data
        self.resampled_aoi_data = {}
    
    """ Getters/Setters """
    def get_resampled_data(self) -> dict:
        return self.resampled_aoi_data
    
    def set_aoi(self, aoi: dict) -> None:
        self.aoi = aoi
        
        
    def set_data(self, data: dict) -> None:
        self.data = data
        
    """ Resampling """
    def add_data_to_group(self, var: str, lon: str, lat: str) -> None:
        """
        Parameters
        ----------
        var : str
            Name of the varibale (keys from the variables to process) to be 
            added
        lon : str
            Name of the longitude variable (taken from the resample
                                            dictionary)
        lat : str
            Name of the latitude variable (also taken from the resample 
                                           dictionary)

        Returns
        -------
        None
            Adds the data (names and coordinates) to the global dictionaries 
            to be grouped afterwards
        """
        variable  = self.data.get_data(var)
        longitude = self.data.get_data(lon)
        latitude  = self.data.get_data(lat)
        
        #check if respective group already exists
        key = f'{lon}{lat}'
        if key not in self.coord.keys():
            #if not create it 
            self.coord[key] = [longitude,latitude]
            self.stack[key] = []
            self.names[key] = []
        #place dataset names and actual data in respective dicts
        self.stack[key].append(variable)
        self.names[key].append(var)
    
        
    def add_groups_to_resample_stack(self) -> None:
        """
        Stacks the already built groups for the final resample process
        """
        stack = {}
        for idx,key in enumerate(self.coord):
            stack[f'grp{idx}'] = {'grid': self.coord[key],
                                  'dset': self.names[key],
                                  'data': np.stack(self.stack[key],axis=2)
                                  }
        #return
        self.resample_stack = stack
        
        
    def resample(self, resample_aois: list) -> dict:
        for aoi in resample_aois:
            #load resampled data into a data dictionary
            rsd = {}
        
            #retrieve aoi grid to resample to
            aoi_grid = self.aoi[aoi].get_aoi_grid()

            #return reference-grid latitude/longitude
            ref_grid_lon, ref_grid_lat = aoi_grid.get_lonlats()
            rsd['lon'] = ref_grid_lon
            rsd['lat'] = ref_grid_lat
            #retrieve resample group keys
            grp_keys = self.resample_stack.keys()
        
            #loop through groups
            for grp in grp_keys:
                #get lon/lat values
                lon = self.resample_stack[grp]['grid'][0]
                lat = self.resample_stack[grp]['grid'][1]
                
                #set-up swath definition
                swath_def = pr.geometry.SwathDefinition(lons=lon, lats=lat)
                #resample using kd tree
                data_stack = self.resample_stack[grp]['data']
                rs = self._kd_tree_resample(swath_def, aoi_grid, data_stack)
    
                #append to dict
                names_stack = self.resample_stack[grp]['dset']
                rsd = {**rsd,**dict(zip(names_stack, rs))}
                
        self.resampled_aoi_data[aoi] = rsd
        

    def _kd_tree_neighbours(self, swath_def, aoi_grid) -> tuple:
        in_idx, out_idx, idx, _ = get_neighbour_info(swath_def,
                                                     aoi_grid,
                                                     radius_of_influence=5000,
                                                     neighbours=1)
        return in_idx, out_idx, idx
    
    
    def _kd_tree_resample(self, swath_def, aoi_grid, stack) -> np.array:
        in_idx, out_idx, idx = self._kd_tree_neighbours(swath_def, aoi_grid)
        
        grid_shape = aoi_grid.shape
        
        rs = get_sample_from_neighbour_info('nn', grid_shape, stack, 
                                            in_idx, out_idx, idx)
        
        return np.transpose(rs, (2,0,1))
        