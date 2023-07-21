# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""

# In[] 
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
        self.coord = {}
        self.stack = {}
        self.names = {}
    
    
    def set_aoi(self, aoi: dict) -> None:
        self.aoi = aoi
        
    def set_data(self, data: dict) -> None:
        self.data = data
        
    
    def resample_to_grid(self,aoi_to_resample):   
        #load resampled data into a data dictionary
        resampled_data_dict = {}
        
        #retrieve aoi grid to resample to
        aoi_grid = self.aoi[aoi_to_resample].return_aoi_grid()

        #return reference-grid latitude/longitude
        ref_grid_lon, ref_grid_lat = aoi_grid.get_lonlats()
        resampled_data_dict['lon'] = ref_grid_lon
        resampled_data_dict['lat'] = ref_grid_lat
        
        #regroup data
        resample_stack = self.regroup_data_to_resample()

        #retrieve resample group keys
        grp_keys = resample_stack.keys()
        
        #loop through groups
        for grp in grp_keys:
            #get lon/lat values
            lon = resample_stack[grp]['grid'][0]
            lat = resample_stack[grp]['grid'][1]
            
            #set-up swath definition
            swath_def = pr.geometry.SwathDefinition(lons=lon,
                                                    lats=lat)
            #get_projection_coordinates_from_lonlat()
            #get neighbour information
            in_idx,out_idx,idx,dist = \
                pr.kd_tree.get_neighbour_info(swath_def,aoi_grid,
                                              radius_of_influence=5000,
                                              neighbours=1,
                                              nprocs=self.cores)
            
            #resample group using neighbor info
            resampled = pr.kd_tree.get_sample_from_neighbour_info('nn',aoi_grid.shape,
                                                                  resample_stack[grp]['data'],
                                                                  in_idx,out_idx,idx)
            
            #rearrange order
            resampled = np.transpose(resampled,(2,0,1))

            #append to dict
            resampled_data_dict = {**resampled_data_dict,\
                                    **dict(zip(resample_stack[grp]['dset'],resampled))}
        
        #re-store data to the normal data_dict dictionary
        self.finalized_aoi_dict[aoi_to_resample] = resampled_data_dict
        
        
    def add_groups_to_resample_stack(self) -> dict:
        resample_stack = {}
        for i,key in enumerate(coord):
            idx = str(i+1)
            resample_stack[f'grp{idx}'] = {'grid': coord[key],
                                           'dset': names[key],
                                           'data': np.stack(stack[key],axis=2)
                                           }
        #return
        return resample_stack
        

    def add_data_to_group(self, var: str, lo: str, lat: str) -> None:
        """
        Parameters
        ----------
        var : str
            DESCRIPTION.
        lon : str
            DESCRIPTION.
        lat : str
            DESCRIPTION.

        Returns
        -------
        None
            Adds the data (names and coordinates) to the global dictionaries
        """
        variable  = self.data.get_data[var]
        longitude = self.data.get_data[lon]
        latitude  = self.data.get_data[lat]
        
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
    
        
        