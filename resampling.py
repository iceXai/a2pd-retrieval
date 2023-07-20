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
        self.coord = []
        self.stack = []
        self.names = []
    
    
    def set_aoi(self, aoi: dict) -> None:
        self.aoi = aoi
        
    
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
        
        
    def add_group_to_resample_stack(self) -> None:
        pass
        
    
    def add_data_to_group(self, 
                          meta: np.array,
                          data: np.array,
                          lon: np.array,
                          lat: np.array) -> None:
                
        import pdb; pdb.set_trace()

        
    
    
        
    def regroup_data_to_resample(self):
        #loop through all data sets that need to be resampled
        vars_list = self.data_dict.keys()
        
        #sensor-specific regrouping for faster resample procedure
        if self.sensor == 'MODIS':
            coord = []
            stack = []
            names = []
            for variable_dict_handle in vars_list:
                if variable_dict_handle in self.resample_dict:
                    stack.append(self.data_dict[variable_dict_handle])
                    names.append(variable_dict_handle)
                    if len(coord) == 0:
                        grid_lon, grid_lat = self.resample_dict[variable_dict_handle]
                        coord = [self.data_dict[grid_lon],
                                 self.data_dict[grid_lat]]  
            #wrap-up and return
            resample_stack = {'grp1': {'grid': coord,
                                       'dset': names,
                                       'data': np.stack(stack,axis=2)}}
            return resample_stack
        if self.sensor == 'SLSTR':
            coord = {}
            stack = {}
            names = {}
            #loop over variables to resample and group them based 
            #on their respective grid
            for variable_dict_handle in vars_list:
                if variable_dict_handle in self.resample_dict:
                    #retrieve grid
                    grid_lon, grid_lat = self.resample_dict[variable_dict_handle]
                    #check if respective group already exists
                    if grid_lon not in coord:
                        #if not create it 
                        coord[grid_lon] = [self.data_dict[grid_lon],
                                           self.data_dict[grid_lat]]
                        stack[grid_lon] = []
                        names[grid_lon] = []
                    #place dataset names and actual data in respective dicts
                    stack[grid_lon].append(self.data_dict[variable_dict_handle])
                    names[grid_lon].append(variable_dict_handle)
            #wrap-up
            resample_stack = {}
            for i,key in enumerate(coord):
                resample_stack['grp'+str(i+1)] = {'grid': coord[key],
                                                  'dset': names[key],
                                                  'data': np.stack(stack[key],
                                                                   axis=2)}
            #return
            return resample_stack
        
        
        