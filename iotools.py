# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[]
from __future__ import annotations

from meta import MetaVariable

##
from aoi import AoiGrid
from pyresample.kd_tree import get_neighbour_info
from pyresample.kd_tree import get_sample_from_neighbour_info
import pyresample as pr
##


from abc import ABC, abstractmethod
from pyhdf.SD import SD, SDC
from datetime import datetime, timedelta
from loguru import logger
from dataclasses import dataclass
from typing import List, Dict

import h5py
import os

import netCDF4 as nc
import numpy as np
import pandas as pd


""" Listing """
# In[]
class ListingIO(object):
    """ Class for all listing-related I/O """
    def __init__(self, out: str):
        self.OUTPUT_PATH = out
        
    def set_listing_file_name(self, lfn: str) -> None:
        self.path = os.path.join(self.OUTPUT_PATH, lfn)

    def to_csv(self, df: pd.DataFrame) -> None:
        df.to_csv(self.path, index=False)

    def from_csv(self) -> pd.DataFrame:
        return pd.read_csv(self.path)


""" Swath handling """
# In[]
@dataclass
class SwathVariable(ABC):
    name: str
    datatype: str

@dataclass
class DataVariable(SwathVariable):
    """ Databaseclass to keep track of and process a loaded variable """
    data: np.array
    
    @property
    def shape(self) -> tuple:
        return self.data.shape
    
    def process(self, metavar: MetaVariable) -> None:
        if metavar.process_parameter is not None:
            self._process(metavar)
    
    @abstractmethod
    def _process(self, metavar: MetaVariable) -> None:
        pass
    

@dataclass
class ResampledVariable(SwathVariable):
    """ Databaseclass to keep track of a resampled variable """
    aoi: str
    data: np.array
    
    @property
    def shape(self) -> tuple:
        return self.data.shape


@dataclass
class HDF4DataVariable(DataVariable):
    attributes: dict
    
    def _process(self, metavar: MetaVariable) -> None:
        #limit data
        FILL_VALUE = self.attributes['_FillValue'][0]
        VALID_MIN = self.attributes['valid_range'][0][0]
        VALID_MAX = self.attributes['valid_range'][0][1]
        DATA = self.data        
        #mask invalid entries
        INVALID = np.logical_or(DATA > VALID_MAX, 
                                DATA < VALID_MIN, 
                                DATA == FILL_VALUE)
        DATA[INVALID] = np.nan
        #apply offset if available
        PROCESS_SPECS = metavar.process_parameter
        IDX = metavar.stack_index
        if 'offset' in PROCESS_SPECS.keys():
            OFFSET = self.attributes[PROCESS_SPECS['offset']][0]
            DATA -= OFFSET[IDX]
        #apply scale if available
        if 'scale' in PROCESS_SPECS.keys() and IDX is None:
            SCALE = self.attributes[PROCESS_SPECS['scale']][0]
            DATA *= SCALE
        if 'scale' in PROCESS_SPECS.keys() and IDX is not None:
            SCALE = self.attributes[PROCESS_SPECS['scale']][0]
            DATA *= SCALE[IDX]
        #compute brightness temperature where applicable
        if 'wavelength' in PROCESS_SPECS.keys():
            #mask values below 0 that sometimes appear
            non_nan = ~np.isnan(DATA)
            non_nan[non_nan] = np.less(DATA[non_nan],0)
            DATA[non_nan] = np.nan
            DATA = self._calculate_Tb(DATA, PROCESS_SPECS['wavelength'])
        #override it
        self.data = DATA
        
    def _calculate_Tb(self, var, wavelength):
        #define constants
        H_PLANCK = 6.62607004 * 10**-34  #[Js]
        C_SOL    = 2.99792458 * 10**8  #[m/s]
        K_BOLTZ  = 1.38064852 * 10**-23  #[J/K]
        
        #calculate and return brightness temperature
        Tb = H_PLANCK * C_SOL / (K_BOLTZ * wavelength * 
             np.log((2.0 * H_PLANCK * C_SOL**2 * wavelength**(-5)) / 
                    (var * 10**6) + 1.0))
        return Tb

@dataclass
class NetCDFDataVariable(DataVariable):
    variable: str = None
    orientation: str = None
    gridtype: str = None
    exclude: np.array = None

    def _process(self, metavar: MetaVariable) -> None:
        import pdb; pdb.set_trace()
        pass
    

    
@dataclass
class DataStack:
    """ Container class to store and handle individual swath variables """
    variables: List[SwathVariable]
    
    def __len__(self) -> int:
        return len(self.variables)
    
    def __iter__(self):
        return iter(self.variables)
    
    def __getitem__(self, item: int | str) -> SwathVariable:
        if type(item) == str and item in self.names:
            return [var for var in self.variables if item == var.name][0]
        elif type(item) == int:
            return self.variables[item]
        else:
            return None
    
    @property
    def names(self) -> List[str]:
        return [var.name for var in self.variables]
    
    @property
    def datatypes(self) -> List[str]:
        return [var.datatype for var in self.variables]
    
    @property
    def aois(self) -> Dict[str, np.array]:
        aoi_dict = {}
        aoi_list = [var.aoi for var in self.variables if hasattr(var, 'aoi')]
        aoi_array = np.array(aoi_list)
        unique_aois = np.unique(aoi_array)
        for aoi in unique_aois:
            aoi_dict[aoi] = np.where(aoi_array==aoi)[0]
        return aoi_dict
    
    @property
    def size(self) -> int:
        return self.__len__()
    
    def subset_by_datatype(self, datatype: str) -> DataStack:
        datatypes = self.datatypes
        if datatype in datatypes:
            idx = [idx for idx, dt in enumerate(datatypes) if dt == datatype]
            subset_vars = [self.variables[i] for i in idx]
            return DataStack(subset_vars)
        else:
            return None
        
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
        self.stack[np.where(self.stack == 0.0)] = np.nan
        
    def export(self) -> List[ResampledVariable]:
        NAMES = self.names
        AOI_ID = self.aoi.area_id
        DATATYPE = 'resampled'
        STACK = self.stack
        return [ResampledVariable(name, DATATYPE, AOI_ID, STACK[idx]) 
                for idx, name in enumerate(NAMES)]
    
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

        

class SwathInput(ABC):
    """ Parentclass for all input-related swath operations """
    @abstractmethod
    def load(self, path: str) -> None:
        """
        Parameters
        ----------
        path : str
            Path to the file/swath to load

        Returns
        -------
        None
            Stores the specific file handle, e.g., within self.fh
        """
        pass
    
    @abstractmethod
    def get_var(self, name: str, datatype: str, variable: str, 
                idx: str = None, **kwargs) -> SwathVariable:
        """
        Parameters
        ----------
        name : str
            DESCRIPTION.
        datatype : str
            DESCRIPTION.
        variable : str
            DESCRIPTION.
        idx : str, optional
            DESCRIPTION. The default is None.
        **kwargs : dict, optional
            additional key-word arguments from the MetaDataVariable 
            dataclass's input_parameter field

        Returns
        -------
        SwathDataVariable
            Returns a SwathDataVariable dataclass or it child with the 
            corresponding data
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Returns
        -------
        None
            Closes the opened file handle by load()
        """
        pass
    

class SwathOutput(ABC):
    """ Parentclass for all output-related swath operations """
    @abstractmethod
    def create(self, path: str) -> None:
        """
        Parameters
        ----------
        path : str
            Path to the output file (to be created)

        Returns
        -------
        None
            Creates the output file together with global attributes
        """
        pass
    
    @abstractmethod
    def set_var(self, data: np.array, group: str, variable: str,
                longname: str) -> None:
        """
        Parameters
        ----------
        data : np.array
            DESCRIPTION.
        group : str
            DESCRIPTION.
        variable : str
            DESCRIPTION.
        longname : str
            DESCRIPTION.

        Returns
        -------
        None
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Returns
        -------
        None
            Closes the opened file handle by save()
        """
        pass

# In[]
class HDF4SwathInput(SwathInput):
    def load(self, path: str) -> None:
        self.fh = SD(path,SDC.READ)
    
    def get_var(self, metavar: MetaVariable) -> HDF4DataVariable:
        #select scientific data set (sds) and corresponding attributes
        VAR = VAR = metavar.input_parameter['variable']
        sds = self.fh.select(VAR)
        attributes = sds.attributes(full=1)
        #index in sds
        IDX = metavar.stack_index
        if IDX is not None:
            data = sds[IDX,:,:].astype(np.float32)
        else:
            data = sds[:,:].astype(np.float32)
        #initialize swath variable data class
        DATA = {'name': metavar.name,
                'datatype': metavar.datatype,
                'data': data,
                'attributes': attributes,
                }
        return HDF4DataVariable(**DATA)
    
    def close(self):
        self.fh.end()


class NetCDFSwathInput(SwathInput):
    def load(self, path: str) -> None:
        self.fh = nc.Dataset(path, 'r')
    
    def get_var(self, metavar: MetaVariable) -> NetCDFDataVariable:
        VAR = metavar.input_parameter['variable']
        data = self.fh.variables[VAR][:,:]
        if metavar.process_parameter is not None:
            EXCLUDE_VAR = metavar.process_parameter['exclusion_variable']
            exclusion_data = self.fh.variables[EXCLUDE_VAR][:,:]
        else:
            exclusion_data = None
        GRIDTYPE = metavar.input_parameter['gridtype']
        ORIENTATION = metavar.input_parameter['orientation']
        #initialize swath variable data class
        DATA = {'name': metavar.name,
                'datatype': metavar.datatype,
                'data': data,
                'variable': VAR,
                'gridtype': GRIDTYPE,
                'orientation': ORIENTATION,
                'exclude': exclusion_data,
                }
        return NetCDFDataVariable(**DATA)

    def close(self) -> None:
        self.fh.close()
        
        
class HDF5SwathInput(SwathInput):
    def load(self, path: str) -> None:
        pass
    
    def get_var(self, metavar: MetaVariable) -> HDF5DataVariable:
        pass

    def close(self) -> None:
        self.fh.close()
        

class HDF5SwathOutput(SwathOutput):
    def create(self, path: str) -> None:
        #create file and open file handle
        self.fh = h5py.File(path,'w')
        #set global attributes
        AUTHOR = "Dr. Stephan Paul (AWI/iceXai)"
        self.fh.attrs.create("author", AUTHOR)
        EMAIL = "stephan.paul@posteo.net"
        self.fh.attrs.create("contact", EMAIL)
        TIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.fh.attrs.create("created", TIMESTAMP)
    
    def set_var(self, data: np.array, group: str, variable: str,
                longname: str) -> None:
        #create dataset in file
        h5ds = self.fh.create_dataset(f'{group}/{variable}',
                                      data=data,
                                      compression="gzip",
                                      compression_opts=9)
        #set data attributes
        h5ds.attrs.create("long_name", longname)
        h5ds.attrs.create("valid_range", [np.nanmin(data),np.nanmax(data)])
    
    def close(self) -> None:
        self.fh.close()


# In[]
class SwathIO(object):
    def __init__(self, swath_in: SwathInput, swath_out: SwathOutput) -> None:
        self.swath_in = swath_in
        self.swath_out = swath_out
        
    def open_input_swath(self, path: str) -> None:
        self.swath_in.load(path)
        
    def get_variable(self, metavar: MetaVariable) -> SwathVariable:
        #TODO change as in set_variable and remove **kwargs here
        return self.swath_in.get_var(metavar)
        
    def close_input_swath(self) -> None:
        self.swath_in.close()
        
    def create_output_swath(self, path: str) -> None:
        self.swath_out.create(path)
        
    def set_variable(self, data: np.array, group: str, variable: str,
                     longname: str) -> None:
        self.swath_out.set_var(data, group, variable, longname)
        
    def close_output_swath(self) -> None:
        self.swath_out.close()
        
    def cleanup(self, path: str) -> None:
        """
        Parameters
        ----------
        path : str
            Path to the raw swath file that shall be removed after processing

        Returns
        -------
        None
        """
        os.remove(path) 
        
    # """
    # Parentclass for all swath-related I/O
    # """
    # def __init__(self, out: str):
    #     self.OUTPUT_PATH = out

    # @abstractmethod
    # def load(self, path: str) -> None:
    #     """
    #     Parameters
    #     ----------
    #     path : str
    #         Path to the file/swath to load

    #     Returns
    #     -------
    #     None
    #         Stores the specific file handle, e.g., within self.fh
    #     """
    #     pass
    
    # @abstractmethod
    # def get_var(self, var: str, grp: str, chspecs: list) -> np.array:
    #     """
    #     Parameters
    #     ----------
    #     var : str
    #         Variable name within the data set taken from the variables to 
    #         process dictionary provided within the sensor-specific meta data
    #     grp : str
    #         Group name within the data set taken from the variables to 
    #         process dictionary provided within the sensor-specific meta data
    #     chspecs : list
    #         List entry with channel specifications from the sensor-specific 
    #         meta file

    #     Returns
    #     -------
    #     np.array
    #         Returns numpy array with corresponding data
    #     """
    #     pass        
    
    # def close(self) -> None:
    #     try:
    #         self.fh.end()
    #     except:
    #         self.fh.close()
    
    # def save(self, path: str) -> None:
    #     #create file and open file handle
    #     self.fh = h5py.File(path,'w')
    #     #set global attributes
    #     AUTHOR = "Dr. Stephan Paul (AWI/iceXai)"
    #     self.fh.attrs.create("author", AUTHOR)
    #     EMAIL = "stephan.paul@posteo.net"
    #     self.fh.attrs.create("contact", EMAIL)
    #     TIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #     self.fh.attrs.create("created", TIMESTAMP)
    
    # def set_var(self, inpath: str, ds: np.array, attr: str) -> None:
    #     #create dataset in file
    #     h5ds = self.fh.create_dataset(inpath,
    #                                   data=ds,
    #                                   compression="gzip",
    #                                   compression_opts=9)
    #     #set data attributes
    #     h5ds.attrs.create("long_name",attr)
    #     h5ds.attrs.create("valid_range",[np.nanmin(ds),np.nanmax(ds)])
    
    # def cleanup(self, path: str) -> None:
    #     os.remove(path) / 
    
    
class ModisSwathIO(SwathIO):
    """
    Childclass for all MODIS swath-related I/O
    """
    def load(self, path: str) -> None:
        self.fh = SD(path,SDC.READ)
        
    def get_var(self, var: str, grp: str, meta: list) -> np.array:
        #select scientific data set (sds) and corresponding attributes
        sds = self.fh.select(var)
        attributes = sds.attributes(full=1)
        #index in sds
        if meta[0] is not None:
            variable = sds[meta[0],:,:].astype(np.float32)
        else:
            variable = sds[:,:].astype(np.float32)
        #scale factor
        if meta[1] is not None:
            scale = attributes[meta[1]][0]
        else:
            scale = 1
        #offset
        if meta[2] is not None:
            offset = attributes[meta[2]][0]
        else:
            offset = 0
        #and retrieve the rest
        fill = attributes['_FillValue'][0]
        valid_min = attributes['valid_range'][0][0]
        valid_max = attributes['valid_range'][0][1]
        
        #mask invalid entries
        invalid = np.logical_or(variable>valid_max, 
                                variable<valid_min, 
                                variable==fill)
        variable[invalid] = np.nan
        
        #apply scale/offset
        if meta[0] is not None:
            variable = (variable - offset[meta[0]]) * scale[meta[0]]
        else:
            variable = (variable - offset) * scale
        
        #compute brightness temperature
        if meta[3] is not None:
            #mask values below 0 that sometimes appear
            non_nan = ~np.isnan(variable)
            non_nan[non_nan] = np.less(variable[non_nan],0)
            variable[non_nan] = np.nan
            variable = self._calculate_Tb(variable, meta[3])
        
        #return variable to caller
        return variable[:,:]
             

    #calculate TB specific for this sensor?
    def _calculate_Tb(self, variable, wavelength):
        #define constants
        H_PLANCK = 6.62607004 * 10**-34  #[Js]
        C_SOL    = 2.99792458 * 10**8  #[m/s]
        K_BOLTZ  = 1.38064852 * 10**-23  #[J/K]
        
        #calculate and return brightness temperature
        #Tb = np.zeros(variable.shape).astype(np.float32)
        Tb = H_PLANCK * C_SOL / (K_BOLTZ * wavelength * 
             np.log((2.0 * H_PLANCK * C_SOL**2 * wavelength**(-5)) / 
                    (variable * 10**6) + 1.0))
        return Tb


class SlstrSwathIO(SwathIO):
    """
    Childclass for all SLSTR swath-related I/O
    """
    def load(self, path: str) -> None:
        self.fh = nc.Dataset(path, 'r')
    
    def get_var(self, var: str, grp: str, meta: list) -> np.array:
        if grp is None:
            return self.fh.variables[var][:,:]
        else:
            return self.fh.groups[grp].variables[var][:,:]
        

class OlciSwathIO(SwathIO):
    """
    Childclass for all OLCI swath-related I/O
    """
    def load(self, path: str) -> None:
        self.fh = nc.Dataset(path, 'r')
    
    def get_var(self, var: str, grp: str, meta: list) -> np.array:
        if grp is None:
            return self.fh.variables[var][:,:]
        else:
            return self.fh.groups[grp].variables[var][:,:]