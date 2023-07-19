# -*- coding: utf-8 -*-
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[] 
from abc import ABC, abstractmethod



# In[]

"""
Meta Information on Sensor/Carrier
"""

class Meta(ABC):
    """
    Abstract base class for all meta information regarding the supported 
    sensor/carrier combinations
    """    
    
    @abstractmethod
    def get_location_ref(self) -> dict:
        pass
    
    @abstractmethod
    def get_vars_to_process(self) -> dict:
        pass
    
    @abstractmethod
    def get_resample_dict(self) -> dict:
        pass
    
    @abstractmethod
    def get_meta_dict(self) -> dict:
        pass
    
    @abstractmethod
    def get_output_dict(self) -> dict:
        pass

    
class ModisSwathMeta(Meta):
    """
    Terra/Aqua MODIS meta information child class tailored to the 
    sensor-specific data processing
    """  
    
    #define reference location file keys, corresponding to the vars_to_process 
    #dict below
    def get_location_ref(self) -> dict:
        return {'lat': 'lat',
                'lon': 'lon'}

    # define dictionary containing key with respective file key for the 
    # zip dictionary/different swaths as well as the group/variable names,
    # e.g., {var_key: [file, grp, var]}
    def get_vars_to_process(self) -> dict:
        return {'lat': [swath[0], None, 'Latitude'],
                'lon': [swath[0], None, 'Longitude'],
                'sat_zen': [swath[0], None, 'SensorZenith'],
                'sat_azi': [swath[0], None, 'SensorAzimuth'],
                'sol_zen': [swath[0], None, 'SolarZenith'],
                'sol_azi': [swath[0], None, 'SolarAzimuth'],
                #'ch01': [swath[1], None, 'EV_250_Aggr1km_RefSB'],
                #'ch02': [swath[1], None, 'EV_250_Aggr1km_RefSB'],
                #'ch03': [swath[1], None, 'EV_500_Aggr1km_RefSB'],
                #'ch04': [swath[1], None, 'EV_500_Aggr1km_RefSB'],
                #'ch05': [swath[1], None, 'EV_500_Aggr1km_RefSB'],
                #'ch06': [swath[1], None, 'EV_500_Aggr1km_RefSB'],
                #'ch07': [swath[1], None, 'EV_500_Aggr1km_RefSB'],
                #'ch17': [swath[1], None, 'EV_1KM_RefSB'],
                #'ch18': [swath[1], None, 'EV_1KM_RefSB'],
                #'ch19': [swath[1], None, 'EV_1KM_RefSB'],
                #'ch26': [swath[1], None, 'EV_1KM_RefSB'],
                'ch20': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch21': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch22': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch23': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch24': [swath[1], None, 'EV_1KM_Emissive'],
                'ch25': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch27': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch28': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch29': [swath[1], None, 'EV_1KM_Emissive'],
                'ch31': [swath[1], None, 'EV_1KM_Emissive'],
                'ch32': [swath[1], None, 'EV_1KM_Emissive'],
                'ch33': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch34': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch35': [swath[1], None, 'EV_1KM_Emissive'],
                #'ch36': [swath[1], None, 'EV_1KM_Emissive']
                }
    
    # define dictionary containing the loaded variable names with corresponding
    # longitude/latitude grid reference for the resample process in case they 
    # vary, e.g., {var_key: [longitude, latitude]}
    def get_resample_dict(self) -> dict: 
        return {'sat_zen': ['lon', 'lat'],
                'sat_azi': ['lon', 'lat'],
                'sol_zen': ['lon', 'lat'],
                'sol_azi': ['lon', 'lat'],
                # 'ch01': ['lon', 'lat'],
                # 'ch02': ['lon', 'lat'],
                # 'ch03': ['lon', 'lat'],
                # 'ch04': ['lon', 'lat'],
                # 'ch05': ['lon', 'lat'],
                # 'ch06': ['lon', 'lat'],
                # 'ch07': ['lon', 'lat'],
                # 'ch17': ['lon', 'lat'],
                # 'ch18': ['lon', 'lat'],
                # 'ch19': ['lon', 'lat'],
                # 'ch26': ['lon', 'lat'],
                'ch20': ['lon', 'lat'],
                # 'ch21': ['lon', 'lat'],
                # 'ch22': ['lon', 'lat'],
                # 'ch23': ['lon', 'lat'],
                # 'ch24': ['lon', 'lat'],
                'ch25': ['lon', 'lat'],
                # 'ch27': ['lon', 'lat'],
                # 'ch28': ['lon', 'lat'],
                # 'ch29': ['lon', 'lat'],
                'ch31': ['lon', 'lat'],
                'ch32': ['lon', 'lat'],
                'ch33': ['lon', 'lat'],
                # 'ch34': ['lon', 'lat'],
                # 'ch35': ['lon', 'lat'],
                # 'ch36': ['lon', 'lat']
               }
    
    # define dictionary containing key with respective file key for the 
    # meta information such as scale/offset etc., e.g.,
    # {var_key: [index, scale, offset, wavelength]}
    def get_meta_dict(self) -> dict:
        return {'lat': [None, None, None, None],
                'lon': [None, None, None, None],
                'sat_zen': [None, 'scale_factor', None, None],
                'sat_azi': [None, 'scale_factor', None, None],
                'sol_zen': [None, 'scale_factor', None, None],
                'sol_azi': [None, 'scale_factor', None, None],
                'ch01': [ 0, 'reflectance_scales', 'reflectance_offsets', None],
                'ch02': [ 1, 'reflectance_scales', 'reflectance_offsets', None],
                'ch03': [ 0, 'reflectance_scales', 'reflectance_offsets', None],
                'ch04': [ 1, 'reflectance_scales', 'reflectance_offsets', None],
                'ch05': [ 2, 'reflectance_scales', 'reflectance_offsets', None],
                'ch06': [ 3, 'reflectance_scales', 'reflectance_offsets', None],
                'ch07': [ 4, 'reflectance_scales', 'reflectance_offsets', None],
                'ch17': [11, 'reflectance_scales', 'reflectance_offsets', None],
                'ch18': [12, 'reflectance_scales', 'reflectance_offsets', None],
                'ch19': [13, 'reflectance_scales', 'reflectance_offsets', None],
                'ch26': [14, 'reflectance_scales', 'reflectance_offsets', None],
                'ch20': [ 0, 'radiance_scales', 'radiance_offsets', 3.7500*10**-6],
                'ch21': [ 1, 'radiance_scales', 'radiance_offsets', 3.9590*10**-6],
                'ch22': [ 2, 'radiance_scales', 'radiance_offsets', 3.9590*10**-6],
                'ch23': [ 3, 'radiance_scales', 'radiance_offsets', 4.0500*10**-6],
                'ch24': [ 4, 'radiance_scales', 'radiance_offsets', 4.4655*10**-6],
                'ch25': [ 5, 'radiance_scales', 'radiance_offsets', 4.5155*10**-6],
                'ch27': [ 6, 'radiance_scales', 'radiance_offsets', 6.7150*10**-6],
                'ch28': [ 7, 'radiance_scales', 'radiance_offsets', 7.3350*10**-6],
                'ch29': [ 8, 'radiance_scales', 'radiance_offsets', 8.5500*10**-6],
                'ch31': [10, 'radiance_scales', 'radiance_offsets',11.030*10**-6],
                'ch32': [11, 'radiance_scales', 'radiance_offsets',12.020*10**-6],
                'ch33': [12, 'radiance_scales', 'radiance_offsets',13.335*10**-6],
                'ch34': [13, 'radiance_scales', 'radiance_offsets',13.635*10**-6],
                'ch35': [14, 'radiance_scales', 'radiance_offsets',13.935*10**-6],
                'ch36': [15, 'radiance_scales', 'radiance_offsets',14.235*10**-6]
                }

    # define dictionary to handle the output structure of the data in the h5 
    # file by providing for each variable name an in-file group name, an 
    # in-file variable name as well as an attribute long name, e.g. 
    # {var_key: [file_grp_name, file_var_name, attr_long_name]}    
    def get_output_dict(self) -> dict:
        return {'lat': ['mod03', 'lat', 'mod03_latitude'],
                'lon': ['mod03', 'lon', 'mod03_longitude'],
                'sat_zen': ['mod03', 'sat_zen', 'mod03_sensor_zenith_angle'],
                'sat_azi': ['mod03', 'sat_azi', 'mod03_sensor_azimuth_angle'],
                'sol_zen': ['mod03', 'sol_zen', 'mod03_solar_zenith_angle'],
                'sol_azi': ['mod03', 'sol_azi', 'mod03_solar_azimuth_angle'],
                # 'ch01': ['mod02', 'ch01', 'mod02_ch01_reflectance_data'],
                # 'ch02': ['mod02', 'ch02', 'mod02_ch02_reflectance_data'],
                #'ch03': ['mod02', 'ch03', 'mod02_ch03_reflectance_data'],
                #'ch04': ['mod02', 'ch04', 'mod02_ch04_reflectance_data'],
                #'ch05': ['mod02', 'ch05', 'mod02_ch05_reflectance_data'],
                #'ch06': ['mod02', 'ch06', 'mod02_ch06_reflectance_data'],
                #'ch07': ['mod02', 'ch07', 'mod02_ch07_reflectance_data'],
                # 'ch17': ['mod02', 'ch17', 'mod02_ch17_reflectance_data'],
                # 'ch18': ['mod02', 'ch18', 'mod02_ch18_reflectance_data'],
                # 'ch19': ['mod02', 'ch19', 'mod02_ch19_reflectance_data'],
                # 'ch26': ['mod02', 'ch26', 'mod02_ch26_reflectance_data'],
                'ch20': ['mod02', 'ch20', 'mod02_ch20_brightness_temperature_data'],
                # 'ch21': ['mod02', 'ch21', 'mod02_ch21_brightness_temperature_data'],
                # 'ch22': ['mod02', 'ch22', 'mod02_ch22_brightness_temperature_data'],
                # 'ch23': ['mod02', 'ch23', 'mod02_ch23_brightness_temperature_data'],
                # 'ch24': ['mod02', 'ch24', 'mod02_ch24_brightness_temperature_data'],
                'ch25': ['mod02', 'ch25', 'mod02_ch25_brightness_temperature_data'],
                # 'ch27': ['mod02', 'ch27', 'mod02_ch27_brightness_temperature_data'],
                # 'ch28': ['mod02', 'ch28', 'mod02_ch28_brightness_temperature_data'],
                # 'ch29': ['mod02', 'ch29', 'mod02_ch29_brightness_temperature_data'],
                'ch31': ['mod02', 'ch31', 'mod02_ch31_brightness_temperature_data'],
                'ch32': ['mod02', 'ch32', 'mod02_ch32_brightness_temperature_data'],
                'ch33': ['mod02', 'ch33', 'mod02_ch33_brightness_temperature_data'],
                # 'ch34': ['mod02', 'ch34', 'mod02_ch34_brightness_temperature_data'],
                # 'ch35': ['mod02', 'ch35', 'mod02_ch35_brightness_temperature_data'],
                # 'ch36': ['mod02', 'ch36', 'mod02_ch36_brightness_temperature_data']
                }

    
class SlstrMeta(Meta):
    """
    Sentinel3-A/B SLSTR meta information child class tailored to the 
    sensor-specific data processing
    """  
    
    def get_location_ref(self) -> dict:
        pass
    
    def get_vars_to_process(self) -> dict:
        pass
    
    def get_resample_dict(self) -> dict:
        pass
    
    def get_meta_dict(self) -> dict:
        pass
    
    def get_output_dict(self) -> dict:
        pass