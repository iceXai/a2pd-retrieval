#!/usr/bin/env python3
#coding: utf-8
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[]

from iotools import HDF5SwathInput

# In[]

path = 'C:/data/testretrieval'
testfile1 = 'ter_modis_2020245_005000_prod-nt-v1p0_brunt.h5'
testfile2 = 's3a_slstr_2020247_010751_prod-nt-v1p0_brunt.h5'

#load input handler
io = HDF5SwathInput()

#load file to data variable
io.load(os.path.join(path, testfile1))
dv_c31 = io.get_var_by_name('ch31', 'mod02')
io.close()

#load file to data variable
io.load(os.path.join(path, testfile2))
dv_s8n = io.get_var_by_name('s8_nadir', 'bt')
dv_s8o = io.get_var_by_name('s8_oblique', 'bt')
io.close()

#setup plot figure
fig, axs = plt.subplots(figsize=(40, 60),nrows=1, ncols=3)
axs = axs.flatten()
#plot it
dv_c31.quicklook(axs[0])
dv_s8n.quicklook(axs[1])
dv_s8o.quicklook(axs[2])


