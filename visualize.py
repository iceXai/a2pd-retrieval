#!/usr/bin/env python3
#coding: utf-8
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[]

import h5py
import os

import matplotlib.pyplot as plt
import numpy as np

# In[]

path = 'C:/data/testretrieval'
testfile1 = 'ter_modis_2020245_005000_prod-nt-v1p0_brunt.h5'
testfile2 = 's3a_slstr_2020247_010751_prod-nt-v1p0_brunt.h5'
test1 = h5py.File(os.path.join(path,testfile1), "r")
test2 = h5py.File(os.path.join(path,testfile2), "r")

print(test1['mod02'].keys())
print(test2['bt'].keys())

fig, axs = plt.subplots(figsize=(40, 60),nrows=1, ncols=3)
axs = axs.flatten()
axs[0].imshow(test1['mod02']['ch20'][:])
ttt = test2['bt']['s8_nadir'][:]
#ttt[np.where(ttt==0.0)] = np.nan
#ttt[np.where(ttt==-32768.)] = np.nan
axs[1].imshow(ttt)
axs[2].imshow(test2['bt']['s8_oblique'][:])
#plt.tight_layout()
#plt.imshow(test['mod02']['ch20'][:])

test1.close()
test2.close()

