#!/usr/bin/env python3
#coding: utf-8
"""
@author: Dr. Stephan Paul (AWI/iceXai; stephan.paul@awi.de)
"""


# In[]

import h5py
import os

import matplotlib.pyplot as plt

# In[]

path = 'C:/data/testretrieval'
testfile1 = 'ter_modis_2020245_005000_prod-nt-v1p0_brunt.h5'
testfile2 = 's3b_slstr_2020245_075847_prod-nt-v1p0_brunt.h5'
test1 = h5py.File(os.path.join(path,testfile1), "r")
test2 = h5py.File(os.path.join(path,testfile2), "r")

print(test1['mod02'].keys())
print(test2['bt'].keys())

fig, axs = plt.subplots(figsize=(40, 60),nrows=1, ncols=3)
axs = axs.flatten()
axs[0].imshow(test1['mod02']['ch20'][:])
axs[1].imshow(test2['bt']['s8n'][:])
axs[2].imshow(test2['bt']['s9exo'][:])
#plt.tight_layout()
#plt.imshow(test['mod02']['ch20'][:])



