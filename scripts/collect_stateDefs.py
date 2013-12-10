#!/usr/bin/env python

# (c) SRON - Netherlands Institute for Space Research (2013).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

## @ingroup pynadc_scripts
## @{

"""@package pynadc.scripts.collect_stateDefs
collect state definitions from Sciamachy level 1b products

Synopsis
--------

   collect_stateDefs.py [options] <hdf5file>

Description
-----------

Collect state definitions from Sciamachy level 1b products and store in HDF5 database. Missing state definitions for states not written to level 1b products can be added; OCR states are hard-coded in this program or by interpolation  

Options
-------

-h, --help
    Show a short help message

-v, --verbose
    Be verbose

--orbit ORBIT
    select Sciamachy level 1b product for given orbit and add extract state definitions to HDF5 database

--file NAME
    read Sciamachy level 1b product with given name and  add extract state definitions to HDF5 database

--mtbl_fill
    update HDF5 database adding missing entries hard-coded or by interpolation

Output
------

Create or modify HDF5 database with state definitions for Sciamachy level 0 proessing

Examples
--------

None 

Author
------

Richard van Hees (r.m.van.hees@sron.nl)

Bug reporting
-------------

Please report issues at the Sciamachy PYNADC Github page:
https://github.com/rmvanhees/pynadc.git

Copyright
---------

Copyright (C) SRON - Netherlands Institute for Space Research (2013).
All rights reserved. This software is released under the BSD 2-clause
License.
"""
from __future__ import print_function
from __future__ import division

import sys

import numpy as np
import h5py

class clusDB:
    def __init__( self, args=None, db_name='./nadc_clusDef.h5',
                  verbose=False ):
        """Initialize the class clusDB."""
        if args:
            self.db_name  = args.db_name
            self.verbose  = args.verbose
        else:
            self.db_name  = db_name
            self.verbose  = verbose

    def create( self ):
        """Create and initialize the state definition database."""
        maxOrbit = 53000

        with h5py.File( self.db_name, 'w', libver='latest' ) as fid:
            mtbl = np.zeros( maxOrbit, 
                             dtype='uint16,uint8,uint8,uint16,uint16' )
            mtbl.dtype.names = ('orbit','num_clus','indx_Clcon',
                                'duration', 'num_info')
            mtbl[:]['orbit'] = np.arange( maxOrbit, dtype='uint16' )
            mtbl[:]['indx_Clcon'] = 2**8-1
            
            for ns in range(1, 71):
                grp = fid.create_group( "State_%02d" % (ns) )
                ds = grp.create_dataset( 'metaTable', data=mtbl,
                                         chunks=(16384 // mtbl.dtype.itemsize,),
                                         compression='gzip', compression_opts=1,
                                         shuffle=True )
            
    def append( self, stateID, mtbl, clusDef ):
        """Append new state cluster definition.

        @param self    : Reference to state definition module object.
        @param stateID : state ID - integer range [1, 70].
        @param mtbl    : metaTable entry.
        @param clusDef : state cluster definition entry.
        """
        with h5py.File( self.db_name, 'r+' ) as fid:
            grp = fid['State_%02d' % (stateID)]
            ds_mtbl = grp['metaTable']
            ds_mtbl[mtbl[0]] = mtbl

            # check if dataset "clusDef" exists, if not create
            if not "clusDef" in grp:
                ds_clus = grp.create_dataset( 'clusDef', 
                                              data=clusDef.reshape(1,64),
                                              maxshape=(None,64) )
            else:
                ds_clus = grp['clusDef']
                clusDef_db = ds_clus[:]
                ax1 = ds_clus.shape[0]
                for ni in range(ax1):
                    if (clusDef_db[ni,:] == clusDef).all():
                        ds_mtbl[mtbl[0],'indx_Clcon'] = ni
                        return

                # new cluster definition: extent dataset
                ds_clus.resize(ax1+1, axis=0)
                ds_clus[ax1,:] = clusDef
                ds_mtbl[mtbl[0],'indx_Clcon'] = ax1

    def add_missing_state_10_13( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=10 added definitions for orbits [3964,3968,4118,4122]
        - stateID=11 added definitions for orbits [3969,4123]
        - stateID=12 added definitions for orbits [3965,3970,4119,4124]
        - stateID=13 added definitions for orbits [3971,4125]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_10']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3964,3968,4118,4122]
            clusDef = ds_clus[2,:]
            indx = np.where( clusDef['chan_id'] > 0 )[0]
            clusDef['pet'][indx] = 0.0625
            clusDef['intg'][indx] = 1
            clusDef['coaddf'][indx] = 1
            clusDef['readouts'][indx] = 8
            clusDef['clus_type'][indx] = 1
            indx = np.where( (clusDef['chan_id'] == 2)
                             | (clusDef['chan_id'] == 6)
                             | (clusDef['chan_id'] == 7) )[0]
            clusDef['pet'][indx] = 0.03125
            indx = np.where( clusDef['chan_id'] > 6 )[0]
            clusDef['intg'][indx] = 8
            clusDef['coaddf'][indx] = 8
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 593
                ds_mtbl[orbit_list,'num_info'] = 528

            grp = fid['State_11']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3969,4123]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 593
                ds_mtbl[orbit_list,'num_info'] = 528

            grp = fid['State_12']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3965,3970,4119,4124]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 593
                ds_mtbl[orbit_list,'num_info'] = 528

            grp = fid['State_13']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3971,4125]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 593
                ds_mtbl[orbit_list,'num_info'] = 528

    def add_missing_state_14( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=14 added definitions for orbits [3958,3959,3962,
                          4086,4087,4088,4089,4091,4092,
                          4111,4112,4113,4114,5994]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_14']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3958,3959,3962,
                          4086,4087,4088,4089,4091,4092,
                          4111,4112,4113,4114,
                          5994]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                grp_15 = fid['State_15']
                ds_15 = grp_15['clusDef']
                clusDef = ds_15[0,:]
                indx = np.where( clusDef['chan_id'] == 1 )[0]
                clusDef['pet'][indx] = 40.0
                clusDef['intg'][indx] = 640
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 1
                indx = np.where( clusDef['chan_id'] == 2 )[0]
                clusDef['pet'][indx] = 40.0
                clusDef['intg'][indx] = 640
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 1
                indx = np.where( clusDef['chan_id'] == 3 )[0]
                clusDef['pet'][indx] = 10.0
                clusDef['intg'][indx] = 640
                clusDef['coaddf'][indx] = 4
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 4 )[0]
                clusDef['pet'][indx] = 4.0
                clusDef['intg'][indx] = 640
                clusDef['coaddf'][indx] = 10
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 5 )[0]
                clusDef['pet'][indx] = 4.0
                clusDef['intg'][indx] = 640
                clusDef['coaddf'][indx] = 10
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 6 )[0]
                clusDef['pet'][indx] = 1.0
                clusDef['intg'][indx] = 640
                clusDef['coaddf'][indx] = 40
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 7 )[0]
                clusDef['pet'][indx] = 1.0
                clusDef['intg'][indx] = 640
                clusDef['coaddf'][indx] = 40
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 8 )[0]
                clusDef['pet'][indx] = 2.0
                clusDef['intg'][indx] = 640
                clusDef['coaddf'][indx] = 20
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1280
                ds_mtbl[orbit_list,'num_info'] = 2
                clus_dim += 1

    def add_missing_state_22( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=22 added definitions for orbits 
                     [4119,4120,4121,4122,4123,4124,4125,4126,4127]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_22']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4119, 4120, 4121, 4122, 4123, 4124, 4125, 4126, 4127]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                clusDef = ds_clus[2,:]
                indx = np.where( clusDef['chan_id'] > 0 )[0]
                clusDef['pet'][indx] = 1.5
                clusDef['intg'][indx] = 24
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 1
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 782
                ds_mtbl[orbit_list,'num_info'] = 29

    def add_missing_state_24( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=24 added definitions for orbits 
                     [3034, 36873:38267, 47994:48075]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_24']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[3034,'indx_Clcon'] == 255):
                clusDef = ds_clus[0,:]
                indx = np.where( clusDef['chan_id'] > 0 )[0]
                clusDef['pet'][indx] = 1.0
                clusDef['intg'][indx] = 16
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 10
                clusDef['clus_type'][indx] = 1
                clusDef['pet'][0:3] = 10.
                clusDef['intg'][0:3] = 160
                clusDef['readouts'][0:3] = 1
                indx = np.where( clusDef['chan_id'] == 6 )[0]
                clusDef['pet'][indx] = 0.5
                clusDef['intg'][indx[2:11]] = 8
                clusDef['readouts'][indx[2:11]] = 5
                clusDef['intg'][indx[np.array([0,1,11])]] = 16
                clusDef['coaddf'][indx[np.array([0,1,11])]] = 2
                clusDef['clus_type'][indx[np.array([0,1,11])]] = 2
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[3034,'num_clus'] = 56
                ds_mtbl[3034,'indx_Clcon'] = clus_dim
                ds_mtbl[3034,'duration'] = 1280
                ds_mtbl[3034,'num_info'] = 160
                clus_dim += 1

            if np.all(ds_mtbl[36873:38267,'indx_Clcon'] == 255):
                clusDef = ds_clus[2,:]
                indx = np.where( clusDef['chan_id'] > 0 )[0]
                clusDef['pet'][indx] = 1.0
                clusDef['intg'][indx] = 16
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 1
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[36873:38267,'num_clus'] = 40
                ds_mtbl[36873:38267,'indx_Clcon'] = clus_dim
                ds_mtbl[36873:38267,'duration'] = 1600
                ds_mtbl[36873:38267,'num_info'] = 100
                clus_dim += 1

            if np.all(ds_mtbl[47994:48075,'indx_Clcon'] == 255):
                clusDef = ds_clus[2,:]
                indx = np.where( clusDef['chan_id'] > 0 )[0]
                clusDef['pet'][indx] = 1.0
                clusDef['intg'][indx] = 16
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 1
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[47994:48074,'num_clus'] = 40
                ds_mtbl[47994:48074,'indx_Clcon'] = clus_dim
                ds_mtbl[47994:48074,'duration'] = 1440
                ds_mtbl[47994:48074,'num_info'] = 90
                clus_dim += 1

    def add_missing_state_25_26( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=25 added definitions for orbits [4088,4111]
        - stateID=26 added definitions for orbits [4089]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_22']
            ds_clus  = grp['clusDef']
            clusDef = ds_clus[2,:]

            grp = fid['State_25']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            indx = np.where( clusDef['chan_id'] > 0 )[0]
            clusDef['intg'][indx] = 4
            clusDef['coaddf'][indx] = 4
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            clusDef['pet'][0] = 0.25        # channel 1
            clusDef['coaddf'][0] = 1
            clusDef['clus_type'][0] = 1
            clusDef['pet'][1] = 0.0625
            clusDef['pet'][2:4] = 0.0625    # channel 2
            clusDef['pet'][4] = 0.0625      # channel 3
            clusDef['pet'][5] = 0.0625      # channel 4
            clusDef['pet'][6] = 0.125       # channel 5
            clusDef['coaddf'][6] = 2
            clusDef['pet'][7] = 0.03125     # channel 6
            clusDef['pet'][8] = 0.0625      # channel 7
            clusDef['pet'][9] = 0.125       # channel 8
            clusDef['coaddf'][9] = 2

            orbit_list = [4088,4111]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 782
                ds_mtbl[orbit_list,'num_info'] = 174

            grp = fid['State_26']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4089]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 782
                ds_mtbl[orbit_list,'num_info'] = 174

    def add_missing_state_27( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=27 added definitions for orbits [44091,44092]
        - stateID=27 added definitions for orbits [44134,44148,44149,44150]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_27']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [44091,44092]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                clusDef = ds_clus[0,:]
                indx = np.where( clusDef['chan_id'] > 0 )[0]
                clusDef['pet'][indx] = 1.5
                clusDef['intg'][indx] = 24
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 6
                clusDef['clus_type'][indx] = 1
                indx = np.where( (clusDef['chan_id'] == 3)
                                 | (clusDef['chan_id'] == 4) )[0]
                clusDef['pet'][0:3] = 0.75
                clusDef['intg'][0:3] = 12
                clusDef['readouts'][0:3] = 12
                indx = np.where( clusDef['chan_id'] == 8 )[0]
                clusDef['pet'][0:3] = 1.5
                clusDef['intg'][0:3] = 144
                clusDef['coaddf'][indx] = 6
                clusDef['readouts'][0:3] = 1
                clusDef['clus_type'][indx] = 2

                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 647
                ds_mtbl[orbit_list,'num_info'] = 24
                clus_dim += 1

            orbit_list = [44134,44148,44149,44150]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                clusDef = ds_clus[0,:]
                indx = np.where( clusDef['chan_id'] > 0 )[0]
                clusDef['pet'][indx] = 1.5
                clusDef['intg'][indx] = 24
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 6
                clusDef['clus_type'][indx] = 1
                indx = np.where( (clusDef['chan_id'] == 3)
                                 | (clusDef['chan_id'] == 4) )[0]
                clusDef['pet'][0:3] = 0.75
                clusDef['intg'][0:3] = 12
                clusDef['readouts'][0:3] = 12
                indx = np.where( clusDef['chan_id'] == 8 )[0]
                clusDef['pet'][0:3] = 1.5
                clusDef['intg'][0:3] = 144
                clusDef['coaddf'][indx] = 12
                clusDef['readouts'][0:3] = 1
                clusDef['clus_type'][indx] = 2

                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 647
                ds_mtbl[orbit_list,'num_info'] = 24
                clus_dim += 1

    def add_missing_state_33_39( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=33 added definitions for orbits [4087,4089,4110,4112]
        - stateID=34 added definitions for orbits [4088,4090,4111,4113]
        - stateID=38 added definitions for orbits [4087,4089,4110,4112]
        - stateID=39 added definitions for orbits [4088,4090,4111,4113]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_35']
            ds_clus  = grp['clusDef']
            clusDef = ds_clus[1,:]

            grp = fid['State_33']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4087,4089,4110,4112]
            indx = np.where( clusDef['chan_id'] == 1 )[0]
            clusDef['pet'][indx] = 2.
            clusDef['intg'][indx] = 32
            clusDef['coaddf'][indx] = 1
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 1
            indx = np.where( clusDef['chan_id'] == 2 )[0]
            clusDef['pet'][indx] = 0.25
            clusDef['intg'][indx] = 32
            clusDef['coaddf'][indx] = 8
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            indx = np.where( clusDef['chan_id'] == 3 )[0]
            clusDef['pet'][indx] = 0.125
            clusDef['intg'][indx] = 32
            clusDef['coaddf'][indx] = 16
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            indx = np.where( clusDef['chan_id'] == 4 )[0]
            clusDef['pet'][indx] = 1 / 32.
            clusDef['intg'][indx] = 32
            clusDef['coaddf'][indx] = 32
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            indx = np.where( clusDef['chan_id'] == 5 )[0]
            clusDef['pet'][indx] = 1 / 32.
            clusDef['intg'][indx] = 32
            clusDef['coaddf'][indx] = 32
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            indx = np.where( clusDef['chan_id'] == 6 )[0]
            clusDef['pet'][indx] = 0.0072
            clusDef['intg'][indx] = 32
            clusDef['coaddf'][indx] = 32
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            indx = np.where( clusDef['chan_id'] == 7 )[0]
            clusDef['pet'][indx] = 0.0036
            clusDef['intg'][indx] = 32
            clusDef['coaddf'][indx] = 32
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            indx = np.where( clusDef['chan_id'] == 8 )[0]
            clusDef['pet'][indx] = 0.0072
            clusDef['intg'][indx] = 32
            clusDef['coaddf'][indx] = 32
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1406
                ds_mtbl[orbit_list,'num_info'] = 42

            grp = fid['State_34']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4088,4090,4111,4113]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1406
                ds_mtbl[orbit_list,'num_info'] = 42

            grp = fid['State_38']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4087,4089,4110,4112]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1406
                ds_mtbl[orbit_list,'num_info'] = 42

            grp = fid['State_39']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4088,4090,4111,4113]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1406
                ds_mtbl[orbit_list,'num_info'] = 42

    def add_missing_state_35_39( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=09 added definitions for orbits [3967,4121]
        - stateID=35 added definitions for orbits [3972,4126]
        - stateID=36 added definitions for orbits [3973,4127]
        - stateID=37 added definitions for orbits [3975]
        - stateID=38 added definitions for orbits [3976]
        - stateID=39 added definitions for orbits [3977]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_35']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3972,4126]
            clusDef = ds_clus[0,:]
            indx = np.where( clusDef['chan_id'] > 0 )[0]
            clusDef['pet'][indx] = 1 / 16.
            clusDef['intg'][indx] = 1
            clusDef['coaddf'][indx] = 1
            clusDef['readouts'][indx] = 8
            clusDef['clus_type'][indx] = 1
            indx = np.where( (clusDef['chan_id'] == 2)
                             | (clusDef['chan_id'] == 6) )[0]
            clusDef['pet'][indx] = 1 / 32.
            clusDef['intg'][indx] = 1
            clusDef['coaddf'][indx] = 1
            clusDef['readouts'][indx] = 8
            clusDef['clus_type'][indx] = 1
            indx = np.where( clusDef['chan_id'] == 7 )[0]
            clusDef['pet'][indx] = 1 / 32.
            clusDef['intg'][indx] = 8
            clusDef['coaddf'][indx] = 8
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            indx = np.where( clusDef['chan_id'] == 8 )[0]
            clusDef['pet'][indx] = 1 / 16.
            clusDef['intg'][indx] = 8
            clusDef['coaddf'][indx] = 8
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 2
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1511
                ds_mtbl[orbit_list,'num_info'] = 1344

            grp = fid['State_36']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3973,4127]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1511
                ds_mtbl[orbit_list,'num_info'] = 1344

            grp = fid['State_37']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3975]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1511
                ds_mtbl[orbit_list,'num_info'] = 1344

            grp = fid['State_38']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3976]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1511
                ds_mtbl[orbit_list,'num_info'] = 1344

            grp = fid['State_39']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3977]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1511
                ds_mtbl[orbit_list,'num_info'] = 1344

            grp = fid['State_09']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3967,4121]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 928
                ds_mtbl[orbit_list,'num_info'] = 928

    def add_missing_state_42( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=42 added definitions for orbits [6778,6779]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_42']
            ds_mtbl  = grp['metaTable']
            orbit_list = [6778,6779]
            if np.all(ds_mtbl[orbit_list,'num_info'] == 0):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 255
                ds_mtbl[orbit_list,'duration'] = 5598
                ds_mtbl[orbit_list,'num_info'] = 2650

    def add_missing_state_43( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=43 added definitions for orbits [6778,6779]
        - stateID=43 added definitions for orbits [7193,7194]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_43']
            ds_mtbl  = grp['metaTable']
            orbit_list = [6778,6779]
            if np.all(ds_mtbl[orbit_list,'num_info'] == 0):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 255
                ds_mtbl[orbit_list,'duration'] = 1118
                ds_mtbl[orbit_list,'num_info'] = 536

            orbit_list = [7193,7194]
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            clusDef = ds_clus[1,:]
            indx = np.where( (clusDef['chan_id'] == 1)
                             | (clusDef['chan_id'] == 2) )[0]
            clusDef['pet'][indx] = 10.
            clusDef['intg'][indx] = 160
            clusDef['coaddf'][indx] = 1
            clusDef['readouts'][indx] = 1
            clusDef['clus_type'][indx] = 1
            indx = np.where( (clusDef['chan_id'] > 2)
                             & (clusDef['chan_id'] < 6) )[0]
            clusDef['pet'][indx] = 2.5
            clusDef['intg'][indx] = 40
            clusDef['coaddf'][indx] = 1
            clusDef['readouts'][indx] = 4
            clusDef['clus_type'][indx] = 1
            indx = np.where( clusDef['chan_id'] >= 6 )[0]
            clusDef['pet'][indx] = 1 / 32.
            clusDef['intg'][indx] = 1
            clusDef['coaddf'][indx] = 16
            clusDef['readouts'][indx] = 10
            clusDef['clus_type'][indx] = 2
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1120
                ds_mtbl[orbit_list,'num_info'] = 84

    def add_missing_state_44( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=44 added definitions for orbits [6778,6779]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_44']
            ds_mtbl  = grp['metaTable']
            orbit_list = [6778,6779]
            if np.all(ds_mtbl[orbit_list,'num_info'] == 0):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 255
                ds_mtbl[orbit_list,'duration'] = 447
                ds_mtbl[orbit_list,'num_info'] = 219

    def add_missing_state_55( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=55 added definitions for orbits [26812:26834]
        - stateID=55 added definitions for orbits [28917:28920, 30836:30850]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_55']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[26812:26834,'indx_Clcon'] == 255):
                clusDef = ds_clus[0,:]
                indx = np.where( clusDef['chan_id'] > 0 )[0]
                clusDef['pet'][indx] = 1/16.
                indx = np.where( clusDef['chan_id'] == 1 )[0]
                clusDef['intg'][indx] = 1
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 2
                clusDef['clus_type'][indx] = 1
                indx = np.where( clusDef['chan_id'] == 2 )[0]
                clusDef['intg'][indx] = 1
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 2
                clusDef['clus_type'][indx] = 1
                indx = np.where( clusDef['chan_id'] == 3 )[0]
                clusDef['intg'][indx] = 2
                clusDef['coaddf'][indx] = 2
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 4 )[0]
                clusDef['intg'][indx] = 2
                clusDef['coaddf'][indx] = 2
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 5 )[0]
                clusDef['intg'][indx] = 2
                clusDef['coaddf'][indx] = 2
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 6 )[0]
                clusDef['intg'][indx] = 2
                clusDef['coaddf'][indx] = 2
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2
                indx = np.where( clusDef['chan_id'] == 7 )[0]
                clusDef['intg'][indx] = 1
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 2
                clusDef['clus_type'][indx] = 1
                indx = np.where( clusDef['chan_id'] == 8 )[0]
                clusDef['intg'][indx] = 2
                clusDef['coaddf'][indx] = 2
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 2

                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[26812:26834,'num_clus'] = 40
                ds_mtbl[26812:26834,'indx_Clcon'] = clus_dim
                ds_mtbl[26812:26834,'duration'] = 640
                ds_mtbl[26812:26834,'num_info'] = 640
                clus_dim += 1

            if np.all(ds_mtbl[28917:28920,'indx_Clcon'] == 255):
                clusDef = ds_clus[0,:]
                indx = np.where( clusDef['chan_id'] > 0 )[0]
                clusDef['pet'][indx] = 0.5
                clusDef['intg'][indx] = 8
                clusDef['coaddf'][indx] = 1
                clusDef['readouts'][indx] = 1
                clusDef['clus_type'][indx] = 1

                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[28917:28920,'num_clus'] = 40
                ds_mtbl[28917:28920,'indx_Clcon'] = clus_dim
                ds_mtbl[28917:28920,'duration'] = 1673
                ds_mtbl[28917:28920,'num_info'] = 186

                ds_mtbl[30836:30850,'num_clus'] = 40
                ds_mtbl[30836:30850,'indx_Clcon'] = clus_dim
                ds_mtbl[30836:30850,'duration'] = 1673
                ds_mtbl[30836:30850,'num_info'] = 186
                clus_dim += 1

    def fill_mtbl( self ):
        """Fill metaTable by interpolation and in a few cases by extrapolation
        """
        with h5py.File( self.db_name, 'r+' ) as fid:
            for ns in range(1, 71):
                grp = fid['State_%02d' % (ns)]
                if "clusDef" in grp:
                    ds_mtbl  = grp['metaTable']
                    mtbl_dim = ds_mtbl.size

                    num_clus   = ds_mtbl[:,'num_clus']
                    indx_Clcon = ds_mtbl[:,'indx_Clcon']
                    duration   = ds_mtbl[:,'duration']
                    num_info   = ds_mtbl[:,'num_info']

                    # skip all undefined entries at the start
                    nj = 0
                    while nj < mtbl_dim and indx_Clcon[nj] == 255:
                        nj += 1

                    # replace undefined entries
                    while nj < mtbl_dim:
                        ni = nj
                        while ni < mtbl_dim and indx_Clcon[ni] != 255:
                            ni += 1

                        val_num_clus = num_clus[ni-1]
                        val_indx     = indx_Clcon[ni-1]
                        val_duration = duration[ni-1]
                        val_num_info = num_info[ni-1]

                        nj = ni + 1
                        while nj < mtbl_dim and indx_Clcon[nj] == 255:
                            nj += 1

                        if nj == mtbl_dim: break

                        if indx_Clcon[nj] == val_indx \
                                and duration[nj] == val_duration:

                            print( 'State_%02d: ' % (ns), ni, nj, val_indx )
                            num_clus[ni:nj]   = val_num_clus
                            indx_Clcon[ni:nj] = val_indx
                            duration[ni:nj]   = val_duration
                            num_info[ni:nj]   = val_num_info

                    ds_mtbl[:,'num_clus']   = num_clus
                    ds_mtbl[:,'indx_Clcon'] = indx_Clcon
                    ds_mtbl[:,'duration']   = duration
                    ds_mtbl[:,'num_info']   = num_info

                    if ds_mtbl[6001,'indx_Clcon'] == 255:
                        ds_mtbl[6001,'num_clus']   = ds_mtbl[6002,'num_clus']
                        ds_mtbl[6001,'indx_Clcon'] = ds_mtbl[6002,'indx_Clcon']
                        ds_mtbl[6001,'duration']   = ds_mtbl[6002,'duration']
                        ds_mtbl[6001,'num_info']   = ds_mtbl[6002,'num_info']

                    if ds_mtbl[40107,'indx_Clcon'] == 255:
                        ds_mtbl[40107,'num_clus']   = ds_mtbl[40108,'num_clus']
                        ds_mtbl[40107,'indx_Clcon'] = ds_mtbl[40108,'indx_Clcon']
                        ds_mtbl[40107,'duration']   = ds_mtbl[40108,'duration']
                        ds_mtbl[40107,'num_info']   = ds_mtbl[40108,'num_info']

                    if ns == 2 and ds_mtbl[6091,'indx_Clcon'] == 255:
                        ds_mtbl[6091,'num_clus']   = ds_mtbl[6108,'num_clus']
                        ds_mtbl[6091,'indx_Clcon'] = ds_mtbl[6108,'indx_Clcon']
                        ds_mtbl[6091,'duration']   = ds_mtbl[6108,'duration']
                        ds_mtbl[6091,'num_info']   = ds_mtbl[6108,'num_info']

                    if ns == 2 and ds_mtbl[6109,'indx_Clcon'] == 255:
                        ds_mtbl[6109,'num_clus']   = ds_mtbl[6108,'num_clus']
                        ds_mtbl[6109,'indx_Clcon'] = ds_mtbl[6108,'indx_Clcon']
                        ds_mtbl[6109,'duration']   = ds_mtbl[6108,'duration']
                        ds_mtbl[6109,'num_info']   = ds_mtbl[6108,'num_info']

                    if ns == 6 and ds_mtbl[7493,'indx_Clcon'] == 255:
                        ds_mtbl[7493,'num_clus']   = ds_mtbl[7494,'num_clus']
                        ds_mtbl[7493,'indx_Clcon'] = ds_mtbl[7494,'indx_Clcon']
                        ds_mtbl[7493,'duration']   = ds_mtbl[7494,'duration']
                        ds_mtbl[7493,'num_info']   = ds_mtbl[7494,'num_info']

                    if ns == 9 and ds_mtbl[4128,'indx_Clcon'] == 255:
                        ds_mtbl[4128,'num_clus']   = ds_mtbl[4129,'num_clus']
                        ds_mtbl[4128,'indx_Clcon'] = ds_mtbl[4129,'indx_Clcon']
                        ds_mtbl[4128,'duration']   = ds_mtbl[4129,'duration']
                        ds_mtbl[4128,'num_info']   = ds_mtbl[4129,'num_info']

                    if ns == 28 and ds_mtbl[45187,'indx_Clcon'] == 255:
                        ds_mtbl[45187,'num_clus']   = ds_mtbl[45186,'num_clus']
                        ds_mtbl[45187,'indx_Clcon'] = ds_mtbl[45186,'indx_Clcon']
                        ds_mtbl[45187,'duration']   = ds_mtbl[45186,'duration']
                        ds_mtbl[45187,'num_info']   = ds_mtbl[45186,'num_info']

                    if ns == 37 and ds_mtbl[4115,'indx_Clcon'] == 255:
                        ds_mtbl[4115,'num_clus']   = ds_mtbl[4090,'num_clus']
                        ds_mtbl[4115,'indx_Clcon'] = ds_mtbl[4090,'indx_Clcon']
                        ds_mtbl[4115,'duration']   = ds_mtbl[4090,'duration']
                        ds_mtbl[4115,'num_info']   = ds_mtbl[4090,'num_info']

                    if ns == 42 and ds_mtbl[3966,'indx_Clcon'] == 255:
                        ds_mtbl[3966,'num_clus']   = ds_mtbl[3974,'num_clus']
                        ds_mtbl[3966,'indx_Clcon'] = ds_mtbl[3974,'indx_Clcon']
                        ds_mtbl[3966,'duration']   = ds_mtbl[3974,'duration']
                        ds_mtbl[3966,'num_info']   = ds_mtbl[3974,'num_info']

                    if ns == 42 and ds_mtbl[7194,'indx_Clcon'] == 255:
                        ds_mtbl[7194,'num_clus']   = ds_mtbl[7193,'num_clus']
                        ds_mtbl[7194,'indx_Clcon'] = ds_mtbl[7193,'indx_Clcon']
                        ds_mtbl[7194,'duration']   = ds_mtbl[7193,'duration']
                        ds_mtbl[7194,'num_info']   = ds_mtbl[7193,'num_info']

                    if ns == 44 and ds_mtbl[7193,'indx_Clcon'] == 255:
                        ds_mtbl[7193,'num_clus']   = ds_mtbl[7194,'num_clus']
                        ds_mtbl[7193,'indx_Clcon'] = ds_mtbl[7194,'indx_Clcon']
                        ds_mtbl[7193,'duration']   = ds_mtbl[7194,'duration']
                        ds_mtbl[7193,'num_info']   = ds_mtbl[7194,'num_info']

                    if ns == 54 and ds_mtbl[5034,'indx_Clcon'] == 255:
                        ds_mtbl[5034,'num_clus']   = ds_mtbl[5019,'num_clus']
                        ds_mtbl[5034,'indx_Clcon'] = ds_mtbl[5019,'indx_Clcon']
                        ds_mtbl[5034,'duration']   = ds_mtbl[5019,'duration']
                        ds_mtbl[5034,'num_info']   = ds_mtbl[5019,'num_info']

                    if ns == 54 and ds_mtbl[22790,'indx_Clcon'] == 255:
                        ds_mtbl[22790,'num_clus']   = ds_mtbl[22789,'num_clus']
                        ds_mtbl[22790,'indx_Clcon'] = ds_mtbl[22789,'indx_Clcon']
                        ds_mtbl[22790,'duration']   = ds_mtbl[22789,'duration']
                        ds_mtbl[22790,'num_info']   = ds_mtbl[22789,'num_info']

                    if ns == 62 and ds_mtbl[4055,'indx_Clcon'] == 255:
                        ds_mtbl[4055,'num_clus']   = ds_mtbl[4056,'num_clus']
                        ds_mtbl[4055,'indx_Clcon'] = ds_mtbl[4056,'indx_Clcon']
                        ds_mtbl[4055,'duration']   = ds_mtbl[4056,'duration']
                        ds_mtbl[4055,'num_info']   = ds_mtbl[4056,'num_info']

                elif ns == 65:
                    ds_mtbl = grp['metaTable']
                    ds_mtbl[2204:52867,'num_clus']   = 40
                    ds_mtbl[2204:52867,'indx_Clcon'] = 0
                    ds_mtbl[2204:52867,'duration']   = 320
                    ds_mtbl[2204:52867,'num_info']   = 40

                    grp_46 = fid['State_46']
                    ds_46 = grp_46['clusDef']
                    clusDef = ds_46[0,:]
                    ds = grp.create_dataset( 'clusDef', 
                                             data=clusDef.reshape(1,64),
                                             maxshape=(None,64) )
                else:
                    print( "Info: skipping state %d" % (ns) )

        
#-------------------------SECTION ARGPARSE----------------------------------
def handleCmdParams():
    """Use argparse to process command-line parameters."""
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser( 
        formatter_class=RawDescriptionHelpFormatter,
        description= 'read Sciamachy level 1b product'
        )
    parser.add_argument( '-v', '--verbose', action='store_true',
                         help='be verbose' )
    parser.add_argument( 'db_name', nargs='?', type=str,
                         default='./nadc_clusDef.h5',
                         help='write to hdf5 database' )    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument( '--orbit', nargs=1, type=int, 
                         help='select data from given orbit, preferably \'W\'' )
    group.add_argument( '--file', type=str, help='read data from given file' )
    group.add_argument( '--mtbl_fill', action='store_true',
                        help='update missing data to metaTable' )
    return parser.parse_args()

#-------------------------SECTION MAIN--------------------------------------
if __name__ == '__main__':
    import os.path
    import scia.db as db
    import scia.lv1 as lv1

    args = handleCmdParams()

    scia_fl = ""
    if args.orbit is not None:
        fileList = db.get_product_by_type( prod_type='1',
                                           proc_stage='W', 
                                           orbits=args.orbit )
        if len(fileList) > 0 and os.path.isfile( fileList[0] ):
            scia_fl = fileList[0]
    elif args.file is not None:
        if os.path.isfile( args.file ):
            scia_fl = args.file
        else:
            fileList = db.get_product_by_name( product=args.file )
            if len(fileList) > 0 and os.path.isfile( fileList[0] ):
                scia_fl = fileList[0]
    else:
        obj_db = clusDB( args )
        obj_db.add_missing_state_10_13()
        obj_db.add_missing_state_14()
        obj_db.add_missing_state_22()
        obj_db.add_missing_state_24()
        obj_db.add_missing_state_25_26()
        obj_db.add_missing_state_27()
        obj_db.add_missing_state_33_39()
        obj_db.add_missing_state_35_39()
        obj_db.add_missing_state_42()
        obj_db.add_missing_state_43()
        obj_db.add_missing_state_44()
        obj_db.add_missing_state_55()
        obj_db.fill_mtbl()
        sys.exit(0)

    if not scia_fl:
        print( "Failed: file not found on your system" )
        sys.exit(0)

    print( scia_fl )
    # create object and open Sciamachy level 1b product
    try:
        obj = lv1.File( scia_fl )
    except lv1.fmtError as e:
        print( e.msg )
        sys.exit(1)

    # check fileSize
    if obj.mph['TOTAL_SIZE'] != os.path.getsize(scia_fl):
        print( 'Fatal: file %s incomplete' % scia_fl)
        sys.exit(1)

    # read STATES GADS
    obj.getSTATES()

    if args.verbose:
        print( obj.states.dtype.names )
        print( obj.states['Clcon'].dtype.names )
        for xx in range( obj.states.size ):
            print( obj.states['state_id'][xx], obj.states['flag_attached'][xx], 
                   obj.states['flag_reason'][xx], obj.states['duration'][xx], 
                   obj.states['length'][xx],
                   obj.states['Clcon'][xx]['intg'][0:obj.states['num_clus'][xx]]
                   )

    # remove corrupted states
    states = obj.states[(obj.states['flag_reason'] != 1)
                        & (obj.states['duration'] != 0)] 

    # create clusterDef database object
    obj_db = clusDB( args )
    if not os.path.exists( obj_db.db_name ):
        obj_db.create()

    # loop over all ID of states
    for stateID in np.unique( states['state_id'] ):
        indx = np.where( states['state_id'] == stateID )[0]
        mtbl = ( obj.mph['ABS_ORBIT'], states['num_clus'][indx[0]],
                 0, states['duration'][indx[0]], states['num_geo'][indx[0]] )
        Clcon = states['Clcon'][indx[0],:]
        if args.verbose:
            print( stateID, ' - ', indx[0], Clcon.shape )
        obj_db.append( stateID, mtbl, Clcon )

    obj.__del__()

## @}
