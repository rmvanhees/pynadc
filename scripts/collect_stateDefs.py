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

Collect state definitions from Sciamachy level 1b products and store in HDF5 
database. Missing state definitions for states not written to level 1b products
can be added; OCR states are hard-coded in this program or by interpolation  

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

Create or modify HDF5 database with state definitions for Sciamachy level 0 processing

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

PROGNAME = str(sys.argv[0])
VERSION = '1.1.1'

def empty_clusDef():
    clusDef = np.zeros( 64, dtype=
          'uint8,uint8,uint16,uint16,float32,uint16,uint16,uint16,uint8' 
    )
    clusDef.dtype.names = ('clus_id','chan_id','start','length','pet',
                            'intg','coaddf','readouts','clus_type')
    return clusDef

def define_clusDef(nclus, pet, coaddf, readouts):
    clusDef = empty_clusDef()

    if nclus == 10:
        clusDef[0:10]['chan_id'] = [
            1, 1, 2, 2, 3, 4, 5, 6, 7, 8
        ]
        clusDef[0:10]['start']   = [
            0, 552, 170, 0, 0, 0, 0, 0, 0, 0
        ]
        clusDef[0:10]['length']  = [
            552, 472, 854, 170, 1024, 1024, 1024, 1024, 1024, 1024
        ]
    elif nclus == 40:
        clusDef[0:40]['chan_id'] = [
            1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 
            5, 5, 5, 5, 5, 6, 6, 6, 6, 6,
            7, 7, 7, 7, 7, 8, 8, 8
        ]
        clusDef[0:40]['start']   = [
            0, 5, 197, 552, 842, 1019, 1019, 948, 170, 76, 5, 0,
            0, 10, 33, 930, 1019, 0, 5, 10, 919, 1019,
            0, 5, 10, 1001, 1019, 0, 10, 24, 997, 1014,
            0, 10, 48, 988, 1014, 0, 10, 1014
        ]
        clusDef[0:40]['length']  = [
            5, 192, 355, 290, 177, 5, 5, 71, 778, 94, 71, 5,
            10, 23, 897, 89, 5, 5, 5, 909, 100, 5,
            5, 5, 991, 18, 5, 10, 14, 973, 17, 10,
            10, 38, 940, 26, 10, 10, 1004, 10
        ]
    elif nclus == 56:
        clusDef[0:56]['chan_id'] = [
            1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3, 3, 3, 3, 
            4, 4, 4, 4, 4, 4, 4, 4, 
            5, 5, 5, 5, 5, 5, 5, 
            6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
            7, 7, 7, 7, 7, 7, 8, 8, 8
        ]
        clusDef[0:56]['start']   = [
            0, 5, 197, 552, 748, 1019, 1019, 834, 170, 76, 0,
            0, 33, 83, 163, 599, 674, 761, 896, 1019,
            0, 10, 46, 78, 613, 747, 853, 1019,
            0, 10, 56, 84, 609, 767, 1019,
            0, 24, 107, 335, 361, 539, 567, 746, 900, 931, 945, 1014,
            0, 48, 293, 441, 883, 1014, 0, 10, 1014
        ]
        clusDef[0:56]['length']  = [
            5, 192, 355, 196, 94, 5, 5, 114, 664, 94, 5,
            10, 50, 80, 436, 75, 87, 135, 34, 5,
            5, 36, 32, 535, 134, 106, 66, 5,
            5, 46, 28, 525, 158, 234, 5,
            10, 83, 228, 26, 178, 28, 179, 154, 31, 14, 52, 10,
            10, 245, 148, 442, 105, 10, 10, 1004, 10
        ]

    clusDef[0:nclus]['clus_id'] = np.arange( 1, nclus+1, dtype='uint8' )
    clusDef[0:nclus]['pet']  = pet
    clusDef[0:nclus]['intg'] = np.asarray( np.clip(pet, 1/16., 1280) 
                                           * 16 * coaddf, dtype='uint16')
    clusDef[0:nclus]['coaddf'] = coaddf
    clusDef[0:nclus]['readouts'] = readouts
    clusDef[0:nclus]['clus_type'] = np.clip( coaddf, 0, 2 )

    return clusDef

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
        from time import gmtime, strftime

        maxOrbit = 53000

        with h5py.File( self.db_name, 'w', libver='latest' ) as fid:
            #
            # add global attributes
            #
            fid.attrs['title'] = 'Sciamachy state-cluster definition database'
            fid.attrs['institution'] = 'SRON (EPS)'
            fid.attrs['history'] = ' '.join(sys.argv)
            fid.attrs['source'] = 'Sciamachy Level 1b (v7.x)'
            fid.attrs['program_version'] = VERSION
            fid.attrs['creation_date'] = strftime('%Y-%m-%d %T %Z', gmtime())
            #
            # initialize metaTable dataset
            #
            mtbl = np.zeros( maxOrbit, 
                             dtype='uint16,uint8,uint8,uint16,uint16' )
            mtbl.dtype.names = ('orbit','num_clus','indx_Clcon',
                                'duration', 'num_info')
            mtbl[:]['orbit'] = np.arange( maxOrbit, dtype='uint16' )
            mtbl[:]['indx_Clcon'] = 2**8-1
            #
            # initialize state-cluster definition dataset
            #            
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

    def add_missing_state_3033( self ):
        """Append OCR state cluster definition.

        Modified states for orbit 3033:
        - stateID = 8, 10, 11, 12, 13, 14, 15, 26, 34, 35, 36, 37, 40, 41 
        """
        orbit_list = [3033]
        with h5py.File( self.db_name, 'r+' ) as fid:
            grp = fid['State_08']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1., 1., 1., .25, .25, .25,
                                 .125, .125, .125, .25, .25,
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 
                                 1/32., 1/32., 1/32., 1/32., 1/32., 1/32., 
                                 1/32., 1/32., 
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                                 1/8., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 .25, .25, .25, .25, .25, .25,
                                 .25, .25, .25 ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 2, 1, 4,
                                    8, 4, 2, 1, 4,
                                    16, 8, 16, 4, 16, 8, 16, 16, 16,
                                    16, 16, 16, 8, 16, 8, 16, 16,
                                    8, 8, 8, 4, 8, 8, 8,
                                    16, 16, 16, 8, 16, 8, 16, 8, 16, 8, 16, 16,
                                    4, 4, 1, 4, 1, 4,
                                    4, 1, 4 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 2, 4, 1,
                                   1, 2, 4, 4, 1,
                                   1, 2, 1, 4, 1, 2, 1, 1, 1,
                                   1, 1, 1, 2, 1, 2, 1, 1,
                                   1, 1, 1, 2, 1, 1, 1,
                                   1, 1, 1, 2, 1, 2, 1, 2, 1, 2, 1, 1,
                                   1, 1, 4, 1, 4, 1,
                                   1, 4, 1 ], dtype='uint16')
                clusDef = define_clusDef(56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1040
                ds_mtbl[orbit_list,'num_info'] = 260

            grp = fid['State_10']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 1
                ds_mtbl[orbit_list,'duration'] = 1280
                ds_mtbl[orbit_list,'num_info'] = 160

            grp = fid['State_11']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 1
                ds_mtbl[orbit_list,'duration'] = 1280
                ds_mtbl[orbit_list,'num_info'] = 320

            grp = fid['State_12']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 1
                ds_mtbl[orbit_list,'duration'] = 1040
                ds_mtbl[orbit_list,'num_info'] = 520

            grp = fid['State_13']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1., 1., 1., .5, .5, .5,
                                 .5, .5, .5, .5, .5,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 
                                 1/8., 1/8., 1/8.,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                                 .25, .25, .25, .25, .25, .25, .25,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                                 1., 1., 1., 1., 1., 1.,
                                 1., 1., 1. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 2,
                                    2, 2, 1, 1, 2,
                                    8, 8, 1, 8, 1, 1, 8, 8, 8, 
                                    8, 8, 8, 1, 8, 1, 8, 8,
                                    4, 4, 4, 1, 4, 1, 4,
                                    8, 2, 8, 1, 8, 1, 8, 1, 8, 1, 8, 8,
                                    1, 1, 1, 1, 1, 1,
                                    1, 1, 1 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 2, 2, 1,
                                   1, 1, 2, 2, 1,
                                   1, 1, 8, 1, 8, 8, 1, 1, 1,
                                   1, 1, 1, 8, 1, 8, 1, 1,
                                   1, 1, 1, 4, 1, 4, 1,
                                   1, 4, 1, 8, 1, 8, 1, 8, 1, 8, 1, 1,
                                   1, 1, 1, 1, 1, 1,
                                   1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1040
                ds_mtbl[orbit_list,'num_info'] = 520

            grp = fid['State_14']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1., 1., 1., .25, .25, .25,
                                 .25, .25, .25, .25, .25,
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16.,
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16.,
                                 .25, .25, .25, .25, .25, .25, .25,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 
                                 .5, .5, .5, .5, .5, .5,
                                 .5, .5, .5 ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 4,
                                    4, 4, 1, 1, 4,
                                    16, 16, 16, 4, 16, 4, 16, 16, 16,
                                    16, 16, 16, 4, 16, 4, 16, 16,
                                    4, 4, 4, 1, 4, 2, 4,
                                    8, 4, 8, 2, 8, 2, 8, 2, 8, 2, 8, 8,
                                    2, 2, 1, 2, 1, 2,
                                    2, 1, 2 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 4, 4, 1,
                                   1, 1, 4, 4, 1,
                                   1, 1, 1, 4, 1, 4, 1, 1, 1,
                                   1, 1, 1, 4, 1, 4, 1, 1,
                                   1, 1, 1, 4, 1, 2, 4,
                                   1, 2, 1, 4, 1, 4, 1, 4, 1, 4, 1, 1,
                                   1, 1, 2, 1, 2, 1,
                                   1, 2, 1 ], dtype='uint16')
                clusDef = define_clusDef(56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1040
                ds_mtbl[orbit_list,'num_info'] = 260

            grp = fid['State_15']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 1
                ds_mtbl[orbit_list,'duration'] = 1040
                ds_mtbl[orbit_list,'num_info'] = 260

            grp = fid['State_26']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 80., 80., 80., 80., 80., 80.,
                                 80., 80., 80., 80., 80.,
                                 80., 80., 80., 80., 80., 80., 80., 80., 80.,
                                 80., 80., 80., 80., 80., 80., 80., 80.,
                                 80., 80., 80., 80., 80., 80., 80.,
                                 10., 10., 10., 10., 10., 10., 
                                 10., 10., 10., 10., 10., 10.,
                                 1., 1., 1., 1., 1., 1.,
                                 1., 1., 1. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                    1, 1, 1, 1, 1, 1,
                                    1, 1, 1 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1, 1,
                                   8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
                                   80, 80, 80, 80, 80, 80,
                                   80, 80, 80 ], dtype='uint16')
                clusDef = define_clusDef(56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1280
                ds_mtbl[orbit_list,'num_info'] = 80

            grp = fid['State_34']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1.5, 1.5, 1.5, 3/8., 3/8., 3/8., 
                                 3/16., 3/16., 3/16., 3/8., 3/8., 3/8., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 3/8., 3/8., 3/8., 3/8., 3/8., 
                                 3/8., 3/8., 3/8., 3/8., 3/8.,
                                 3/8., 3/8., 3/8., 3/8., 3/8.,
                                 3/8., 3/8., 3/8. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 4, 4, 8, 8, 2, 1, 4, 4, 
                                    24, 24, 6, 24, 24, 24, 24, 6, 24, 24, 
                                    4, 4, 1, 4, 4, 4, 4, 1, 4, 4, 
                                    4, 4, 1, 4, 4, 4, 1, 4 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 4, 1, 1, 1, 1, 4, 4, 1, 1, 
                                   1, 1, 4, 1, 1, 1, 1, 4, 1, 1, 
                                   1, 1, 4, 1, 1, 1, 1, 4, 1, 1, 
                                   1, 1, 4, 1, 1, 1, 4, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 944
                ds_mtbl[orbit_list,'num_info'] = 140

            grp = fid['State_35']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 0
                ds_mtbl[orbit_list,'duration'] = 944
                ds_mtbl[orbit_list,'num_info'] = 140

            grp = fid['State_36']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 0
                ds_mtbl[orbit_list,'duration'] = 944
                ds_mtbl[orbit_list,'num_info'] = 280

            grp = fid['State_37']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 1
                ds_mtbl[orbit_list,'duration'] = 944
                ds_mtbl[orbit_list,'num_info'] = 140

            grp = fid['State_40']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 0
                ds_mtbl[orbit_list,'duration'] = 944
                ds_mtbl[orbit_list,'num_info'] = 70

            grp = fid['State_41']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1.5, 1.5, 1.5, 3/8., 3/8., 3/8., 
                                 3/16., 3/16., 3/16., 3/8., 3/8., 3/8., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 3/8., 3/8., 3/8., 3/8., 3/8., 
                                 3/8., 3/8., 3/8., 3/8., 3/8.,
                                 3/8., 3/8., 3/8., 3/8., 3/8.,
                                 3/8., 3/8., 3/8. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 4, 4, 8, 8, 2, 1, 4, 4, 
                                    24, 24, 6, 24, 24, 24, 24, 6, 24, 24, 
                                    4, 4, 1, 4, 4, 4, 4, 1, 4, 4, 
                                    4, 4, 1, 4, 4, 4, 1, 4 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 4, 1, 1, 1, 1, 4, 4, 1, 1, 
                                   1, 1, 4, 1, 1, 1, 1, 4, 1, 1, 
                                   1, 1, 4, 1, 1, 1, 1, 4, 1, 1, 
                                   1, 1, 4, 1, 1, 1, 4, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 944
                ds_mtbl[orbit_list,'num_info'] = 140

    def add_missing_state_3034( self ):
        """Append OCR state cluster definition.

        Modified states for orbit 3034:
        - stateID = 17, 23, 24, 25, 26, 42, 43, 44, 51 
        """
        orbit_list = [3034]
        with h5py.File( self.db_name, 'r+' ) as fid:
            grp = fid['State_17']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 0
                ds_mtbl[orbit_list,'duration'] = 480
                ds_mtbl[orbit_list,'num_info'] = 30

            grp = fid['State_23']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 0
                ds_mtbl[orbit_list,'duration'] = 1280
                ds_mtbl[orbit_list,'num_info'] = 80

            grp = fid['State_24']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 0
                ds_mtbl[orbit_list,'duration'] = 1280
                ds_mtbl[orbit_list,'num_info'] = 160

            grp = fid['State_25']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 0
                ds_mtbl[orbit_list,'duration'] = 1280
                ds_mtbl[orbit_list,'num_info'] = 320

            grp = fid['State_26']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 2
                ds_mtbl[orbit_list,'duration'] = 1280
                ds_mtbl[orbit_list,'num_info'] = 80

            grp = fid['State_42']
            ds_mtbl  = grp['metaTable']
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = 1
                ds_mtbl[orbit_list,'duration'] = 1040
                ds_mtbl[orbit_list,'num_info'] = 520

            grp = fid['State_43']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1., 1., 1., .5, .5, .5,
                                 .5, .5, .5, .5, .5,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 
                                 1/8., 1/8., 1/8.,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                                 .25, .25, .25, .25, .25, .25, .25,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 
                                 1., 1., 1., 1., 1., 1.,
                                 1., 1., 1. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 2,
                                    2, 2, 1, 1, 2,
                                    8, 8, 1, 8, 1, 1, 8, 8, 8,
                                    8, 8, 8, 1, 8, 1, 8, 8,
                                    4, 4, 4, 1, 4, 1, 4,
                                    8, 2, 8, 1, 8, 1, 8, 1, 8, 1, 8, 8,
                                    1, 1, 1, 1, 1, 1,
                                    1, 1, 1 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 2, 2, 1,
                                   1, 1, 2, 2, 1,
                                   1, 1, 8, 1, 8, 8, 1, 1, 1,
                                   1, 1, 1, 8, 1, 8, 1, 1,
                                   1, 1, 1, 4, 1, 4, 1,
                                   1, 1, 1, 8, 1, 8, 1, 8, 1, 8, 1, 1,
                                   1, 1, 1, 1, 1, 1,
                                   1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1040
                ds_mtbl[orbit_list,'num_info'] = 520

            grp = fid['State_44']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1., 1., 1., .25, .25, .25,
                                 .25, .25, .25, .25, .25,
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16.,
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16.,
                                 .25, .25, .25, .25, .25, .25, .25,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                                 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 
                                 .5, .5, .5, .5, .5, .5,
                                 .5, .5, .5 ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 4,
                                    4, 4, 1, 1, 4,
                                    16, 16, 16, 4, 16, 4, 16, 16, 16,
                                    16, 16, 16, 4, 16, 4, 16, 16,
                                    4, 4, 4, 1, 4, 2, 4,
                                    8, 4, 8, 2, 8, 2, 8, 2, 8, 2, 8, 8,
                                    2, 2, 1, 2, 1, 2,
                                    2, 1, 2 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 4, 4, 1,
                                   1, 1, 4, 4, 1,
                                   1, 1, 1, 4, 1, 4, 1, 1, 1,
                                   1, 1, 1, 4, 1, 4, 1, 1,
                                   1, 1, 1, 4, 1, 2, 4,
                                   1, 2, 1, 4, 1, 4, 1, 4, 1, 4, 1, 1,
                                   1, 1, 2, 1, 2, 1,
                                   1, 2, 1 ], dtype='uint16')
                clusDef = define_clusDef(56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 56
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1040
                ds_mtbl[orbit_list,'num_info'] = 260

            grp = fid['State_51']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/32., 1/32., 1/32., 1/32., 1/32.,
                                 1/32., 1/32., 1/32., 1/32., 1/32.,
                                 1/16., 1/16., 1/16. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 
                                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 
                                    1, 1, 1, 1, 1, 2, 2, 2 ], dtype='uint8')
                nread = np.array([ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 
                                   1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                   2, 2, 2, 2, 2, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 1024
                ds_mtbl[orbit_list,'num_info'] = 1024

    def add_missing_state_09( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=09 added definitions for orbits [3981]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_09']
            ds_mtbl  = grp['metaTable']
            orbit_list = [3981]
            if np.all(ds_mtbl[orbit_list,'num_info'] == 0):
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = 255
                ds_mtbl[orbit_list,'duration'] = 918
                ds_mtbl[orbit_list,'num_info'] = 166

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
            pet = np.array([ 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                             1/32., 1/32., 1/32., 1/32., 1/32., 1/32., 
                             1/16., 1/16., 1/16., 1/16., 1/16., 
                             1/16., 1/16., 1/16., 1/16., 1/16., 
                             1/16., 1/16., 1/16., 1/16., 1/16., 
                             1/32., 1/32., 1/32., 1/32., 1/32.,
                             1/32., 1/32., 1/32., 1/32., 1/32.,
                             1/16., 1/16., 1/16. ], dtype='float32')
            coaddf = np.array([ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                8, 8, 8, 8, 8, 8, 8, 8 ], dtype='uint8')
            nread = np.array([ 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
                               8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
                               8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
                               1, 1, 1, 1, 1, 1, 1, 1 ], dtype='uint16')
            clusDef = define_clusDef(40, pet, coaddf, nread)
            orbit_list = [3964,3968,4118,4122]
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
                pet = np.array([ 40., 40., 40., 40., 
                                 10., 4., 4., 1., 1., 2. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 
                                    4, 10, 10, 40, 40, 20 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(10, pet, coaddf, nread)
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
                pet = np.array([ 1.5, 1.5, 1.5, 1.5, 
                                 1.5, 1.5, 1.5, 1.5, 
                                 1.5, 1.5 ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(10, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 10
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 782
                ds_mtbl[orbit_list,'num_info'] = 29

    def add_missing_state_24( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=24 added definitions for orbits [36873:38267, 47994:48075]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:
            grp = fid['State_24']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all( np.append(ds_mtbl[36873:38267,'indx_Clcon'],
                                 ds_mtbl[47994:48075,'indx_Clcon']) == 255 ):
                pet = np.array([ 1., 1., 1., 1., 1., 1., 
                                 1., 1., 1., 1., 1., 1., 
                                 1., 1., 1., 1., 1., 
                                 1., 1., 1., 1., 1., 
                                 1., 1., 1., 1., 1., 
                                 1., 1., 1., 1., 1.,
                                 1., 1., 1., 1., 1.,
                                 1., 1., 1. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[36873:38267,'num_clus'] = 40
                ds_mtbl[36873:38267,'indx_Clcon'] = clus_dim
                ds_mtbl[36873:38267,'duration'] = 1600
                ds_mtbl[36873:38267,'num_info'] = 100

                ds_mtbl[47994:48075,'num_clus'] = 40
                ds_mtbl[47994:48075,'indx_Clcon'] = clus_dim
                ds_mtbl[47994:48075,'duration'] = 1440
                ds_mtbl[47994:48075,'num_info'] = 90

    def add_missing_state_25_26( self ):
        """Append OCR state cluster definition.

        Modified states:
        - stateID=25 added definitions for orbits [4088,4111]
        - stateID=26 added definitions for orbits [4089]
        """
        with h5py.File( self.db_name, 'r+' ) as fid:
            grp = fid['State_25']
            ds_mtbl  = grp['metaTable']
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4088,4111]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1/4., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/8., 1/32., 1/16., 1/8. ], dtype='float32')
                coaddf = np.array([ 1, 4, 4, 4, 
                                    4, 4, 2, 4, 4, 2 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(10, pet, coaddf, nread)
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
                pet = np.array([ 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 
                                 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 
                                 .75, .75, .75, .75, .75, 
                                 .75, .75, .75, .75, .75, 
                                 1.5, 1.5, 1.5, 1.5, 1.5, 
                                 1.5, 1.5, 1.5, 1.5, 1.5,
                                 1.5, 1.5, 1.5, 1.5, 1.5,
                                 1.5, 1.5, 1.5 ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 6, 6, 6 ], dtype='uint8')
                nread = np.array([ 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
                                   12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 
                                   6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
                                   6, 6, 6, 6, 6, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 647
                ds_mtbl[orbit_list,'num_info'] = 48
                clus_dim += 1

            orbit_list = [44134,44148,44149,44150]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 
                                 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 
                                 .75, .75, .75, .75, .75, 
                                 .75, .75, .75, .75, .75, 
                                 1.5, 1.5, 1.5, 1.5, 1.5, 
                                 1.5, 1.5, 1.5, 1.5, 1.5,
                                 1.5, 1.5, 1.5, 1.5, 1.5,
                                 1.5, 1.5, 1.5 ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 12, 12, 12 ], dtype='uint8')
                nread = np.array([ 12, 12, 12, 12, 12, 12, 
                                   12, 12, 12, 12, 12, 12, 
                                   24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 
                                   12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 
                                   12, 12, 12, 12, 12, 
                                   1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[orbit_list,'num_clus'] = 40
                ds_mtbl[orbit_list,'indx_Clcon'] = clus_dim
                ds_mtbl[orbit_list,'duration'] = 647
                ds_mtbl[orbit_list,'num_info'] = 48
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
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 2., 2., 1/4., 1/4., 
                                 1/8., 1/32., 1/32., 
                                 .0072, .0036, .0072 ], dtype='float32')
                coaddf = np.array([ 1, 1, 8, 8, 
                                    16, 32, 32, 32, 32, 32 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(10, pet, coaddf, nread)
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
                pet = np.array([ 2., 2., 1/4., 1/4., 
                                 1/8., 1/32., 1/32., 
                                 .0072, .0036, .0072 ], dtype='float32')
                coaddf = np.array([ 1, 1, 8, 8, 
                                    16, 32, 32, 32, 32, 32 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(10, pet, coaddf, nread)
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
                pet = np.array([ 2., 2., 1/4., 1/4., 
                                 1/8., 1/32., 1/32., 
                                 .0072, .0036, .0072 ], dtype='float32')
                coaddf = np.array([ 1, 1, 8, 8, 
                                    16, 32, 32, 32, 32, 32 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(10, pet, coaddf, nread)
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
                pet = np.array([ 2., 2., 1/4., 1/4., 
                                 1/8., 1/32., 1/32., 
                                 .0072, .0036, .0072 ], dtype='float32')
                coaddf = np.array([ 1, 1, 8, 8, 
                                    16, 32, 32, 32, 32, 32 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 
                                   1, 1, 1, 1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(10, pet, coaddf, nread)
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
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/32., 1/32., 1/32., 1/32., 1/32., 1/32., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/32., 1/32., 1/32., 1/32., 1/32.,
                                 1/32., 1/32., 1/32., 1/32., 1/32.,
                                 1/16., 1/16., 1/16. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
                                    8, 8, 8, 8, 8, 8, 8, 8 ], dtype='uint8')
                nread = np.array([ 8, 8, 8, 8, 8, 8, 
                                   8, 8, 8, 8, 8, 8, 
                                   8, 8, 8, 8, 8,
                                   8, 8, 8, 8, 8,
                                   8, 8, 8, 8, 8,
                                   8, 8, 8, 8, 8,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
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

            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [7193, 7194]
            if np.all(ds_mtbl[orbit_list,'indx_Clcon'] == 255):
                pet = np.array([ 10., 10., 10., 10., 10., 10., 
                                 10., 10., 10., 10., 10., 10., 
                                 2.5, 2.5, 2.5, 2.5, 2.5, 
                                 2.5, 2.5, 2.5, 2.5, 2.5, 
                                 2.5, 2.5, 2.5, 2.5, 2.5, 
                                 1/32., 1/32., 1/32., 1/32., 1/32.,
                                 1/32., 1/32., 1/32., 1/32., 1/32.,
                                 1/32., 1/32., 1/32. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 
                                    16, 16, 16, 16, 16, 
                                    16, 16, 16, 16, 16, 
                                    16, 16, 16 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1,
                                   4, 4, 4, 4, 4,
                                   4, 4, 4, 4, 4,
                                   4, 4, 4, 4, 4,
                                   10, 10, 10, 10, 10,
                                   10, 10, 10, 10, 10,
                                   10, 10, 10 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
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
                pet = np.array([ 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 
                                 1/16., 1/16., 1/16., 1/16., 1/16.,
                                 1/16., 1/16., 1/16., 1/16., 1/16.,
                                 1/16., 1/16., 1/16., 1/16., 1/16.,
                                 1/16., 1/16., 1/16., 1/16., 1/16.,
                                 1/16., 1/16., 1/16., 1/16., 1/16.,
                                 1/16., 1/16., 1/16. ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 
                                    2, 2, 2, 2, 2, 
                                    2, 2, 2, 2, 2, 
                                    2, 2, 2, 2, 2, 
                                    2, 2, 2, 2, 2, 
                                    1, 1, 1, 1, 1,
                                    2, 2, 2 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1,
                                   2, 2, 2, 2, 2,
                                   2, 2, 2, 2, 2,
                                   2, 2, 2, 2, 2,
                                   1, 1, 1, 1, 1,
                                   2, 2, 2, 2, 2,
                                   1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[26812:26834,'num_clus'] = 40
                ds_mtbl[26812:26834,'indx_Clcon'] = clus_dim
                ds_mtbl[26812:26834,'duration'] = 640
                ds_mtbl[26812:26834,'num_info'] = 640
                clus_dim += 1

            if np.all( np.append(ds_mtbl[28917:28920,'indx_Clcon'],
                                 ds_mtbl[30836:30850,'indx_Clcon']) == 255 ):
                pet = np.array([ .5, .5, .5, .5, .5, .5, 
                                 .5, .5, .5, .5, .5, .5, 
                                 .5, .5, .5, .5, .5,
                                 .5, .5, .5, .5, .5,
                                 .5, .5, .5, .5, .5,
                                 .5, .5, .5, .5, .5,
                                 .5, .5, .5, .5, .5,
                                 .5, .5, .5 ], dtype='float32')
                coaddf = np.array([ 1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 
                                    1, 1, 1, 1, 1, 
                                    1, 1, 1 ], dtype='uint8')
                nread = np.array([ 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1 ], dtype='uint16')
                clusDef = define_clusDef(40, pet, coaddf, nread)
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

                    if ns == 49 and ds_mtbl[4381,'duration'] == 2078:
                        ds_mtbl[4381,'duration']   = 2080
                        ds_mtbl[4381,'num_info']   = 2080

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
        obj_db.add_missing_state_3033()
        obj_db.add_missing_state_3034()
        obj_db.add_missing_state_09()
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
