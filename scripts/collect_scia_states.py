#!/usr/bin/env python
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Collect state definitions from Sciamachy level 1b products

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

--all-lv1

  --force

--add_ocr

--finalize

Output
------

Create or modify HDF5 database with state definitions for Sciamachy level 0
processing

Examples
--------

None

Author
------

Richard van Hees (r.m.van.hees@sron.nl)

Bug reporting
-------------

Please report issues at the pyNADC Github page:
https://github.com/rmvanhees/pynadc.git

Copyright (c) 2016-2018 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  Standard 3-clause BSD
"""
from pathlib import Path

import h5py
import numpy as np

# - global parameters ------------------------------
VERSION = '2.0.0'

MAX_ORBIT = 53000


# - local functions --------------------------------
def fill_clus_conf(clus_conf, nclus, pet, coaddf, readouts):
    """
    define cluster configuration for a given state
    """
    if nclus == 10:
        clus_conf['channel'][0:10] = [
            1, 1, 2, 2, 3, 4, 5, 6, 7, 8
        ]
        clus_conf['start'][0:10] = [
            0, 552, 170, 0, 0, 0, 0, 0, 0, 0
        ]
        clus_conf['length'][0:10] = [
            552, 472, 854, 170, 1024, 1024, 1024, 1024, 1024, 1024
        ]
    elif nclus == 40:
        clus_conf['channel'][0:40] = [
            1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 4, 4, 4, 4, 4,
            5, 5, 5, 5, 5, 6, 6, 6, 6, 6,
            7, 7, 7, 7, 7, 8, 8, 8
        ]
        clus_conf['start'][0:40] = [
            0, 5, 197, 552, 842, 1019, 1019, 948, 170, 76, 5, 0,
            0, 10, 33, 930, 1019, 0, 5, 10, 919, 1019,
            0, 5, 10, 1001, 1019, 0, 10, 24, 997, 1014,
            0, 10, 48, 988, 1014, 0, 10, 1014
        ]
        clus_conf['length'][0:40] = [
            5, 192, 355, 290, 177, 5, 5, 71, 778, 94, 71, 5,
            10, 23, 897, 89, 5, 5, 5, 909, 100, 5,
            5, 5, 991, 18, 5, 10, 14, 973, 17, 10,
            10, 38, 940, 26, 10, 10, 1004, 10
        ]
    elif nclus == 56:
        clus_conf['channel'][0:56] = [
            1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3, 3, 3, 3,
            4, 4, 4, 4, 4, 4, 4, 4,
            5, 5, 5, 5, 5, 5, 5,
            6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
            7, 7, 7, 7, 7, 7, 8, 8, 8
        ]
        clus_conf['start'][0:56] = [
            0, 5, 197, 552, 748, 1019, 1019, 834, 170, 76, 0,
            0, 33, 83, 163, 599, 674, 761, 896, 1019,
            0, 10, 46, 78, 613, 747, 853, 1019,
            0, 10, 56, 84, 609, 767, 1019,
            0, 24, 107, 335, 361, 539, 567, 746, 900, 931, 945, 1014,
            0, 48, 293, 441, 883, 1014, 0, 10, 1014
        ]
        clus_conf['length'][0:56] = [
            5, 192, 355, 196, 94, 5, 5, 114, 664, 94, 5,
            10, 50, 80, 436, 75, 87, 135, 34, 5,
            5, 36, 32, 535, 134, 106, 66, 5,
            5, 46, 28, 525, 158, 234, 5,
            10, 83, 228, 26, 178, 28, 179, 154, 31, 14, 52, 10,
            10, 245, 148, 442, 105, 10, 10, 1004, 10
        ]

    clus_conf['id'][0:nclus] = np.arange(1, nclus+1, dtype='u1')
    clus_conf['coaddf'][0:nclus] = coaddf
    clus_conf['type'][0:nclus] = np.clip(coaddf, 0, 2)
    clus_conf['intg'][0:nclus] = np.asarray(np.clip(pet, 1/16., 1280)
                                            * 16 * coaddf, dtype='u2')
    clus_conf['n_read'][0:nclus] = readouts
    clus_conf['pet'][0:nclus] = pet


class ClusDB:
    """
    define class to collect Sciamachy instrument settings per state
    """
    def __init__(self, args=None, db_name='./scia_state_db.h5',
                 verbose=False):
        """
        Initialize the class ClusDB.
        """
        self.fid = None
        if args:
            self.db_name = args.db_name
            self.verbose = args.verbose
        else:
            self.db_name = db_name
            self.verbose = verbose

    def close(self):
        """
        Close resources
        """
        if self.fid is not None:
            self.fid.close()

    @staticmethod
    def mtbl_dtype():
        """
        Returns numpy-dtype definition for a metaTable record
        """
        return np.dtype([
            ('type_clus', 'u1'),
            ('num_clus', 'u1'),
            ('duration', 'u2'),
            ('num_info', 'u2'),
        ])

    @staticmethod
    def clus_dtype():
        """
        Returns numpy-dtype definition for a clusDef record
        """
        return np.dtype([
            ('id', 'u1'),
            ('channel', 'u1'),
            ('coaddf', 'u1'),
            ('type', 'u1'),
            ('start', 'u2'),
            ('length', 'u2'),
            ('intg', 'u2'),
            ('n_read', 'u2'),
            ('pet', 'f4')
        ])

    def create(self):
        """
        Create and initialize the state definition database.
        """
        from time import gmtime, strftime

        self.fid = h5py.File(self.db_name, 'w',
                             driver='core', backing_store=True,
                             libver='latest')
        #
        # add global attributes
        #
        self.fid.attrs['title'] = 'Sciamachy state-cluster definition database'
        self.fid.attrs['institution'] = \
                        'SRON Netherlands Institute for Space Research (Earth)'
        self.fid.attrs['source'] = 'Sciamachy Level 1b (SCIA/8.01)'
        self.fid.attrs['program_version'] = VERSION
        self.fid.attrs['creation_date'] = strftime('%Y-%m-%d %T %Z', gmtime())
        #
        # initialize metaTable dataset
        #
        mtbl = np.zeros(MAX_ORBIT, dtype=self.mtbl_dtype())
        mtbl['type_clus'][:] = 0xFF
        #
        # initialize state-cluster definition dataset
        #
        for ni in range(1, 71):
            grp = self.fid.create_group('State_{:02d}'.format(ni))
            _ = grp.create_dataset('metaTable', data=mtbl,
                                   chunks=(12288 // mtbl.dtype.itemsize,),
                                   shuffle=True)
            _ = grp.create_dataset('clusDef', (0, 64),
                                   dtype=self.clus_dtype(),
                                   chunks=(1, 64), maxshape=(None, 64),
                                   shuffle=True)

    def add_missing_state_3033(self):
        """
        Append OCR state cluster definition.

        Modified states for orbit 3033:
        - stateID = 8, 10, 11, 12, 13, 14, 15, 26, 34, 35, 36, 37, 40, 41
        """
        orbit_list = 3033
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_08']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1040
                ds_mtbl[orbit_list]['num_info'] = 260
                pet = np.array([
                    1., 1., 1., .25, .25, .25,
                    .125, .125, .125, .25, .25,
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16.,
                    1/32., 1/32., 1/32., 1/32., 1/32., 1/32., 1/32., 1/32.,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    .25, .25, .25, .25, .25, .25,
                    .25, .25, .25], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 2, 1, 4,
                    8, 4, 2, 1, 4,
                    16, 8, 16, 4, 16, 8, 16, 16, 16,
                    16, 16, 16, 8, 16, 8, 16, 16,
                    8, 8, 8, 4, 8, 8, 8,
                    16, 16, 16, 8, 16, 8, 16, 8, 16, 8, 16, 16,
                    4, 4, 1, 4, 1, 4,
                    4, 1, 4], dtype='u1')
                nread = np.array([
                    1, 1, 1, 2, 4, 1,
                    1, 2, 4, 4, 1,
                    1, 2, 1, 4, 1, 2, 1, 1, 1,
                    1, 1, 1, 2, 1, 2, 1, 1,
                    1, 1, 1, 2, 1, 1, 1,
                    1, 1, 1, 2, 1, 2, 1, 2, 1, 2, 1, 1,
                    1, 1, 4, 1, 4, 1,
                    1, 4, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_10']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 1
                ds_mtbl[orbit_list]['duration'] = 1280
                ds_mtbl[orbit_list]['num_info'] = 160

            grp = fid['State_11']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 1
                ds_mtbl[orbit_list]['duration'] = 1280
                ds_mtbl[orbit_list]['num_info'] = 320

            grp = fid['State_12']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 1
                ds_mtbl[orbit_list]['duration'] = 1040
                ds_mtbl[orbit_list]['num_info'] = 520

            grp = fid['State_13']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1040
                ds_mtbl[orbit_list]['num_info'] = 520
                pet = np.array([
                    1., 1., 1., .5, .5, .5,
                    .5, .5, .5, .5, .5,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1/8., 1/8., 1/8.,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    .25, .25, .25, .25, .25, .25, .25,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1., 1., 1., 1., 1., 1.,
                    1., 1., 1.], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 1, 2,
                    2, 2, 1, 1, 2,
                    8, 8, 1, 8, 1, 1, 8, 8, 8,
                    8, 8, 8, 1, 8, 1, 8, 8,
                    4, 4, 4, 1, 4, 1, 4,
                    8, 2, 8, 1, 8, 1, 8, 1, 8, 1, 8, 8,
                    1, 1, 1, 1, 1, 1,
                    1, 1, 1], dtype='u1')
                nread = np.array([
                    1, 1, 1, 2, 2, 1,
                    1, 1, 2, 2, 1,
                    1, 1, 8, 1, 8, 8, 1, 1, 1,
                    1, 1, 1, 8, 1, 8, 1, 1,
                    1, 1, 1, 4, 1, 4, 1,
                    1, 4, 1, 8, 1, 8, 1, 8, 1, 8, 1, 1,
                    1, 1, 1, 1, 1, 1,
                    1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_14']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1040
                ds_mtbl[orbit_list]['num_info'] = 260
                pet = np.array([
                    1., 1., 1., .25, .25, .25,
                    .25, .25, .25, .25, .25,
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16.,
                    .25, .25, .25, .25, .25, .25, .25,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    .5, .5, .5, .5, .5, .5,
                    .5, .5, .5], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 1, 4,
                    4, 4, 1, 1, 4,
                    16, 16, 16, 4, 16, 4, 16, 16, 16,
                    16, 16, 16, 4, 16, 4, 16, 16,
                    4, 4, 4, 1, 4, 2, 4,
                    8, 4, 8, 2, 8, 2, 8, 2, 8, 2, 8, 8,
                    2, 2, 1, 2, 1, 2,
                    2, 1, 2], dtype='u1')
                nread = np.array([
                    1, 1, 1, 4, 4, 1,
                    1, 1, 4, 4, 1,
                    1, 1, 1, 4, 1, 4, 1, 1, 1,
                    1, 1, 1, 4, 1, 4, 1, 1,
                    1, 1, 1, 4, 1, 2, 4,
                    1, 2, 1, 4, 1, 4, 1, 4, 1, 4, 1, 1,
                    1, 1, 2, 1, 2, 1,
                    1, 2, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_15']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 1
                ds_mtbl[orbit_list]['duration'] = 1040
                ds_mtbl[orbit_list]['num_info'] = 260

            grp = fid['State_26']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1280
                ds_mtbl[orbit_list]['num_info'] = 80
                pet = np.array([
                    80., 80., 80., 80., 80., 80.,
                    80., 80., 80., 80., 80.,
                    80., 80., 80., 80., 80., 80., 80., 80., 80.,
                    80., 80., 80., 80., 80., 80., 80., 80.,
                    80., 80., 80., 80., 80., 80., 80.,
                    10., 10., 10., 10., 10., 10.,
                    10., 10., 10., 10., 10., 10.,
                    1., 1., 1., 1., 1., 1.,
                    1., 1., 1.], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1,
                    1, 1, 1], dtype='u1')
                nread = np.array([
                    1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1,
                    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
                    80, 80, 80, 80, 80, 80,
                    80, 80, 80], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_34']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 944
                ds_mtbl[orbit_list]['num_info'] = 140
                pet = np.array([
                    1.5, 1.5, 1.5, 3/8., 3/8., 3/8.,
                    3/16., 3/16., 3/16., 3/8., 3/8., 3/8.,
                    1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16.,
                    3/8., 3/8., 3/8., 3/8., 3/8.,
                    3/8., 3/8., 3/8., 3/8., 3/8.,
                    3/8., 3/8., 3/8., 3/8., 3/8.,
                    3/8., 3/8., 3/8.], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 4, 4, 8, 8, 2, 1, 4, 4,
                    24, 24, 6, 24, 24, 24, 24, 6, 24, 24,
                    4, 4, 1, 4, 4, 4, 4, 1, 4, 4,
                    4, 4, 1, 4, 4, 4, 1, 4], dtype='u1')
                nread = np.array([
                    1, 1, 1, 4, 1, 1, 1, 1, 4, 4, 1, 1,
                    1, 1, 4, 1, 1, 1, 1, 4, 1, 1,
                    1, 1, 4, 1, 1, 1, 1, 4, 1, 1,
                    1, 1, 4, 1, 1, 1, 4, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_35']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 0
                ds_mtbl[orbit_list]['duration'] = 944
                ds_mtbl[orbit_list]['num_info'] = 140

            grp = fid['State_36']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 0
                ds_mtbl[orbit_list]['duration'] = 944
                ds_mtbl[orbit_list]['num_info'] = 280

            grp = fid['State_37']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 1
                ds_mtbl[orbit_list]['duration'] = 944
                ds_mtbl[orbit_list]['num_info'] = 140

            grp = fid['State_40']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 0
                ds_mtbl[orbit_list]['duration'] = 944
                ds_mtbl[orbit_list]['num_info'] = 70

            grp = fid['State_41']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 944
                ds_mtbl[orbit_list]['num_info'] = 140
                pet = np.array([
                    1.5, 1.5, 1.5, 3/8., 3/8., 3/8.,
                    3/16., 3/16., 3/16., 3/8., 3/8., 3/8.,
                    1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16.,
                    3/8., 3/8., 3/8., 3/8., 3/8.,
                    3/8., 3/8., 3/8., 3/8., 3/8.,
                    3/8., 3/8., 3/8., 3/8., 3/8.,
                    3/8., 3/8., 3/8.], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 4, 4, 8, 8, 2, 1, 4, 4,
                    24, 24, 6, 24, 24, 24, 24, 6, 24, 24,
                    4, 4, 1, 4, 4, 4, 4, 1, 4, 4,
                    4, 4, 1, 4, 4, 4, 1, 4], dtype='u1')
                nread = np.array([
                    1, 1, 1, 4, 1, 1, 1, 1, 4, 4, 1, 1,
                    1, 1, 4, 1, 1, 1, 1, 4, 1, 1,
                    1, 1, 4, 1, 1, 1, 1, 4, 1, 1,
                    1, 1, 4, 1, 1, 1, 4, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_3034(self):
        """
        Append OCR state cluster definition.

        Modified states for orbit 3034:
        - stateID = 17, 23, 24, 25, 26, 42, 43, 44, 51
        """
        orbit_list = [3034]
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_17']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 0
                ds_mtbl[orbit_list]['duration'] = 480
                ds_mtbl[orbit_list]['num_info'] = 30

            grp = fid['State_23']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 0
                ds_mtbl[orbit_list]['duration'] = 1280
                ds_mtbl[orbit_list]['num_info'] = 80

            grp = fid['State_24']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 0
                ds_mtbl[orbit_list]['duration'] = 1280
                ds_mtbl[orbit_list]['num_info'] = 160

            grp = fid['State_25']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 0
                ds_mtbl[orbit_list]['duration'] = 1280
                ds_mtbl[orbit_list]['num_info'] = 320

            grp = fid['State_26']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 2
                ds_mtbl[orbit_list]['duration'] = 1280
                ds_mtbl[orbit_list]['num_info'] = 80

            grp = fid['State_42']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = 1
                ds_mtbl[orbit_list]['duration'] = 1040
                ds_mtbl[orbit_list]['num_info'] = 520

            grp = fid['State_43']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1040
                ds_mtbl[orbit_list]['num_info'] = 520
                pet = np.array([
                    1., 1., 1., .5, .5, .5,
                    .5, .5, .5, .5, .5,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    .25, .25, .25, .25, .25, .25, .25,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1., 1., 1., 1., 1., 1.,
                    1., 1., 1.], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 1, 2,
                    2, 2, 1, 1, 2,
                    8, 8, 1, 8, 1, 1, 8, 8, 8,
                    8, 8, 8, 1, 8, 1, 8, 8,
                    4, 4, 4, 1, 4, 1, 4,
                    8, 2, 8, 1, 8, 1, 8, 1, 8, 1, 8, 8,
                    1, 1, 1, 1, 1, 1,
                    1, 1, 1], dtype='u1')
                nread = np.array([
                    1, 1, 1, 2, 2, 1,
                    1, 1, 2, 2, 1,
                    1, 1, 8, 1, 8, 8, 1, 1, 1,
                    1, 1, 1, 8, 1, 8, 1, 1,
                    1, 1, 1, 4, 1, 4, 1,
                    1, 1, 1, 8, 1, 8, 1, 8, 1, 8, 1, 1,
                    1, 1, 1, 1, 1, 1,
                    1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_44']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 56
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1040
                ds_mtbl[orbit_list]['num_info'] = 260
                pet = np.array([
                    1., 1., 1., .25, .25, .25,
                    .25, .25, .25, .25, .25,
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    .25, .25, .25, .25, .25, .25, .25,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
                    .5, .5, .5, .5, .5, .5,
                    .5, .5, .5], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 1, 4,
                    4, 4, 1, 1, 4,
                    16, 16, 16, 4, 16, 4, 16, 16, 16,
                    16, 16, 16, 4, 16, 4, 16, 16,
                    4, 4, 4, 1, 4, 2, 4,
                    8, 4, 8, 2, 8, 2, 8, 2, 8, 2, 8, 8,
                    2, 2, 1, 2, 1, 2,
                    2, 1, 2], dtype='u1')
                nread = np.array([
                    1, 1, 1, 4, 4, 1,
                    1, 1, 4, 4, 1,
                    1, 1, 1, 4, 1, 4, 1, 1, 1,
                    1, 1, 1, 4, 1, 4, 1, 1,
                    1, 1, 1, 4, 1, 2, 4,
                    1, 2, 1, 4, 1, 4, 1, 4, 1, 4, 1, 1,
                    1, 1, 2, 1, 2, 1,
                    1, 2, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 56, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_51']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1024
                ds_mtbl[orbit_list]['num_info'] = 1024
                pet = np.array([
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/16., 1/16., 1/16., 1/16., 1/16.,
                    1/32., 1/32., 1/32., 1/32., 1/32.,
                    1/32., 1/32., 1/32., 1/32., 1/32.,
                    1/16., 1/16., 1/16.], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                    1, 1, 1, 1, 1, 2, 2, 2], dtype='u1')
                nread = np.array([
                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    2, 2, 2, 2, 2, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_09(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=09 added definitions for orbits [3981]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_09']
            ds_mtbl = grp['metaTable']

            orbit_list = 3981
            if np.all(ds_mtbl[orbit_list]['num_info'] == 0):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 0xFF
                ds_mtbl[orbit_list]['duration'] = 918
                ds_mtbl[orbit_list]['num_info'] = 166

    def add_missing_state_10_13(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=10 added definitions for orbits [3964,3968,4118,4122]
        - stateID=11 added definitions for orbits [3969,4123]
        - stateID=12 added definitions for orbits [3965,3970,4119,4124]
        - stateID=13 added definitions for orbits [3971,4125]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_10']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            pet = np.array([
                1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                1/32., 1/32., 1/32., 1/32., 1/32., 1/32.,
                1/16., 1/16., 1/16., 1/16., 1/16.,
                1/16., 1/16., 1/16., 1/16., 1/16.,
                1/16., 1/16., 1/16., 1/16., 1/16.,
                1/32., 1/32., 1/32., 1/32., 1/32.,
                1/32., 1/32., 1/32., 1/32., 1/32.,
                1/16., 1/16., 1/16.], dtype='f4')
            coaddf = np.array([
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                8, 8, 8, 8, 8, 8, 8, 8], dtype='u1')
            nread = np.array([
                8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
                8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
                8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
                1, 1, 1, 1, 1, 1, 1, 1], dtype='u2')
            clus_conf = np.zeros(64, dtype=ds_clus.dtype)
            fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
            orbit_list = [3964, 3968, 4118, 4122]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 593
                ds_mtbl[orbit_list]['num_info'] = 528
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_11']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3969, 4123]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 593
                ds_mtbl[orbit_list]['num_info'] = 528
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_12']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3965, 3970, 4119, 4124]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 593
                ds_mtbl[orbit_list]['num_info'] = 528
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_13']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3971, 4125]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 593
                ds_mtbl[orbit_list]['num_info'] = 528
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_14(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=14 added definitions for orbits [
                          3958, 3959, 3962,
                          4086, 4087, 4088, 4089, 4091, 4092,
                          4111, 4112, 4113, 4114,
                          5994]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_14']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3958, 3959, 3962,
                          4086, 4087, 4088, 4089, 4091, 4092,
                          4111, 4112, 4113, 4114,
                          5994]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 10
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1280
                ds_mtbl[orbit_list]['num_info'] = 2
                pet = np.array([40., 40., 40., 40., 10., 4., 4., 1., 1., 2.],
                               dtype='f4')
                coaddf = np.array([1, 1, 1, 1, 4, 10, 10, 40, 40, 20],
                                  dtype='u1')
                nread = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 10, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_22(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=22 added definitions for orbits
                     [4119, 4120, 4121, 4122, 4123, 4124, 4125, 4126, 4127]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_22']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4119, 4120, 4121, 4122, 4123, 4124, 4125, 4126, 4127]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 10
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 782
                ds_mtbl[orbit_list]['num_info'] = 29
                pet = np.array([1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5, 1.5, 1.5], dtype='f4')
                coaddf = np.array([1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1], dtype='u1')
                nread = np.array([1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 10, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_24(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=24 added definitions for orbits [36873:38267, 47994:48075]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_24']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(np.append(ds_mtbl[36873:38267]['type_clus'],
                                ds_mtbl[47994:48075]['type_clus']) == 0xFF):
                ds_mtbl[36873:38267]['num_clus'] = 40
                ds_mtbl[36873:38267]['type_clus'] = clus_dim
                ds_mtbl[36873:38267]['duration'] = 1600
                ds_mtbl[36873:38267]['num_info'] = 100

                ds_mtbl[47994:48075]['num_clus'] = 40
                ds_mtbl[47994:48075]['type_clus'] = clus_dim
                ds_mtbl[47994:48075]['duration'] = 1440
                ds_mtbl[47994:48075]['num_info'] = 90
                pet = np.array([
                    1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
                    1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
                    1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
                    1., 1., 1., 1., 1., 1., 1., 1., 1., 1.], dtype='f4')
                coaddf = np.array([
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1], dtype='u1')
                nread = np.array([
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_25_26(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=25 added definitions for orbits [4088,4111]
        - stateID=26 added definitions for orbits [4089]
        """
        pet = np.array([1/4., 1/16., 1/16., 1/16., 1/16.,
                        1/16., 1/8., 1/32., 1/16., 1/8.], dtype='f4')
        coaddf = np.array([1, 4, 4, 4, 4, 4, 2, 4, 4, 2], dtype='u1')
        nread = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1], dtype='u2')

        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_25']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            clus_conf = np.zeros(64, dtype=ds_clus.dtype)
            fill_clus_conf(clus_conf, 10, pet, coaddf, nread)

            orbit_list = [4088, 4111]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 10
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 782
                ds_mtbl[orbit_list]['num_info'] = 174
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_26']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]

            orbit_list = [4089]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 10
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 782
                ds_mtbl[orbit_list]['num_info'] = 174
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_27(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=27 added definitions for orbits [44091,44092]
        - stateID=27 added definitions for orbits [44134,44148,44149,44150]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_27']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [44091, 44092]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 647
                ds_mtbl[orbit_list]['num_info'] = 48
                pet = np.array([1.5, 1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5, 1.5, 1.5, 1.5,
                                .75, .75, .75, .75, .75,
                                .75, .75, .75, .75, .75,
                                1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5], dtype='f4')
                coaddf = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 6, 6, 6], dtype='u1')
                nread = np.array([6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
                                  12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
                                  6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
                                  6, 6, 6, 6, 6, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf
                clus_dim += 1

            orbit_list = [44134, 44148, 44149, 44150]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 647
                ds_mtbl[orbit_list]['num_info'] = 48
                pet = np.array([1.5, 1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5, 1.5, 1.5, 1.5,
                                .75, .75, .75, .75, .75,
                                .75, .75, .75, .75, .75,
                                1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5, 1.5, 1.5,
                                1.5, 1.5, 1.5], dtype='f4')
                coaddf = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 12, 12, 12], dtype='u1')
                nread = np.array([12, 12, 12, 12, 12, 12,
                                  12, 12, 12, 12, 12, 12,
                                  24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
                                  12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
                                  12, 12, 12, 12, 12,
                                  1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_33_39(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=33 added definitions for orbits [4087,4089,4110,4112]
        - stateID=34 added definitions for orbits [4088,4090,4111,4113]
        - stateID=38 added definitions for orbits [4087,4089,4110,4112]
        - stateID=39 added definitions for orbits [4088,4090,4111,4113]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_35']
            ds_clus = grp['clusDef']
            clus_conf = ds_clus[1, :]

            grp = fid['State_33']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4087, 4089, 4110, 4112]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 10
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1406
                ds_mtbl[orbit_list]['num_info'] = 42
                pet = np.array([2., 2., 1/4., 1/4.,
                                1/8., 1/32., 1/32.,
                                .0072, .0036, .0072], dtype='f4')
                coaddf = np.array([1, 1, 8, 8,
                                   16, 32, 32, 32, 32, 32], dtype='u1')
                nread = np.array([1, 1, 1, 1,
                                  1, 1, 1, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 10, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_34']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4088, 4090, 4111, 4113]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 10
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1406
                ds_mtbl[orbit_list]['num_info'] = 42
                pet = np.array([2., 2., 1/4., 1/4.,
                                1/8., 1/32., 1/32.,
                                .0072, .0036, .0072], dtype='f4')
                coaddf = np.array([1, 1, 8, 8,
                                   16, 32, 32, 32, 32, 32], dtype='u1')
                nread = np.array([1, 1, 1, 1,
                                  1, 1, 1, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 10, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_38']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4087, 4089, 4110, 4112]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 10
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1406
                ds_mtbl[orbit_list]['num_info'] = 42
                pet = np.array([2., 2., 1/4., 1/4.,
                                1/8., 1/32., 1/32.,
                                .0072, .0036, .0072], dtype='f4')
                coaddf = np.array([1, 1, 8, 8,
                                   16, 32, 32, 32, 32, 32], dtype='u1')
                nread = np.array([1, 1, 1, 1,
                                  1, 1, 1, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 10, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_39']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [4088, 4090, 4111, 4113]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 10
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1406
                ds_mtbl[orbit_list]['num_info'] = 42
                pet = np.array([2., 2., 1/4., 1/4.,
                                1/8., 1/32., 1/32.,
                                .0072, .0036, .0072], dtype='f4')
                coaddf = np.array([1, 1, 8, 8,
                                   16, 32, 32, 32, 32, 32], dtype='u1')
                nread = np.array([1, 1, 1, 1,
                                  1, 1, 1, 1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 10, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_35_39(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=09 added definitions for orbits [3967,4121]
        - stateID=35 added definitions for orbits [3972,4126]
        - stateID=36 added definitions for orbits [3973,4127]
        - stateID=37 added definitions for orbits [3975]
        - stateID=38 added definitions for orbits [3976]
        - stateID=39 added definitions for orbits [3977]
        """
        pet = np.array([1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                        1/32., 1/32., 1/32., 1/32., 1/32., 1/32.,
                        1/16., 1/16., 1/16., 1/16., 1/16.,
                        1/16., 1/16., 1/16., 1/16., 1/16.,
                        1/16., 1/16., 1/16., 1/16., 1/16.,
                        1/32., 1/32., 1/32., 1/32., 1/32.,
                        1/32., 1/32., 1/32., 1/32., 1/32.,
                        1/16., 1/16., 1/16.], dtype='f4')
        coaddf = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                           1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                           1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                           8, 8, 8, 8, 8, 8, 8, 8], dtype='u1')
        nread = np.array([8, 8, 8, 8, 8, 8,
                          8, 8, 8, 8, 8, 8,
                          8, 8, 8, 8, 8,
                          8, 8, 8, 8, 8,
                          8, 8, 8, 8, 8,
                          8, 8, 8, 8, 8,
                          1, 1, 1, 1, 1,
                          1, 1, 1], dtype='u2')

        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_35']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            clus_conf = np.zeros(64, dtype=ds_clus.dtype)
            fill_clus_conf(clus_conf, 40, pet, coaddf, nread)

            orbit_list = [3972, 4126]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1511
                ds_mtbl[orbit_list]['num_info'] = 1344
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_36']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3973, 4127]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1511
                ds_mtbl[orbit_list]['num_info'] = 1344
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_37']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3975]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1511
                ds_mtbl[orbit_list]['num_info'] = 1344
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_38']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3976]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1511
                ds_mtbl[orbit_list]['num_info'] = 1344
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_39']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3977]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1511
                ds_mtbl[orbit_list]['num_info'] = 1344
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

            grp = fid['State_09']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            orbit_list = [3967, 4121]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 928
                ds_mtbl[orbit_list]['num_info'] = 928
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_42(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=42 added definitions for orbits [6778, 6779]
        """
        orbit_list = [6778, 6779]
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_42']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['num_info'] == 0):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 0xFF
                ds_mtbl[orbit_list]['duration'] = 5598
                ds_mtbl[orbit_list]['num_info'] = 2650


    def add_missing_state_43(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=43 added definitions for orbits [6778, 6779]
        - stateID=43 added definitions for orbits [7193, 7194]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_43']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]

            orbit_list = [6778, 6779]
            if np.all(ds_mtbl[orbit_list]['num_info'] == 0):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 0xFF
                ds_mtbl[orbit_list]['duration'] = 1118
                ds_mtbl[orbit_list]['num_info'] = 536

            orbit_list = [7193, 7194]
            if np.all(ds_mtbl[orbit_list]['type_clus'] == 0xFF):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = clus_dim
                ds_mtbl[orbit_list]['duration'] = 1120
                ds_mtbl[orbit_list]['num_info'] = 84
                pet = np.array([10., 10., 10., 10., 10., 10.,
                                10., 10., 10., 10., 10., 10.,
                                2.5, 2.5, 2.5, 2.5, 2.5,
                                2.5, 2.5, 2.5, 2.5, 2.5,
                                2.5, 2.5, 2.5, 2.5, 2.5,
                                1/32., 1/32., 1/32., 1/32., 1/32.,
                                1/32., 1/32., 1/32., 1/32., 1/32.,
                                1/32., 1/32., 1/32.], dtype='f4')
                coaddf = np.array([1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   16, 16, 16, 16, 16,
                                   16, 16, 16, 16, 16,
                                   16, 16, 16], dtype='u1')
                nread = np.array([1, 1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1, 1,
                                  4, 4, 4, 4, 4,
                                  4, 4, 4, 4, 4,
                                  4, 4, 4, 4, 4,
                                  10, 10, 10, 10, 10,
                                  10, 10, 10, 10, 10,
                                  10, 10, 10], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def add_missing_state_44(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=44 added definitions for orbits [6778, 6779]
        """
        orbit_list = [6778, 6779]
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_44']
            ds_mtbl = grp['metaTable']
            if np.all(ds_mtbl[orbit_list]['num_info'] == 0):
                ds_mtbl[orbit_list]['num_clus'] = 40
                ds_mtbl[orbit_list]['type_clus'] = 0xFF
                ds_mtbl[orbit_list]['duration'] = 447
                ds_mtbl[orbit_list]['num_info'] = 219

    def add_missing_state_55(self):
        """
        Append OCR state cluster definition.

        Modified states:
        - stateID=55 added definitions for orbits [26812:26834]
        - stateID=55 added definitions for orbits [28917:28920, 30836:30850]
        """
        with h5py.File(self.db_name, 'r+') as fid:
            grp = fid['State_55']
            ds_mtbl = grp['metaTable']
            ds_clus = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[26812:26834]['type_clus'] == 0xFF):
                ds_mtbl[26812:26834]['num_clus'] = 40
                ds_mtbl[26812:26834]['type_clus'] = clus_dim
                ds_mtbl[26812:26834]['duration'] = 640
                ds_mtbl[26812:26834]['num_info'] = 640
                pet = np.array([1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                                1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
                                1/16., 1/16., 1/16., 1/16., 1/16.,
                                1/16., 1/16., 1/16., 1/16., 1/16.,
                                1/16., 1/16., 1/16., 1/16., 1/16.,
                                1/16., 1/16., 1/16., 1/16., 1/16.,
                                1/16., 1/16., 1/16., 1/16., 1/16.,
                                1/16., 1/16., 1/16.], dtype='f4')
                coaddf = np.array([1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1,
                                   2, 2, 2, 2, 2,
                                   2, 2, 2, 2, 2,
                                   2, 2, 2, 2, 2,
                                   2, 2, 2, 2, 2,
                                   1, 1, 1, 1, 1,
                                   2, 2, 2], dtype='u1')
                nread = np.array([1, 1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1, 1,
                                  2, 2, 2, 2, 2,
                                  2, 2, 2, 2, 2,
                                  2, 2, 2, 2, 2,
                                  1, 1, 1, 1, 1,
                                  2, 2, 2, 2, 2,
                                  1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf
                clus_dim += 1

            if np.all(np.append(ds_mtbl[28917:28920]['type_clus'],
                                ds_mtbl[30836:30850]['type_clus']) == 0xFF):
                ds_mtbl[28917:28920]['num_clus'] = 40
                ds_mtbl[28917:28920]['type_clus'] = clus_dim
                ds_mtbl[28917:28920]['duration'] = 1673
                ds_mtbl[28917:28920]['num_info'] = 186

                ds_mtbl[30836:30850]['num_clus'] = 40
                ds_mtbl[30836:30850]['type_clus'] = clus_dim
                ds_mtbl[30836:30850]['duration'] = 1673
                ds_mtbl[30836:30850]['num_info'] = 186
                pet = np.array([.5, .5, .5, .5, .5, .5,
                                .5, .5, .5, .5, .5, .5,
                                .5, .5, .5, .5, .5,
                                .5, .5, .5, .5, .5,
                                .5, .5, .5, .5, .5,
                                .5, .5, .5, .5, .5,
                                .5, .5, .5, .5, .5,
                                .5, .5, .5], dtype='f4')
                coaddf = np.array([1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1], dtype='u1')
                nread = np.array([1, 1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1,
                                  1, 1, 1], dtype='u2')
                clus_conf = np.zeros(64, dtype=ds_clus.dtype)
                fill_clus_conf(clus_conf, 40, pet, coaddf, nread)
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim, :] = clus_conf

    def fill_mtbl(self):
        """
        Fill metaTable by interpolation and in a few cases by extrapolation
        """
        with h5py.File(self.db_name, 'r+') as fid:
            for state_id in range(1, 71):
                grp = fid['State_{:02d}'.format(state_id)]
                if state_id == 65:
                    mtbl = grp['metaTable'][2204:52867]
                    mtbl[:] = (0, 40, 320, 40)
                    grp['metaTable'][2204:52867] = mtbl

                    ds_clus = grp['clusDef']
                    ds_clus.resize(1, axis=0)
                    ds_clus[0, :] = fid['State_46/clusDef'][0, :]
                    print("updated group State 65")
                    continue

                if grp['clusDef'].size == 0:
                    print("Info: skipping state {}".format(state_id))
                    continue

                mtbl = grp['metaTable'][:]
                mtbl_dim = mtbl.size

                # skip all undefined entries at the start
                nj = 0
                while nj < mtbl_dim and mtbl['type_clus'][nj] == 0xFF:
                    nj += 1

                # replace undefined entries
                while nj < mtbl_dim:
                    ni = nj
                    while ni < mtbl_dim and mtbl['type_clus'][ni] != 0xFF:
                        ni += 1

                    mtbl_last = mtbl[ni-1]

                    nj = ni + 1
                    while nj < mtbl_dim and mtbl['type_clus'][nj] == 0xFF:
                        nj += 1

                    if nj == mtbl_dim:
                        break

                    if mtbl['type_clus'][nj] == mtbl_last['type_clus'] \
                       and mtbl['duration'][nj] == mtbl_last['duration']:
                        print('State_{:02d}: '.format(state_id), ni, nj,
                              mtbl_last['type_clus'])
                        mtbl[ni:nj] = mtbl_last

                # write updated array 'mtbl'
                grp['metaTable'][:] = mtbl

                # 'manually' correct several other entries
                ds_mtbl = grp['metaTable']
                if ds_mtbl[6001]['type_clus'] == 0xFF:
                    if ds_mtbl[6002]['type_clus'] == 0xFF:
                        print("no update of entry 6001 of state {}".format(
                            state_id))
                    ds_mtbl[6001] = ds_mtbl[6002]

                if ds_mtbl[40107]['type_clus'] == 0xFF:
                    if ds_mtbl[40108]['type_clus'] == 0xFF:
                        print("no update of entry 40107 of state {}".format(
                            state_id))
                    ds_mtbl[40107] = ds_mtbl[40108]

                if state_id == 2 and ds_mtbl[6091]['type_clus'] == 0xFF:
                    ds_mtbl[6091] = ds_mtbl[6108]

                if state_id == 2 and ds_mtbl[6109]['type_clus'] == 0xFF:
                    ds_mtbl[6109] = ds_mtbl[6108]

                if state_id == 6 and ds_mtbl[7493]['type_clus'] == 0xFF:
                    ds_mtbl[7493] = ds_mtbl[7494]

                if state_id == 9 and ds_mtbl[4128]['type_clus'] == 0xFF:
                    ds_mtbl[4128] = ds_mtbl[4129]

                if state_id == 28 and ds_mtbl[45187]['type_clus'] == 0xFF:
                    ds_mtbl[45187] = ds_mtbl[45186]

                if state_id == 37 and ds_mtbl[4115]['type_clus'] == 0xFF:
                    ds_mtbl[4115] = ds_mtbl[4090]

                if state_id == 42 and ds_mtbl[3966]['type_clus'] == 0xFF:
                    ds_mtbl[3966] = ds_mtbl[3974]

                if state_id == 42 and ds_mtbl[7194]['type_clus'] == 0xFF:
                    ds_mtbl[7194] = ds_mtbl[7193]

                if state_id == 44 and ds_mtbl[7193]['type_clus'] == 0xFF:
                    ds_mtbl[7193] = ds_mtbl[7194]

                if state_id == 49 and ds_mtbl[4381]['duration'] == 2078:
                    mtbl = grp['metaTable'][4381]
                    mtbl['duration'] = 2080
                    mtbl['num_info'] = 2080
                    grp['metaTable'][4381] = mtbl
                    print("updated group State 49")

                if state_id == 54 and ds_mtbl[5034]['type_clus'] == 0xFF:
                    ds_mtbl[5034] = ds_mtbl[5019]

                if state_id == 54 and ds_mtbl[22790]['type_clus'] == 0xFF:
                    ds_mtbl[22790] = ds_mtbl[22789]

                if state_id == 62 and ds_mtbl[4055]['type_clus'] == 0xFF:
                    ds_mtbl[4055] = ds_mtbl[4056]

    def update(self, orbit, state_id, mtbl, clus_conf_l1b):
        """
        update database entry

        Parameters
        ----------
         orbit     : revolution counter
         state_id  : state ID - integer range [1, 70].
         mtbl      : metaTable entry
         clus_conf : state cluster definition entry from L1B product
        """
        grp = self.fid['State_{:02d}'.format(state_id)]
        ds_mtbl = grp['metaTable']
        ds_clus = grp['clusDef']
        clus_dim = ds_clus.shape[0]
        clus_conf = ds_clus[:]

        # convert L1b struct to database struct
        clus_conf_new = np.zeros(64, dtype=ds_clus.dtype)
        for key in clus_conf_new.dtype.names:
            clus_conf_new[key][:] = clus_conf_l1b[key][:]

        # update database
        indx = np.where((clus_conf == clus_conf_new).all(axis=1))[0]
        if indx.size == 0:
            # new cluster definition: extent dataset
            ds_clus.resize(clus_dim+1, axis=0)
            ds_clus[clus_dim, :] = clus_conf_new
            mtbl['type_clus'] = clus_dim
        else:
            # cluster definition exists
            mtbl['type_clus'] = indx[0]
        ds_mtbl[orbit] = mtbl


# - main code --------------------------------------
def main():
    """
    main function
    """
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    from pynadc.scia import db, lv1

    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description='combine Sciamachy state cluster definitions into database')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="be verbose")
    parser.add_argument('--db_name', type=str, default='./scia_state_db.h5',
                        help="write to hdf5 database")
    parser.add_argument('--proc', default='Y', type=str,
                        help="select entries on ESA processor baseline")
    parser.add_argument('--force', action='store_true',
                        help="necessary to replace existing database")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all_lv1', action='store_true',
                       help="process all archived Sciamachy L1B products")
    group.add_argument('--add_ocr', action='store_true',
                       help="add OCR state definitions, not stored in L1B")
    group.add_argument('--finalize', action='store_true',
                       help="finalize state definition database")
    args = parser.parse_args()

    # open access to clusterDef database (or create one)
    clusdb = ClusDB(args)
    if args.all_lv1:
        if Path(clusdb.db_name).is_file():
            if args.force:
                Path(clusdb.db_name).unlink()
            else:
                print("use '--force' to overwite an existing database")
                return

        # create new database
        clusdb.create()

        # all cluster-configurations of all L1B products
        # for orbit in range(1, 2300):
        for orbit in range(1, MAX_ORBIT+1):
            file_list = db.get_product_by_type(prod_type='1',
                                               proc_stage=args.proc,
                                               orbits=[orbit])
            if not file_list:
                continue

            if not Path(file_list[0]).is_file():
                raise ValueError('File {} not found'.format(file_list[0]))

            # open Sciamachy level 1b product
            print(file_list[0])
            try:
                scia = lv1.File(file_list[0])
            except:
                print('exception occurred in module pynadc.scia.lv1')
                raise

            # read STATES GADS
            states = scia.get_states()

            # remove corrupted states
            states = states[(states['flag_reason'] != 1)
                            & (states['duration'] != 0)]

            # loop over all ID of states
            for state_id in np.unique(states['state_id']):
                indx = np.where(states['state_id'] == state_id)[0]
                mtbl = np.empty(1, dtype=clusdb.mtbl_dtype())
                mtbl['num_clus'] = states['num_clus'][indx[0]]
                mtbl['duration'] = states['duration'][indx[0]]
                mtbl['num_info'] = states['num_geo'][indx[0]]
                clus_conf = states['Clcon'][indx[0], :]
                if args.verbose:
                    print(state_id, ' - ', orbit, indx[0], clus_conf.shape)
                clusdb.update(orbit, state_id, mtbl, clus_conf)

        clusdb.close()
    elif args.add_ocr:
        if not Path(clusdb.db_name).is_file():
            print("database not found, please create one using '--all_lv1'")
        else:
            clusdb.add_missing_state_3033()
            clusdb.add_missing_state_3034()
            clusdb.add_missing_state_09()
            clusdb.add_missing_state_10_13()
            clusdb.add_missing_state_14()
            clusdb.add_missing_state_22()
            clusdb.add_missing_state_24()
            clusdb.add_missing_state_25_26()
            clusdb.add_missing_state_27()
            clusdb.add_missing_state_33_39()
            clusdb.add_missing_state_35_39()
            clusdb.add_missing_state_42()
            clusdb.add_missing_state_43()
            clusdb.add_missing_state_44()
            clusdb.add_missing_state_55()
    elif args.finalize:
        if not Path(clusdb.db_name).is_file():
            print("database not found, please create one using '--all_lv1'")
        else:
            clusdb.fill_mtbl()
    else:
        raise ValueError('unknown commandline parameter')


# -------------------------
if __name__ == '__main__':
    main()
