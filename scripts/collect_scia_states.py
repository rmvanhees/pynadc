#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Collect Sciamachy instrument settings (states) from level 1b products

Synopsis
--------

   collect_scia_states.py [options] <hdf5file>

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

Problems reported with L1B, procstage Y
---------------------------------------
 3792 : skipped state configuration for ID 49 invalid number of clusters 41
 4384 : skipped state configuration for ID 01 invalid number of clusters 36
 8192 : skipped state configuration for ID 49 invalid number of clusters 41
 8379 : skipped state configuration for ID 02 invalid number of clusters 57
 8436 : skipped state configuration for ID 03 invalid number of clusters 57
 8694 : skipped state configuration for ID 01 invalid number of clusters 57
 8696 : skipped state configuration for ID 49 invalid number of clusters 41
 8717 : skipped state configuration for ID 03 invalid number of clusters 57
 8728 : skipped state configuration for ID 09 invalid number of clusters 57
 8728 : skipped state configuration for ID 63 invalid number of clusters 41
 8728 : skipped state configuration for ID 67 invalid number of clusters 41
14431 : skipped state configuration for ID 49 invalid number of clusters 41
18027 : skipped state configuration for ID 49 invalid number of clusters 41
19072 : skipped state configuration for ID 33 invalid number of clusters 42
28910 : skipped state configuration for ID 62 invalid number of clusters 41
29411 : skipped state configuration for ID 28 invalid number of clusters 41
33348 : skipped state configuration for ID 47 invalid number of clusters 41
37684 : skipped state configuration for ID 02 invalid number of clusters 57
38258 : skipped state configuration for ID 29 invalid number of clusters 41
38471 : skipped state configuration for ID 02 invalid number of clusters 57
39083 : skipped state configuration for ID 07 invalid number of clusters 57
45822 : skipped state configuration for ID 46 invalid number of clusters 35
49963 : skipped state configuration for ID 49 invalid number of clusters 41
52294 : skipped state configuration for ID 49 invalid number of clusters 41

Author
------
Richard van Hees (r.m.van.hees at sron.nl)

Bug reporting
-------------
Please report issues at the pyNADC Github page:
https://github.com/rmvanhees/pynadc.git

Copyright (c) 2016-2021` SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
from time import gmtime, strftime

import h5py
import numpy as np

from pynadc.scia import clus_def, db, lv1


# - global parameters ------------------------------
VERSION = '2.1.0'

MAX_ORBIT = 53000


# - local functions --------------------------------

# ---------- special state configurations obtained from level 0 ----------
def add_missing_state_3033(db_name):
    """
    Append special state configuration for orbit 3033:
    - stateID = 8, 10, 11, 12, 13, 14, 15, 26, 34, 35, 36, 37, 40, 41
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 56
        state_conf = fid['State_08/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1040
        state_conf['num_geo'] = 260
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 2, 1, 4,
            8, 4, 2, 1, 4,
            16, 8, 16, 4, 16, 8, 16, 16, 16,
            16, 16, 16, 8, 16, 8, 16, 16,
            8, 8, 8, 4, 8, 8, 8,
            16, 16, 16, 8, 16, 8, 16, 8, 16, 8, 16, 16,
            4, 4, 1, 4, 1, 4,
            4, 1, 4)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 2, 4, 1,
            1, 2, 4, 4, 1,
            1, 2, 1, 4, 1, 2, 1, 1, 1,
            1, 1, 1, 2, 1, 2, 1, 1,
            1, 1, 1, 2, 1, 1, 1,
            1, 1, 1, 2, 1, 2, 1, 2, 1, 2, 1, 1,
            1, 1, 4, 1, 4, 1,
            1, 4, 1)
        state_conf['pet'][:nclus] = (
            1., 1., 1., .25, .25, .25,
            .125, .125, .125, .25, .25,
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/32., 1/32., 1/32., 1/32., 1/32., 1/32., 1/32., 1/32.,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            .25, .25, .25, .25, .25, .25,
            .25, .25, .25)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_08/state_conf'][3303] = state_conf

        nclus = 56
        state_conf = fid['State_10/state_conf'][0]
        state_conf['id'] = 0xFF      # 1
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1280
        state_conf['num_geo'] = 160
        fid['State_10/state_conf'][3303] = state_conf

        nclus = 56
        state_conf = fid['State_11/state_conf'][0]
        state_conf['id'] = 0xFF      # 1
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1280
        state_conf['num_geo'] = 320
        fid['State_11/state_conf'][3303] = state_conf

        nclus = 56
        state_conf = fid['State_12/state_conf'][0]
        state_conf['id'] = 0xFF      # 1
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1040
        state_conf['num_geo'] = 520
        fid['State_12/state_conf'][3303] = state_conf

        nclus = 56
        state_conf = fid['State_13/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1040
        state_conf['num_geo'] = 520
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 2,
            2, 2, 1, 1, 2,
            8, 8, 1, 8, 1, 1, 8, 8, 8,
            8, 8, 8, 1, 8, 1, 8, 8,
            4, 4, 4, 1, 4, 1, 4,
            8, 2, 8, 1, 8, 1, 8, 1, 8, 1, 8, 8,
            1, 1, 1, 1, 1, 1,
            1, 1, 1)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 2, 2, 1,
            1, 1, 2, 2, 1,
            1, 1, 8, 1, 8, 8, 1, 1, 1,
            1, 1, 1, 8, 1, 8, 1, 1,
            1, 1, 1, 4, 1, 4, 1,
            1, 4, 1, 8, 1, 8, 1, 8, 1, 8, 1, 1,
            1, 1, 1, 1, 1, 1,
            1, 1, 1)
        state_conf['pet'][:nclus] = (
            1., 1., 1., .5, .5, .5,
            .5, .5, .5, .5, .5,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            1/8., 1/8., 1/8.,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            .25, .25, .25, .25, .25, .25, .25,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            1., 1., 1., 1., 1., 1.,
            1., 1., 1.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_13/state_conf'][3303] = state_conf

        nclus = 56
        state_conf = fid['State_14/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1040
        state_conf['num_geo'] = 260
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 4,
            4, 4, 1, 1, 4,
            16, 16, 16, 4, 16, 4, 16, 16, 16,
            16, 16, 16, 4, 16, 4, 16, 16,
            4, 4, 4, 1, 4, 2, 4,
            8, 4, 8, 2, 8, 2, 8, 2, 8, 2, 8, 8,
            2, 2, 1, 2, 1, 2,
            2, 1, 2)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 4, 4, 1,
            1, 1, 4, 4, 1,
            1, 1, 1, 4, 1, 4, 1, 1, 1,
            1, 1, 1, 4, 1, 4, 1, 1,
            1, 1, 1, 4, 1, 2, 4,
            1, 2, 1, 4, 1, 4, 1, 4, 1, 4, 1, 1,
            1, 1, 2, 1, 2, 1,
            1, 2, 1)
        state_conf['pet'][:nclus] = (
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
            .5, .5, .5)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_14/state_conf'][3303] = state_conf

        nclus = 56
        state_conf = fid['State_15/state_conf'][0]
        state_conf['id'] = 0xFF      # 1
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1040
        state_conf['num_geo'] = 260
        fid['State_15/state_conf'][3303] = state_conf

        nclus = 56
        state_conf = fid['State_26/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1280
        state_conf['num_geo'] = 80
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1,
            1, 1, 1)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1,
            8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
            80, 80, 80, 80, 80, 80,
            80, 80, 80)
        state_conf['pet'][:nclus] = (
            80., 80., 80., 80., 80., 80.,
            80., 80., 80., 80., 80.,
            80., 80., 80., 80., 80., 80., 80., 80., 80.,
            80., 80., 80., 80., 80., 80., 80., 80.,
            80., 80., 80., 80., 80., 80., 80.,
            10., 10., 10., 10., 10., 10.,
            10., 10., 10., 10., 10., 10.,
            1., 1., 1., 1., 1., 1.,
            1., 1., 1.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_26/state_conf'][3303] = state_conf

        nclus = 40
        state_conf = fid['State_34/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 944
        state_conf['num_geo'] = 140
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 4, 4, 8, 8, 2, 1, 4, 4,
            24, 24, 6, 24, 24, 24, 24, 6, 24, 24,
            4, 4, 1, 4, 4, 4, 4, 1, 4, 4,
            4, 4, 1, 4, 4, 4, 1, 4)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 4, 1, 1, 1, 1, 4, 4, 1, 1,
            1, 1, 4, 1, 1, 1, 1, 4, 1, 1,
            1, 1, 4, 1, 1, 1, 1, 4, 1, 1,
            1, 1, 4, 1, 1, 1, 4, 1)
        state_conf['pet'][:nclus] = (
            1.5, 1.5, 1.5, 3/8., 3/8., 3/8.,
            3/16., 3/16., 3/16., 3/8., 3/8., 3/8.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            3/8., 3/8., 3/8., 3/8., 3/8.,
            3/8., 3/8., 3/8., 3/8., 3/8.,
            3/8., 3/8., 3/8., 3/8., 3/8.,
            3/8., 3/8., 3/8.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_34/state_conf'][3303] = state_conf

        nclus = 40
        state_conf = fid['State_35/state_conf'][0]
        state_conf['id'] = 0xFF      # 0
        state_conf['nclus'] = nclus
        state_conf['duration'] = 944
        state_conf['num_geo'] = 140
        fid['State_35/state_conf'][3303] = state_conf

        nclus = 40
        state_conf = fid['State_36/state_conf'][0]
        state_conf['id'] = 0xFF      # 0
        state_conf['nclus'] = nclus
        state_conf['duration'] = 944
        state_conf['num_geo'] = 280
        fid['State_36/state_conf'][3303] = state_conf

        nclus = 40
        state_conf = fid['State_37/state_conf'][0]
        state_conf['id'] = 0xFF      # 1
        state_conf['nclus'] = nclus
        state_conf['duration'] = 944
        state_conf['num_geo'] = 140
        fid['State_37/state_conf'][3303] = state_conf

        nclus = 40
        state_conf = fid['State_40/state_conf'][0]
        state_conf['id'] = 0xFF      # 0
        state_conf['nclus'] = nclus
        state_conf['duration'] = 944
        state_conf['num_geo'] = 70
        fid['State_40/state_conf'][3303] = state_conf

        nclus = 40
        state_conf = fid['State_41/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 944
        state_conf['num_geo'] = 140
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 4, 4, 8, 8, 2, 1, 4, 4,
            24, 24, 6, 24, 24, 24, 24, 6, 24, 24,
            4, 4, 1, 4, 4, 4, 4, 1, 4, 4,
            4, 4, 1, 4, 4, 4, 1, 4)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 4, 1, 1, 1, 1, 4, 4, 1, 1,
            1, 1, 4, 1, 1, 1, 1, 4, 1, 1,
            1, 1, 4, 1, 1, 1, 1, 4, 1, 1,
            1, 1, 4, 1, 1, 1, 4, 1)
        state_conf['pet'][:nclus] = (
            1.5, 1.5, 1.5, 3/8., 3/8., 3/8.,
            3/16., 3/16., 3/16., 3/8., 3/8., 3/8.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            3/8., 3/8., 3/8., 3/8., 3/8.,
            3/8., 3/8., 3/8., 3/8., 3/8.,
            3/8., 3/8., 3/8., 3/8., 3/8.,
            3/8., 3/8., 3/8.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_41/state_conf'][3303] = state_conf


def add_missing_state_3034(db_name):
    """
    Append OCR state cluster definition.

    Modified states for orbit 3034:
    - stateID = 17, 23, 24, 25, 26, 42, 43, 44, 51
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_17/state_conf'][0]
        state_conf['id'] = 0xFF      # 0
        state_conf['nclus'] = nclus
        state_conf['duration'] = 480
        state_conf['num_geo'] = 30
        fid['State_17/state_conf'][3304] = state_conf

        nclus = 56
        state_conf = fid['State_23/state_conf'][0]
        state_conf['id'] = 0xFF      # 0
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1280
        state_conf['num_geo'] = 80
        fid['State_23/state_conf'][3304] = state_conf

        nclus = 56
        state_conf = fid['State_24/state_conf'][0]
        state_conf['id'] = 0xFF      # 0
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1280
        state_conf['num_geo'] = 160
        fid['State_24/state_conf'][3304] = state_conf

        nclus = 56
        state_conf = fid['State_25/state_conf'][0]
        state_conf['id'] = 0xFF      # 0
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1280
        state_conf['num_geo'] = 320
        fid['State_25/state_conf'][3304] = state_conf

        nclus = 56
        state_conf = fid['State_26/state_conf'][0]
        state_conf['id'] = 0xFF      # 2
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1280
        state_conf['num_geo'] = 80
        fid['State_26/state_conf'][3304] = state_conf

        nclus = 56
        state_conf = fid['State_42/state_conf'][0]
        state_conf['id'] = 0xFF      # 1
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1040
        state_conf['num_geo'] = 520
        fid['State_42/state_conf'][3304] = state_conf

        nclus = 56
        state_conf = fid['State_43/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1040
        state_conf['num_geo'] = 520
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 2,
            2, 2, 1, 1, 2,
            8, 8, 1, 8, 1, 1, 8, 8, 8,
            8, 8, 8, 1, 8, 1, 8, 8,
            4, 4, 4, 1, 4, 1, 4,
            8, 2, 8, 1, 8, 1, 8, 1, 8, 1, 8, 8,
            1, 1, 1, 1, 1, 1,
            1, 1, 1)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 2, 2, 1,
            1, 1, 2, 2, 1,
            1, 1, 8, 1, 8, 8, 1, 1, 1,
            1, 1, 1, 8, 1, 8, 1, 1,
            1, 1, 1, 4, 1, 4, 1,
            1, 1, 1, 8, 1, 8, 1, 8, 1, 8, 1, 1,
            1, 1, 1, 1, 1, 1,
            1, 1, 1)
        state_conf['pet'][:nclus] = (
            1., 1., 1., .5, .5, .5,
            .5, .5, .5, .5, .5,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            .25, .25, .25, .25, .25, .25, .25,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            1., 1., 1., 1., 1., 1.,
            1., 1., 1.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_43/state_conf'][3304] = state_conf

        nclus = 56
        state_conf = fid['State_44/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1040
        state_conf['num_geo'] = 260
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 4,
            4, 4, 1, 1, 4,
            16, 16, 16, 4, 16, 4, 16, 16, 16,
            16, 16, 16, 4, 16, 4, 16, 16,
            4, 4, 4, 1, 4, 2, 4,
            8, 4, 8, 2, 8, 2, 8, 2, 8, 2, 8, 8,
            2, 2, 1, 2, 1, 2,
            2, 1, 2)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 4, 4, 1,
            1, 1, 4, 4, 1,
            1, 1, 1, 4, 1, 4, 1, 1, 1,
            1, 1, 1, 4, 1, 4, 1, 1,
            1, 1, 1, 4, 1, 2, 4,
            1, 2, 1, 4, 1, 4, 1, 4, 1, 4, 1, 1,
            1, 1, 2, 1, 2, 1,
            1, 2, 1)
        state_conf['pet'][:nclus] = (
            1., 1., 1., .25, .25, .25,
            .25, .25, .25, .25, .25,
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            .25, .25, .25, .25, .25, .25, .25,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            1/8., 1/8., 1/8., 1/8., 1/8., 1/8.,
            .5, .5, .5, .5, .5, .5,
            .5, .5, .5)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_44/state_conf'][3304] = state_conf

        nclus = 40
        state_conf = fid['State_51/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1024
        state_conf['num_geo'] = 1024
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            1, 1, 1, 1, 1, 2, 2, 2)
        state_conf['n_read'][:nclus] = (
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 1, 1, 1)
        state_conf['pet'][:nclus] = (
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/32., 1/32., 1/32., 1/32., 1/32.,
            1/32., 1/32., 1/32., 1/32., 1/32.,
            1/16., 1/16., 1/16.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_51/state_conf'][3304] = state_conf


def add_missing_state_09(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=09 added definitions for orbits [3981]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_09/state_conf'][0]
        state_conf['id'] = 0xFF     # ?
        state_conf['nclus'] = nclus
        state_conf['duration'] = 918
        state_conf['num_geo'] = 166
    fid['State_09/state_conf'][3981] = state_conf


def add_missing_state_10_13(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=10 added definitions for orbits [3964,3968,4118,4122]
    - stateID=11 added definitions for orbits [3969,4123]
    - stateID=12 added definitions for orbits [3965,3970,4119,4124]
    - stateID=13 added definitions for orbits [3971,4125]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_10/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 593
        state_conf['num_geo'] = 528
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            8, 8, 8, 8, 8, 8, 8, 8)
        state_conf['n_read'][:nclus] = (
            8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
            8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
            8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
            1, 1, 1, 1, 1, 1, 1, 1)
        state_conf['pet'][:nclus] = (
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/32., 1/32., 1/32., 1/32., 1/32., 1/32.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/32., 1/32., 1/32., 1/32., 1/32.,
            1/32., 1/32., 1/32., 1/32., 1/32.,
            1/16., 1/16., 1/16.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        for orbit in [3964, 3968, 4118, 4122]:
            fid['State_10/state_conf'][orbit] = state_conf

        for orbit in [3969, 4123]:
            fid['State_11/state_conf'][orbit] = state_conf

        for orbit in [3965, 3970, 4119, 4124]:
            fid['State_12/state_conf'][orbit] = state_conf

        for orbit in [3971, 4125]:
            fid['State_13/state_conf'][orbit] = state_conf


def add_missing_state_14(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=14 added definitions for orbits [
                          3958, 3959, 3962,
                          4086, 4087, 4088, 4089, 4091, 4092,
                          4111, 4112, 4113, 4114,
                          5994]
    """
    orbit_list = [3958, 3959, 3962,
                  4086, 4087, 4088, 4089, 4091, 4092,
                  4111, 4112, 4113, 4114,
                  5994]
    with h5py.File(db_name, 'r+') as fid:
        nclus = 10
        state_conf = fid['State_14/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1280
        state_conf['num_geo'] = 2
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 4, 10, 10, 40, 40, 20)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        state_conf['pet'][:nclus] = (
            40., 40., 40., 40., 10., 4., 4., 1., 1., 2.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        for orbit in orbit_list:
            fid['State_14/state_conf'][orbit] = state_conf


def add_missing_state_22(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=22 added definitions for orbits
                    [4119, 4120, 4121, 4122, 4123, 4124, 4125, 4126, 4127]
    """
    orbit_list = [4119, 4120, 4121, 4122, 4123, 4124, 4125, 4126, 4127]
    with h5py.File(db_name, 'r+') as fid:
        nclus = 10
        state_conf = fid['State_22/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 782
        state_conf['num_geo'] = 29
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        state_conf['pet'][:nclus] = (
            1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        for orbit in orbit_list:
            fid['State_22/state_conf'][orbit] = state_conf


def add_missing_state_24(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=24 added definitions for orbits [36873:38267] & [47994:48075]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_24/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        state_conf['pet'][:nclus] = (
            1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
            1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
            1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
            1., 1., 1., 1., 1., 1., 1., 1., 1., 1.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        state_conf['duration'] = 1600
        state_conf['num_geo'] = 100
        fid['State_24/state_conf'][36873:38267] = state_conf

        state_conf['duration'] = 1440
        state_conf['num_geo'] = 90
        fid['State_24/state_conf'][47994:48075] = state_conf


def add_missing_state_25_26(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=25 added definitions for orbits [4088,4111]
    - stateID=26 added definitions for orbits [4089]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 10
        state_conf = fid['State_25/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 782
        state_conf['num_geo'] = 174
        state_conf['coaddf'][:nclus] = (
            1, 4, 4, 4, 4, 4, 2, 4, 4, 2)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        state_conf['pet'][:nclus] = (
            1/4., 1/16., 1/16., 1/16., 1/16., 1/16., 1/8., 1/32., 1/16., 1/8.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        for orbit in [4088, 4111]:
            fid['State_25/state_conf'][orbit] = state_conf

        for orbit in [4089]:
            fid['State_26/state_conf'][orbit] = state_conf


def add_missing_state_27(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=27 added definitions for orbits [44091,44092]
    - stateID=27 added definitions for orbits [44134,44148,44149,44150]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_27/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 647
        state_conf['num_geo'] = 48
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 6, 6, 6)
        state_conf['n_read'][:nclus] = (
            6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
            12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
            6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
            6, 6, 6, 6, 6, 1, 1, 1)
        state_conf['pet'][:nclus] = (
            1.5, 1.5, 1.5, 1.5, 1.5, 1.5,
            1.5, 1.5, 1.5, 1.5, 1.5, 1.5,
            .75, .75, .75, .75, .75,
            .75, .75, .75, .75, .75,
            1.5, 1.5, 1.5, 1.5, 1.5,
            1.5, 1.5, 1.5, 1.5, 1.5,
            1.5, 1.5, 1.5, 1.5, 1.5,
            1.5, 1.5, 1.5)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        for orbit in [44091, 44092]:
            fid['State_27/state_conf'][orbit] = state_conf

        nclus = 40
        state_conf = fid['State_27/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 647
        state_conf['num_geo'] = 48
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 12, 12, 12)
        state_conf['n_read'][:nclus] = (
            12, 12, 12, 12, 12, 12,
            12, 12, 12, 12, 12, 12,
            24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
            12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
            12, 12, 12, 12, 12,
            1, 1, 1)
        state_conf['pet'][:nclus] = (
            1.5, 1.5, 1.5, 1.5, 1.5, 1.5,
            1.5, 1.5, 1.5, 1.5, 1.5, 1.5,
            .75, .75, .75, .75, .75,
            .75, .75, .75, .75, .75,
            1.5, 1.5, 1.5, 1.5, 1.5,
            1.5, 1.5, 1.5, 1.5, 1.5,
            1.5, 1.5, 1.5, 1.5, 1.5,
            1.5, 1.5, 1.5)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        for orbit in [44134, 44148, 44149, 44150]:
            fid['State_27/state_conf'][orbit] = state_conf


def add_missing_state_33_39(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=33 added definitions for orbits [4087, 4089, 4110, 4112]
    - stateID=34 added definitions for orbits [4088, 4090, 4111, 4113]
    - stateID=38 added definitions for orbits [4087, 4089, 4110, 4112]
    - stateID=39 added definitions for orbits [4088, 4090, 4111, 4113]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 10
        state_conf = fid['State_33/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1406
        state_conf['num_geo'] = 42
        state_conf['coaddf'][:nclus] = (
            1, 1, 8, 8, 16, 32, 32, 32, 32, 32)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        state_conf['pet'][:nclus] = (
            2., 2., 1/4., 1/4., 1/8., 1/32., 1/32., .0072, .0036, .0072)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        for orbit in [4087, 4089, 4110, 4112]:
            fid['State_33/state_conf'][orbit] = state_conf

        for orbit in [4088, 4090, 4111, 4113]:
            fid['State_34/state_conf'][orbit] = state_conf

        for orbit in [4087, 4089, 4110, 4112]:
            fid['State_38/state_conf'][orbit] = state_conf

        for orbit in [4088, 4090, 4111, 4113]:
            fid['State_39/state_conf'][orbit] = state_conf


def add_missing_state_35_39(db_name):
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
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_09/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            8, 8, 8, 8, 8, 8, 8, 8)
        state_conf['n_read'][:nclus] = (
            8, 8, 8, 8, 8, 8,
            8, 8, 8, 8, 8, 8,
            8, 8, 8, 8, 8,
            8, 8, 8, 8, 8,
            8, 8, 8, 8, 8,
            8, 8, 8, 8, 8,
            1, 1, 1, 1, 1,
            1, 1, 1)
        state_conf['pet'][:nclus] = (
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/32., 1/32., 1/32., 1/32., 1/32., 1/32.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/32., 1/32., 1/32., 1/32., 1/32.,
            1/32., 1/32., 1/32., 1/32., 1/32.,
            1/16., 1/16., 1/16.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        state_conf['duration'] = 928
        state_conf['num_geo'] = 928
        for orbit in [3967, 4121]:
            fid['State_09/state_conf'][orbit] = state_conf

        state_conf['duration'] = 1511
        state_conf['num_geo'] = 1344
        for orbit in [3972, 4126]:
            fid['State_35/state_conf'][orbit] = state_conf

        for orbit in [3973, 4127]:
            fid['State_36/state_conf'][orbit] = state_conf

        for orbit in [3975]:
            fid['State_37/state_conf'][orbit] = state_conf

        for orbit in [3976]:
            fid['State_38/state_conf'][orbit] = state_conf

        for orbit in [3977]:
            fid['State_39/state_conf'][orbit] = state_conf


def add_missing_state_42(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=42 added definitions for orbits [6778, 6779]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_42/state_conf'][0]
        state_conf['id'] = 0xFF
        state_conf['nclus'] = nclus
        state_conf['duration'] = 5598
        state_conf['num_geo'] = 2650
        for orbit in [6778, 6779]:
            fid['State_42/state_conf'][orbit] = state_conf


def add_missing_state_43(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=43 added definitions for orbits [6778, 6779]
    - stateID=43 added definitions for orbits [7193, 7194]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_43/state_conf'][0]
        state_conf['id'] = 0xFF      # ???
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1118
        state_conf['num_geo'] = 536
        for orbit in [6778, 6779]:
            fid['State_43/state_conf'][orbit] = state_conf

        nclus = 40
        state_conf = fid['State_43/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1120
        state_conf['num_geo'] = 84
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            16, 16, 16, 16, 16,
            16, 16, 16, 16, 16,
            16, 16, 16)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1,
            4, 4, 4, 4, 4,
            4, 4, 4, 4, 4,
            4, 4, 4, 4, 4,
            10, 10, 10, 10, 10,
            10, 10, 10, 10, 10,
            10, 10, 10)
        state_conf['pet'][:nclus] = (
            10., 10., 10., 10., 10., 10.,
            10., 10., 10., 10., 10., 10.,
            2.5, 2.5, 2.5, 2.5, 2.5,
            2.5, 2.5, 2.5, 2.5, 2.5,
            2.5, 2.5, 2.5, 2.5, 2.5,
            1/32., 1/32., 1/32., 1/32., 1/32.,
            1/32., 1/32., 1/32., 1/32., 1/32.,
            1/32., 1/32., 1/32.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        for orbit in [7193, 7194]:
            fid['State_43/state_conf'][orbit] = state_conf


def add_missing_state_44(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=44 added definitions for orbits [6778, 6779]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_44/state_conf'][0]
        state_conf['id'] = 0xFF      # ???
        state_conf['nclus'] = nclus
        state_conf['duration'] = 447
        state_conf['num_geo'] = 219
        for orbit in [6778, 6779]:
            fid['State_44/state_conf'][orbit] = state_conf


def add_missing_state_55(db_name):
    """
    Append OCR state cluster definition.

    Modified states:
    - stateID=55 added definitions for orbits [26812:26834]
    - stateID=55 added definitions for orbits [28917:28920, 30836:30850]
    """
    with h5py.File(db_name, 'r+') as fid:
        nclus = 40
        state_conf = fid['State_55/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 640
        state_conf['num_geo'] = 640
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2,
            2, 2, 2, 2, 2,
            2, 2, 2, 2, 2,
            2, 2, 2, 2, 2,
            1, 1, 1, 1, 1,
            2, 2, 2)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2,
            2, 2, 2, 2, 2,
            2, 2, 2, 2, 2,
            1, 1, 1, 1, 1,
            2, 2, 2, 2, 2,
            1, 1, 1)
        state_conf['pet'][:nclus] = (
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16., 1/16., 1/16.,
            1/16., 1/16., 1/16.)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_55/state_conf'][26812:2683] = state_conf

        nclus = 40
        state_conf = fid['State_55/state_conf'][0]
        state_conf['nclus'] = nclus
        state_conf['duration'] = 1673
        state_conf['num_geo'] = 186
        state_conf['coaddf'][:nclus] = (
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1)
        state_conf['n_read'][:nclus] = (
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1)
        state_conf['pet'][:nclus] = (
            .5, .5, .5, .5, .5, .5,
            .5, .5, .5, .5, .5, .5,
            .5, .5, .5, .5, .5,
            .5, .5, .5, .5, .5,
            .5, .5, .5, .5, .5,
            .5, .5, .5, .5, .5,
            .5, .5, .5, .5, .5,
            .5, .5, .5)
        state_conf['intg'][:nclus] = np.asarray(16 * state_conf['coaddf'],
                                                np.clip(state_conf['pet'],
                                                        1/16, 1280),
                                                dtype='u2')
        fid['State_55/state_conf'][28917:28920] = state_conf
        fid['State_55/state_conf'][30836:30850] = state_conf


# ---------- obsolete ??? ----------
def fill_mtbl(db_name):
    """
    Fill metaTable by interpolation and in a few cases by extrapolation
    """
    with h5py.File(db_name, 'r+') as fid:
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


# -------------------------
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

    def close(self) -> None:
        """
        Close resources
        """
        if self.fid is not None:
            self.fid.close()

    def create(self) -> None:
        """
        Create and initialize the state definition database.
        """
        self.fid = h5py.File(self.db_name, 'w', libver='latest',
                             driver='core', backing_store=True)
        #
        # add global attributes
        #
        self.fid.attrs['title'] = \
            'Sciamachy state-cluster definition database'
        self.fid.attrs['institution'] = \
            'SRON Netherlands Institute for Space Research (Earth)'
        self.fid.attrs['source'] = 'Sciamachy Level 1b (SCIA/8.01)'
        self.fid.attrs['program_version'] = VERSION
        self.fid.attrs['creation_date'] = strftime('%Y-%m-%d %T %Z', gmtime())
        #
        # initialize state-cluster definition dataset
        #
        for ni in range(1, 71):
            grp = self.fid.create_group('State_{:02d}'.format(ni))
            _ = grp.create_dataset('state_conf', (MAX_ORBIT,),
                                   dtype=clus_def.state_dtype(),
                                   chunks=(64,), fletcher32=True,
                                   compression=1, shuffle=True)

    def update(self, orbit, states_l1b) -> None:
        """
        update database entry from a level 1b product

        Parameters
        ----------
         orbit  :   revolution counter
         states :   state configurations with equal state ID
        """
        msg = "Warning [{:05d}] skipped state configuration for ID {:02d}, {}"

        # Measurements with a given state ID can be performed more than once.
        # The instrument settings of these state executions  are collected
        # in state_conf
        state_id = states_l1b['state_id'][0]
        grp = self.fid['State_{:02d}'.format(state_id)]

        states = []
        for state in states_l1b:
            nclus = state['num_clus']
            if nclus not in [10, 29, 40, 56]:
                continue

            state_conf = grp['state_conf'][0]
            state_conf['nclus'] = nclus
            state_conf['duration'] = state['duration']
            state_conf['num_geo'] = state['num_geo']
            state_conf['coaddf'][:nclus] = state['Clcon']['coaddf'][:nclus]
            state_conf['n_read'][:nclus] = state['Clcon']['n_read'][:nclus]
            state_conf['intg'][:nclus] = state['Clcon']['intg'][:nclus]
            state_conf['pet'][:nclus] = state['Clcon']['pet'][:nclus]
            states.append(state_conf)

        # Check if each L1B state definition has an invalid number of cluster
        if not states:
            print(msg.format(orbit, state_id,
                             'invalid number of clusters'), nclus)
            return

        # Are all state definitions equal?
        # If not then the states may differ in duration (not critical) or
        # one of the critical settings are different (warn the user)
        state = np.unique(states)
        if len(state) > 1:
            skip = False
            mesg = msg.format(orbit, state_id, 'inconsistent')
            for key in state.dtype.names:
                if np.all(state[key] == state[key][0]):
                    continue

                mesg += ' ' + key
                if key not in ('duration', 'num_geo'):
                    skip = True

            # Skip with a warning because a critical state parameter differs
            if skip:
                print(mesg)
                return
            # Only the duration differs then inform the user
            print("Info [{:05d}] state {:02d} of unexpected duration".format(
                orbit, state_id))

        grp['state_conf'][orbit] = state[0]

    def finalize(self):
        """
        fill empty entries
        """
        with h5py.File(self.db_name, 'r+') as fid:
            for state_id in range(1, 71):
                state_conf = fid['State_{:02d}/state_conf'.format(state_id)][:]
                uniq, indices, counts = np.unique(state_conf,
                                                  return_counts=True,
                                                  return_index=True)
                for _id in range(uniq.size):
                    state_conf['id'][np.where(indices == _id)[0]] = _id


# - main code --------------------------------------
def main():
    """
    main function
    """
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
        msg = "{:5d} : add state configuration for ID {:02d}"

        if Path(clusdb.db_name).is_file():
            if args.force:
                Path(clusdb.db_name).unlink()
            else:
                print("use '--force' to overwite an existing database")
                return

        # create new database
        clusdb.create()

        # all cluster-configurations of all L1B products
        # for orbit in range(8600, 8700):
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
                if args.verbose:
                    print(msg.format(orbit, state_id))
                clusdb.update(orbit, states[indx])

        clusdb.close()
    elif args.add_ocr:
        if not Path(clusdb.db_name).is_file():
            print("database not found, please create one using '--all_lv1'")
        else:
            add_missing_state_3033(clusdb.db_name)
            add_missing_state_3034(clusdb.db_name)
            add_missing_state_09(clusdb.db_name)
            add_missing_state_10_13(clusdb.db_name)
            add_missing_state_14(clusdb.db_name)
            add_missing_state_22(clusdb.db_name)
            add_missing_state_24(clusdb.db_name)
            add_missing_state_25_26(clusdb.db_name)
            add_missing_state_27(clusdb.db_name)
            add_missing_state_33_39(clusdb.db_name)
            add_missing_state_35_39(clusdb.db_name)
            add_missing_state_42(clusdb.db_name)
            add_missing_state_43(clusdb.db_name)
            add_missing_state_44(clusdb.db_name)
            add_missing_state_55(clusdb.db_name)
    elif args.finalize:
        if not Path(clusdb.db_name).is_file():
            print("database not found, please create one using '--all_lv1'")
        else:
            clusdb.finalize()
    else:
        raise ValueError('unknown commandline parameter')


# -------------------------
if __name__ == '__main__':
    main()
