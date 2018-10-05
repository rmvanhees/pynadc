"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Definition of Sciamachy cluster definitions

Copyright (c) 2012-2018 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  Standard 3-clause BSD
"""
import numpy as np


def clus_def(nclus):
    """
    Returns Sciamachy cluster definition given the number of clusters

    Valid for the whole Sciamachy mission
    """
    clusdef_dtype = np.dtype(
        ('chan_id', 'u1'),
        ('clus_id', 'u1'),
        ('start', 'u2'),
        ('length', 'u2'),
        # ('coaddf', 'u2'),
        # ('intg', 'u2'),
        # ('readouts', 'u2'),
        # ('pet', 'f8'),
        # ('data', 'O'),
    )

    if nclus == 10:
        clusdef_tuple = (
            (1, 0, 0, 552), (1, 1, 552, 472),
            (2, 2, 170, 854), (2, 3, 0, 170),
            (3, 4, 0, 1024),
            (4, 5, 0, 1024),
            (5, 6, 0, 1024),
            (6, 7, 0, 1024),
            (7, 8, 0, 1024),
            (0, 9, 0, 1024)
        )
        return np.array(clusdef_tuple, dtype=clusdef_dtype)

    if nclus == 40:
        clusdef_tuple = (
            (1, 0, 0, 5), (1, 1, 5, 192), (1, 2, 197, 355), (1, 3, 552, 290),
            (1, 4, 842, 177), (1, 5, 1019, 5),
            (2, 0, 1024, 5), (2, 1, 1029, 71), (2, 2, 1100, 778),
            (2, 3, 1878, 94), (2, 4, 1972, 71), (2, 5, 2043, 5),
            (3, 0, 2048, 10), (3, 1, 2058, 23), (3, 2, 2081, 897),
            (3, 3, 2978, 89), (3, 4, 3067, 5),
            (4, 0, 3072, 5), (4, 1, 3077, 5), (4, 2, 3082, 909),
            (4, 3, 3991, 100), (4, 4, 4091, 5),
            (5, 0, 4096, 5), (5, 1, 4101, 5), (5, 2, 4106, 991),
            (5, 3, 5097, 18), (5, 4, 5115, 5),
            (6, 0, 5120, 10), (6, 1, 5130, 14), (6, 2, 5144, 973),
            (6, 3, 6117, 17), (6, 4, 6134, 10),
            (7, 0, 6144, 10), (7, 1, 6154, 38), (7, 2, 6192, 940),
            (7, 3, 7132, 26), (7, 4, 7158, 10),
            (8, 0, 7168, 10), (8, 1, 7178, 1004), (8, 2, 8182, 10)
        )
        return np.array(clusdef_tuple, dtype=clusdef_dtype)

    if nclus == 56:
        clusdef_tuple = (
            (1, 0, 0, 5), (1, 1, 5, 192), (1, 2, 197, 355), (1, 3, 552, 196),
            (1, 4, 748, 94), (1, 5, 1019, 5),
            (2, 0, 1024, 5), (2, 1, 1100, 114), (2, 2, 1214, 664),
            (2, 3, 1878, 94), (2, 4, 2043, 5),
            (3, 0, 2048, 10), (3, 1, 2081, 50), (3, 2, 2131, 80),
            (3, 3, 2211, 436), (3, 4, 2647, 75), (3, 5, 2722, 87),
            (3, 6, 2809, 135), (3, 7, 2944, 34), (3, 8, 3067, 5),
            (4, 0, 3072, 5), (4, 1, 3082, 36), (4, 2, 3118, 32),
            (4, 3, 3150, 535), (4, 4, 3685, 134), (4, 5, 3819, 106),
            (4, 6, 3925, 66), (4, 7, 4091, 5),
            (5, 0, 4096, 5), (5, 1, 4106, 46), (5, 2, 4152, 28),
            (5, 3, 4180, 525), (5, 4, 4705, 158), (5, 5, 4863, 234),
            (5, 6, 5115, 5),
            (6, 0, 5120, 10), (6, 1, 5144, 83), (6, 2, 5227, 228),
            (6, 3, 5455, 26), (6, 4, 5481, 178), (6, 5, 5659, 28),
            (6, 6, 5687, 179), (6, 7, 5866, 154), (6, 8, 6020, 31),
            (6, 9, 6051, 14), (6, 10, 6065, 52), (6, 11, 6134, 10),
            (7, 0, 6144, 10), (7, 1, 6192, 245), (7, 2, 6437, 148),
            (7, 3, 6585, 442), (7, 4, 7027, 105), (7, 5, 7158, 10),
            (8, 0, 7168, 10), (8, 1, 7178, 1004), (8, 2, 8182, 10)
        )
        return np.array(clusdef_tuple, dtype=clusdef_dtype)

    return None


def state_clus_def(state_id):
    """
    Returns Sciamachy cluster definition for a given state_id

    Exceptions, OCR state cluster definition with only 10 clusters:
     - state_id=14: [3958,3959,3962, 4086,4087,4088,4089,4091,4092,
                     4111,4112,4113,4114,5994]
     - state_id=22: [4119,4120,4121,4122,4123,4124,4125,4126,4127]
     - state_id=25: [4088, 4111]
     - state_id=26: [4089]
     - state_id=33: [4087,4089,4110,4112]
     - state_id=34: [4088,4090,4111,4113]
     - state_id=38: [4087,4089,4110,4112]
     - state_id=39: [4088,4090,4111,4113]
    """
    # define state cluster index for all 70 possible state identifiers
    clusdef_index = (0,
                     3, 3, 3, 3, 3, 3, 3, 1, 3, 3,
                     3, 3, 3, 3, 3, 1, 1, 1, 1, 1,
                     1, 1, 3, 3, 3, 1, 1, 1, 1, 1,
                     1, 1, 1, 1, 1, 1, 1, 3, 1, 1,
                     1, 3, 3, 3, 3, 1, 1, 1, 1, 1,
                     1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                     1, 1, 1, 1, 1, 1, 1, 1, 1, 1)

    if state_id < 1 or state_id >= len(clusdef_index):
        raise ValueError('state_id {} is in valid'.format(state_id))

    if clusdef_index[state_id] == 1:
        return clus_def(40)

    if clusdef_index[state_id] == 3:
        return clus_def(56)

    return None
