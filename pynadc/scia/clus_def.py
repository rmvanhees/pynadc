"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Definition of Sciamachy instrument and cluster configurations

Copyright (c) 2012-2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
from operator import itemgetter

import h5py
import numpy as np

from .hk import get_det_vis_pet, get_det_ir_pet


def clusdef_dtype():
    """
    Returns numpy-dtype definition for a cluster configuration

    Structure elements
    ------------------
    - chan_id  :  channel identifier, integer between [1, 8]
    - clus_id  :  cluster identifier, integer between [1, 64]
    - start    :  pixel number within channel, integer between [0, 1023]
    - length   :  number of pixels, integer between [1, 1024]
    """
    return np.dtype([
        ('chan_id', 'u1'),
        ('clus_id', 'u1'),
        ('start', 'u2'),
        ('length', 'u2')
    ])


def state_dtype():
    """
    Returns numpy-dtype definition for a state configuration

    Structure elements
    ------------------
    - id      :  unique identifier, integer (default 0)
    - nclus   :  number of clusters, integer in [10, 29, 40, 56]
    - duration  :  duration of state (1/16 sec), integer
    - num_geo   :  number of geolocations, integer
    - coaddf  :  coadding-factor of each cluster, integer
    - n_read  :  number of readouts of each cluster, integer
    - intg    :  integration time (1/16 sec) of each cluster, integer
    - pet     :  pixel exposure time of each cluster, float
    """
    return np.dtype([
        ('id', 'u1'),
        ('nclus', 'u1'),
        ('duration', 'u2'),
        ('num_geo', 'u2'),
        ('coaddf', 'u1', (56)),
        ('n_read', 'u2', (56)),
        ('intg', 'u2', (56)),
        ('pet', 'f4', (56))
    ])


def clus_conf(nclus):
    """
    Returns Sciamachy cluster configuration for a given number of clusters,
    only defined for 10, 29, 40 or 56 clusters else the function return 'None'.

    Valid for the whole Sciamachy mission
    """
    if nclus == 56:        # mostly nadir measurements
        clusdef_tuple = [
            (1, 1, 0, 5), (1, 2, 5, 192), (1, 3, 197, 355),
            (1, 4, 552, 196), (1, 5, 748, 94), (1, 6, 1019, 5),
            (2, 7, 1019, 5), (2, 8, 834, 114), (2, 9, 170, 664),
            (2, 10, 76, 94), (2, 11, 0, 5),
            (3, 12, 0, 10), (3, 13, 33, 50), (3, 14, 83, 80),
            (3, 15, 163, 436), (3, 16, 599, 75), (3, 17, 674, 87),
            (3, 18, 761, 135), (3, 19, 896, 34), (3, 20, 1019, 5),
            (4, 21, 0, 5), (4, 22, 10, 36), (4, 23, 46, 32),
            (4, 24, 78, 535), (4, 25, 613, 134), (4, 26, 747, 106),
            (4, 27, 853, 66), (4, 28, 1019, 5),
            (5, 29, 0, 5), (5, 30, 10, 46), (5, 31, 56, 28),
            (5, 32, 84, 525), (5, 33, 609, 158), (5, 34, 767, 234),
            (5, 35, 1019, 5),
            (6, 36, 0, 10), (6, 37, 24, 83), (6, 38, 107, 228),
            (6, 39, 335, 26), (6, 40, 361, 178), (6, 41, 539, 28),
            (6, 42, 567, 179), (6, 43, 746, 154), (6, 44, 900, 31),
            (6, 45, 931, 14), (6, 46, 945, 52), (6, 47, 1014, 10),
            (7, 48, 0, 10), (7, 49, 48, 245), (7, 50, 293, 148),
            (7, 51, 441, 442), (7, 52, 883, 105), (7, 53, 1014, 10),
            (8, 54, 0, 10), (8, 55, 10, 1004), (8, 56, 1014, 10)
        ]
        return np.array(clusdef_tuple, dtype=clusdef_dtype())

    if nclus == 40:        # mostly limb & occultation measurements
        clusdef_tuple = [
            (1, 1, 0, 5), (1, 2, 5, 192), (1, 3, 197, 355),
            (1, 4, 552, 290), (1, 5, 842, 177), (1, 6, 1019, 5),
            (2, 7, 1019, 5), (2, 8, 948, 71), (2, 9, 170, 778),
            (2, 10, 76, 94), (2, 11, 5, 71), (2, 12, 0, 5),
            (3, 13, 0, 10), (3, 14, 10, 23), (3, 15, 33, 897),
            (3, 16, 930, 89), (3, 17, 1019, 5),
            (4, 18, 0, 5), (4, 19, 5, 5), (4, 20, 10, 909),
            (4, 21, 919, 100), (4, 22, 1019, 5),
            (5, 23, 0, 5), (5, 24, 5, 5), (5, 25, 10, 991),
            (5, 26, 1001, 18), (5, 27, 1019, 5),
            (6, 28, 0, 10), (6, 29, 10, 14), (6, 30, 24, 973),
            (6, 31, 997, 17), (6, 32, 1014, 10),
            (7, 33, 0, 10), (7, 34, 10, 38), (7, 35, 48, 940),
            (7, 36, 988, 26), (7, 37, 1014, 10),
            (8, 38, 0, 10), (8, 39, 10, 1004), (8, 40, 1014, 10)
        ]
        return np.array(clusdef_tuple, dtype=clusdef_dtype())

    # only, used by dedicated measurements
    if nclus == 29:
        clusdef_tuple = [
            (1, 1, 0, 5), (1, 2, 5, 10), (1, 3, 216, 528),
            (1, 4, 744, 64), (1, 5, 1009, 10), (1, 6, 1019, 5),
            (2, 7, 1019, 5), (2, 8, 190, 739), (2, 9, 94, 96),
            (2, 10, 5, 10), (2, 11, 0, 5),
            (3, 12, 0, 5), (3, 13, 46, 930), (3, 14, 1019, 5),
            (4, 15, 0, 5), (4, 16, 46, 931), (4, 17, 1019, 5),
            (5, 18, 0, 5), (5, 19, 54, 914), (5, 20, 1019, 5),
            (6, 21, 0, 10), (6, 22, 45, 933), (6, 23, 1014, 10),
            (7, 24, 0, 10), (7, 25, 73, 877), (7, 26, 1014, 10),
            (8, 27, 0, 10), (8, 28, 73, 878), (8, 29, 1014, 10)
        ]
        return np.array(clusdef_tuple, dtype=clusdef_dtype())

    # only, used by dedicated measurements
    if nclus == 10:
        clusdef_tuple = [
            (1, 1, 0, 552), (1, 2, 552, 472),
            (2, 3, 170, 854), (2, 4, 0, 170),
            (3, 5, 0, 1024),
            (4, 6, 0, 1024),
            (5, 7, 0, 1024),
            (6, 8, 0, 1024),
            (7, 9, 0, 1024),
            (8, 10, 0, 1024)
        ]
        return np.array(clusdef_tuple, dtype=clusdef_dtype())

    return None


def state_clus_conf(state_id):
    """
    Returns Sciamachy cluster configuration for a given state_id

    Exceptions, state cluster definition with only 10 clusters:
     - state_id=01: [5984 5985 5986 5987 5988 5989 5990 5991 5992 5993 5995
                     5996 5997 5998 5999 6000]
     - state_id=02: [5984 5986 5988 5990 5992 5996 5998 6000]
     - state_id=03: [5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=04: [5984 5986 5988 5990 5992 5996 5998 6000]
     - state_id=05: [5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=06: [5984 5986 5988 5990 5992 5996 5998 6000]
     - state_id=07: [5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=08: [1714 4087 4089 4110 4112]
     - state_id=09: [1622 1650]
     - state_id=10: [1623 1651 3958 3962 4086 4088 4091 4111 4113 5994]
     - state_id=11: [1624 1652 1684 3958 3962 4086 4088 4091 4111 4113 5994]
     - state_id=12: [1625 1653 3958 3962 4086 4088 4091 4111 4113 5994]
     - state_id=13: [1597 1626 1654 1714 4087 4089 4110 4112]
     - state_id=14: [1684 1700 3958 3959 3962 4086 4087 4088 4089 4091 4092
                     4111 4112 4113 4114 5994]
     - state_id=15: [3959 3960 3961 3962 4085 4086 4090 4091 4092 4114 4115
                     4116 5994]
     - state_id=16: [1684 3958 3962 4086 4088 4091 4111 4113 5994]
     - state_id=17: [1603 1604 1615 1618 1633 1658 1659 1660 1692 5984 5986
                     5988 5990 5992 5996 5998 6000]
     - state_id=21: [1610 1611 1639 1640 1668 1669 5984 5986 5988 5990 5992
                     5996 5998 6000]
     - state_id=22: [1636 1665 4119 4120 4121 4122 4123 4124 4125 4126 4127
                     5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=23: [5984 5986 5988 5990 5992 5996 5998 6000]
     - state_id=24: [5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=27: [5984 5986 5988 5990 5992 5996 5998 6000]
     - state_id=28: [5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=29: [5984 5986 5988 5990 5992 5996 5998 6000]
     - state_id=30: [5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=31: [5984 5986 5988 5990 5992 5996 5998 6000]
     - state_id=32: [5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=34: [1637 1666 1686 1699 1700 1701 4088 4090 4111 4113]
     - state_id=35: [5984 5985 5986 5987 5988 5989 5990 5991 5992 5993 5995
                     5996 5997 5998 5999 6000]
     - state_id=36: [1708 1709 1710 1712 1713 1724 1726 1727 1728 5985 5986
                     5987 5988 5989 5990 5991 5992 5993 5995 5996 5997 5998
                     5999 6000]
     - state_id=37: [3960 4090]
     - state_id=38: [4087 4089 4110 4112]
     - state_id=39: [4088 4090 4111 4113]
     - state_id=40: [5985 5987 5989 5991 5993 5995 5997 5999]
     - state_id=42: [1714]
     - state_id=48: [1684 3958 3962 4086 4088 4091 4111 4113 5994]
     - state_id=52: [3961 4092 4116]
     - state_id=59: [1684 3958 3962 4086 4088 4091 4111 4113 5994]
     - state_id=61: [1684 3958 3962 4086 4088 4091 4111 4113 5994]
     - state_id=62: [3962 4091 5994]
     - state_id=69: [1684]
     - state_id=70: [1684 3958 3962 4086 4088 4091 4111 4113 5994]

    Exceptions, state cluster definition with only 29 clusters:
     - state_id=08: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=09: [1698]
     - state_id=10: [1698]
     - state_id=11: [1698]
     - state_id=23: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=24: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=25: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=26: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=27: [1691 1692 1705 1706 1708 1709 1710 1712 1713 1736 1739
                     1744]
     - state_id=28: [1691 1692 1705 1706 1708 1709 1710 1712 1713 1723 1724
                     1726 1727 1728 1736 1739 1740 1744]
     - state_id=29: [1691 1692 1705 1706 1708 1709 1710 1712 1713 1736 1739
                     1740 1744]
     - state_id=30: [1691 1692 1705 1706 1708 1709 1710 1712 1713 1736 1739
                     1740 1744]
     - state_id=31: [1691 1692 1705 1706 1708 1709 1710 1712 1713 1736 1739
                     1740 1744]
     - state_id=32: [1691 1692 1705 1706 1708 1709 1710 1712 1713 1736 1739
                     1740 1744]
     - state_id=35: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=40: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=41: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=44: [1572 1573 1574 1575 1576 1577 1578]
     - state_id=47: [1692 1708 1709 1710 1712 1713 1723 1724 1726 1727 1728
                     1736 1739 1740]
     - state_id=49: [1691 1744]
     - state_id=51: [1705 1706]
     - state_id=62: [1736]
     - state_id=64: [1708 1710 1712 1713 1723 1727]
     - state_id=66: [1724 1728]
     - state_id=68: [1692 1709 1726 1736 1739 1740]
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
        return clus_conf(40)

    if clusdef_index[state_id] == 3:
        return clus_conf(56)

    return None


def state_conf_db(state_id, orbit,
                  db_name='/SCIA/share/nadc_tools/scia_state_db.h5'):
    """
    Obtains state configuration from database

    Parameters
    ----------
    * state_id :   state identifier [0, 70]
    * orbit    :   revolution counter

    Returns
    -------
    state configuration
    """
    with h5py.File(db_name, 'r') as fid:
        grp = fid["State_{:02d}".format(state_id)]
        state_conf = grp['state_conf'][orbit]
        if state_conf['nclus'] > 0:
            return state_conf

    return None


def state_conf_data(det_isp):
    """
    Derive state configuration from level 0 measurements

    Parameters
    ----------
    * det_isp  :  SCIA level 0 detector ISPs of one state execution

    Returns
    -------
    state configuration
    """
    # check input data
    state_id = det_isp['data_hdr']['state_id']
    if not np.all(state_id == state_id[0]):
        raise ValueError('you should provide ISP of the same state')

    # collect parameters which define the state configuration
    clus_list = []
    for dsr in det_isp:
        num_chan = dsr['pmtc_hdr']['num_chan']
        for chan in dsr['chan_data'][:num_chan]:
            chan_id = chan['hdr']['id_is_lu'] >> 4
            if chan_id < 6:
                pet = None
                pet_list, vir_chan_b = get_det_vis_pet(chan['hdr'])
                if isinstance(pet_list, float):
                    pet = pet_list
            else:
                vir_chan_b = 0
                pet = get_det_ir_pet(chan['hdr'])
                pet_list = None

            num_clus = chan['hdr']['clusters']
            for clus in chan['clus_hdr'][:num_clus]:
                clus_id = clus['id']
                coaddf = clus['coaddf']
                start = clus['start'] % 1024
                length = clus['length']
                if isinstance(pet_list, list):
                    if start >= vir_chan_b:
                        pet = pet_list[1]
                    else:
                        pet = pet_list[0]
                if chan_id == 2:
                    start = 1024 - start - length
                clus_list.append((chan_id, clus_id, start, length, coaddf, pet))

    # find unique settings in 'clus_list'
    clus_set = sorted(set(clus_list), key=itemgetter(0, 1))
    # clus_set.append((9, 0, 0, 10, 1, 1.0))
    nclus = len(clus_set)
    if nclus < 10:
        print('# Fatal - failed with number of cluster equal to ', nclus)
        return None

    if nclus not in [10, 29, 40, 56]:
        print('# Warning - number of cluster equal to %d, try fix this' % nclus)
        clus_def = np.zeros(nclus, dtype=clusdef_dtype())
        for ni, clus in enumerate(clus_set):
            clus_def['chan_id'][ni] = clus[0]
            clus_def['clus_id'][ni] = ni + 1
            clus_def['start'][ni] = clus[2]
            clus_def['length'][ni] = clus[3]

        # obtain reference cluster definitions
        while nclus not in [10, 29, 40, 56]:
            nclus -= 1
        clus_ref_def = clus_conf(nclus)

        # find and remove wrong entries from set
        clus_error = np.setdiff1d(clus_def, clus_ref_def)
        for indx in np.where(clus_def == clus_error)[0]:
            del clus_set[indx]

        if nclus not in [10, 29, 40, 56]:
            print('# Fatal - failed with number of cluster equal to ', nclus)
            return None

    # count number of ISP per state execution
    _, counts = np.unique(det_isp['data_hdr']['icu_time'],
                          return_counts=True)

    # fill the output structure
    state_conf = np.squeeze(np.zeros(1, dtype=state_dtype()))
    state_conf['nclus'] = nclus
    state_conf['duration'] = det_isp['pmtc_hdr']['bcps'].max()
    if counts.size > 1:
        state_conf['num_geo'] = sorted(counts)[counts.size // 2]
    else:
        state_conf['num_geo'] = counts[0]
    for ni, clus in enumerate(clus_set):
        state_conf['coaddf'][ni] = clus[4]
        state_conf['intg'][ni] = max(1, int(16 * clus[4] * clus[5]))
        state_conf['pet'][ni] = clus[5]

    # finally, add number of readouts
    for ni in range(nclus):
        state_conf['n_read'][ni] = (
            state_conf['intg'].max() // state_conf['intg'][ni])

    return state_conf
