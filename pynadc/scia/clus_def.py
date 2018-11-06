"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Definition of Sciamachy cluster configurations

Copyright (c) 2012-2018 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  Standard 3-clause BSD
"""
import numpy as np


def clus_conf(nclus):
    """
    Returns Sciamachy cluster configuration for a given number of clusters,
    only defined for 10, 40 or 56 clusters else the function return 'None'.

    Valid for the whole Sciamachy mission
    """
    clusdef_dtype = np.dtype(
        ('chan_id', 'u1'),
        ('clus_id', 'u1'),
        ('start', 'u2'),
        ('length', 'u2')
    )

    if nclus == 56:        # mostly nadir measurements
        clusdef_tuple = (
            (1, 0, 0, 5), (1, 1, 5, 192), (1, 2, 197, 355),
            (1, 3, 552, 196), (1, 4, 748, 94), (1, 5, 1019, 5),
            (2, 6, 1019, 5), (2, 7, 834, 114), (2, 8, 170, 664),
            (2, 9, 76, 94), (2, 10, 0, 5),
            (3, 11, 0, 10), (3, 12, 33, 50), (3, 13, 83, 80),
            (3, 14, 163, 436), (3, 15, 599, 75), (3, 16, 674, 87),
            (3, 17, 761, 135), (3, 18, 896, 34), (3, 19, 1019, 5),
            (4, 20, 0, 5), (4, 21, 10, 36), (4, 22, 46, 32),
            (4, 23, 78, 535), (4, 24, 613, 134), (4, 25, 747, 106),
            (4, 26, 853, 66), (4, 27, 1019, 5),
            (5, 28, 0, 5), (5, 29, 10, 46), (5, 30, 56, 28),
            (5, 31, 84, 525), (5, 32, 609, 158), (5, 33, 767, 234),
            (5, 34, 1019, 5),
            (6, 35, 0, 10), (6, 36, 24, 83), (6, 37, 107, 228),
            (6, 38, 335, 26), (6, 39, 361, 178), (6, 40, 539, 28),
            (6, 41, 567, 179), (6, 42, 746, 154), (6, 43, 900, 31),
            (6, 44, 931, 14), (6, 45, 945, 52), (6, 46, 1014, 10),
            (7, 47, 0, 10), (7, 48, 48, 245), (7, 49, 293, 148),
            (7, 50, 441, 442), (7, 51, 883, 105), (7, 52, 1014, 10),
            (8, 53, 0, 10), (8, 54, 10, 1004), (8, 55, 1014, 10)
        )
        return np.array(clusdef_tuple, dtype=clusdef_dtype)

    if nclus == 40:        # mostly limb & occultation measurements
        clusdef_tuple = (
            (1, 0, 0, 5), (1, 1, 5, 192), (1, 2, 197, 355),
            (1, 3, 552, 290), (1, 4, 842, 177), (1, 5, 1019, 5),
            (2, 6, 1019, 5), (2, 7, 948, 71), (2, 8, 170, 778),
            (2, 9, 76, 94), (2, 10, 5, 71), (2, 11, 0, 5),
            (3, 12, 0, 10), (3, 13, 10, 23), (3, 14, 33, 897),
            (3, 15, 930, 89), (3, 16, 1019, 5),
            (4, 17, 0, 5), (4, 18, 5, 5), (4, 19, 10, 909),
            (4, 20, 919, 100), (4, 21, 1019, 5),
            (5, 22, 0, 5), (5, 23, 5, 5), (5, 24, 10, 991),
            (5, 25, 1001, 18), (5, 26, 1019, 5),
            (6, 27, 0, 10), (6, 28, 10, 14), (6, 29, 24, 973),
            (6, 30, 997, 17), (6, 31, 1014, 10),
            (7, 32, 0, 10), (7, 33, 10, 38), (7, 34, 48, 940),
            (7, 35, 988, 26), (7, 36, 1014, 10),
            (8, 37, 0, 10), (8, 38, 10, 1004), (8, 39, 1014, 10)
        )
        return np.array(clusdef_tuple, dtype=clusdef_dtype)

    # only, used by dedicated measurements
    if nclus == 29:
        clusdef_tuple = (
            (1, 0, 0, 5), (1, 1, 5, 10), (1, 2, 216, 528),
            (1, 3, 744, 64), (1, 4, 1009, 10), (1, 5, 1019, 5),
            (2, 6, 1019, 5), (2, 7, 190, 739), (2, 8, 94, 96),
            (2, 9, 5, 10), (2, 10, 0, 5),
            (3, 11, 0, 5), (3, 12, 46, 930), (3, 13, 1019, 5),
            (4, 14, 0, 5), (4, 15, 46, 931), (4, 16, 1019, 5),
            (5, 17, 0, 5), (5, 18, 54, 914), (5, 19, 1019, 5),
            (6, 20, 0, 10), (6, 21, 45, 933), (6, 22, 1014, 10),
            (7, 23, 0, 10), (7, 24, 73, 877), (7, 25, 1014, 10),
            (8, 26, 0, 10), (8, 27, 73, 878), (8, 28, 1014, 10)
        )
        return np.array(clusdef_tuple, dtype=clusdef_dtype)

    # only, used by dedicated measurements
    if nclus == 10:
        clusdef_tuple = (
            (1, 0, 0, 552), (1, 1, 552, 472),
            (2, 2, 170, 854), (2, 3, 0, 170),
            (3, 4, 0, 1024),
            (4, 5, 0, 1024),
            (5, 6, 0, 1024),
            (6, 7, 0, 1024),
            (7, 8, 0, 1024),
            (8, 9, 0, 1024)
        )
        return np.array(clusdef_tuple, dtype=clusdef_dtype)

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
    import h5py

    with h5py.File(db_name, 'r') as fid:
        grp = fid["State_{:02d}".format(state_id)]
        state_conf = grp['state_conf'][orbit]
        if state_conf['nclus'] > 0:
            return state_conf

    return None


def state_conf_data(det_mds):
    """
    Derive state configuration from level 0 measurements

    Parameters
    ----------
    * det_mds  :  SCIA level 0 detector DSRs of one state execution

    Returns
    -------
    state configuration
    """
    from .hk import get_det_vis_pet, get_det_ir_pet

    mtbl_dtype = np.dtype([
        ('type_clus', 'u1'),
        ('num_clus', 'u1'),
        ('duration', 'u2'),
        ('num_info', 'u2'),
    ])

    clus_dtype = np.dtype([
        ('id', 'u1'),              # 1 <= id <= 64
        ('channel', 'u1'),         # 1 <= channel <= 8
        ('coaddf', 'u1'),
        ('type', 'u1'),            # coaddf == 1 ? 1 : 2
        ('start', 'u2'),           # 0 <= start < 1023
        ('length', 'u2'),          # 0 <= length <= 1024
        ('intg', 'u2'),            # int(16 * coaddf * pet)
        ('n_read', 'u2'),
        ('pet', 'f4')
    ])

    bcps = 0
    clus_list = []
    for det in det_mds:
        first = True
        num_chan = det['pmtc_hdr']['num_chan']
        for chan in det['chan_data'][:num_chan]:
            chan_id = chan['hdr']['id_is_lu'] >> 4
            if first:
                bcps += chan['hdr']['bcps']
                first = False
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
                clus_list.append((chan_id, clus_id, start, length, coaddf, pet))

    # fill the output structure
    clus_set = sorted(set(clus_list), key=itemgetter(0, 2))
    clus_def = np.empty(len(clus_set), dtype=clus_dtype)
    for ni, clus in enumerate(clus_set):
        clus_def[ni]['id'] = ni + 1
        clus_def[ni]['channel'] = clus[0]
        clus_def[ni]['coaddf'] = clus[4]
        clus_def[ni]['type'] = min(2, clus[4])
        clus_def[ni]['start'] = clus[2]
        clus_def[ni]['length'] = clus[3]
        clus_def[ni]['intg'] = max(1, int(16 * clus[4] * clus[5]))
        clus_def[ni]['n_read'] = 0
        clus_def[ni]['pet'] = clus[5]

    # finally, add number of readouts
    for clus in clus_def:
        clus['n_read'] = clus_def['intg'].max() // clus['intg']
        # print(clus)

    mtbl = np.zeros(1, dtype=mtbl_dtype)
    mtbl['num_clus'] = len(clus_set)
    mtbl['duration'] = (det_mds[-1]['pmtc_hdr']['bcps'] // det_mds.size
                        * det_mds.size)
    mtbl['num_info'] = det_mds.size

    return mtbl, clus_def
