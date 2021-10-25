#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Read Sciamachy level 0 products in ENVISAT format

Copyright (c) 2016-2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path

import numpy as np

from pynadc.scia import db, lv0


# - global parameters ------------------------------


# - local functions --------------------------------
def check_dsr_in_states(isp_in, verbose=False):
    """
    This module combines L0 ISP per state ID based on parameter icu_time.
    """
    # selects complete ISPs
    isp = isp_in[isp_in['fep_hdr']['_quality'] == 0]

    # combine L0 ISP on parameter icu_time
    # alternatively one could use parameter state_id
    _arr = isp['data_hdr']['icu_time']
    _arr = np.concatenate(([-1], _arr, [-1]))
    indx = np.where(np.diff(_arr) != 0)[0]
    num_dsr = np.diff(indx)
    icu_time = isp['data_hdr']['icu_time'][indx[:-1]]
    state_id = isp['data_hdr']['state_id'][indx[:-1]]
    if 'pmtc_frame' in isp.dtype.names:
        bcps = isp['pmtc_frame']['bcp']['bcps'][:, 0, 0].astype(int)
    elif 'pmd_data' in isp.dtype.names:
        bcps = isp['pmd_data']['bcps'][:, 0].astype(int)
    else:
        bcps = isp['pmtc_hdr']['bcps'].astype(int)

    if not verbose:
        return isp

    diff_bcps = None
    for ni in range(num_dsr.size):
        if ni+1 == num_dsr.size:
            continue

        diff_bcps = np.diff(bcps[indx[ni]:indx[ni+1]])

        if len(diff_bcps) == 1:
            print("# {:3d} state_{:02d} {:5d} {:4d}".format(
                ni, state_id[ni], indx[ni], num_dsr[ni]),
                  icu_time[ni])
        else:
            print("# {:3d} state_{:02d} {:5d} {:4d}".format(
                ni, state_id[ni], indx[ni], num_dsr[ni]),
                  icu_time[ni],
                  np.all(diff_bcps > 0),
                  np.unique(diff_bcps))

    return isp


# - main code --------------------------------------
def main():
    """
    main function of module 'scia_lv0'
    """
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description='read Sciamachy level 0 product'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--orbit', nargs=1, type=int,
                       help='select data from given orbit')
    group.add_argument('file', nargs='?', type=str,
                       help='read data from given file')
    parser.add_argument('--only_headers', action='store_true',
                        help='read only the product headers')
    parser.add_argument('--state', nargs='+', type=int,
                        help='must be the last argument on the command-line')
    args = parser.parse_args()

    scia_fl = ""
    if args.orbit is not None:
        file_list = db.get_product_by_type(prod_type='0',
                                           proc_best=True,
                                           orbits=args.orbit)
        if file_list and Path(file_list[0]).is_file():
            scia_fl = file_list[0]
    elif args.file is not None:
        if Path(args.file).is_file():
            scia_fl = args.file
        else:
            file_list = db.get_product_by_name(product=args.file)
            if file_list and Path(file_list[0]).is_file():
                scia_fl = file_list[0]

    if not scia_fl:
        print('Failed: file not found on your system')
        return

    print(scia_fl)
    # create object and open Sciamachy level 0 product
    try:
        fid = lv0.File(scia_fl, only_headers=args.only_headers)
    except:
        print('exception occurred in module pynadc.scia.lv0')
        raise

    # show the ASCII headers of the level 0 product and exit
    if args.only_headers:
        for key, value in fid.mph.items():
            print('MPH: ', key, value)
        for key, value in fid.sph.items():
            print('SPH: ', key, value)
        for ni, dsd_rec in enumerate(fid.dsd):
            for key in dsd_rec:
                print('DSD[{:02d}]: '.format(ni), key, dsd_rec[key])
        return

    # repair the info-records
    fid.repair_info()

    # read level 0 ISP
    (det_isp, aux_isp, pmd_isp) = fid.get_isp(state_id=args.state)
    check_dsr_in_states(det_isp, verbose=True)
    check_dsr_in_states(aux_isp, verbose=False)
    check_dsr_in_states(pmd_isp, verbose=False)


# --------------------------------------------------
if __name__ == '__main__':
    main()
