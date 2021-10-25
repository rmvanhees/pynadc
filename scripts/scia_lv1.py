#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Read Sciamachy level 1b products in ENVISAT format

Copyright (c) 2016-2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path

from pynadc.scia import db, lv1


# - global parameters ------------------------------


# - local functions --------------------------------


# - main code --------------------------------------
def main():
    """
    main function of module 'scia_lv1'
    """
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description='read Sciamachy level 1b product'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--orbit', nargs=1, type=int,
                       help='select data from given orbit, preferably \'Y\'')
    group.add_argument('file', nargs='?', type=str,
                       help='read data from given file')
    parser.add_argument('--state', nargs='+', type=int,
                        help='must be the last argument on the command-line')
    args = parser.parse_args()

    scia_fl = ""
    if args.orbit is not None:
        file_list = db.get_product_by_type(prod_type='1',
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

    # create object and open Sciamachy level 1b product
    try:
        obj = lv1.File(scia_fl)
    except:
        print('exception occurred in module pynadc.scia.lv1')
        raise

    _ = obj.get_sqads()
    _ = obj.get_lads()

    _ = obj.get_sip()

    _ = obj.get_clcp()
    _ = obj.get_vlcp()

    _ = obj.get_base()
    _ = obj.get_scp()

    _ = obj.get_srs()

    _ = obj.get_pspn()
    _ = obj.get_pspl()
    _ = obj.get_pspo()

    _ = obj.get_rspl()
    _ = obj.get_rspo()
    _ = obj.get_ekd()

    _ = obj.get_sfp()
    _ = obj.get_asfp()

    _ = obj.get_states()

    mds_list = obj.get_mds(state_id=args.state)
    if mds_list:
        for key in mds_list[0][0].dtype.names:
            print(key, ' : ', mds_list[0][0][key])


if __name__ == '__main__':
    main()
