#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

.. ADD DESCRIPTION ..

Copyright (c) 2016 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  Standard 3-clause BSD
"""
from pathlib import Path

# - global parameters ------------------------------


# - local functions --------------------------------


# - main code --------------------------------------
def main():
    """
    main function of module 'scia_lv0'
    """
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    from pynadc.scia import db, lv0

    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description='read Sciamachy level 1b product'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--orbit', nargs=1, type=int,
                       help='select data from given orbit, preferably \'W\'')
    group.add_argument('--file', type=str, help='read data from given file')
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
        obj = lv0.File(scia_fl)
    except:
        print('exception occurred in module pynadc.scia.lv0')
        raise

    obj.get_mds()


if __name__ == '__main__':
    main()
