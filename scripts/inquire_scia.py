#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Perform pre-defined queries on the SQLite database of a Sciamachy archive

Copyright (c) 2016 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
import argparse

from pynadc.scia import db

DB_NAME = '/SCIA/share/db/sron_scia.db'


# - local functions --------------------------------------------
def scia_orbit_range(string) -> list:
    """
    Generate list from comma separated string
    """
    res = [int(str) for str in string.split(',')]
    if len(res) > 2:
        msg = '%r is not a orbit number or range' % string
        raise argparse.ArgumentTypeError(msg)
    if len(res) == 2 and res[0] > res[1]:
        res.sort()
    return res


# - main code --------------------------------------------------
def main() -> None:
    """
    main function which parses the commandline parameters
    and calls a query function
    """
    proc_opts = ['B', 'N', 'O', 'P', 'R', 'S', 'U', 'W', 'Y']
    rtime_opts = (['{}h'.format(x) for x in range(1, 24)]
                  + ['{}d'.format(x) for x in range(1, 8)])

    parser = argparse.ArgumentParser()
    parser.add_argument('--dbname', type=str, default=DB_NAME,
                        help='name of SQLite database')
    parser.add_argument('--dump', action='store_true', default=False,
                        help='return database dump instead pathFilename')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='show SQL query, but do nothing')

    # define subparsers for queries on product name
    subparsers = parser.add_subparsers(title='subcommands',
                                       help='select on product name or type')
    parser_name = subparsers.add_parser('name',
                                        help='select on product name')
    parser_name.add_argument('product', type=str,
                             help='name of entry to select (no path!)')
    parser_name.set_defaults(func=db.get_product_by_name)

    # define subparsers for queries on product type
    parser_type = subparsers.add_parser('type',
                                        help='options to select product type')
    parser_type.add_argument('type', nargs='?', choices=['0', '1', '2'],
                             help='type of product to select')
    parser_type.add_argument('--orbit', type=scia_orbit_range,
                             help='select entries on orbit number or range')
    parser_type.add_argument('--best', action='store_true', default=False,
                             help='select latest consolidated products')
    parser_type.add_argument('--proc', nargs='+', choices=proc_opts,
                             help='select entries on ESA processor ID')
    parser_type.add_argument('--rtime', type=str, choices=rtime_opts,
                             help='select entries on receive time: xh or xd')
    parser_type.add_argument('--date', type=str,
                             help="""
        select entries on start time of science data;
        [yyyy]: returns entries between yyyy and (yyyy+1);
        [yyyymm]: returns entries between yyyymm and yyyy(mm+1);
        [yyyymmdd]: returns entries between yyyymmdd and yyyymm(dd+1);
        [yyyymmddhh]: returns entries between yyyymmddhh and yyyymmdd(hh+1);
        [yyyymmddhhmm]: returns entries between yyyymmddhh and yyyymmddhh(mm+1)
        """)
    parser_type.set_defaults(func=db.get_product_by_type)
    args = parser.parse_args()
    if 'func' not in args:
        parser.print_help()
    else:
        _ = args.func(args, to_screen=True)


if __name__ == '__main__':
    main()
