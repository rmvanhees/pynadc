#!/usr/bin/env python
'''
This file is part of pynadc

https://github.com/rmvanhees/pynadc

.. ADD DESCRIPTION ..

Copyright (c) 2016 SRON - Netherlands Institute for Space Research 
   All Rights Reserved

License:  Standard 3-clause BSD

'''
from __future__ import print_function
from __future__ import division

from pynadc.tropomi import db

DB_NAME = '/nfs/TROPOMI/ical/share/db/sron_s5p_icm_patched.db'

def __orbit_range__(string):
    res = [int(str) for str in string.split(',')]
    res.sort()

    if len(res) > 2:
        msg = '%r is not a orbit number or range' % string
        raise argparse.ArgumentTypeError(msg)
    
    return res

#- main code -------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='''\
        Parameter with special syntax definitions:
        * orbit : orbit or orbit-range as one or two integers (komma-seperated)
        * rtime : receive time is defined w.r.t. 'now', recognized values are:
           '1h','2h','3h','4h','5h','6h','7h','8h','9h','10h',
           '11h','12h','13h','14h','15h','16h','17h','18h','19h','20h',
           '21h','22h','23h','1d','2d','3d','4d','5d','6d','7d'.
        * date  : date-intervals can be defined as follows
           YYMM        : [YYMM, YY(MM+1)]
           YYMMDD      : [YYMMDD, YYMM(DD+1)]
           YYMMDDhh    : [YYMMDDhh, YYMMDD(hh+1)]
           YYMMDDhhmm  : [YYMMDDhhmm, YYMMDDhh(mm+1)]
           date1,date2   : date1 and date2 in ISO-format (komma-seperated).
        ''')
    parser.add_argument( '--dbname', type=str, default=DB_NAME, 
                         help='name of SQLite database' )
    parser.add_argument( '--mode', type=str, default='location',
                         choices=['location', 'meta', 'content'],
                         help='query on product: location, meta-data, content' )
    parser.add_argument( '--debug', action='store_true', default=False,
                         help='show SQL query, but do nothing' )
    subparsers = parser.add_subparsers( title='subcommands' )

    #-------------------------
    # define subparser for queries on product date (range)
    parser_date = subparsers.add_parser( 'date',
                                help='select ICM products on time-coverage' )
    parser_date.add_argument( 'date', type=str, default=None,
                              help='select on dateTimeStart of measurements')
    parser_date.set_defaults( func=db.get_product_by_date )

    #-------------------------
    # define subparser for queries on product name
    parser_name = subparsers.add_parser( 'name',
                                        help='select ICM products on its name' )
    parser_name.add_argument( 'product', type=str,
                              help='name of entry to select (no path!)' )
    parser_name.set_defaults( func=db.get_product_by_name )
    
    #-------------------------
    # define subparser for queries on product orbit (range)
    parser_orbit = subparsers.add_parser( 'orbit',
                                help='select ICM products on reference orbit' )
    parser_orbit.add_argument( 'orbit', type=__orbit_range__,
                               help='select measurement on orbit (range)' )
    parser_orbit.set_defaults( func=db.get_product_by_orbit )

    #-------------------------
    # define subparser for queries on product receive time
    parser_rtime = subparsers.add_parser( 'rtime',
                                help='select ICM products on receive time' )
    rtime_opts = ('1h','2h','3h','4h','5h','6h','7h','8h','9h','10h',
                  '11h','12h','13h','14h','15h','16h','17h','18h','19h',
                  '20h','21h','22h','23h','1d','2d','3d','4d','5d','6d','7d')
    parser_rtime.add_argument( 'rtime', type=str, choices=rtime_opts,
                              help='select on receive time of products' )
    parser_rtime.set_defaults( func=db.get_product_by_rtime )

    #-------------------------
    # define subparser for queries on measurement type or dynamic CKD
    parser_icid = subparsers.add_parser( 'icid',
                                         help='select ICM products on ICID' )
    parser_icid.add_argument( 'icid', type=str, default=None,
                              help='ICIDs, comma separated' )
    parser_icid.add_argument( '--after_dn2v', action='store_true',
                              default=False,
                              help='select dataset(s) calibrated upto DN2V' )
    parser_icid.add_argument( '--date', type=str, default=None,
                              help='select on dateTimeStart of measurements' )
    parser_icid.add_argument( '--orbit', type=__orbit_range__,
                              help='select measurement on orbit (range)' )
    parser_icid.set_defaults( func=db.get_product_by_icid )
    
    #-------------------------
    # define subparser for queries on measurement type or dynamic CKD
    parser_type = subparsers.add_parser( 'type',
                            help='select ICM products on HDF5 dataset name' )
    parser_type.add_argument( 'dataset', type=str, default=None,
                              help='name of measurement/CKD type' )
    parser_type.add_argument( '--after_dn2v', action='store_true',
                              default=False,
                              help='select dataset(s) calibrated upto DN2V' )
    parser_type.add_argument( '--date', type=str, default=None,
                              help='select on dateTimeStart of measurements' )
    parser_type.add_argument( '--orbit', type=__orbit_range__,
                              help='select measurement on orbit (range)' )
    parser_type.set_defaults( func=db.get_product_by_type )
    
    #-------------------------
    # define subparser for queries ICID table
    parser_tbl_icid = subparsers.add_parser( 'tbl_icid',
                                             help='show instrument settings' )
    parser_tbl_icid.add_argument( 'icid', nargs='?', type=int, default=None,
                              help='show info of given ic_id' )
    parser_tbl_icid.add_argument( '--check', action='store_true', default=False,
                              help='provide info to check ic_id parameters' )
    parser_tbl_icid.set_defaults( func=db.get_instrument_settings )

    args = parser.parse_args()
    if args.debug:
        print( args )
    
    res = args.func( args, toScreen=True )
