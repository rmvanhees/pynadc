#!/usr/bin/env python

# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

#
from __future__ import print_function
from __future__ import division

from pynadc.tropomi import db

DB_NAME = '/nfs/TROPOMI/ical/share/db/sron_s5p_icm.db'


#- main code -------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument( '--dbname', type=str, default=DB_NAME, 
                         help='name of SQLite database' )
    parser.add_argument( '--query', type=str, default='location',
                         choices=['location', 'meta', 'content'],
                         help='query on product: location, meta-data, content' )
    parser.add_argument( '--debug', action='store_true', default=False,
                         help='show SQL query, but do nothing' )

    #-------------------------
    # define subparsers for queries on product name
    subparsers = parser.add_subparsers( title='subcommands',
                                        help='select on product name or type' )
    parser_name = subparsers.add_parser( 'name',
                                     help='perform selection on product name' )
    parser_name.add_argument( 'product', type=str,
                              help='name of entry to select (no path!)' )
    parser_name.set_defaults( func=db.get_product_by_name )
    
    #-------------------------
    # define subparsers for queries on product receive time
    parser_rtime = subparsers.add_parser( 'rtime',
                                help='perform selection on receive time' )
    parser_rtime.set_defaults( func=db.get_product_by_rtime )

    #-------------------------
    # define subparsers for queries on product date (range)
    parser_date = subparsers.add_parser( 'date',
                                help='perform selection on date of product' )
    parser_date.set_defaults( func=db.get_product_by_date )

    #-------------------------
    # define subparsers for queries on product orbit (range)
    parser_orbit = subparsers.add_parser( 'orbit',
                                help='perform selection on reference orbit' )
    parser_orbit.set_defaults( func=db.get_product_by_orbit )

    #-------------------------
    # define subparsers for queries on product type
    parser_type = subparsers.add_parser( 'type',
                                     help='perform selection on product type' )
    parser_type.add_argument( 'mclass', nargs='?', type=str, 
                              choices=['analys', 'analysis',
                                       'calib', 'calibration',
                                       'rad', 'radiance',
                                       'irrad', 'irradiance' ],
                              help='type of measurement class to select' )
    group_msm = parser_type.add_mutually_exclusive_group(required=True)
    group_msm.add_argument( '--msm_name', type=str,
                            help='full name of measurement to select' )
    group_msm.add_argument( '--msm_type', type=str,
                            help='general name of measurement to select' )
    group_msm.add_argument( '--msm_icid', type=int,
                            help='select measurement on ICID' )
    group_msm.add_argument( '--msm_texp', type=float,
                            help='select measurement(s) on exposure time' )
    parser_type.add_argument( '--msm_coadd', type=int,
                            help='select measurement(s) on co-adding factor' )
    parser_type.set_defaults( func=db.get_product_by_type )
    args = parser.parse_args()
    if args.debug:
        print( args )
    
    res = args.func( args, toScreen=True )
