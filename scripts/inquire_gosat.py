#!/usr/bin/env python
#
from __future__ import print_function
from __future__ import division

from pynadc.gosat import db

DB_NAME = '/GOSAT/share/db/sron_gosat.db'

def scia_orbit_range(string):
    res = [int(str) for str in string.split(',')]
    if len(res) > 2:
        msg = '%r is not a orbit number or range' % string
        raise argparse.ArgumentTypeError(msg)
    if len(res) == 2 and res[0] > res[1]: res.sort()
    return res
    
#- main code -------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument( '--dbname', type=str, default=DB_NAME, 
                         help='name of SQLite database' )
    parser.add_argument( '--dump', action='store_true', default=False,
                         help='return database dump instead pathFilename' )
    parser.add_argument( '--debug', action='store_true', default=False,
                         help='show SQL query, but do nothing' )

    # define subparsers for queries on product name
    subparsers = parser.add_subparsers( title='subcommands',
                                        help='select on product name or type' )
    parser_name = subparsers.add_parser( 'name',
                                     help='perform selection on product name' )
    parser_name.add_argument( 'product', type=str,
                              help='name of entry to select (no path!)' )
    parser_name.set_defaults( func=db.get_product_by_name )
    
    # define subparsers for queries on product type
    parser_type = subparsers.add_parser( 'type',
                                     help='perform selection on product type' )
    parser_type.add_argument( 'type', nargs='?', type=str, 
                              choices=['TCAI_2', 'TFTS_1', 'tcai_2', 'tfts_1'],
                              help='type of product to select' )
    obs_opts = ('OB1D', 'OB1N', 'SPOD', 'SPON' )
    parser_type.add_argument( '--obs_mode', type=str, choices=obs_opts,
                              help='select FTS entries on observation mode' )
    parser_type.add_argument( '--prod_version', type=str,
                              help='select FTS entries on product version' )
    parser_type.add_argument( '--date', type=str,
                         help='''select entries on start time of science data;
         [yyyy]: returns entries between yyyy and (yyyy+1);
         [yyyymm]: returns entries between yyyymm and yyyy(mm+1);
         [yyyymmdd]: returns entries between yyyymmdd and yyyymm(dd+1);
         [yyyymmddhh]: returns entries between yyyymmddhh and yyyymmdd(hh+1);
         [yyyymmddhhmm]: returns entries between yyyymmddhh and yyyymmddhh(mm+1)
         ''')
    rtime_opts = ('1h','2h','3h','4h','5h','6h','7h','8h','9h','10h',
                  '11h','12h','13h','14h','15h','16h','17h','18h','19h','20h',
                  '21h','22h','23h','1d','2d','3d','4d','5d','6d','7d')
    parser_type.add_argument( '--rtime', dest='rtime', type=str,
                              choices=rtime_opts,
                              help='select entries on receive time: xh or xd' )
    parser_type.set_defaults( func=db.get_product_by_type )
    args = parser.parse_args()
    
    res = args.func( args, toScreen=True )
