# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

"""
Methods to query the NADC S5p Tropomi SQLite database
"""

from __future__ import print_function
from __future__ import division

import socket
import os.path
import sqlite3

DB_NAME = '/nfs/TROPOMI/share/db/sron_s5p_icm.db'

#---------------------------------------------------------------------------
def get_product_by_name( args=None, dbname=DB_NAME, product=None, 
                         query='location', toScreen=False ):
    '''
    Query NADC GOSAT SQLite database on product name. The following options are implemented:
     'location': query the file location
     'meta':     query the file meta-data
     'content':  query the file content

    The result(s) of a query will always be returned by this function.

    The parameter "toScreen" controls if the query result is printed on STDOUT
    

    Input
    -----
    args     : argparse object with keys dbname, product, query
    dbname   : full path to S5p Tropomi SQLite database [default: DB_NAME]
    product  : name of product [value required]
    toScreen : print query result to standard output [default: False]
    '''
    if args:
        dbname   = args.dbname
        product  = args.product
        query    = args.query

    if not os.path.isfile( dbname ):
        print( '*** Fatal, can not find SQLite database: %s' % dbname )
        return []

    table = 'ICM_SIR_META'
    if query == 'location':
        query_str = 'select pathID,name from {} where name=\'{}\''.format(table, product)
        conn = sqlite3.connect( dbname )
        cu = conn.cursor()
        cu.execute( query_str )
        row = cu.fetchone()
        if row is None:
            cu.close()
            conn.close()
            return []

        ## obtain root directories (local or NFS)
        case_str = 'case when hostName == \'{}\''\
            ' then localPath else nfsPath end'.format(socket.gethostname())
        query_str = \
            'select {} from ICM_SIR_LOCATION where pathID={}'.format(case_str, row[0])
        cu.execute( query_str )
        root = cu.fetchone()
        cu.close()
        conn.close()

        if toScreen:
            print( os.path.join(root[0], row[1]) )

        return os.path.join(root[0], row[1])
    elif query == 'meta' or query == 'content':
        query_str = 'select * from {} where name=\'{}\''.format(table, product)
        conn = sqlite3.connect( dbname )
        conn.row_factory = sqlite3.Row
        cu = conn.cursor()
        cu.execute( query_str )
        row = cu.fetchone()
        cu.close()
        conn.close()
        if row is None:
            return []

        metaID = row.__getitem__('metaID')
        if toScreen:
            for key_name in row.keys():
                print( key_name, '\t', row[key_name] )

        if query == 'content':
            conn = sqlite3.connect( dbname )
            cu = conn.cursor()

            table = 'ICM_SIR_ANALYSIS'
            col = 'name, svn_revision, scanline'
            query_str = 'select {} from {} where metaID={}'.format(col, table, metaID)
            cu.execute( query_str )
            rows = cu.fetchall()
            if toScreen:
                print( '---------- {} ----------'.format(table) )
                for row in rows:
                    print( row )
            
            table = 'ICM_SIR_CALIBRATION'
            col = 'name, dateTimeStart, ic_id, ic_version, scanline'
            query_str = 'select {} from {} where metaID={}'.format(col, table, metaID)
            cu.execute( query_str )
            rows = cu.fetchall()
            if toScreen:
                print( '---------- {} ----------'.format(table) )
                for row in rows:
                    print( row )

            table = 'ICM_SIR_IRRADIANCE'
            col = 'name, dateTimeStart, ic_id, ic_version, scanline'
            query_str = 'select {} from {} where metaID={}'.format(col, table, metaID)
            cu.execute( query_str )
            rows = cu.fetchall()
            if toScreen:
                print( '---------- {} ----------'.format(table) )
                for row in rows:
                    print( row )

            table = 'ICM_SIR_RADIANCE'
            col = 'name, dateTimeStart, ic_id, ic_version, scanline'
            query_str = 'select {} from {} where metaID={}'.format(col, table, metaID)
            cu.execute( query_str )
            rows = cu.fetchall()
            if toScreen:
                print( '---------- {} ----------'.format(table) )
                for row in rows:
                    print( row )

            cu.close()
            conn.close()

        return row
    else:
        print( '*** Fatal, query option not recognized: %s' % query )
        return []

#---------------------------------------------------------------------------
def get_product_by_type( args=None, dbname=DB_NAME, prod_type=None, 
                         proc_stage=None, proc_best=None,
                         orbits=None, date=None, rtime=None, 
                         toScreen=False, dump=False, debug=False ):
    '''
    Query NADC Sciamachy SQLite database on product type with data selections
    Input
    -----
    args       : dictionary with keys dbname, type, proc, best, orbit, date, 
                 rtime, toScreen, dump, debug
    dbname     : full path to Sciamachy SQLite database [default: DB_NAME]
    prod_type  : level of product, available 0, 1, 2 [value required]
    prod_stage ; baseline of product (PROC_STAGE): N, R, P, R, U, W, ...
                 [default: None]
    prod_best  ; select highest available baseline [default: None]
    orbit      : select on absolute orbit number [default: None]
    date       : select on dateTimeStart [default: None]
    rtime      : select on receiveTime [default: None]
    toScreen   : print query result to standard output [default: False]
    debug      : do not query data base, but display SQL query [default: False]

    Output
    ------
    return full-path to selected products [default] 

    '''
    if args:
        dbname = args.dbname
        prod_type = args.type
        proc_stage = args.proc
        proc_best = args.best
        orbits = args.orbit
        date = args.date
        rtime = args.rtime
        dump = args.dump
        debug = args.debug

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: %s' % dbname )
        return []
