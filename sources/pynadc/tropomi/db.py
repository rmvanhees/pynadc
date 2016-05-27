# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

"""
Methods to query the NADC S5p-Tropomi ICM-database (sqlite)
"""

from __future__ import print_function
from __future__ import division

import socket
import os.path
import sqlite3

DB_NAME = '/nfs/TROPOMI/ical/share/db/sron_s5p_icm.db'

#---------------------------------------------------------------------------
def get_product_by_name( args=None, dbname=DB_NAME, product=None, 
                         query='location', toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on product-name. 
    The following queries are implemented:
     'location': query the file location
     'meta':     query the file meta-data
     'content':  query the file content

    The result(s) of a query will always be returned by this function.

    The parameter "toScreen" controls if the query result is printed on STDOUT
    
    Input
    -----
    - args     : argparse object with keys dbname, product, query
    - dbname   : full path to S5p Tropomi SQLite database [default: DB_NAME]
    - product  : name of product [value required]
    - query    : location, meta or content [default: location]
    - toScreen : print query result to standard output [default: False]

    '''
    if args:
        dbname   = args.dbname
        product  = args.product
        query    = args.query

    if product is None:
        print( '*** Fatal, a product-name has to be provided' )
        return []

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
def get_product_by_orbit( args=None, dbname=DB_NAME, orbit=None,
                          query='location', toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on reference orbit. 
    The following queries are implemented:
     'location': query the file location
     'meta':     query the file meta-data
     'content':  query the file content

    The result(s) of a query will always be returned by this function.

    The parameter "toScreen" controls if the query result is printed on STDOUT
    
    Input
    -----
    - args     : argparse object with keys dbname, product, query
    - dbname   : full path to S5p Tropomi SQLite database [default: DB_NAME]
    - orbit    : single value or range [value required]
    - query    : location, meta or content [default: location]
    - toScreen : print query result to standard output [default: False]

    '''
    if orbit is None:
        print( '*** Fatal, no reference orbit provided' )
        return []

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: {}'.format(dbname) )
        return []

#---------------------------------------------------------------------------
def get_product_by_date( args=None, dbname=DB_NAME, startdate=None,
                         query='location', toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on start date of measurements. 
    The following queries are implemented:
     'location': query the file location
     'meta':     query the file meta-data
     'content':  query the file content

    The result(s) of a query will always be returned by this function.

    The parameter "toScreen" controls if the query result is printed on STDOUT
    
    Input
    -----
    - args      : argparse object with keys dbname, product, query
    - dbname    : full path to S5p Tropomi SQLite database [default: DB_NAME]
    - startdate : start date-time of first measurement or range [value required]
    - query     : location, meta or content [default: location]
    - toScreen  : print query result to standard output [default: False]

    '''
    if startdate is None:
        print( '*** Fatal, no measurement start date provided' )
        return []

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: {}'.format(dbname) )
        return []

#---------------------------------------------------------------------------
def get_product_by_rtime( args=None, dbname=DB_NAME, rtime=None,
                          query='location', toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on reference orbit. 
    The following queries are implemented:
     'location': query the file location
     'meta':     query the file meta-data
     'content':  query the file content

    The result(s) of a query will always be returned by this function.

    The parameter "toScreen" controls if the query result is printed on STDOUT
    
    Input
    -----
    - args     : argparse object with keys dbname, product, query
    - dbname   : full path to S5p Tropomi SQLite database [default: DB_NAME]
    - rtime    : receive time (or interval) of products  [value required]
    - query    : location, meta or content [default: location]
    - toScreen : print query result to standard output [default: False]

    '''
    if rtime is None:
        print( '*** Fatal, no receive time provided' )
        return []

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: {}'.format(dbname) )
        return []

#---------------------------------------------------------------------------
def get_product_by_type( args=None, dbname=DB_NAME, msm_class=None, 
                         msm_name=None, msm_type=None, msm_icid=None,
                         msm_texp=None, msm_coadd=None,
                         orbits=None, data=None, rtime=None,
                         query='location', toScreen=False ):
    '''
    Query NADC Sciamachy SQLite database on product type with data selections
    Input
    -----
    args       : dictionary with keys dbname, ...
    dbname     : full path to Tropomi SQLite database [default: DB_NAME]
    msm_class  : 
    orbit      : select on absolute orbit number [default: None]
    date       : select on dateTimeStart [default: None]
    rtime      : select on receiveTime [default: None]
    toScreen   : print query result to standard output [default: False]
    debug      : do not query data base, but display SQL query [default: False]

    Output
    ------
    return full-path to selected products [default] 

    '''
    rows = []
    if args:
        dbname    = args.dbname
        msm_class = args.mclass
        msm_name  = args.msm_name
        msm_type  = args.msm_type
        msm_icid  = args.msm_icid 
        msm_texp  = args.msm_texp
        msm_coadd = args.msm_coadd
        #orbits    = args.orbit
        #date      = args.date
        #rtime     = args.rtime
        query     = args.query

    if msm_class is None:
        print( '*** Fatal, no measurement class provided' )
        return []

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: {}'.format(dbname) )
        return []

    meta_tbl = 'ICM_SIR_META'
    if msm_class[0:6] == 'analys':
        class_tbl = 'ICM_SIR_ANALYSIS'
    elif msm_class[0:5] == 'calib':
        class_tbl = 'ICM_SIR_CALIBRATION'
    elif msm_class[0:5] == 'irrad':
        class_tbl = 'ICM_SIR_IRRADIANCE'
    elif msm_class[0:3] == 'rad':
        class_tbl = 'ICM_SIR_RADIANCE'
    else:
        print( 'Fatal, unknown measurement class {}'.format(msm_class) )
        return []

    ## obtain root directories (local or NFS)
    case_str = 'case when hostName == \'{}\''\
               ' then localPath else nfsPath end'.format(socket.gethostname())

    if msm_name is not None:
        subquery_str = 'select distinct metaID from {} where name like \'{}\''
        query_str = 'select pathID, name from {} where metaID in ({})'
        q_str = query_str.format(meta_tbl,
                                 subquery_str.format(class_tbl, msm_name))
        print( q_str )
        conn = sqlite3.connect( dbname )
        cu  = conn.cursor()
        cuu = conn.cursor()
        cu.execute( q_str )

        rowList = []
        for row in cu:
            qq_str = 'select {} from ICM_SIR_LOCATION where pathID={}'.format(case_str, row[0])
            cuu.execute( qq_str )
            root = cuu.fetchone()
            
            if toScreen:
                print( os.path.join(root[0], row[1]) )

    if msm_type is not None:
        subquery_str = 'select distinct metaID from {} where name like \'{}_%\''
        query_str = 'select name from {} where metaID in ({})'
        q_str = query_str.format(meta_tbl,
                                 subquery_str.format(class_tbl, msm_type))
        print( q_str )
        conn = sqlite3.connect( dbname )
        cu = conn.cursor()
        cu.execute( q_str )
        rows = cu.fetchall()
        if toScreen:
            for row in rows:
                print( row[0] )
        
    if msm_icid is not None:
        subquery_str = 'select distinct metaID from {} where ic_id = {}'
        query_str = 'select name from {} where metaID in ({})'
        q_str = query_str.format(meta_tbl,
                                 subquery_str.format(class_tbl, msm_icid))
        print( q_str )
        conn = sqlite3.connect( dbname )
        cu = conn.cursor()
        cu.execute( q_str )
        rows = cu.fetchall()
        if toScreen:
            for row in rows:
                print( row[0] )

    if msm_texp is not None:
        pass

    #if msm_coadd is not None:
    #    pass
    
    return rows
