'''
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Methods to query the NADC GOSAT SQLite database

Copyright (c) 2016 SRON - Netherlands Institute for Space Research 
   All Rights Reserved

License:  Standard 3-clause BSD

'''
from __future__ import print_function
from __future__ import division

import socket
import os.path
import sqlite3

DB_NAME = '/GOSAT/share/db/sron_gosat.db'

# define function to construct date sub-directories from FTS product-name
date_subdir = lambda n: os.path.join(n[9:13], n[13:15], n[15:17])

#---------------------------------------------------------------------------
def get_product_by_name( args=None, dbname=DB_NAME, product=None, 
                         toScreen=False, dump=False, debug=False ):
    '''
    Query NADC GOSAT SQLite database on product name

    Input
    -----
    args     : dictionary with keys dbname, product, toScreen, dump, debug
    dbname   : full path to GOSAT SQLite database [default: DB_NAME]
    product  : name of product [value required]
    toScreen : print query result to standard output [default: False]
    dump     : return database content about product, instead of full-path
    debug    : do not query data base, but display SQL query [default: False]

    Output
    ------
    return full-path to product [default] or show database content about product

    '''
    if args:
        dbname = args.dbname
        product = args.product
        dump = args.dump
        debug = args.debug

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: %s' % dbname )
        return []

    level_1X = False
    if product[0:9] == 'GOSATTCAI':
        table = 'tcai__2P'
        select_str = 'pathID. name'
    else:
        table = 'tfts__1P'
        if product.find('_1X') > 0:
            level_1X = True
        select_str = 'pathID, observationMode, productVersion, name'
    if dump:
        select_str = '*'

    query_str = \
        'select {} from {} where name=\'{}\''.format(select_str, table, product)

    ## perform query on database
    conn = sqlite3.connect( dbname )
    if dump:
        conn.row_factory = sqlite3.Row
    cu = conn.cursor()
    if debug:
        print( query_str )
        row = (1, product)
    else:
        cu.execute( query_str )
        row = cu.fetchone()
        if row is None: 
            conn.close()
            return []

    ## obtain root directories (local or NFS)
    if not dump:
        case_str = 'case when hostName == \'{}\''\
            ' then localPath else nfsPath end'.format(socket.gethostname())
        query_str = \
            'select {} from rootPaths where pathID={}'.format(case_str, row[0])
        if debug:
            print( query_str )
            root = '/fakePath/toProduct'
        else:
            cu.execute( query_str )
            root = cu.fetchone()[0]
    conn.close()

    if dump:
        if toScreen:
            for name in row.keys():
                print( name, '\t', row[name] )
        return row
    else:
        if len(row) == 2:
            full_path = os.path.join(root, row[1])
        else:                    ## should check for len(row) equals 4
            if level_1X:
                full_path = os.path.join( root, row[1] + '_1X', row[2],
                                          date_subdir(row[3]), row[3] )
            else:
                full_path = os.path.join( root, row[1], row[2],
                                          date_subdir(row[3]), row[3] )
        if toScreen:
            print( full_path )
        return full_path

#---------------------------------------------------------------------------
def get_product_by_type( args=None, dbname=DB_NAME, prod_type=None, 
                         date=None, rtime=None,
                         obs_mode=None, prod_version=None,
                         toScreen=False, debug=False ):
    '''
    Query NADC GOSAT SQLite database on product type with data selections

    Input
    -----
    args      : dictionary with keys dbname, type, date, rtime, obs_mode, 
                prod_version, toScreen, dump, debug
    dbname    : full path to GOSAT SQLite database [default: DB_NAME]
    prod_type : type of product, supported TFTS_1 and TCAI_2 [value required]
    date      : select on dateTimeStart [default: None]
    rtime     : select on receiveTime [default: None]
    obs_mode  : (FTS only) select on observationMode: OB1D, OB1N, SPOD, SPON
                [default: None]
    prod_version : (FTS only) select on product version: 
                algorithmVersion + parameterVersion [default: None]
    toScreen  : print query result to standard output [default: False]
    debug     : do not query data base, but display SQL query [default: False]

    Output
    ------
    return full-path to selected products [default] 

    '''
    if args:
        dbname = args.dbname
        prod_type = args.type
        obs_mode = args.obs_mode
        prod_version = args.prod_version
        date = args.date
        rtime = args.rtime
        debug = args.debug

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: %s' % dbname )
        return []

    if prod_type.upper() == 'TFTS_1':
        table = 'tfts__1P'
        select_str = 'pathID, observationMode, productVersion, name'
    else:
        table = 'tcai__2P'
        select_str = 'pathID, name'

    ## define query on meta-Table
    query_str = ['select {} from {}'.format(select_str, table)]

    ## define query to obtain root directories (local or NFS)
    case_str = 'case when hostName == \'{}\''\
               ' then localPath else nfsPath end'.format(socket.gethostname())
    query_str2 = \
                'select {} from rootPaths where pathID={}'

    ## perform selection on datetime
    if date:
        if len(query_str) == 1:
            query_str.append(' where')
        else:
            query_str.append(' and')

        year = int(date[0:4])
        dtime = '+1 year'
        if len( date ) >= 6:
            month = int(date[4:6])
            dtime = '+1 month'
        else:
            month = 1
        if len( date ) >= 8:
            day = int(date[6:8])
            dtime = '+1 day'
        else:
            day = 1
        if len( date ) >= 10:
            hour = int(date[8:10])
            dtime = '+1 hour'
        else:
            hour = 0
        if len( date ) >= 12:
            minu = int(date[10:12])
            dtime = '+1 minute'
        else:
            minu = 0
        d1 = '%04d-%02d-%02d %02d:%02d:%02d' % (year,month,day,hour,minu,0)

        mystr = ' dateTimeStart between \'%s\' and datetime(\'%s\',\'%s\')'
        query_str.append(mystr % (d1, d1, dtime))

    if rtime:
        if len(query_str) == 1:
            query_str.append(' where')
        else:
            query_str.append(' and')

        mystr = ' receiveDate between datetime(\'now\',\'-%-d %s\')' \
            + ' and datetime(\'now\')'
        if rtime[-1] == 'h':
            query_str.append(mystr % (int(rtime[0:-1]), 'hour'))
        else:
            query_str.append(mystr % (int(rtime[0:-1]), 'day'))

    if prod_type.upper() == 'TFTS_1':
        if obs_mode: 
            if len(query_str) == 1:
                query_str.append(' where observationMode == \'%s\'' % obs_mode )
            else:
                query_str.append(' and observationMode == \'%s\'' % obs_mode )

        if prod_version:
            if len(query_str) == 1:
                query_str.append(' where productVersion == \'%s\'' % prod_version )
            else:
                query_str.append(' and productVersion == \'%s\'' % prod_version)

    if debug:
        print( ''.join(query_str) )
        print( query_str2.format(case_str, 1) )
        return []

    conn = sqlite3.connect( dbname )
    conn.row_factory = sqlite3.Row
    cu  = conn.cursor()
    cuu = conn.cursor()
    cu.execute( ''.join(query_str) )

    rowList = []
    for row in cu:
        cuu.execute( query_str2.format(case_str, row[0]) )
        root = cuu.fetchone()[0]

        if len(row) == 2:
            full_path = os.path.join(root, row[1])
        else:   ## should check for len(row) equals 4
            if row[3].find('_1X') > 0:
                full_path = os.path.join( root, row[1] + '_1X', row[2],
                                          date_subdir(row[3]), row[3] )
            else:
                full_path = os.path.join( root, row[1], row[2],
                                          date_subdir(row[3]), row[3] )
        if toScreen:
            print( full_path )

        rowList.append( full_path )
                    
    conn.close()
    return rowList
