# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to query the NADC S5p-Tropomi ICM-database (sqlite)
'''

from __future__ import print_function
from __future__ import division

import socket
import os.path
import sqlite3

DB_NAME = '/nfs/TROPOMI/ical/share/db/sron_s5p_icm.db'

#---------------------------------------------------------------------------
def get_product_by_name( args=None, product=None, dbname=DB_NAME, 
                         mode='location', toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on product-name. 

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, product, query
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "product"  : name of product
                   [value required]
    - "mode"     : defines the returned information:
        'location' :  query the file location
        'meta'     :  query the file meta-data
        'content'  :  query the file content
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    Output
    ------
    [mode='location'], a tuple with:
      file_path  :  path to ICM_SIR product
      file_name  :  name of ICM_SIR product

    [mode='meta'], a dictionary with metaTable content

    [mode='content'], a tuple with dictionaries of available datasets
    '''
    if args:
        dbname   = args.dbname
        product  = args.product
        mode     = args.mode

    assert product, \
        '*** Fatal, no product-name provided'

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)

    table = 'ICM_SIR_META'
    if mode == 'location':
        query_str = 'select pathID,name from {} where name=\'{}\''.format(table, product)
        conn = sqlite3.connect( dbname )
        conn.row_factory = sqlite3.Row
        cu = conn.cursor()
        cu.execute( query_str )
        row = cu.fetchone()
        if row is None:
            cu.close()
            conn.close()
            return ()

        ## obtain root directories (local or NFS)
        case_str = 'case when hostName == \'{}\''\
            ' then localPath else nfsPath end'.format(socket.gethostname())
        query_str = 'select {} from ICM_SIR_LOCATION where pathID={}'.format(case_str, row[0])
        cu.execute( query_str )
        root = cu.fetchone()
        cu.close()
        conn.close()

        if toScreen:
            print( os.path.join(root[0], row[1]) )

        return (root[0], row[1])
    elif mode == 'meta' or mode == 'content':
        query_str = 'select * from {} where name=\'{}\''.format(table, product)
        conn = sqlite3.connect( dbname )
        conn.row_factory = sqlite3.Row
        cu = conn.cursor()
        cu.execute( query_str )
        row = cu.fetchone()
        cu.close()
        conn.close()
        if row is None:
            return ()

        if mode == 'meta':
            row_list = {}
            for key_name in row.keys():
                row_list[key_name] = row[key_name]
                if toScreen:
                    print( key_name, '\t', row[key_name] )
            return row_list

        ## remainder only when mode == 'content'
        row_list = ()
        metaID = row.__getitem__('metaID')

        conn = sqlite3.connect( dbname )
        conn.row_factory = sqlite3.Row
        cu = conn.cursor()

        table = 'ICM_SIR_ANALYSIS'
        if toScreen:
            print( '#---------- {} ----------'.format(table) )
        columns = 'name, svn_revision, scanline'
        query_str = 'select {} from {} where metaID={}'.format(columns, table, metaID)
        cu.execute( query_str )
        rows = cu.fetchall()
        for row in rows:
            row_entry = {}
            for key_name in row.keys():
                row_entry[key_name] = row[key_name]
            row_list += (row_entry,)
            if toScreen:
                print( row_entry.values() )
            
        table = 'ICM_SIR_CALIBRATION'
        if toScreen:
            print( '#---------- {} ----------'.format(table) )
        columns = 'name, dateTimeStart, ic_id, ic_version, scanline'
        query_str = 'select {} from {} where metaID={}'.format(columns, table, metaID)
        cu.execute( query_str )
        rows = cu.fetchall()
        for row in rows:
            row_entry = {}
            for key_name in row.keys():
                row_entry[key_name] = row[key_name]
            row_list += (row_entry,)
            if toScreen:
                print( row_entry.values() )

        table = 'ICM_SIR_IRRADIANCE'
        if toScreen:
            print( '#---------- {} ----------'.format(table) )
        columns = 'name, dateTimeStart, ic_id, ic_version, scanline'
        query_str = 'select {} from {} where metaID={}'.format(columns, table, metaID)
        cu.execute( query_str )
        rows = cu.fetchall()
        for row in rows:
            row_entry = {}
            for key_name in row.keys():
                row_entry[key_name] = row[key_name]
            row_list += (row_entry,)
            if toScreen:
                print( row_entry.values() )

        table = 'ICM_SIR_RADIANCE'
        if toScreen:
            print( '#---------- {} ----------'.format(table) )
        columns = 'name, dateTimeStart, ic_id, ic_version, scanline'
        query_str = 'select {} from {} where metaID={}'.format(columns, table, metaID)
        cu.execute( query_str )
        rows = cu.fetchall()
        for row in rows:
            row_entry = {}
            for key_name in row.keys():
                row_entry[key_name] = row[key_name]
            row_list += (row_entry,)
            if toScreen:
                print( row_entry.values() )

        cu.close()
        conn.close()

        return row_list
    else:
        print( '*** Fatal, mode-option not recognized: {}'.format(mode) )
        return ()

#---------------------------------------------------------------------------
def get_product_by_orbit( args=None, dbname=DB_NAME, orbit=None,
                          mode='location', toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on reference orbit. 

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, orbit, mode
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "orbit"    : reference orbit, single value or range
                   [value required]
    - "mode"     : defines the returned information:
        'location' :  query the file location
        'meta'     :  query the file meta-data
        'content'  :  query the file content
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    '''
    if args:
        dbname     = args.dbname
        orbit      = args.orbit
        mode       = args.mode

    assert orbit, \
        '*** Fatal, no reference orbit provided'

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)

#---------------------------------------------------------------------------
def get_product_by_date( args=None, dbname=DB_NAME, startdate=None,
                         mode='location', toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on start date of measurements. 

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, startdate, mode
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "startdate":  select on dateTimeStart of measurements in dataset
          select a period: date=[dateTime1, dateTime2]
          select one minute: date=YYMMDDhhmm
          select one hour: date=YYMMDDhh
          select one day: date=YYMMDD
          select one month: date=YYMM
    - "mode"     : defines the returned information:
        'location' :  query the file location
        'meta'     :  query the file meta-data
        'content'  :  query the file content
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    '''
    if args:
        dbname     = args.dbname
        date       = args.date
        mode       = args.mode

    assert startdate, \
        '*** Fatal, no measurement start date provided'

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)

#---------------------------------------------------------------------------
def get_product_by_rtime( args=None, dbname=DB_NAME, rtime=None,
                          mode='location', toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on receive time of products.

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, rtime, mode
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "rtime"    : receive time (or interval) of products  
                   [value required]
    - "mode"     : defines the returned information:
        'location' :  query the file location
        'meta'     :  query the file meta-data
        'content'  :  query the file content
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    '''
    if args:
        dbname     = args.dbname
        rtime      = args.rtime
        mode       = args.mode

    assert rtime, \
        '*** Fatal, no receive time of product provided'

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)

#---------------------------------------------------------------------------
def get_product_by_type( args=None, dbname=DB_NAME, dataset=None,
                         after_dn2v=False, orbit=[], date=None,
                         mode='location', toScreen=False ):
    '''
    Query NADC Sciamachy SQLite database on product type with data selections

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, dataset, mode
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "dataset"  : name or abbreviation of dataset, for example
         'DARK_MODE_1605' : exact name match
         'DARK_%'  : all dark measurements with different ICIDs
         '%_1605'  : all measurements with ICID equals 1605
    - "after_dn2v": select measurement on calibration up to DN2V 
                   [default: False]
    - "orbit"     : select on reference orbit number or range
                   [default: empty list]
    - "date"      :  select on dateTimeStart of measurements in dataset
          select a period: date=[dateTime1, dateTime2] as "YYYY-MM-DDTHH:MM:SS"
          select one minute: date=YYMMDDhhmm
          select one hour: date=YYMMDDhh
          select one day: date=YYMMDD
          select one month: date=YYMM
                   [default: None]
    - "mode"     : defines the returned information:
        'location' :  query the file location
        'meta'     :  query the file meta-data
        'content'  :  query the file content
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]

    Output
    ------
    return full-path to selected products [default] 

    '''
    if args:
        dbname     = args.dbname
        dataset    = args.dataset
        after_dn2v = args.after_dn2v
        orbit      = args.orbit
        date       = args.date
        mode       = args.mode

    assert dataset, \
        '*** Fatal, no dataset name provided'

    assert isinstance(orbit, (tuple, list)), \
        '*** Fatal, parameter orbit is not a list or tuple'

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)

    ## define list of tables to find the dataset
    tables = ()
    table_list = ('ICM_SIR_ANALYSIS', 'ICM_SIR_CALIBRATION',
                  'ICM_SIR_IRRADIANCE', 'ICM_SIR_RADIANCE')

    q_str = 'select distinct name, metaID from {} where name like \'{}\''
    if after_dn2v:
        q_str += ' and after_dn2v != 0'
    if date:
        ll = list(date[0+i:2+i] for i in range(0, len(date), 2))
        if len(ll) == 2:
            ll += ['00', '00', '00', '00']
            dtime = '+1 month'
        elif len(ll) == 3:
            ll += ['00', '00', '00']
            dtime = '+1 day'
        elif len(ll) == 4:
            ll += ['00', '00']
            dtime = '+1 hour'
        elif len(ll) == 5:
            ll += ['00']
            dtime = '+1 minute'
        else:
            pass
        d1 = '20{}-{}-{}T{}:{}:{}'.format(*ll)
        mystr = ' and dateTimeStart between \'{}\' and datetime(\'{}\',\'{}\')'
        q_str += mystr.format( d1, d1, dtime )
        print(q_str)
        
    conn = sqlite3.connect( dbname )
    cu  = conn.cursor()
    for tbl in table_list:
        cu.execute( q_str.format(tbl, dataset) )
        rows = cu.fetchall()
        if len(rows) > 0:
            tables += ({'nameList' : ', '.join(str(e[0]) for e in rows),
                        'metaList' : ', '.join(str(e[1]) for e in rows)},)
    cu.close()
    conn.close()

    if len(tables) == 0:
        print( '*** Warning, no dataset with name \"{}\" not found'.format(dataset) )
        return ()

    meta_table = 'ICM_SIR_META'
    if mode == 'location':
        ## obtain root directories (local or NFS)
        case_str = 'case when hostName == \'{}\''\
            ' then localPath else nfsPath end'.format(socket.gethostname())
        qq_str = 'select {} from ICM_SIR_LOCATION where pathID={}'

        if orbit:
            if len(orbit) == 1:
                orbit_str = 'referenceOrbit = {}'.format(orbit[0])
            else:
                orbit_str = 'referenceOrbit between {} and {}'.format(orbit[0], orbit[1])
            q_str = 'select distinct pathID, name from {} where ' + orbit_str
            q_str += ' and metaID in ({})'
        else:
            q_str = 'select distinct pathID, name from {} where metaID in ({})'
        conn = sqlite3.connect( dbname )
        cu = conn.cursor()
        cuu = conn.cursor()

        row_list = ()
        for entry in tables:
            cu.execute( q_str.format(meta_table, entry['metaList']) )
            for row in cu:
                cuu.execute( qq_str.format(case_str, row[0]) )
                root = cuu.fetchone()
                if toScreen:
                    print( os.path.join(root[0], row[1]) )
                row_list += ([root[0], row[1]],)
        cuu.close()
        cu.close()
        conn.close()
        if len(row_list) == 1:
            return row_list[0]
        else:
            return row_list
    
    return ()

#---------------------------------------------------------------------------
def show_details_icid( args=None, dbname=DB_NAME, ic_id=None,
                       check=False, toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on ICID

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, icid, check 
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "ic_id"    : select an ICID 
                   or list all ICID parameters in table ICM_SIR_TBL_ICID
                   [ic_version to be added in a future release]
    - "check"    : perform check on ICID parameters
                   [default: False]
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    Output
    ------
      file_path  :  path to ICM_SIR product
      file_name  :  name of ICM_SIR product
    '''
    if args:
        dbname = args.dbname
        ic_id  = args.icid
        check  = args.check

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)
    
    conn = sqlite3.connect( dbname )
    conn.row_factory = sqlite3.Row
    cu = conn.cursor()
    
    table = 'ICM_SIR_TBL_ICID'
    if ic_id is None:
        query_str = 'select * from {} order by ic_id,ic_version'.format(table)
        cu.execute( query_str )
        rows = cu.fetchall()

        row_list = ()
        for row in rows:
            row_entry = {}
            for key_name in row.keys():
                row_entry[key_name] = row[key_name]
            row_list += (row_entry,)
            if toScreen:
                print( row_entry.values() )
        
    else:
        query_str = 'select * from {} where ic_id={} order by ic_version'.format(table, ic_id)
        cu.execute( query_str )
        row = cu.fetchone()
        if row is None:
            cu.close()
            conn.close()
            return ()

        row_list = {}
        for key_name in row.keys():
            row_list[key_name] = row[key_name]
            if toScreen:
                print( '{:25}\t{}'.format(key_name, row[key_name]) )

        if check:
            texp = 1.25 * (65540 + row['int_hold'] - row['int_delay'])
            dtexp = 1.25 * (row['int_delay'] + 0.5)
            tdead = row['exposure_period_us'] - texp - dtexp + 5.625
            treset = (row['exposure_period_us'] - texp - 315) / 1e6

            print( '#---------- {}'.format('Calculated timing parameters for reference:') )
            print( '{:25}\t{}'.format('exposure_time (us)', texp) )
            print( '{:25}\t{}'.format('dead_time (us)', tdead) )
            print( '{:25}\t{}'.format('exposure_shift (us)', dtexp) )
            print( '{:25}\t{}'.format('reset_time (s)', treset) )

    ## close connection and return result
    cu.close()
    conn.close()
    return row_list
   
