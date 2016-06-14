# (c) SRON - Netherlands Institute for Space Research (2016).
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

class S5pDB( object ):
    '''
    '''
    def __init__( self, dbname ):
        '''
        '''
        assert os.path.isfile( dbname ),\
            '*** Fatal, can not find SQLite database: {}'.format(dbname)

        self.__conn = sqlite3.connect( dbname )
        cur = self.__conn.cursor()
        cur.execute( 'PRAGMA foreign_keys = ON' )
        cur.close()

    def __query_location__( self, cols=None ):
        case_str = 'case when hostName == \'{}\' then localPath'\
                   ' else nfsPath end'.format(socket.gethostname())
        if cols is None:
            return 'select {} from ICM_SIR_LOCATION'.format(case_str)
        else:
            return 'select {},{} from ICM_SIR_LOCATION'.format(case_str, cols)
        
    def cursor( self, Row=False ):
        if Row:
            self.__conn.row_factory = sqlite3.Row
        return self.__conn.cursor()
    
    def __del__( self ):
        '''
        '''
        self.__conn.close()
        print( '*** Info: connection to database closed' )

class S5pDB_name( S5pDB ):
    '''
    class definition for queries given the ICM_CA_SIR product name
    '''
    def __init__( self, dbname ):
        super().__init__( dbname )
        
    def location( self, product ):
        '''
        '''
        table = 'ICM_SIR_META'
        
        q1_str = 'select pathID,name from {} where name=\'{}\''.format(table, product)
        q2_str = self.__query_location__() + ' where pathID={}'

        cur = self.cursor()
        cur.execute( q1_str )
        row = cur.fetchone()
        if row is None:
            cur.close()
            return ()
        else:
            cur.execute( q2_str.format(row[0]) )
            root = cur.fetchone()
            cur.close()
            return (root[0], row[1])
    
    def meta( self, product ):
        '''
        '''
        table = 'ICM_SIR_META'
        
        query_str = 'select * from {} where name=\'{}\''.format(table, product)

        cur = self.cursor( Row=True )
        cur.execute( query_str )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return ()

        row_list = {}
        for key_name in row.keys():
            row_list[key_name] = row[key_name]

        return row_list

    def content( self, product ):
        '''
        '''
        table = 'ICM_SIR_META'
        
        q1_str = 'select metaID from {} where name=\'{}\''
        q2_str = 'select {} from {} where metaID={}'

        cur = self.cursor( Row=True )
        cur.execute( q1_str.format(table, product) )
        row = cur.fetchone()
        if row is None:
            cur.close()
            return ()
        meta_id = row[0]

        row_list = ()
        table_list = ('ICM_SIR_ANALYSIS', 'ICM_SIR_CALIBRATION',
                      'ICM_SIR_IRRADIANCE', 'ICM_SIR_RADIANCE')
        for table in table_list:
            if table == 'ICM_SIR_ANALYSIS':
                columns = 'name, svn_revision, scanline'
            else:
                columns = 'name, ic_id, dateTimeStart, scanline'

            cur.execute( q2_str.format(columns, table, meta_id) )
            for row in cur:
                row_entry = {}
                row_entry['table'] = table
                for key_name in row.keys():
                    row_entry[key_name] = row[key_name]
                row_list += (row_entry,)

        cur.close()
        return row_list


class S5pDB_icid( S5pDB ):
    '''
    class definition for queries on instrument settings
    '''
    def __init__( self, dbname ):
        super().__init__( dbname )

    def all( self ):
        '''
        '''
        table = 'ICM_SIR_TBL_ICID'

        query_str = 'select * from {} order by ic_id,ic_version'.format(table)

        cur = self.cursor( Row=True )
        cur.execute( query_str )
        
        row_list = ()
        for row in cur:
            row_entry = {}
            for key_name in row.keys():
                row_entry[key_name] = row[key_name]
            row_list += (row_entry,)

        cur.close()
        return row_list

    def one( self, ic_id ):
        '''
        '''
        table = 'ICM_SIR_TBL_ICID'

        query_str = 'select * from {} where ic_id={} order by ic_version'

        cur = self.cursor( Row=True )
        cur.execute( query_str.format(table, ic_id) )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return ()

        row_list = {}
        for key_name in row.keys():
            row_list[key_name] = row[key_name]

        return row_list

class S5pDB_type( S5pDB ):
    '''
    '''
    def __init__( self, dbname, dataset ):
        super().__init__( dbname )
        self.tables = ()
        self.dataset = dataset

    def __query_ds__( self, table, after_dn2v, date ):
        cols = 'metaID,name,after_dn2v'
        
        q_str = 'select {} from {}'.format(cols, table)
        q_str +=  ' where name like \'{}\''.format(self.dataset)

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

        return q_str
        
    def __query_meta__( self, orbit ):
        table = 'ICM_SIR_META'
        cols = 'pathID,s1.name as prod_name,s2.name as ds_name,after_dn2v'

        q_str = 'select {} from {}'.format(cols,table)
        if orbit:
            if len(orbit) == 1:
                orbit_str = ' where referenceOrbit = {}'.format(orbit[0])
            else:
                orbit_str = ' where referenceOrbit between {} and {}'.format(orbit[0], orbit[1])
            q_str += orbit_str

        return q_str + ' as s1'
    
    def __query_path__( self ):
        cols = 'prod_name,ds_name,after_dn2v'
        
        return self.__query_location__(cols) + ' as s3'
    
    def location( self, orbit, after_dn2v, date ):
        '''
        '''
        q1_str = self.__query_path__()
        q2_str = self.__query_meta__( orbit )

        table_list = ('ICM_SIR_ANALYSIS', 'ICM_SIR_CALIBRATION',
                      'ICM_SIR_IRRADIANCE', 'ICM_SIR_RADIANCE')

        row_list = ()
        cur = self.cursor()
        for table in table_list:
            q3_str = self.__query_ds__( table, after_dn2v, date )
            q_str = q1_str + ' join (' + q2_str + ' join (' + q3_str + ')'
            q_str += ' as s2 on s1.metaID=s2.metaID)'
            q_str += ' as s4 on s3.pathID=s4.pathID'
            cur.execute( q_str )
            rows = cur.fetchall()
            if len(rows) == 0:
                continue
            for e in rows:
                ee = list(e)
                ee.append(table)
                row_list += (ee,)
           
        cur.close()
        return row_list
    
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

    db = S5pDB_name( dbname )
    if mode == 'location':
        result = db.location( product )
        if toScreen:
            print( os.path.join(result[0], result[1]) )

        return result
    elif mode == 'meta':
        result = db.meta( product )
        if toScreen:
            for key_name in result.keys():
                print( key_name, '\t', result[key_name] )

        return result
    elif mode == 'content':
        result = db.content( product )
        if toScreen:
            for res in result:
                print( res['table'], '\t', res['name'], end='\t' )
                if 'dateTimeStart' in res.keys():
                    print( res['dateTimeStart'], end='\t' )
                if 'svn_revision' in res.keys():
                    print( res['svn_revision'], end='\t' )
                if 'ic_id' in res.keys():
                    print( res['ic_id'], end='\t' )
                print( res['scanline'] )

        return result
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

    assert orbit is None or isinstance(orbit, (tuple, list)), \
        '*** Fatal, parameter orbit is not a list or tuple'

    db = S5pDB_type( dbname, dataset )

    if mode == 'location':
        result = db.location( orbit, after_dn2v, date )
        if toScreen:
            for entry in result:
                h5_grp = 'BAND[78]_' + entry[4].split('_')[2]
                if entry[3] == 1:
                    h5_sgrp = entry[2] + '_AFTER_DN2V'
                else:
                    h5_sgrp = entry[2]

                print( os.path.join(entry[0], entry[1],
                                        h5_grp, h5_sgrp) )
        return result
    
    return ()

#---------------------------------------------------------------------------
def get_instrument_settings( args=None, dbname=DB_NAME, ic_id=None,
                             check=False, toScreen=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on ICID to obtain instrument settings

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

    db = S5pDB_icid( dbname )
    if ic_id is None:
        result = db.all()

        if toScreen:
            key_list = list(result[0].keys())
            key_list.sort()
            print( '#', ' '.join(key_list) )

            for row in result:
                for key_name in key_list:
                    print( row[key_name], end=' ' )
                print( '')

    else:
        result = db.one( ic_id )

        if toScreen:
            for key_name in result.keys():
                print( '{:25}\t{}'.format(key_name, result[key_name]) )

        if check:
            texp = 1.25 * (65540 + result['int_hold'] - result['int_delay'])
            dtexp = 1.25 * (result['int_delay'] + 0.5)
            tdead = result['exposure_period_us'] - texp - dtexp + 5.625
            treset = (result['exposure_period_us'] - texp - 315) / 1e6

            print( '#---------- {}'.format('Calculated timing parameters for reference:') )
            print( '{:25}\t{}'.format('exposure_time (us)', texp) )
            print( '{:25}\t{}'.format('dead_time (us)', tdead) )
            print( '{:25}\t{}'.format('exposure_shift (us)', dtexp) )
            print( '{:25}\t{}'.format('reset_time (s)', treset) )

    return result
   
