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

from tabulate import tabulate

DB_NAME = '/nfs/TROPOMI/ical/share/db/sron_s5p_icm_patched.db'

def check_format_datetime( datetime_str ):
    '''
    Check if the datetime string is in ISO format without 'tzinfo'
     - valid are 'yyyy-mm-ddThh:mm:ss' and 'yyyy-mm-dd hh:mm:ss'
    '''
    import re

    # answer obtained from Stackoverflow (funkworm)
    pattern=r'^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])'\
        + r'|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)'\
        + r'|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])'\
        + r'|(?:[2468][048]|[13579][26])00)-02-29)[T\ ](?:[01]\d|2[0-3])'\
        + r':[0-5]\d:[0-5]\d$'
    
    return re.match(pattern, datetime_str) is not None

#--------------------------------------------------
class S5pDB( object ):
    '''
    Defines superclass for ICM_SIR database access
    '''
    def __init__( self, dbname, verbose=False ):
        assert os.path.isfile( dbname ),\
            '*** Fatal, can not find SQLite database: {}'.format(dbname)

        self.__conn = sqlite3.connect( dbname )
        self.__verbose = verbose
        cur = self.__conn.cursor()
        cur.execute( 'PRAGMA foreign_keys = ON' )
        cur.close()

    def __query_location__( self, cols=None ):
        '''
        '''
        case_str = 'case when hostName == \'{}\' then localPath'\
                   ' else nfsPath end'.format(socket.gethostname())
        if cols is None:
            return 'select {} from ICM_SIR_LOCATION'.format(case_str)
        else:
            return 'select {},{} from ICM_SIR_LOCATION'.format(case_str, cols)
        
    def __get_relpath__( self, flname ):
        '''
        '''
        ll = flname.split('_')
        if len(ll) == 11:
            return( os.path.join(ll[9],ll[5][0:4],ll[5][4:6],ll[5][6:8]) )
        else:
            return( os.path.join(ll[8],ll[4][0:4],ll[4][4:6],ll[4][6:8]) )
    
    def __select_on_date__( self, datetime_str, prefix='' ):
        '''
        return SQL string to perform selection on date-time
        
        keyword 'prefix' is ignored when datetime_str is None
        '''
        if datetime_str is None:
            return ''

        if len(datetime_str.split(',')) == 1:
            assert (len(datetime_str) >= 4 and len(datetime_str) <= 10), \
                '*** Fatal, invalid date-string: {}'.format(datetime_str)
            
            ll = list(datetime_str[0+i:2+i] for i in range(0, len(datetime_str), 2))
            if len(ll) == 2:
                ll += ['01', '00', '00', '00']
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
            
            d1 = '20{}-{}-{} {}:{}:{}'.format(*ll)
            mystr = ' dateTimeStart between \'{}\' and datetime(\'{}\',\'{}\')'
            return prefix + mystr.format( d1, d1, dtime )
        else:
            date_list = datetime_str.replace('T', ' ').split(',')
            assert (check_format_datetime( date_list[0] )
                    and check_format_datetime( date_list[1] )),\
                '*** Fatal date-time {} not in ISO format'.format(datetime_str)

            mystr = ' dateTimeStart between \'{}\' and \'{}\''
            return prefix + mystr.format(*date_list)
    
    def cursor( self, Row=False ):
        '''
        '''
        if Row:
            self.__conn.row_factory = sqlite3.Row
        return self.__conn.cursor()
    
    def __del__( self ):
        '''
        '''
        self.__conn.close()
        if self.__verbose:
            print( '*** Info: connection to database closed' )

#--------------------------------------------------
class S5pDB_date( S5pDB ):
    '''
    '''
    def __init__( self, dbname, verbose=False ):
        super().__init__( dbname )
        self.__verbose = verbose

    def __query_meta__( self, date ):
        '''
        '''
        table = 'ICM_SIR_META'
        cols = 'pathID,name'

        q_str = 'select {} from {}'.format(cols,table)
        q_str += self.__select_on_date__( date, prefix=' where' )
        if self.__verbose:
            print( q_str )
        return q_str + ' order by referenceOrbit'
    
    def location( self, date ):
        '''
        '''
        row_list = ()

        q1_str = self.__query_location__('name')
        q2_str = self.__query_meta__( date )
        if self.__verbose:
            print(q2_str)
        q_str = q1_str + ' as s1 join (' + q2_str + ') as s2' 
        q_str += ' on s1.pathID=s2.pathID'
        
        cur = self.cursor()
        cur.execute( q_str )
        for row in cur:
            row_list += ([os.path.join(row[0], self.__get_relpath__(row[1])),
                          row[1]],)
        cur.close()
        return row_list
    
#--------------------------------------------------
class S5pDB_name( S5pDB ):
    '''
    class definition for queries given the ICM_CA_SIR product name
    '''
    def __init__( self, dbname, verbose=False ):
        super().__init__( dbname )
        self.__verbose = verbose
        
    def location( self, product ):
        '''
        '''
        table = 'ICM_SIR_META'
        
        q1_str = 'select pathID,name from {} where name=\'{}\''.format(table, product)
        q2_str = self.__query_location__() + ' where pathID={}'

        cur = self.cursor()
        if self.__verbose:
            print( q1_str )
        cur.execute( q1_str )
        row = cur.fetchone()
        if row is None:
            cur.close()
            return ()
        else:
            if self.__verbose:
                print( q2_str.format(row[0]) )
            cur.execute( q2_str.format(row[0]) )
            root = cur.fetchone()
            cur.close()
            return (os.path.join(root[0], self.__get_relpath__(row[1])), row[1])
    
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
                columns = 'name, validityDate, svn_revision, scanline'
            else:
                columns = 'name, ic_id, dateTimeStart, scanline'

            if self.__verbose:
                q2_str.format(columns, table, meta_id)
            cur.execute( q2_str.format(columns, table, meta_id) )
            for row in cur:
                row_entry = {}
                row_entry['table'] = table
                for key_name in row.keys():
                    row_entry[key_name] = row[key_name]
                row_list += (row_entry,)

        cur.close()
        return row_list

#--------------------------------------------------
class S5pDB_orbit( S5pDB ):
    '''
    '''
    def __init__( self, dbname, verbose=False ):
        super().__init__( dbname )
        self.__verbose = verbose

    def __query_meta__( self, orbit ):
        table = 'ICM_SIR_META'
        cols = 'pathID,name'

        q_str = 'select {} from {}'.format(cols,table)
        if len(orbit) == 1:
            orbit_str = ' where referenceOrbit = {}'.format(orbit[0])
        else:
            orbit_str = ' where referenceOrbit between {} and {}'.format(*orbit)

        return q_str + orbit_str + ' order by referenceOrbit'
    
    def location( self, orbit ):
        '''
        '''
        row_list = ()

        q1_str = self.__query_location__('name')
        q2_str = self.__query_meta__( orbit )
        q_str = q1_str + ' as s1 join (' + q2_str + ') as s2' 
        q_str += ' on s1.pathID=s2.pathID'

        cur = self.cursor()
        cur.execute( q_str )
        for row in cur:
            row_list += ([os.path.join(row[0], self.__get_relpath__(row[1])),
                          row[1]],)
        cur.close()
        return row_list

    def latest( self ):
        '''
        '''
        table = 'ICM_SIR_META'
        q_str = 'select max(referenceOrbit) from {}'.format(table)

        cur = self.cursor()
        cur.execute( q_str )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return None
        else:
            return row[0]
    
    def date( self, date ):
        '''
        '''
        row_list = ()

        table = 'ICM_SIR_META'
        date_str = 'dateTimeStart between \'{}\' and \'{}\''.format(
            date + ' 00:00:00', date + ' 23:59:59' )
        q_str = 'select referenceOrbit from {} where {}'.format(table, date_str)
        if self.__verbose:
            print( q_str )
        
        cur = self.cursor()
        cur.execute( q_str )
        for row in cur:
            row_list += (row[0],)
        cur.close()
        
        return row_list

    
#--------------------------------------------------
class S5pDB_rtime( S5pDB ):
    '''
    '''
    def __init__( self, dbname, verbose=False ):
        super().__init__( dbname )
        self.__verbose = verbose

    def __query_meta__( self, rtime ):
        table = 'ICM_SIR_META'
        cols = 'pathID,name'

        q_str = 'select {} from {}'.format(cols,table)
        if rtime is None:
            return q_str + ' order by receiveDate'
        
        mystr = ' where receiveDate between datetime(\'now\',\'-{} {}\')' \
            + ' and datetime(\'now\')'
        if rtime[-1] == 'h':
            ll = (int(rtime[0:-1]), 'hour')
        else:
            ll = (int(rtime[0:-1]), 'day')

        return q_str + mystr.format(*ll) + ' order by receiveDate'
    
    def location( self, rtime ):
        '''
        '''
        row_list = ()

        q1_str = self.__query_location__('name')
        q2_str = self.__query_meta__( rtime )
        q_str = q1_str + ' as s1 join (' + q2_str + ') as s2' 
        q_str += ' on s1.pathID=s2.pathID'
        
        cur = self.cursor()
        cur.execute( q_str )
        for row in cur:
            row_list += ([os.path.join(row[0], self.__get_relpath__(row[1])),
                          row[1]],)
        cur.close()
        return row_list
    
#--------------------------------------------------
class S5pDB_icid( S5pDB ):
    '''
    '''
    def __init__( self, dbname, verbose=False ):
        super().__init__( dbname )
        self.__verbose = verbose

    def __query_icid__( self, table, icid, date, after_dn2v ):
        cols = 'metaID,after_dn2v,group_concat(name) as names'
        
        q_str = 'select {} from {}'.format(cols, table)
        q_str +=  ' where ic_id in (' + str(icid)[1:-1] + ')'
        if after_dn2v:
            q_str += ' and after_dn2v!=0'
        else:
            q_str += ' and after_dn2v=0'
        
        q_str += self.__select_on_date__( date, prefix=' and' )
        q_str += ' GROUP BY metaID HAVING count(*)={}'.format(len(icid))

        return q_str

    def __query_meta__( self ):
        table = 'ICM_SIR_META'
        cols = 'pathID,s2.name as prod_name,s3.names as ds_name,after_dn2v'

        return 'select {} from {} as s2'.format(cols, table)
    
    def location( self, icid, orbit, date, after_dn2v=False ):
        '''
        '''
        row_list = ()
        
        q1_str = self.__query_location__('prod_name,ds_name,after_dn2v') 
        q2_str = self.__query_meta__()
        if orbit is None:
            orbit_str = ''
        elif len(orbit) == 1:
            orbit_str = ' where referenceOrbit={}'.format(orbit[0])
        else:
            orbit_str = ' where referenceOrbit between {} and {}'.format(*orbit)

        table_list = ('ICM_SIR_ANALYSIS', 'ICM_SIR_CALIBRATION',
                      'ICM_SIR_IRRADIANCE', 'ICM_SIR_RADIANCE')

        cur = self.cursor()
        for table in table_list:
            if table == 'ICM_SIR_ANALYSIS':
                continue
            
            q3_str = self.__query_icid__( table, icid, date, after_dn2v )
            q_str = q1_str   + ' as s1' \
                    + ' join (' + q2_str + ' join (' + q3_str + ')'
            q_str += ' as s3 on s2.metaID=s3.metaID' + orbit_str + ')'
            q_str += ' as s4 on s1.pathID=s4.pathID'

            if self.__verbose:
                print( q_str )
            cur.execute( q_str )
            for row in cur:
                for col in row[2].split(','):
                    row_list += ([os.path.join(row[0],
                                               self.__get_relpath__(row[1])),
                                  row[1], table, col, row[3]],)           
        cur.close()
        return row_list
    
#--------------------------------------------------
class S5pDB_type( S5pDB ):
    '''
    '''
    def __init__( self, dbname, verbose=False ):
        super().__init__( dbname )
        self.__verbose = verbose

    def __query_ds__( self, table, dataset, after_dn2v, date ):
        cols = 'metaID,name,after_dn2v'
        
        q_str = 'select {} from {}'.format(cols, table)
        q_str +=  ' where name like \'{}\''.format(dataset)

        # dynamic CKD are note selected on 'after_dn2v' or 'date'
        if table == 'ICM_SIR_ANALYSIS':
            return q_str
        
        if after_dn2v:
            q_str += ' and after_dn2v != 0'
        else:
            q_str += ' and after_dn2v = 0'

        q_str += self.__select_on_date__( date, prefix=' and' )

        return q_str

    def __query_meta__( self ):
        table = 'ICM_SIR_META'
        cols = 'pathID,s2.name as prod_name,s3.name as ds_name,after_dn2v'

        return 'select {} from {} as s2'.format(cols, table)
    
    def location( self, dataset, orbit, after_dn2v, date ):
        '''
        '''
        row_list = ()
        q1_str = self.__query_location__('prod_name,ds_name,after_dn2v') 
        q2_str = self.__query_meta__()
        if orbit is None:
            orbit_str = ''
        elif len(orbit) == 1:
            orbit_str = ' where referenceOrbit={}'.format(orbit[0])
        else:
            orbit_str = ' where referenceOrbit between {} and {}'.format(*orbit)

        table_list = ('ICM_SIR_ANALYSIS', 'ICM_SIR_CALIBRATION',
                      'ICM_SIR_IRRADIANCE', 'ICM_SIR_RADIANCE')

        cur = self.cursor()
        for table in table_list:
            q3_str = self.__query_ds__( table, dataset, after_dn2v, date )
            q_str = q1_str   + ' as s1' \
                    + ' join (' + q2_str + ' join (' + q3_str + ')'
            q_str += ' as s3 on s2.metaID=s3.metaID' + orbit_str + ')'
            q_str += ' as s4 on s1.pathID=s4.pathID'

            if self.__verbose:
                print( q_str )
            cur.execute( q_str )
            for row in cur:
                row_list += ([os.path.join(row[0],
                                           self.__get_relpath__(row[1])),
                              row[1], table, row[2], row[3]],)           
        cur.close()
        return row_list
    
#--------------------------------------------------
class S5pDB_tbl_icid( S5pDB ):
    '''
    class definition for queries on instrument settings
    '''
    def __init__( self, dbname, verbose=False ):
        super().__init__( dbname )
        self.__verbose = verbose

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
        if self.__verbose:
            print( query_str.format(table, ic_id) )
        cur.execute( query_str.format(table, ic_id) )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return ()

        row_list = {}
        for key_name in row.keys():
            row_list[key_name] = row[key_name]

        return row_list

#---------------------------------------------------------------------------
def get_orbit_latest( args=None, dbname=DB_NAME, 
                      toScreen=False, verbose=False ):
    '''
    Query NADC S5p-Tropomi ICM-database for the largest available referenceOrbit

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    '''
    if args:
        dbname  = args.dbname
        verbose = args.debug

    db = S5pDB_orbit( dbname, verbose=verbose )

    result = db.latest()
    if toScreen:
        print( result )
    return result
   
#---------------------------------------------------------------------------
def get_orbit_for_date( date, args=None, dbname=DB_NAME,
                        toScreen=False, verbose=False ):
    '''
    Query NADC S5p-Tropomi ICM-database for all orbits during a given day

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "date"     : date in ISA-format (YYYY-mm-dd)
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    '''
    if args:
        dbname  = args.dbname
        verbose = args.debug
        date    = args.date

    if date is None:
        from datetime import date
        
        date = date.today().isoformat()

    db = S5pDB_orbit( dbname, verbose=verbose )

    result = db.date( date )
    if toScreen:
        print( result )
    return result
   
#---------------------------------------------------------------------------
def get_product_by_date( args=None, dbname=DB_NAME, date=None,
                         mode='location', toScreen=False, verbose=False ):
    '''
    Query NADC S5p-Tropomi ICM-database on start date of measurements. 

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, startdate, mode
    - "dbname"   : full path to S5p Tropomi SQLite database 
                   [default: DB_NAME]
    - "date"     :  select on dateTimeStart of measurements in dataset
          select a period: date=[dateTime1, dateTime2]
          select one minute: date=YYMMDDhhmm
          select one hour: date=YYMMDDhh
          select one day: date=YYMMDD
          select one month: date=YYMM
    - "mode"     : defines the returned information:
        'location' :  query the file location
        'meta'     :  query the file meta-data [not implemented, TBD]
        'content'  :  query the file content [not implemented, TBD]
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    '''
    if args:
        dbname  = args.dbname
        verbose = args.debug
        date    = args.date
        mode    = args.mode

    assert date, \
        '*** Fatal, no measurement start date provided'

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)

    db = S5pDB_date( dbname, verbose=verbose )

    if mode == 'location':
        result = db.location( date )
        if toScreen:
            for entry in result:
                print( os.path.join(entry[0], entry[1]) )
        return result
    
    return ()

#---------------------------------------------------------------------------
def get_product_by_name( args=None, product=None, dbname=DB_NAME, 
                         mode='location', toScreen=False, verbose=False ):
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
        dbname  = args.dbname
        verbose = args.debug
        product = args.product
        mode    = args.mode

    assert product, \
        '*** Fatal, no product-name provided'

    db = S5pDB_name( dbname, verbose=verbose )
    if mode == 'location':
        result = db.location( product )
        if toScreen and len(result) == 2:
            print( os.path.join(result[0], result[1]) )

        return result
    elif mode == 'meta':
        result = db.meta( product )
        if toScreen and len(result) > 0:
            keys_list = list(result.keys())
            keys_list.sort()
            for key_name in keys_list:
                print( key_name, '\t', result[key_name] )

        return result
    elif mode == 'content':
        result = db.content( product )
        if toScreen:
            table = []
            for res in result:
                data = [res['table'], res['name']]
                if 'dateTimeStart' in res.keys():
                    data.append( res['dateTimeStart'] )
                if 'validityDate' in res.keys():
                    data.append( res['validityDate'] )
                if 'svn_revision' in res.keys():
                    data.append( res['svn_revision'] )
                if 'ic_id' in res.keys():
                    data.append( res['ic_id'] )
                data.append( res['scanline'] )
                table.append( data )
            print( tabulate(table, tablefmt='plain') )

        return result
    else:
        print( '*** Fatal, mode-option not recognized: {}'.format(mode) )
        return ()

#---------------------------------------------------------------------------
def get_product_by_orbit( args=None, dbname=DB_NAME, orbit=None,
                          mode='location', toScreen=False, verbose=False ):
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
        'meta'     :  query the file meta-data [not implemented, TBD]
        'content'  :  query the file content [not implemented, TBD]
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    '''
    if args:
        dbname  = args.dbname
        verbose = args.debug
        orbit   = args.orbit
        mode    = args.mode

    assert orbit is None or isinstance(orbit, (tuple, list)), \
        '*** Fatal, parameter orbit is not a list or tuple'

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)

    db = S5pDB_orbit( dbname, verbose=verbose )

    if mode == 'location':
        result = db.location( orbit )
        if toScreen:
            for entry in result:
                print( os.path.join(entry[0], entry[1]) )
        return result
    
    return ()

#---------------------------------------------------------------------------
def get_product_by_rtime( args=None, dbname=DB_NAME, rtime=None,
                          mode='location', toScreen=False, verbose=False ):
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
        'meta'     :  query the file meta-data [not implemented, TBD]
        'content'  :  query the file content [not implemented, TBD]
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]
    '''
    if args:
        dbname  = args.dbname
        verbose = args.debug
        rtime   = args.rtime
        mode    = args.mode

    assert rtime, \
        '*** Fatal, no receive time of product provided'

    assert os.path.isfile( dbname ),\
        '*** Fatal, can not find SQLite database: {}'.format(dbname)

    db = S5pDB_rtime( dbname, verbose=verbose )

    if mode == 'location':
        result = db.location( rtime )
        if toScreen:
            for entry in result:
                print( os.path.join(entry[0], entry[1]) )
        return result
     
    return ()

#---------------------------------------------------------------------------
def get_product_by_icid( args=None, dbname=DB_NAME, icid=None,
                         after_dn2v=False, orbit=None, date=None, 
                         mode='location', toScreen=False, verbose=False ):
    '''
    Query NADC Tropomi SQLite database on products which contain measurements 
    of all listed ICIDs

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, icid, after_dn2v, 
                   orbit, date, mode
    - "dbname"   : full path to S5p Tropomi SQLite database 
                  [default: DB_NAME]
    - "icid"     : list of ICIDs
    - "orbit"    : select on reference orbit number or range
                   [default: empty list]
    - "date"     :  select on dateTimeStart of measurements in dataset
          select a period: date=[dateTime1, dateTime2] as "YYYY-MM-DDTHH:MM:SS"
          select one minute: date=YYMMDDhhmm
          select one hour: date=YYMMDDhh
          select one day: date=YYMMDD
          select one month: date=YYMM
                   [default: None]
    - "after_dn2v": select measurement(s) on calibration up to DN2V 
                   [default: False]
    - "mode"     : defines the returned information:
        'location' :  query the file location
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]

    Output
    ------
    return full-path to selected products [default] 

    '''
    if args:
        dbname     = args.dbname
        verbose    = args.debug
        icid       = [int(s) for s in args.icid.split(',')]
        after_dn2v = args.after_dn2v
        orbit      = args.orbit
        date       = args.date
        mode       = args.mode

    assert isinstance(icid, (tuple, list)), \
        '*** Fatal, parameter icid is not a list or tuple'

    assert orbit is None or isinstance(orbit, (tuple, list)), \
        '*** Fatal, parameter orbit is not a list or tuple'

    db = S5pDB_icid( dbname, verbose=verbose )

    if mode == 'location':
        result = db.location( icid, orbit, date, after_dn2v )
        if toScreen:
            for entry in result:
                h5_grp = 'BAND%_' + entry[2].split('_')[2]
                if entry[4] == 1:
                    h5_sgrp = entry[3] + '_AFTER_DN2V'
                else:
                    h5_sgrp = entry[3]

                print( os.path.join(entry[0], entry[1],
                                    h5_grp, h5_sgrp) )
        return result
    
    return ()

#---------------------------------------------------------------------------
def get_product_by_type( args=None, dbname=DB_NAME, dataset=None,
                         after_dn2v=False, orbit=None, date=None,
                         mode='location', toScreen=False, verbose=False ):
    '''
    Query NADC Tropomi SQLite database on product type with data selections

    Parameters:
    -----------
    - "args"     : argparse object with keys dbname, dataset, after_dn2v,
                   orbit, date, mode, 
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
                   [default: 'location']
    - "toScreen" : controls if the query result is printed on STDOUT 
                   [default: False]

    Output
    ------
    return full-path to selected products [default] 

    '''
    if args:
        dbname     = args.dbname
        verbose    = args.debug
        dataset    = args.dataset
        after_dn2v = args.after_dn2v
        orbit      = args.orbit
        date       = args.date
        mode       = args.mode

    assert dataset, \
        '*** Fatal, no dataset name provided'

    assert orbit is None or isinstance(orbit, (tuple, list)), \
        '*** Fatal, parameter orbit is not a list or tuple'

    db = S5pDB_type( dbname, verbose=verbose )

    if mode == 'location':
        result = db.location( dataset, orbit, after_dn2v, date )
        if toScreen:
            for entry in result:
                h5_grp = 'BAND%_' + entry[2].split('_')[2]
                if entry[4] == 1:
                    h5_sgrp = entry[3] + '_AFTER_DN2V'
                else:
                    h5_sgrp = entry[3]

                print( os.path.join(entry[0], entry[1],
                                    h5_grp, h5_sgrp) )
        return result
    
    return ()

#---------------------------------------------------------------------------
def get_instrument_settings( args=None, dbname=DB_NAME, ic_id=None,
                             check=False, toScreen=False, verbose=False ):
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
        dbname  = args.dbname
        verbose = args.debug
        ic_id   = args.icid
        check   = args.check

    db = S5pDB_tbl_icid( dbname, verbose=verbose )
    if ic_id is None:
        result = db.all()

        if toScreen:
            newdict={}
            for k,v in [(key,d[key]) for d in result for key in d]:
                if k not in newdict:
                    newdict[k]=[v]
                else:
                    newdict[k].append(v)
            print( '#', ' '.join(newdict.keys()) )
            print( tabulate(newdict, tablefmt="plain") )
            #print( tabulate(newdict, headers="keys") )
    else:
        result = db.one( ic_id )

        if toScreen:
            keys_list = list(result.keys())
            keys_list.sort()
            for key_name in keys_list:
                print( '{:25}\t{}'.format(key_name, result[key_name]) )

        if check:
            texp = 1.25 * (65540 + result['int_hold'] - result['int_delay'])
            dtexp = 1.25 * (result['int_delay'] + 0.5)
            tdead = result['exposure_period_us'] - texp - dtexp + 5.625
            treset = (result['exposure_period_us'] - texp - 315) / 1e6

            print( '#---------- {}'.format('Calculated timing parameters for reference:') )
            print( '{:25}\t{}'.format('dead_time (us)', tdead) )
            print( '{:25}\t{}'.format('exposure_shift (us)', dtexp) )
            print( '{:25}\t{}'.format('exposure_time (us)', texp) )
            print( '{:25}\t{}'.format('reset_time (s)', treset) )

    return result

#--------------------------------------------------
def fast_test_db():
    '''
    Quick check to test module tropomi.db.py
    '''
    result = get_orbit_latest(toScreen=True)
    
    print( '''*** Info: test function 'get_product_by_date' ''' )
    result = get_product_by_date( date='1209', toScreen=True )
    result = get_product_by_date( date='120919', toScreen=True )
    result = get_product_by_date( date='12091905', toScreen=True )
    result = get_product_by_date( date='2012-09-19 05:17:19,2012-12-19 05:17:20',
                                  toScreen=True )

    print( '''*** Info: test function 'get_product_by_name' ''' )
    result = get_product_by_name( product='S5P_ICM_CA_SIR_20120919T051721_20120919T065655_01932_01_001000_20151002T140000.h5',
                                  toScreen=True )

    print( '''*** Info: test function 'get_product_by_orbit' ''' )
    result = get_product_by_orbit( orbit=[1890], toScreen=True )
    result = get_product_by_orbit( orbit=[1890,1905], toScreen=True )

    print( '''*** Info: test function 'get_product_by_rtime' ''' )
    result = get_product_by_rtime( rtime='+7d', toScreen=True )

    print( '''*** Info: test function 'get_product_by_type' ''' )
    result = get_product_by_type( dataset='BACKGROUND_MODE_170%',
                                  toScreen=True )
    result = get_product_by_type( dataset='%_1611', orbit=[1926,1930],
                                  toScreen=True )
    result = get_product_by_type( dataset='%_1611',
                                  date='2012-09-19 05:17:19,2012-12-19 05:17:20',
                                  toScreen=True )

    print( '''*** Info: test function 'get_instrument_settings' ''' )
    result = get_instrument_settings( ic_id=1703, check=True, toScreen=True )

#-------------------------SECTION MAIN--------------------------------------
if __name__ == '__main__':

    fast_test_db()
