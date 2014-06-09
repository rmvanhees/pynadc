# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

"""
Methods to query the NADC Sciamachy SQLite database
"""
#
from __future__ import print_function
from __future__ import division

import os.path
import sqlite3

DB_NAME = '/SCIA/share/db/sron_scia.db'

#---------------------------------------------------------------------------
def get_product_by_name( args=None, dbname=DB_NAME, product=None, 
                         toScreen=False, dump=False, debug=False ):
    '''
    Query NADC Sciamachy SQLite database on product name

    Input
    -----
    args     : dictionary with keys dbname, product, toScreen, dump, debug
    dbname   : full path to Sciamachy SQLite database [default: DB_NAME]
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

    if product[0:10] == 'SCI_NL__0P':
        table = 'meta__0P'
    elif product[0:10] == 'SCI_NL__1P':
        table = 'meta__1P'
    else:
        table = 'meta__1P'

    if dump:
        select_str = '*'
    else:
        select_str = 'path,name,compression'

    query_str = \
        'select {} from {} where name=\'{}\''.format(select_str, table, product)

    conn = sqlite3.connect( dbname )
    if dump:
        conn.row_factory = sqlite3.Row
    cu = conn.cursor()
    if debug:
        print( query_str )
        cu.close()
        conn.close()
        return []
    else:
        cu.execute( query_str )
        row = cu.fetchone()
        if row is None:
            cu.close()
            conn.close()
            return []

    if toScreen:
        if dump:
            for name in row.keys():
                print( name, '\t', row[name] )
        else:
            if row[2] == 0:
                print( row[0] + '/' +  row[1] )
            else:
                print( row[0] + '/' +  row[1] + '.gz' )

    if dump:
        return row
    else:
        if row[2] == 0:
            return row[0] + '/' +  row[1]
        else:
            return row[0] + '/' +  row[1] + '.gz'

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

    if dump:
        query_str = ['select * from meta__%sP' % prod_type]
    else:
        query_str = ['select path,name,compression from meta__%sP' % prod_type]
    if proc_best:
        if prod_type == '0':
            query_str.append(' as s1 join (select absOrbit,MAX(q_flag)' )
            query_str.append(' as qflag from meta__%sP' % prod_type)
        else:
            query_str.append(' as s1 join (select absOrbit,MAX(procStage)' )
            query_str.append(' as proc from meta__%sP' % prod_type)

    if orbits:
        if not ' where' in query_str:
            query_str.append(' where')
        else:
            query_str.append(' and')

        if len(orbits) == 1:
            mystr = ' absOrbit=%-d' % orbits[0]
        else:
            mystr = ' absOrbit between %-d and %-d' % (orbits[0], orbits[1])
        query_str.append(mystr)

    if proc_stage:
        if not ' where' in query_str:
            query_str.append(' where')
        else:
            query_str.append(' and')

        mystr = ' procStage in ('
        for c in proc_stage:
            if mystr[-1] != '(':  mystr += ','
            mystr += '\'' + c + '\''
        mystr += ')'
        query_str.append(mystr)

    if date:
        if not ' where' in query_str:
            query_str.append(' where')
        else:
            query_str.append(' and')

        dtime = '+1 second'
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
        if not ' where' in query_str:
            query_str.append(' where')
        else:
            query_str.append(' and')

        mystr = ' receiveDate between datetime(\'now\',\'-%-d %s\')' \
            + ' and datetime(\'now\')'
        if rtime[-1] == 'h':
            query_str.append(mystr % (int(rtime[0:-1]), 'hour'))
        else:
            query_str.append(mystr % (int(rtime[0:-1]), 'day'))

    if proc_best:
        query_str.append(' GROUP by absOrbit) as s2 on' )
        if prod_type == '0':
            query_str.append(' s1.absOrbit=s2.absOrbit and s1.q_flag=s2.qflag')
        else:
            query_str.append(' s1.absOrbit=s2.absOrbit and s1.procStage=s2.proc')
        query_str.append(' order by absOrbit ASC')
    else:
        query_str.append(' order by absOrbit ASC, procStage DESC')


    if debug:
        print( ''.join(query_str) )
        return []

    rowList = []
    conn = sqlite3.connect( dbname )
    if dump:
        conn.row_factory = sqlite3.Row
    cu = conn.cursor()
    cu.execute( ''.join(query_str) )
    for row in cu:
        if toScreen:
            if dump:
                print( row )
            else:
                if row[2] == 0:
                    print( row[0] + '/' +  row[1] )
                else:
                    print( row[0] + '/' +  row[1] + '.gz' )
        else:
            if dump:
                rowList.append( row )
            else:
                if row[2] == 0:
                    rowList.append( row[0] + '/' +  row[1] )
                else:
                    rowList.append( row[0] + '/' +  row[1] + '.gz' )
                
    cu.close()
    conn.close()
    return rowList
