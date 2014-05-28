#
from __future__ import print_function
from __future__ import division

import os.path
import sqlite3

DB_NAME = '/GOSAT/share/db/sron_gosat.db'

#---------------------------------------------------------------------------
def get_product_by_name( args=None, toScreen=False, dbname=DB_NAME, 
                         product=None, dump=False, debug=False ):
    '''
    '''
    if args:
        dbname = args.dbname
        product = args.product
        dump = args.dump
        debug = args.debug

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: %s' % dbname )
        return []

    if product[0:9] == 'GOSATTCAI':
        query_str = ['SELECT * from tcai__2P']
    else:
        query_str = ['SELECT * from tfts__1P']
    query_str.append(' WHERE name=\'%s\'' % product)

    if debug:
        print( ''.join(query_str) )
        return []

    conn = sqlite3.connect( dbname )
    conn.row_factory = sqlite3.Row
    cu = conn.cursor()
    cu.execute( ''.join(query_str) )
    row = cu.fetchone()
    cu.close()
    conn.close()
    if row is None: 
        return []

    rowList = []
    if toScreen:
        if dump:
            for name in row.keys():
                print( name, '\t', row[name] )
        else:
            print( row['path'] + '/' +  row['name'] )
    else:
        if dump:
            rowList.append( row )
        else:
            rowList.append( row['path'] + '/' +  row['name'] )

    return rowList

#---------------------------------------------------------------------------
def get_product_by_type( args=None, toScreen=False, dbname=DB_NAME, 
                         prod_type=None, proc_stage=None, proc_best=None,
                         orbits=None, date=None, rtime=None, 
                         dump=False, debug=False ):
    '''
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

    query_str = ['select * from meta__%sP' % prod_type]
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
        secnd = 0
        year = int(date[0:4])
        if year < 2002:
            print( 'You should specify a valid year' )
            sys.exit(1)
        dtime = '+1 year'

        if len( date ) >= 6:
            month = int(date[4:6])
            if month < 1 or month > 12:
                print( 'You should specify a valid month' )
                sys.exit(1)
            dtime = '+1 month'
        else:
            month = 1

        if len( date ) >= 8:
            day = int(date[6:8])
            if day < 1 or day > 31:
                print( 'You should specify a valid day' )
                sys.exit(1)
            dtime = '+1 day'
        else:
            day = 1
        
        if len( date ) >= 10:
            hour = int(date[8:10])
            if hour < 0 or hour > 24:
                print( 'You should specify a valid hour' )
                sys.exit(1)
            dtime = '+1 hour'
        else:
            hour = 0
        
        if len( date ) >= 12:
            minu = int(date[10:12])
            if minu < 0 or minu > 59:
                print( 'You should specify a valid minute' )
                sys.exit(1)
            dtime = '+1 minute'
        else:
            minu = 0
        d1 = '%04d-%02d-%02d %02d:%02d:%02d' % (year,month,day,hour,minu,secnd)

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

    if not os.path.isfile( dbname ):
        print( 'Fatal, can not find SQLite database: %s' % dbname )
        return []

    rowList = []
    conn = sqlite3.connect( dbname )
    conn.row_factory = sqlite3.Row
    cu = conn.cursor()
    cu.execute( ''.join(query_str) )
    for row in cu:
        if toScreen:
            if dump:
                print( row )
            else:
                if row['compression'] == 0:
                    print( row['path'] + '/' +  row['name'] )
                else:
                    print( row['path'] + '/' +  row['name'] + '.gz' )
        else:
            if dump:
                rowList.append( row )
            else:
                if row['compression'] == 0:
                    rowList.append( row['path'] + '/' +  row['name'] )
                else:
                    rowList.append( row['path'] + '/' +  row['name'] + '.gz' )
                
    cu.close()
    conn.close()
    return rowList
