#!/usr/bin/env python

# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

"""
DEPRECATED MODULE -- PLEASE CONSIDER USING inquire_scia.py

Perform predefined SQL queries on Sciamachy SQLite database

Synopsis
--------

    inquire_db.py [InquireOptions] [OutputOptions] [dbname]

Description
-----------

Perform predefined SQL queries on Sciamachy SQLite database

Inquire Options
---------------

--level=<0|1|2> 
        Select product level
--name=<filename>
        Select entries with requested product name (no path!)
--rtime=xh[our]/xd[ay]
        Select entries with receive time less than x hour or x days,
        where "x" is an positive integer
--orbit=<number> or --orbit=<min_num>,<max_num>
        Select entries with requested (absolute) orbit number or range
--proc[stage]=[N,O,P,..]
        Select entries with requested ProcStage flags
--soft[Version]=SCIA/x.xx
        Select entries on software version
        - to select on version 6 and up , use SCIA/6.%%
        - to select on version 6.0 and up , use SCIA/6.0%
--date=yyyy[mm[dd[hh[mm]]]]
        Select entries with states within the requested time-window
        - yyyy: returns entries between yyyy and (yyyy+1)
        - yyyymm: returns entries between yyyymm and yyyy(mm+1)
        - yyyymmdd: returns entries between yyyymmdd and yyyymm(dd+1)
        - yyyymmddhh: returns entries between yyyymmddhh and yyyymmdd(hh+1)
        - yyyymmddhhmm: returns entries between yyyymmddhh
                                            and yyyymmddhh(mm+1)
--best
        Select latest consolidated products. 
        (The analysis by Manfred Gottwald are used to select cl0)

Output Options
--------------

--file
        Only location and filename of selected entries are returned [default]
--header
        All information contained in the header table is returned
--list
        Summary of info about a product, listed are:
      	'absOrbit, name, compression, location, receiveDate, totSize'
--caption
        Add column names to output of header and list

Notes
-----

The output rows are sorted on orbit number and software version

Author
------

Richard van Hees (r.m.van.hees at sron.nl)

Bug reporting
-------------

Please report issues at the PYNADC Github page:
https://github.com/rmvanhees/pynadc

Copyright
---------

Copyright (C) 2014 SRON. This software is released under the
Simplified BSD License.
"""
#
from __future__ import print_function
from __future__ import division

import sys
import sqlite3

from string   import atoi
from time     import time, localtime, strptime, strftime
from datetime import datetime, timedelta

#++++++++++++++++++++++++++++++++++++++++++++++++++
def version_message():
    print( sys.argv[0] + ' -- SciaDC tools version 5.0' )
    sys.exit(0)

#++++++++++++++++++++++++++++++++++++++++++++++++++
def help_message():
    print( '\nDEPRECATED MODULE -- PLEASE CONSIDER USING inquire_scia.py\n\n'
           + 'Usage ' + sys.argv[0] 
           + ' [--date=] [--orbit=] [--proc[stage]=] [--soft[Version]=]'
           + ' [--rtime=] [--level= --name=] [--debug]'
           + '\n\t [[--file [--best]] [--header --list [--caption]]] [dbname]'
           + '\n\t by default dbname=/SCIA/share/db/sron_scia.db' )
    sys.exit(0)

#+++++++++++++++++++++++++
def get_opts():
    import getopt

    try:
        opts,args = getopt.getopt( sys.argv[1:], 'hV', \
                                   ['level=', 'name=', 'date=', 'orbit=', 
                                    'proc=', 'procstage=', 'softVersion=', 
                                    'rtime=', 'file', 'header', 'list', 
                                    'best', 'last',
                                    'debug', 'caption', 'help', 'version'] )

    except getopt.error, msg:
        print( msg )
        print( 'Use -h for help' )
        sys.exit(1)

# initialize dictionary with default values
    dict_param = {}
    dict_param['Level'] = None
    dict_param['Orbit'] = None
    dict_param['Name'] = None
    dict_param['ProcStage'] = None
    dict_param['softVersion'] = None
    dict_param['Date'] = None
    dict_param['Received'] = None
    dict_param['Debug'] = False
    dict_param['Best'] = False
    dict_param['Output'] = 'file'
    dict_param['Caption'] = False
    if args != []:
        dict_param['dbname'] = args[0]
    else:
        dict_param['dbname'] = '/SCIA/share/db/sron_scia.db'

    for opt, arg in opts[:]:
        if opt in ('-h', '--help'):
            help_message()
        elif opt in ('-V', '--version'):
            version_message()
        elif opt == '--debug':
            dict_param['Debug'] = True
        elif opt == '--level':
            if arg != '':
                dict_param['Level'] = arg
            else:
                print( opt + ' expect an argument' )
                sys.exit(1)
        elif opt == '--orbit':
            if arg != '':
                dict_param['Orbit'] = arg
            else:
                print( opt + ' expect an argument' )
                sys.exit(1)
        elif opt == '--name':
            if arg != '':
                dict_param['Name'] = arg
            else:
                print( opt + ' expect an argument' )
                sys.exit(1)
        elif opt in ( '--proc', '--procstage'):
            if arg != '':
                dict_param['ProcStage'] = arg
            else:
                print( opt + ' expect an argument' )
                sys.exit(1)
        elif opt in ( '--soft', '--softVersion'):
            if arg != '':
                dict_param['softVersion'] = arg
            else:
                print( opt + ' expect an argument' )
                sys.exit(1)
        elif opt == '--date':
            if arg != '':
                dict_param['Date'] = arg
                if len( dict_param['Date'] ) < 4:
                    print( 'You should atleast specify a year' )
                    sys.exit(1)
            else:
                print( opt + ' expect an argument' )
                sys.exit(1)
        elif opt == '--rtime':
            if arg != '':
                if arg[-1] == 'h':
                    dd = datetime.utcnow() - timedelta(hours=atoi(arg[0:-1]))
                elif arg[-1] == 'd':
                    dd = datetime.utcnow() - timedelta(days=atoi(arg[0:-1]))
                else:
                    print( opt + ' expects xh or xd' )
                    sys.exit(1)
                dict_param['Received'] = \
                    dd.replace( microsecond=0 ).isoformat( ' ' )
            else:
                print( opt, ' expect an argument' )
                sys.exit(1)
        elif opt in ( '--best', '--last' ):
            dict_param['Best'] = True
        elif opt == '--file':
            dict_param['Output'] = 'file'
        elif opt == '--header':
            dict_param['Output'] = 'header'
        elif opt == '--list':
            dict_param['Output'] = 'list'
        elif opt == '--caption':
            dict_param['Caption'] = True

    if dict_param['Output'] != 'file' and dict_param['Best']:
        print( ' Error: option --best only works in combination with --file' )
        sys.exit(1)

    return dict_param

#+++++++++++++++++++++++++
def GetTableNames( dbname ):

    cx = sqlite3.connect( dbname )
    cu = cx.cursor()
    cu.execute( 'PRAGMA database_list' )
    exec_str = 'SELECT name FROM sqlite_master ' \
               + ' WHERE type="table" ORDER BY name'
    cu.execute( exec_str )
    tables = {}
    tables['useTBL'] = []
    while 1:
        table = cu.fetchone()
        if table == None: break
        tables[table[0]] = []
        tables['useTBL'].append( 0 )

    for table in tables.keys():
        cu.execute( 'PRAGMA table_info(' + table + ')' )
        while 1:
            row = cu.fetchone()
            if row == None: break
            tables[table].append( row[1] )

    cu.close()
    cx.close()

    return tables

#+++++++++++++++++++++++++
def BuildWhereList( dict_param ):

    where_lst = []
#
# selection on the Header table(s)
#
    if dict_param['Name']:
        if dict_param['Name'][0:10] == 'SCI_NL__0P':
	    dict_param['Level'] = '0'
            HeaderTable = 'meta__0P.'
        elif dict_param['Name'][0:10] == 'SCI_NL__1P':
	    dict_param['Level'] = '1'
            HeaderTable = 'meta__1P.'
        elif dict_param['Name'][0:10] == 'SCI_OL__2P':
	    dict_param['Level'] = '2'
            HeaderTable = 'meta__2P.'
        else:
            print( 'Unknown product -> no entry' )
            sys.exit(1)

        mystr = HeaderTable + 'name=\'' + dict_param['Name'] + '\''
        where_lst.append( mystr )
    elif dict_param['Level']:
        if dict_param['Level'] == '0':
            HeaderTable = 'meta__0P.'
        elif dict_param['Level'] == '1':
            HeaderTable = 'meta__1P.'
        elif dict_param['Level'] == '2':
            HeaderTable = 'meta__2P.'
        else:
            print( 'Unknown product -> no entry' )
            sys.exit(1)
    else:
        print( 'Please select at least on name or product level' )
        sys.exit(1)

# define list of tables
    tbl_meta = HeaderTable[:-1]

# handle all optional selection criteria
    if dict_param['Received']:
        mystr = HeaderTable \
                 + 'receiveDate >= \'' + dict_param['Received'] + '\''
        where_lst.append( mystr )

    if dict_param['ProcStage']:
        if len( dict_param['ProcStage'] ) == 1:
            where_lst.append( HeaderTable + 'procStage=\'' \
                              + dict_param['ProcStage'] + '\'' )
        else:
            mystr = HeaderTable + 'procStage IN ('
            for c in dict_param['ProcStage']:
                if mystr[-1] != '(':  mystr += ','
                mystr += '\'' + c + '\''
            mystr += ')'
            where_lst.append( mystr )
#
# the following restriction can be applied to state_info or meta__?P
#
    if dict_param['softVersion']:
        tableName = HeaderTable
        where_lst.append( HeaderTable + 'softVersion like \'' \
                          + dict_param['softVersion'] + '\'' )

    if dict_param['Orbit']:
        tableName = HeaderTable
        
        orbitList = dict_param['Orbit'].split(',')
        if len( orbitList ) == 1:
            mystr = tableName + 'absOrbit=' + orbitList[0]
        else:
            mystr = tableName + 'absOrbit BETWEEN '\
                    +  orbitList[0] + ' AND ' + orbitList[1]
        where_lst.append( mystr )

    if dict_param['Date']:
        year = int(dict_param['Date'][0:4])
        if year < 2002:
            print( 'You should specify a valid year' )
            sys.exit(1)
        dtime = 'year'

        if len( dict_param['Date'] ) >= 6:
            month = int(dict_param['Date'][4:6])
            if month < 1 or month > 12:
                print( 'You should specify a valid month' )
                sys.exit(1)
            dtime = 'month'
        else:
            month = 1

        if len( dict_param['Date'] ) >= 8:
            day = int(dict_param['Date'][6:8])
            if day < 1 or day > 31:
                print( 'You should specify a valid day' )
                sys.exit(1)
            dtime = 'day'
        else:
            day = 1
        
        if len( dict_param['Date'] ) >= 10:
            hour = int(dict_param['Date'][8:10])
            if hour < 0 or hour > 24:
                print( 'You should specify a valid hour' )
                sys.exit(1)
            dtime = 'hour'
        else:
            hour = 0
        
        if len( dict_param['Date'] ) >= 12:
            minu = int(dict_param['Date'][10:12])
            if minu < 0 or minu > 59:
                print( 'You should specify a valid minu' )
                sys.exit(1)
            dtime = 'minu'
        else:
            minu = 0
        secnd = 0
        d1 = datetime( year, month, day, hour, minu, secnd )

        if dtime == 'year':
            d2 = datetime( year+1, month, day, hour, minu, secnd )
        elif dtime == 'month':
            month += 1
            if month > 12:
                year += 1
                month = 1
            d2 = datetime( year, month, day, hour, minu, secnd )
        elif dtime == 'day':
            dd = timedelta(days=1)
            d2 = d1 + dd
        elif dtime == 'hour':
            dd = timedelta(hours=1)
            d2 = d1 + dd
        else:
            dd = timedelta(hours=1)
            d2 = d1 + dd

        mystr = HeaderTable + 'dateTimeStart BETWEEN \'' \
            + d1.isoformat(' ') + '\' AND \'' + d2.isoformat(' ') + '\''
        where_lst.append( mystr )

    return (tbl_meta, where_lst)

#+++++++++++++++++++++++++
def QueryFile( tbl_meta, where_tbl, rows='file' ):

    # combine where-actions
    where_pre = None
    where_opt = None
    where_expr = None
    if where_tbl.has_key( tbl_meta ):
        where_expr = where_tbl[tbl_meta] 

    # build a list of rows to be shown by the select statement
    if rows == 'header':
        row_lst = '*'
        where_opt = ' ORDER BY absOrbit,procStage,procTime'
    elif rows == 'list':
        row_lst = 'absOrbit,name,compression,path,receiveDate,fileSize'
        where_opt = ' ORDER BY absOrbit,procStage,procTime'
    elif rows == 'fileBest':
        row_lst = 'path,name,compression'
        if tbl_meta == 'meta__0P':
            where_pre = ' AS s1 JOIN (SELECT absOrbit,MAX(q_flag) AS qf from '
            where_pre += tbl_meta
            where_opt = ' GROUP by absOrbit) AS s2 ON s1.absOrbit=s2.absOrbit'
            where_opt += ' AND s1.q_flag=s2.qf;'
        else:
            where_pre = ' AS s1 JOIN (SELECT absOrbit,MAX(procStage) AS proc'
            where_pre += ' from ' + tbl_meta
            where_opt = ' GROUP by absOrbit) AS s2 ON s1.absOrbit=s2.absOrbit'
            where_opt += ' AND s1.procStage=s2.proc;'
    else:                                 # rows == 'file'
        row_lst = 'path,name,compression'
        where_opt = ' ORDER BY absOrbit,procStage,procTime'

    #
    # set up SELECT query as:
    # SELECT 'row_lst' FROM 'from_lst' WHERE 'where_expr' 'where_opt'
    #
    query_str = 'SELECT ' + row_lst + ' FROM ' + tbl_meta
    if where_pre:  query_str += where_pre
    if where_expr: query_str += ' WHERE ' + where_expr
    if where_opt:  query_str += where_opt

    return query_str

#+++++++++++++++++++++++++ Main Routine +++++++++++++++++++++++++
def inquire_db( dict_param ):
    from os.path import isfile

# first, check if database exists
    if isfile( dict_param['dbname'] ) == 0:
        print( 'Database: ', dict_param['dbname'], 
               ' does not exists or is unreadable' )
        sys.exit(1)

# secondly, try to obtain the definition of its tables
    table_dict = GetTableNames( dict_param['dbname'] )

# ---
# build SELECT-action
    (tbl_meta, where_lst) = BuildWhereList( dict_param )
    if dict_param['Debug']:
        print( 'where_lst: ', where_lst, '\n' )

# build where-action split over the main tables (state/meta)
    where_tbl = {}
    for mystr in where_lst:
        tbl_name = mystr.split('.')[0].strip('(')
        
        if where_tbl.has_key( tbl_name ):
            mystr += ' AND ' + where_tbl[tbl_name]
        where_tbl[tbl_name] = mystr
    if dict_param['Debug']:
        print( 'where_tbl: ', where_tbl, '\n' )

# build the whole query
    if dict_param['Output'] == 'header':
        if dict_param['Caption']:
            print( '#', ' '.join(str(x) for x in table_dict[tbl_meta]) )
        query_str = QueryFile( tbl_meta, where_tbl, rows='header' )
    elif dict_param['Output'] == 'list':
        if dict_param['Caption']:
            print( '#' )
            print( '# orbit_num\tfile_name'
                   + '\tdirectory\tdate (yyyymmddhh)\tsize (bytes)' )
            print( '#' )
        query_str = QueryFile( tbl_meta, where_tbl, rows='list' )
    else:
        rows = 'file'
        if dict_param['Best']: rows += 'Best'
        query_str = QueryFile( tbl_meta, where_tbl, rows=rows )

    if query_str == '': sys.exit(0)
    if dict_param['Debug']:
        print( 'query_str: ', query_str )
        sys.exit(0)

# finaly, do the actual select-action
    cx = sqlite3.connect( dict_param['dbname'] )
    cu = cx.cursor()
    cu.execute( query_str )
    for row in cu:
        if dict_param['Output'] == 'file':
            if row[2] == 0:
                print( row[0] + '/' + row[1] )
            else:
                print( row[0] + '/' + row[1] + '.gz' )
        elif dict_param['Output'] == 'list':
            rtime = strftime('%Y%m%d%H', strptime(row[4], '%Y-%m-%d %H:%M:%S'))
            if row[2] == 0:
                print( '%05d\t %s     %s   %s  %10d' % 
                       (row[0], row[1], row[3], rtime, row[5]) )
            else:
                print( '%05d\t %s.gz  %s   %s  %10d' %
                       (row[0], row[1], row[3], rtime, row[5]) )
        else:
            print( ' '.join(str(x) for x in row) )

    cu.close()
    cx.close()

#+++++++++++++++++++++++++
if __name__ == '__main__':
    dict_param = get_opts()
    inquire_db( dict_param )
