#!/usr/bin/env python

# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

#
from __future__ import print_function
from __future__ import division

import os.path
import sqlite3

#--------------------------------------------------
def update_sqlite_gosat( args ):
    import string
    import numpy

    fp = open( args.input_file, 'r' )

    startTime = None
    cldIfovNyes = None
    cldIfovNno = None
    cldIfovFlags = []
    cldOfovNyes = None
    cldOfovNno = None
    cldOfovFlags = []
    zsfMet = None
    zsfMetMin = None
    zsfMetMax = None
    windU = None
    windV = None

    while True:
        line = fp.readline()
        if not line: break

        if 'Index_SWIR' in line:
            line = fp.readline()
            numFrame = len(line.split())

        if 'Date/Time' in line:
            startTime = []
            for x in xrange(numFrame):
                startTime.append('')

            line = fp.readline()  # year
            ii = 0
            for str in line.split():
                startTime[ii] = str
                ii += 1
            line = fp.readline()  # month
            ii = 0
            for str in line.split():
                startTime[ii] += '-' + str
                ii += 1
            line = fp.readline()  # day
            ii = 0
            for str in line.split():
                startTime[ii] += '-' + str
                ii += 1
            line = fp.readline()  # hour
            ii = 0
            for str in line.split():
                startTime[ii] += ' ' + str
                ii += 1
            line = fp.readline()  # minute
            ii = 0
            for str in line.split():
                startTime[ii] += ':' + str
                ii += 1
            line = fp.readline()  # second
            ii = 0
            for str in line.split():
                startTime[ii] += ':%09.6f' % float(str)
                ii += 1

        if 'Cloud Conf. Inner Field Of View' in line:
            line = fp.readline()
            cldIfovNyes = numpy.array( [int(str) for str in line.split()] )
            line = fp.readline()
            cldIfovNno = numpy.array( [int(str) for str in line.split()] )
            cldIfovFlags = numpy.zeros( (numFrame,16), dtype='int')
            for x in xrange(16):
                line = fp.readline()
                cldIfovFlags[:,x] = [int(str) for str in line.split()]
        if 'Cloud Conf. Outer Field Of View' in line:
            num = int(line.split()[0])
            line = fp.readline()
            cldOfovNyes = numpy.array( [int(str) for str in line.split()] )
            line = fp.readline()
            cldOfovNno = numpy.array( [int(str) for str in line.split()] )
            cldOfovFlags = numpy.zeros( (numFrame,16), dtype='int')
            for x in xrange(16):
                line = fp.readline()
                cldOfovFlags[:,x] = [int(str) for str in line.split()]
        if 'GTOPO30 surface' in line:
            line = fp.readline()
            zsfMet = numpy.array( [float(str) for str in line.split()] )

        if 'GTOPO30 min surface' in line:
            line = fp.readline()
            zsfMetMin = numpy.array( [float(str) for str in line.split()] )

        if 'GTOPO30 max surface' in line:
            line = fp.readline()
            zsfMetMax = numpy.array( [float(str) for str in line.split()] )

        if 'wind u' in line:
            line = fp.readline()
            windU = numpy.array( [float(str) for str in line.split()] )

        if 'wind v' in line:
            line = fp.readline()
            windV = numpy.array( [float(str) for str in line.split()] )
    fp.close()
    if args.debug:
        print( startTime )
        print( cldIfovNyes, cldIfovNno, cldIfovFlags )
        print( cldOfovNyes, cldOfovNno, cldOfovFlags )
        print( zsfMet, zsfMetMin, zsfMetMax )
        print( windU, windV )

    # update database
    con = sqlite3.connect( args.dbname )
    cur = con.cursor()
    cur.execute( 'PRAGMA synchronous=OFF' )

    query_str = 'select ROWID from frameAttributes where (dateTimeStart > \'%s\' and dateTimeStart < \'%s\')'
    ii = 0
    for str in startTime:
        sec = float(str[17:23])
        s1 = str[0:17]+"%06.3f" % sec
        s2 = str[0:17]+"%06.3f" % (sec+0.001)
        if args.debug:
            print( query_str % (s1,s2) )
        cur.execute( query_str % (s1,s2) )
        row = cur.fetchone()
        if row == None:
            print( 'Falied to update row with dateTimeStart=', str )
            continue
        where_str = ' where ROWID=%-d' % row[0]

        update_str = ['update frameAttributes set cldIfovNyes=%-d' 
                      % cldIfovNyes[ii]]
        update_str.append( ',cldIfovNno=%-d' % cldIfovNno[ii] )
        for x in xrange(16):
            update_str.append( ',cldIfovFlag_%02d=%-d' 
                               % ((x+1), cldIfovFlags[ii,x]) )
        update_str.append( ',cldOfovNyes=%-d' % cldOfovNyes[ii] )
        update_str.append( ',cldOfovNno=%-d' % cldOfovNno[ii] )
        for x in xrange(16):
            update_str.append( ',cldOfovFlag_%02d=%-d' 
                               % ((x+1), cldOfovFlags[ii,x]) )
        update_str.append( ',zsfMet=%-f' % zsfMet[ii] )
        update_str.append( ',zsfMetMin=%-f' % zsfMetMin[ii] )
        update_str.append( ',zsfMetMax=%-f' % zsfMetMax[ii] )
        update_str.append( ',windU=%-f' % windU[ii] )
        update_str.append( ',windV=%-f' % windV[ii] )
        update_str.append( where_str )
        if args.debug:
            print( ''.join(update_str) )
        else:
            cur.execute( ''.join(update_str) )
        ii += 1
    update_str = 'update tfts__1P set flagPreProcess=1 where name=\'%s\''
    if args.debug:
        print( update_str % os.path.basename(args.input_file)[0:-4] )
    else:
        cur.execute( update_str % os.path.basename(args.input_file)[0:-4] )
    cur.close()
    con.commit()
    con.close()

#--------------------------------------------------
def check_sqlite_gosat( dbname, gosatfl, replace=False ):
    query_str = 'select flagPreProcess from tfts__1P where name=\'%s\''
    if gosatfl[0:9] == 'GOSATTFTS':
        table = 'tfts__1P'
    else:
        return True

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    cur.execute( query_str % gosatfl[0:-4] )
    row = cur.fetchone()
    cur.close()
    con.close()
    if row == None: 
        return False       # file not in database
    elif not replace and row[0] == True:
        return False       # file already preprocessed (no replace)
    else:
        return True        # replace or file not yet preprocessed

#- main code -------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument( '--debug', action='store_true', default=False,
                         help='show what will be done, but do nothing' )
    parser.add_argument( '--replace', action='store_true', default=False,
                         help='replace SQL data of INPUT_FILE in database' )
    parser.add_argument( '--dbname', dest='dbname', type=str,
                         default='sron_gosat.db', 
                         help='name of GOSAT/SQLite database' )
    parser.add_argument( 'input_file', nargs='?', type=str,
                         help='read from INPUT_FILE (a FTS ini-file)' )
    args = parser.parse_args()

    gosat_fl = os.path.basename( args.input_file )
    if not os.path.isfile( args.input_file ) \
            or gosat_fl[0:9] != 'GOSATTFTS' or gosat_fl[-3:] != 'ini':
        print( 'Info: \"%s\" is not a valid GOSAT/FTS ini-file' % args.input_file )
        sys.exit(1)

    if not os.path.isfile( args.dbname ):
        print( 'Info: \"%s\" is not a valid file' % args.dbname )
        sys.exit(1)

     # Check if product is already in database
    if not check_sqlite_gosat( args.dbname, gosat_fl, args.replace ):
        print( 'Info: %s is not in database or already preprocessed' % gosat_fl )
        sys.exit(1)

    update_sqlite_gosat( args )
