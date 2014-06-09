#!/usr/bin/env python

# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

#
from __future__ import print_function
from __future__ import division

import os.path
import gzip
import sqlite3

from random   import randint
from datetime import datetime

#--------------------------------------------------
def cre_sqlite_scia_db( dbname ):
    con = sqlite3.connect( dbname )
    cur = con.cursor()
    cur.execute( '''create table meta__0P (
	name		text PRIMARY KEY,
	path            text NOT NULL,
	compression	boolean NOT NULL,
	procStage	char(1) NOT NULL,
	procCenter	text NOT NULL,
	softVersion	text NOT NULL,
	qualityFlag     char(7) NOT NULL default 'UNKNOWN',
	receiveDate	datetime NOT NULL default '0000-00-00 00:00:00',
	procTime        datetime NOT NULL default '0000-00-00 00:00:00',
	dateTimeStart	datetime NOT NULL default '0000-00-00 00:00:00',
        muSeconds       integer NOT NULL,
        duration        float NOT NULL,
	absOrbit	integer NOT NULL,
	relOrbit	smallint NOT NULL,
	numDataSets	smallint NOT NULL,
	fileSize	integer NOT NULL,
        q_flag          integer NOT NULL
       )''' )
    cur.execute( 'create index absOrbitIndex0 on meta__0P(absOrbit)' )
    cur.execute( '''create table meta__1P (
	name		text PRIMARY KEY,
	path            text NOT NULL,
	compression	boolean NOT NULL,
	procStage	char(1) NOT NULL,
	procCenter	text NOT NULL,
	softVersion	text NOT NULL,
	keydataVersion	text NOT NULL,
	mFactorVersion	text NOT NULL,
	spectralCal	char(5) NOT NULL,
	saturatedPix	char(5) NOT NULL,
	deadPixels	char(5) NOT NULL,
	qualityFlag     char(7) NOT NULL default 'UNKNOWN',
	receiveDate	datetime NOT NULL default '0000-00-00 00:00:00',
	procTime        datetime NOT NULL default '0000-00-00 00:00:00',
	dateTimeStart	datetime NOT NULL default '0000-00-00 00:00:00',
        muSeconds       integer NOT NULL,
        duration        float NOT NULL,
	absOrbit	integer NOT NULL,
	relOrbit	smallint NOT NULL,
	numDataSets	smallint NOT NULL,
	nadirStates	smallint NOT NULL,
	limbStates	smallint NOT NULL,
	occulStates	smallint NOT NULL,
	monitorStates	smallint NOT NULL,
	noProcStates	smallint NOT NULL,
	fileSize	integer NOT NULL
       )''' )
    cur.execute( 'create index absOrbitIndex1 on meta__1P(absOrbit)' )
    cur.execute( '''create table meta__2P (
	name		text PRIMARY KEY,
	path            text NOT NULL,
	compression	boolean NOT NULL,
	procStage	char(1) NOT NULL,
	procCenter	text NOT NULL,
	softVersion	text NOT NULL,
	fittingErrSum	char(5) NOT NULL default 'GOOD',
	qualityFlag     char(7) NOT NULL default 'UNKNOWN',
	receiveDate	datetime NOT NULL default '0000-00-00 00:00:00',
	procTime        datetime NOT NULL default '0000-00-00 00:00:00',
	dateTimeStart	datetime NOT NULL default '0000-00-00 00:00:00',
        muSeconds       integer NOT NULL,
        duration        float NOT NULL,
	absOrbit	integer NOT NULL,
	relOrbit	smallint NOT NULL,
	numDataSets	smallint NOT NULL,
	nadirProducts	text NOT NULL,
	limbProducts	text NOT NULL,
	fileSize	integer NOT NULL
       )''' )
    cur.execute( 'create index absOrbitIndex2 on meta__2P(absOrbit)')
    cur.execute( '''create table quality_definition (
       flag    integer PRIMARY KEY,
       id      text NOT NULL UNIQUE,
       descr   text NOT NULL
      )''' )
    cur.execute( '''insert into quality_definition values( 0, 'UNKNOWN', 
       'Default value should not be used in database' )''' )
    cur.execute( '''insert into quality_definition values( 1, 'REJECT', 
       'Obsolete product will be removed from archive' )''' )
    cur.execute( '''insert into quality_definition values( 2, '2SHORT', 
       'Product contains less MDS than expected' )''' )
    cur.execute( '''insert into quality_definition values( 3, 'ORB_OFF', 
       'Orbit number is not correct' )''' )
    cur.execute( '''insert into quality_definition values( 4, 'GOOD', 
       'Default flag for NRT products' )''' )
    cur.execute( '''insert into quality_definition values( 5, '2LONG', 
       'Product contains more MDS than expected' )''' )
    cur.execute( '''insert into quality_definition values( 6, 'ERROR', 
       'Product errors detected - use with care, cl0 only' )''' )
    cur.execute( '''insert into quality_definition values( 7, 'SOLOMON', 
       'Too many Reed Solomon corrections - use with care, cl0 only' )''' )
    cur.execute( '''insert into quality_definition values( 8, 'NOT_USED8', 
       'Flag not yet used in database' )''' )
    cur.execute( '''insert into quality_definition values( 9, 'SODAP', 
       'Consolidated SODAP product - no verification' )''' )
    cur.execute( '''insert into quality_definition values( 10, 'CONS', 
       'File checked and passed succesfully' )''' )
    cur.close()
    con.commit()
    con.close()

def get_scia_level( sciafl ):
    if sciafl[0:10] == 'SCI_NL__0P':
        level = 0
    elif sciafl[0:10] == 'SCI_NL__1P':
        level = 1
    elif sciafl[0:10] == 'SCI_OL__2P':
        level = 2
    elif sciafl[0:10] == 'SCI_NL__2P':
        level = 2
    else:
        print( 'Level of Sciamachy product is unknown' )
        level = -1
    return level

#--------------------------------------------------
def del_sqlite_scia( dbname, sciafl ):
    level = get_scia_level(sciafl)
    if level < 0: return

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    query_str = 'select name from meta__%-dP where name=\'%s\''
    cur.execute( query_str % (level, sciafl) )
    row = cur.fetchone()
    if row == None: return

    query_str = 'delete from meta__%-dP where name=\'%s\''
    cur.execute( query_str % (level, sciafl) )
    cur.close()
    con.commit()
    con.close()

#--------------------------------------------------
def check_sqlite_scia( dbname, sciafl ):
    level = get_scia_level(sciafl)
    if level < 0: return True

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    query_str = 'select name from meta__%-dP where name=\'%s\''
    cur.execute( query_str % (level, sciafl) )
    row = cur.fetchone()
    cur.close()
    con.commit()
    con.close()
    if row == None: 
        return False
    else:
        return True

#--------------------------------------------------
def read_scia_lv0( sciafl ):
    nr = 0
    dict_hdr = {}

    if '.gz' in sciafl:
        fp = gzip.open( sciafl, "rb" )
    else:
        fp = open( sciafl, "rb" )

    while nr < 50:
        line = fp.readline()
        if not line: break
        nr += 1

        words = line.split( '=' )
        if len( words ) < 2: continue

        if words[0] == "PRODUCT":
            dict_hdr['product'] = words[1][1:-2]
        elif words[0] == "PROC_STAGE":
            dict_hdr['procStage'] = words[1][0:-1]
        elif words[0] == "PROC_CENTER":
            dict_hdr['procCenter'] = words[1][1:-2].rstrip()
        elif words[0] == "PROC_TIME":
            dt = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            if dt.microsecond == 0:
                dt = dt.replace( microsecond=randint(0,99999) )
            dict_hdr['procTime'] = dt.strftime( '%Y-%m-%d %H:%M:%S.%f' )
        elif words[0] == "SOFTWARE_VER":
            dict_hdr['softVersion'] = words[1][1:-2].rstrip()
        elif words[0] == "SENSING_START":
            dt1 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            dict_hdr['dateTimeStart'] = dt1.strftime( '%Y-%m-%d %H:%M:%S' )
            dict_hdr['muSeconds' ] = dt1.microsecond
        elif words[0] == "SENSING_STOP":
            dt2 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
        elif words[0] == "REL_ORBIT":
            dict_hdr['relOrbit'] = int( words[1] )
        elif words[0] == "ABS_ORBIT":
            dict_hdr['absOrbit'] = int( words[1] )
        elif words[0] == "TOT_SIZE":
            dict_hdr['fileSize'] = int( words[1][0:21] )
        elif words[0] == "NUM_DATA_SETS":
            dict_hdr['numDataSets'] = int( words[1] )
        elif words[0] == "SPH_DESCRIPTOR":
            break

    fp.close()

    dt = dt2 - dt1
    dict_hdr['duration'] = dt.seconds + dt.microseconds / 1e6
    return dict_hdr

#++++++++++++++++++++++++++++++++++++++++++++++++++
def read_scia_lv1( sciafl ):
    nr = 0
    dict_hdr = {}

    if sciafl.find( '.gz' ) != -1:
        fp = gzip.open( sciafl, "rb" )
    else:
        fp = open( sciafl, "rb" )

    while nr < 100:
        line = fp.readline()
        if not line: break
        nr += 1

        words = line.split( '=' )
        if len( words ) < 2: continue

        if words[0] == "PRODUCT":
            dict_hdr['product'] = words[1][1:-2]
        elif words[0] == "PROC_STAGE":
            dict_hdr['procStage'] = words[1][0:-1]
        elif words[0] == "PROC_CENTER":
            dict_hdr['procCenter'] = words[1][1:-2].rstrip()
        elif words[0] == "PROC_TIME":
            dt = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            if dt.microsecond == 0:
                dt = dt.replace( microsecond=randint(0,99999) )
            dict_hdr['procTime'] = dt.strftime( '%Y-%m-%d %H:%M:%S.%f' )
        elif words[0] == "SOFTWARE_VER":
            dict_hdr['softVersion'] = words[1][1:-2].rstrip()
        elif words[0] == "REL_ORBIT":
            dict_hdr['relOrbit'] = int( words[1] )
        elif words[0] == "ABS_ORBIT":
            dict_hdr['absOrbit'] = int( words[1] )
        elif words[0] == "TOT_SIZE":
            dict_hdr['fileSize'] = int( words[1][0:21] )
        elif words[0] == "NUM_DATA_SETS":
            dict_hdr['numDataSets'] = int( words[1] )
        elif words[0] == "START_TIME":
            dt1 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            dict_hdr['dateTimeStart'] = dt1.strftime( '%Y-%m-%d %H:%M:%S' )
            dict_hdr['muSeconds' ] = dt1.microsecond
        elif words[0] == "STOP_TIME":
            dt2 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
        elif words[0] == "KEY_DATA_VERSION":
            dict_hdr['keydataVersion'] = words[1][1:-2].rstrip()
        elif words[0] == "M_FACTOR_VERSION":
            dict_hdr['mFactorVersion'] = words[1][1:-2].rstrip()
        elif words[0] == "SPECTRAL_CAL_CHECK_SUM":
            dict_hdr['spectralCal'] = words[1][1:-2].rstrip()
        elif words[0] == "SATURATED_PIXEL":
            dict_hdr['saturatedPix'] = words[1][1:-2]
        elif words[0] == "DEAD_PIXEL":
            dict_hdr['deadPixels'] = words[1][1:-2].rstrip()
        elif words[0] == "NO_OF_NADIR_STATES":
            dict_hdr['nadirStates'] = int( words[1] )
        elif words[0] == "NO_OF_LIMB_STATES":
            dict_hdr['limbStates'] = int( words[1] )
        elif words[0] == "NO_OF_OCCULTATION_STATES":
            dict_hdr['occulStates'] = int( words[1] )
        elif words[0] == "NO_OF_MONI_STATES":
            dict_hdr['monitorStates'] = int( words[1] )
        elif words[0] == "NO_OF_NOPROC_STATES":
            dict_hdr['noProcStates'] = int( words[1] )
        elif words[0] == "DS_NAME":
            break

    fp.close()

    dt = dt2 - dt1
    dict_hdr['duration'] = dt.seconds + dt.microseconds / 1e6
    return dict_hdr

#++++++++++++++++++++++++++++++++++++++++++++++++++
def read_scia_lv2( sciafl ):
    nr = 0
    dict_hdr = {}
    nadirProducts = []
    limbProducts = []

    if sciafl.find( '.gz' ) != -1:
        fp = gzip.open( sciafl, "rb" )
    else:
        fp = open( sciafl, "rb" )

    while nr < 150:
        line = fp.readline()
        if not line: break
        nr += 1

        words = line.split( '=' )
        if len( words ) < 2: continue

        if words[0] == "PRODUCT":
            dict_hdr['product'] = words[1][1:-2]
        elif words[0] == "PROC_STAGE":
            dict_hdr['procStage'] = words[1][0:-1]
        elif words[0] == "PROC_CENTER":
            dict_hdr['procCenter'] = words[1][1:-2].rstrip()
        elif words[0] == "PROC_TIME":
            dt = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            if dt.microsecond == 0:
                dt = dt.replace( microsecond=randint(0,99999) )
            dict_hdr['procTime'] = dt.strftime( '%Y-%m-%d %H:%M:%S.%f' )
        elif words[0] == "SOFTWARE_VER":
            dict_hdr['softVersion'] = words[1][1:-2].rstrip()
        elif words[0] == "REL_ORBIT":
            dict_hdr['relOrbit'] = int( words[1] )
        elif words[0] == "ABS_ORBIT":
            dict_hdr['absOrbit'] = int( words[1] )
        elif words[0] == "TOT_SIZE":
            dict_hdr['fileSize'] = int( words[1][0:21] )
        elif words[0] == "NUM_DATA_SETS":
            dict_hdr['numDataSets'] = int( words[1] )
        elif words[0] == "START_TIME":
            dt1 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            dict_hdr['dateTimeStart'] = dt1.strftime( '%Y-%m-%d %H:%M:%S' )
            dict_hdr['muSeconds' ] = dt1.microsecond
        elif words[0] == "STOP_TIME":
            dt2 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
        elif words[0] == "FITTING_ERROR_SUM":
            dict_hdr['fittingErrSum'] = words[1][1:-2].rstrip()
        elif words[0].find( "NAD_FIT_WINDOW_" ) != -1 \
                and words[1].find( "EMPTY" ) == -1:
            if nadirProducts: nadirProducts.append( ',' )
            nadirProducts.append( words[1][1:-2].replace('- ','-').strip() )
        elif words[0].find( "LIM_FIT_WINDOW_" ) != -1 \
                and words[1].find( "EMPTY" ) == -1:
            if limbProducts: limbProducts.append( ',' )
            limbProducts.append( words[1][1:-2].replace('- ','-').strip() )
        elif words[0] == "DS_NAME":
            break
    fp.close()

    dt = dt2 - dt1
    dict_hdr['duration'] = dt.seconds + dt.microseconds / 1e6
    dict_hdr['nadirProducts'] = ''.join( nadirProducts )
    dict_hdr['limbProducts'] = ''.join( limbProducts )
    return dict_hdr

#--------------------------------------------------
def add_sqlite_scia( dbname, dict_scia ):
    if dict_scia['procStage'] == 'N':
        dict_scia['qualityFlag'] = 'GOOD'
        dict_scia['q_flag'] = 5
    else:
        dict_scia['qualityFlag'] = 'CONS'
        dict_scia['q_flag'] = 10

    if dict_scia['level'] == 0:
        str_sql = 'insert into meta__0P values' \
            '(\'%(product)s\',\'%(filePath)s\',%(compress)d,\'%(procStage)s\''\
            ',\'%(procCenter)s\',\'%(softVersion)s\',\'%(qualityFlag)s\''\
            ',\'%(receiveDate)s\',\'%(procTime)s\',\'%(dateTimeStart)s\''\
            ',%(muSeconds)d,%(duration)f,%(absOrbit)d,%(relOrbit)d'\
            ',%(numDataSets)d,%(fileSize)d,%(q_flag)d)'
    elif dict_scia['level'] == 1:
        str_sql = 'insert into meta__1P values' \
            '(\'%(product)s\',\'%(filePath)s\',%(compress)d,\'%(procStage)s\''\
            ',\'%(procCenter)s\',\'%(softVersion)s\',\'%(keydataVersion)s\''\
            ',\'%(mFactorVersion)s\',\'%(spectralCal)s\',\'%(saturatedPix)s\''\
            ',\'%(deadPixels)s\',\'%(qualityFlag)s\''\
            ',\'%(receiveDate)s\',\'%(procTime)s\',\'%(dateTimeStart)s\''\
            ',%(muSeconds)d,%(duration)f,%(absOrbit)d,%(relOrbit)d'\
            ',%(numDataSets)d,%(nadirStates)d,%(limbStates)d,%(occulStates)d'\
            ',%(monitorStates)d,%(noProcStates)d,%(fileSize)d)'
    elif dict_scia['level'] == 2:
        str_sql = 'insert into meta__2P values' \
            '(\'%(product)s\',\'%(filePath)s\',%(compress)d,\'%(procStage)s\''\
            ',\'%(procCenter)s\',\'%(softVersion)s\',\'%(fittingErrSum)s\''\
            ',\'%(qualityFlag)s\',\'%(receiveDate)s\',\'%(procTime)s\''\
            ',\'%(dateTimeStart)s\',%(muSeconds)d,%(duration)f,%(absOrbit)d'\
            ',%(relOrbit)d,%(numDataSets)d,\'%(nadirProducts)s\''\
            ',\'%(limbProducts)s\',%(fileSize)d)'
    else:
        return

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    cur.execute( str_sql % dict_scia )
    cur.close()
    con.commit()
    con.close()

#- main code -------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    import sys
    from time import gmtime, strftime

    parser = argparse.ArgumentParser()
    parser.add_argument( '--debug', action='store_true', default=False,
                         help='show what will be done, but do nothing' )
    parser.add_argument( '--remove', action='store_true', default=False,
                         help='remove SQL data of INPUT_FILE from database' )
    parser.add_argument( '--replace', action='store_true', default=False,
                         help='replace SQL data of INPUT_FILE in database' )
    parser.add_argument( '--tmpPath', dest='tempDir', type=str,
                         default='/dev/shm', 
			 help='path to directory to store tempory files' )
    parser.add_argument( '--dbname', dest='dbname', type=str,
                         default='/SCIA/share/db/sron_scia.db', 
			 help='name of SCIA/SQLite database' )
    parser.add_argument( 'input_file', nargs='?', type=str,
                         help='read from INPUT_FILE' )
    args = parser.parse_args()

    sciafl = os.path.basename( args.input_file )
    sciaLevel = get_scia_level( sciafl )
    if sciaLevel < 0:
        print( 'Info: \'%s\' is not a valid Sciamachy product' % args.input_file )
        sys.exit(0)

    if not os.path.isfile( args.input_file ):
        print( 'Info: \"%s\" is not a valid file' % args.input_file )
        sys.exit(1)

    if not os.path.isfile( args.dbname ):
        cre_sqlite_scia_db( args.dbname )

    # Check if product is already in database
    if not args.debug:
        if args.remove or args.replace:
            del_sqlite_scia( args.dbname, sciafl )
        elif check_sqlite_scia( args.dbname, sciafl ):
            print( 'Info: %s is already stored in database' % sciafl )
            sys.exit(0)
        if args.remove: sys.exit(0)

    if sciaLevel == 0:
        dict_scia = read_scia_lv0( args.input_file )
    elif sciaLevel == 1:
        dict_scia = read_scia_lv1( args.input_file )
    elif sciaLevel == 2:
        dict_scia = read_scia_lv2( args.input_file )
    dict_scia['filePath'] = os.path.dirname( args.input_file )
    dict_scia['fileName'] = os.path.basename( args.input_file )
    if args.input_file[-3:] == '.gz':
        dict_scia['compress'] = True
    else:
        dict_scia['compress'] = False
    dict_scia['level'] = sciaLevel
    dict_scia['receiveDate'] = \
        strftime("%F %T", gmtime(os.path.getctime( args.input_file )))
    if args.debug:
        print( dict_scia )
        sys.exit(0)

    add_sqlite_scia( args.dbname, dict_scia )
    sys.exit(0)
