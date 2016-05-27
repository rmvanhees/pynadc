#!/usr/bin/env python

# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.
'''
Defines class ArchiveScia to add new entries to Sciamachy SQLite database
'''
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
    '''
    function to define database for Sciamachy database and tables
    '''
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

#--------------------------------------------------
class ArchiveScia( object ):
    '''
    '''
    def __init__( self, db_name='./sron_scia.db' ):
        '''
        '''
        self.dbname = db_name
        
        if not os.path.isfile( db_name ):
            cre_sqlite_scia_db( db_name )

    #-------------------------
    def rd_lv0( self, sciafl_full ):
        '''
        '''
        nr = 0
        dict_meta = {}
        dict_meta['filePath'] = os.path.dirname( sciafl_full )
        dict_meta['receiveDate'] = \
                strftime("%F %T", gmtime(os.path.getctime( sciafl_full )))
        if sciafl_full.endswith('.gz'):
            dict_meta['fileName'] = os.path.basename( sciafl_full )[0:-3]
            dict_meta['compress'] = True
            fp = gzip.open( sciafl_full, "rb" )
        else:
            dict_meta['fileName'] = os.path.basename( sciafl_full )
            dict_meta['compress'] = False
            fp = open( sciafl_full, "rb" )

        while nr < 50:
            line = fp.readline()
            if not line:
                break
            nr += 1

            words = line.decode('ascii').split( '=' )
            if len( words ) < 2:
                continue

            if words[0] == "PRODUCT":
                dict_meta['product'] = words[1][1:-2]
            elif words[0] == "PROC_STAGE":
                dict_meta['procStage'] = words[1][0:-1]
                if dict_meta['procStage'] == 'N':
                    dict_meta['qualityFlag'] = 'GOOD'
                    dict_meta['q_flag'] = 5
                else:
                    dict_meta['qualityFlag'] = 'CONS'
                    dict_meta['q_flag'] = 10
            elif words[0] == "PROC_CENTER":
                dict_meta['procCenter'] = words[1][1:-2].rstrip()
            elif words[0] == "PROC_TIME":
                dt = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
                if dt.microsecond == 0:
                    dt = dt.replace( microsecond=randint(0,99999) )
                dict_meta['procTime'] = dt.strftime( '%Y-%m-%d %H:%M:%S.%f' )
            elif words[0] == "SOFTWARE_VER":
                dict_meta['softVersion'] = words[1][1:-2].rstrip()
            elif words[0] == "SENSING_START":
                dt1 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
                dict_meta['dateTimeStart'] = dt1.strftime( '%Y-%m-%d %H:%M:%S' )
                dict_meta['muSeconds' ] = dt1.microsecond
            elif words[0] == "SENSING_STOP":
                dt2 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            elif words[0] == "REL_ORBIT":
                dict_meta['relOrbit'] = int( words[1] )
            elif words[0] == "ABS_ORBIT":
                dict_meta['absOrbit'] = int( words[1] )
            elif words[0] == "TOT_SIZE":
                dict_meta['fileSize'] = int( words[1][0:21] )
            elif words[0] == "NUM_DATA_SETS":
                dict_meta['numDataSets'] = int( words[1] )
            elif words[0] == "SPH_DESCRIPTOR":
                break

        fp.close()

        dt = dt2 - dt1
        dict_meta['duration'] = dt.seconds + dt.microseconds / 1e6
        return dict_meta
        
    #-------------------------
    def rd_lv1( self, sciafl_full ):
        '''
        '''
        nr = 0
        dict_meta = {}
        dict_meta['filePath'] = os.path.dirname( sciafl_full )
        dict_meta['receiveDate'] = \
                strftime("%F %T", gmtime(os.path.getctime( sciafl_full )))
        if sciafl_full.endswith('.gz'):
            dict_meta['fileName'] = os.path.basename( sciafl_full )[0:-3]
            dict_meta['compress'] = True
            fp = gzip.open( sciafl_full, "rb" )
        else:
            dict_meta['fileName'] = os.path.basename( sciafl_full )
            dict_meta['compress'] = False
            fp = open( sciafl_full, "rb" )

        while nr < 100:
            line = fp.readline()
            if not line:
                break
            nr += 1

            words = line.decode('ascii').split( '=' )
            if len( words ) < 2:
                continue

            if words[0] == "PRODUCT":
                dict_meta['product'] = words[1][1:-2]
            elif words[0] == "PROC_STAGE":
                dict_meta['procStage'] = words[1][0:-1]
                if dict_meta['procStage'] == 'N':
                    dict_meta['qualityFlag'] = 'GOOD'
                    dict_meta['q_flag'] = 5
                else:
                    dict_meta['qualityFlag'] = 'CONS'
                    dict_meta['q_flag'] = 10
            elif words[0] == "PROC_CENTER":
                dict_meta['procCenter'] = words[1][1:-2].rstrip()
            elif words[0] == "PROC_TIME":
                dt = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
                if dt.microsecond == 0:
                    dt = dt.replace( microsecond=randint(0,99999) )
                dict_meta['procTime'] = dt.strftime( '%Y-%m-%d %H:%M:%S.%f' )
            elif words[0] == "SOFTWARE_VER":
                dict_meta['softVersion'] = words[1][1:-2].rstrip()
            elif words[0] == "REL_ORBIT":
                dict_meta['relOrbit'] = int( words[1] )
            elif words[0] == "ABS_ORBIT":
                dict_meta['absOrbit'] = int( words[1] )
            elif words[0] == "TOT_SIZE":
                dict_meta['fileSize'] = int( words[1][0:21] )
            elif words[0] == "NUM_DATA_SETS":
                dict_meta['numDataSets'] = int( words[1] )
            elif words[0] == "START_TIME":
                dt1 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
                dict_meta['dateTimeStart'] = dt1.strftime( '%Y-%m-%d %H:%M:%S' )
                dict_meta['muSeconds' ] = dt1.microsecond
            elif words[0] == "STOP_TIME":
                dt2 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            elif words[0] == "KEY_DATA_VERSION":
                dict_meta['keydataVersion'] = words[1][1:-2].rstrip()
            elif words[0] == "M_FACTOR_VERSION":
                dict_meta['mFactorVersion'] = words[1][1:-2].rstrip()
            elif words[0] == "SPECTRAL_CAL_CHECK_SUM":
                dict_meta['spectralCal'] = words[1][1:-2].rstrip()
            elif words[0] == "SATURATED_PIXEL":
                dict_meta['saturatedPix'] = words[1][1:-2]
            elif words[0] == "DEAD_PIXEL":
                dict_meta['deadPixels'] = words[1][1:-2].rstrip()
            elif words[0] == "NO_OF_NADIR_STATES":
                dict_meta['nadirStates'] = int( words[1] )
            elif words[0] == "NO_OF_LIMB_STATES":
                dict_meta['limbStates'] = int( words[1] )
            elif words[0] == "NO_OF_OCCULTATION_STATES":
                dict_meta['occulStates'] = int( words[1] )
            elif words[0] == "NO_OF_MONI_STATES":
                dict_meta['monitorStates'] = int( words[1] )
            elif words[0] == "NO_OF_NOPROC_STATES":
                dict_meta['noProcStates'] = int( words[1] )
            elif words[0] == "DS_NAME":
                break

        fp.close()

        dt = dt2 - dt1
        dict_meta['duration'] = dt.seconds + dt.microseconds / 1e6
        return dict_meta
        
    #-------------------------
    def rd_lv2( self, sciafl_full ):
        '''
        '''
        nr = 0
        dict_meta = {}
        nadir_products = []
        limb_products = []
        dict_meta['filePath'] = os.path.dirname( sciafl_full )
        dict_meta['receiveDate'] = \
                strftime("%F %T", gmtime(os.path.getctime( sciafl_full )))
        if sciafl_full.endswith('.gz'):
            dict_meta['fileName'] = os.path.basename( sciafl_full )[0:-3]
            dict_meta['compress'] = True
            fp = gzip.open( sciafl_full, "rb" )
        else:
            dict_meta['fileName'] = os.path.basename( sciafl_full )
            dict_meta['compress'] = False
            fp = open( sciafl_full, "rb" )

        while nr < 150:
            line = fp.readline()
            if not line:
                break
            nr += 1

            words = line.decode('ascii').split( '=' )
            if len( words ) < 2:
                continue

            if words[0] == "PRODUCT":
                dict_meta['product'] = words[1][1:-2]
            elif words[0] == "PROC_STAGE":
                dict_meta['procStage'] = words[1][0:-1]
                if dict_meta['procStage'] == 'N':
                    dict_meta['qualityFlag'] = 'GOOD'
                    dict_meta['q_flag'] = 5
                else:
                    dict_meta['qualityFlag'] = 'CONS'
                    dict_meta['q_flag'] = 10
            elif words[0] == "PROC_CENTER":
                dict_meta['procCenter'] = words[1][1:-2].rstrip()
            elif words[0] == "PROC_TIME":
                dt = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
                if dt.microsecond == 0:
                    dt = dt.replace( microsecond=randint(0,99999) )
                dict_meta['procTime'] = dt.strftime( '%Y-%m-%d %H:%M:%S.%f' )
            elif words[0] == "SOFTWARE_VER":
                dict_meta['softVersion'] = words[1][1:-2].rstrip()
            elif words[0] == "REL_ORBIT":
                dict_meta['relOrbit'] = int( words[1] )
            elif words[0] == "ABS_ORBIT":
                dict_meta['absOrbit'] = int( words[1] )
            elif words[0] == "TOT_SIZE":
                dict_meta['fileSize'] = int( words[1][0:21] )
            elif words[0] == "NUM_DATA_SETS":
                dict_meta['numDataSets'] = int( words[1] )
            elif words[0] == "START_TIME":
                dt1 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
                dict_meta['dateTimeStart'] = dt1.strftime( '%Y-%m-%d %H:%M:%S' )
                dict_meta['muSeconds' ] = dt1.microsecond
            elif words[0] == "STOP_TIME":
                dt2 = datetime.strptime( words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f' )
            elif words[0] == "FITTING_ERROR_SUM":
                dict_meta['fittingErrSum'] = words[1][1:-2].rstrip()
            elif words[0].find( "NAD_FIT_WINDOW_" ) != -1 \
                and words[1].find( "EMPTY" ) == -1:
                if nadir_products:
                    nadir_products.append( ',' )
                nadir_products.append( words[1][1:-2].replace('- ','-').strip() )
            elif words[0].find( "LIM_FIT_WINDOW_" ) != -1 \
                and words[1].find( "EMPTY" ) == -1:
                if limb_products:
                    limb_products.append( ',' )
                limb_products.append( words[1][1:-2].replace('- ','-').strip() )
            elif words[0] == "DS_NAME":
                break
        fp.close()

        dt = dt2 - dt1
        dict_meta['duration'] = dt.seconds + dt.microseconds / 1e6
        dict_meta['nadirProducts'] = ''.join( nadir_products )
        dict_meta['limbProducts'] = ''.join( limb_products )
        return dict_meta

    #-------------------------
    def check_entry( self, sciafl, verbose=False ):
        '''
        '''
        if sciafl[0:10] == 'SCI_NL__0P':
            query_str = 'select name from meta__0P where name=\'{}\''.format(sciafl)
        elif sciafl[0:10] == 'SCI_NL__1P':
            query_str = 'select name from meta__1P where name=\'{}\''.format(sciafl)
        elif sciafl[0:10] == 'SCI_OL__2P' or sciafl[0:10] == 'SCI_NL__2P':
            query_str = 'select name from meta__2P where name=\'{}\''.format(sciafl)
        else:
            print( 'Level of Sciamachy product is unknown' )
            return

        if verbose:
            print( query_str )
        
        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( query_str )
        row = cur.fetchone()
        cur.close()
        con.close()
        if row == None: 
            return False
        else:
            return True
    
    #-------------------------
    def remove_entry( self, sciafl, verbose=False ):
        '''
        '''
        if not self.check_entry( sciafl ):
            return
            
        if sciafl[0:10] == 'SCI_NL__0P':
            query_str = 'delete from meta__0P where name=\'{}\''.format(sciafl)
        elif sciafl[0:10] == 'SCI_NL__1P':
            query_str = 'delete from meta__1P where name=\'{}\''.format(sciafl)
        elif sciafl[0:10] == 'SCI_OL__2P' or sciafl[0:10] == 'SCI_NL__2P':
            query_str = 'delete from meta__2P where name=\'{}\''.format(sciafl)
        else:
            print( 'Level of Sciamachy product is unknown' )
            return

        if verbose:
            print( query_str )
            
        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( query_str )
        cur.close()
        con.commit()
        con.close()

    #-------------------------
    def add_entry( self, sciafl_full, debug=False ):
        '''
        '''
        sciafl = os.path.basename( sciafl_full )
        if sciafl[0:10] == 'SCI_NL__0P':
            str_sql = 'insert into meta__0P values' \
                      '(\'%(product)s\',\'%(filePath)s\',%(compress)d'\
                      ',\'%(procStage)s\',\'%(procCenter)s\''\
                      ',\'%(softVersion)s\',\'%(qualityFlag)s\''\
                      ',\'%(receiveDate)s\',\'%(procTime)s\''\
                      ',\'%(dateTimeStart)s\''\
                      ',%(muSeconds)d,%(duration)f,%(absOrbit)d,%(relOrbit)d'\
                      ',%(numDataSets)d,%(fileSize)d,%(q_flag)d)'
            str_sql_meta = str_sql % self.rd_lv0( sciafl_full )
        elif sciafl[0:10] == 'SCI_NL__1P':
            str_sql = 'insert into meta__1P values' \
                      '(\'%(product)s\',\'%(filePath)s\',%(compress)d'\
                      ',\'%(procStage)s\',\'%(procCenter)s\''\
                      ',\'%(softVersion)s\',\'%(keydataVersion)s\''\
                      ',\'%(mFactorVersion)s\',\'%(spectralCal)s\''\
                      ',\'%(saturatedPix)s\',\'%(deadPixels)s\''\
                      ',\'%(qualityFlag)s\',\'%(receiveDate)s\''\
                      ',\'%(procTime)s\',\'%(dateTimeStart)s\''\
                      ',%(muSeconds)d,%(duration)f,%(absOrbit)d,%(relOrbit)d'\
                      ',%(numDataSets)d,%(nadirStates)d,%(limbStates)d'\
                      ',%(occulStates)d,%(monitorStates)d,%(noProcStates)d'\
                      ',%(fileSize)d)'
            str_sql_meta = str_sql % self.rd_lv1( sciafl_full )
        elif sciafl[0:10] == 'SCI_OL__2P' or sciafl[0:10] == 'SCI_NL__2P':
            str_sql = 'insert into meta__2P values' \
                      '(\'%(product)s\',\'%(filePath)s\',%(compress)d'\
                      ',\'%(procStage)s\',\'%(procCenter)s\''\
                      ',\'%(softVersion)s\',\'%(fittingErrSum)s\''\
                      ',\'%(qualityFlag)s\',\'%(receiveDate)s\''\
                      ',\'%(procTime)s\',\'%(dateTimeStart)s\''\
                      ',%(muSeconds)d,%(duration)f,%(absOrbit)d'\
                      ',%(relOrbit)d,%(numDataSets)d,\'%(nadirProducts)s\''\
                      ',\'%(limbProducts)s\',%(fileSize)d)'
            str_sql_meta = str_sql % self.rd_lv2( sciafl_full )
        else:
            print( 'Level of Sciamachy product is unknown' )
            return

        if debug:
            print( str_sql_meta )
        else:
            con = sqlite3.connect( self.dbname )
            cur = con.cursor()
            cur.execute( str_sql_meta )
            cur.close()
            con.commit()
            con.close()

#--------------------------------------------------
def main( dbname, input_file, remove=False, replace=False, debug=False ):
    '''
    '''
    db = ArchiveScia( dbname )

    # Check if product is already in database
    if not debug:
        sciafl = os.path.basename( input_file )
        if remove or replace:
            db.remove_entry( sciafl )
        elif db.check_entry( sciafl ):
            print( 'Info: {} is already stored in database'.format(sciafl) )
            sys.exit(0)
        if remove:
            sys.exit(0)

    db.add_entry( input_file, debug=debug )

    
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
    parser.add_argument( '--dbname', dest='dbname', type=str,
                         default='/SCIA/share/db/sron_scia.db',
			 help='name of SCIA/SQLite database' )
    parser.add_argument( 'input_file', nargs='?', type=str,
                         help='read from INPUT_FILE' )
    args = parser.parse_args()

    if not os.path.isfile( args.input_file ):
        print( 'Info: \"%s\" is not a valid file' % args.input_file )
        sys.exit(1)

    main( args.dbname, args.input_file,
          replace=args.replace, remove=args.remove, debug=args.debug )
