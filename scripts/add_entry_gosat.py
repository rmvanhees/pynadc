#!/usr/bin/env python

# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

#
from __future__ import print_function
from __future__ import division

import os.path
import sqlite3

import numpy as np
import h5py

#--------------------------------------------------
def cre_sqlite_gosat_db( dbname ):
    con = sqlite3.connect( dbname )
    cur = con.cursor()
    cur.execute( 'PRAGMA foreign_keys = ON' )

    cur.execute( '''create table rootPaths (
        pathID           integer  PRIMARY KEY AUTOINCREMENT,
        hostName         text     NOT NULL,
        localPath        text     NOT NULL,
        nfsPath          text     NOT NULL )''' )
    
    cur.execute( '''create table tcai__2P (
        caiID            integer  PRIMARY KEY AUTOINCREMENT,
        name             char(44) NOT NULL UNIQUE,
        pathID           integer  NOT NULL,
	passNumber	 smallint NOT NULL,
	frameNumber	 smallint NOT NULL,
        productCode      char(4)  NOT NULL,
	productVersion   char(6)  NOT NULL,
        dateTimeStart    datetime NOT NULL default '0000-00-00 00:00:00',
	acquisitionDate  datetime NOT NULL default '0000-00-00 00:00:00',
	creationDate     datetime NOT NULL default '0000-00-00',
        receiveDate      datetime NOT NULL default '0000-00-00 00:00:00',
        missingPixelRate integer  NOT NULL,
        numLine          integer  NOT NULL,
        numPixel         integer  NOT NULL,
        fileSize         integer  NOT NULL,
        FOREIGN KEY(pathID) REFERENCES rootPaths(pathID) )''' )
    cur.execute( 'create index dateTimeStartIndex2 on tcai__2P(dateTimeStart)' )
    cur.execute( 'create index receiveDateIndex2 on tcai__2P(receiveDate)' )

    cur.execute( '''create table tfts__1P (
        ftsID            integer  PRIMARY KEY AUTOINCREMENT,
        name             char(44) NOT NULL UNIQUE,
        pathID           integer  NOT NULL,
	passNumber	 smallint NOT NULL,
	frameNumber	 smallint NOT NULL,
	productVersion   char(6)  NOT NULL,
	algorithmName    char(7)  NOT NULL,
	algorithmVersion char(3)  NOT NULL,
	paramVersion     char(3)  NOT NULL,
	observationMode  char(4)  NOT NULL,
        dateTimeStart    datetime NOT NULL default '0000-00-00 00:00:00',
	acquisitionDate  datetime NOT NULL default '0000-00-00 00:00:00',
	creationDate     datetime NOT NULL default '0000-00-00',
        receiveDate      datetime NOT NULL default '0000-00-00 00:00:00',
        numPoints        integer[2]  NOT NULL,
        fileSize         integer  NOT NULL,
        FOREIGN KEY(pathID) REFERENCES rootPaths(pathID) )''' )
    cur.execute( 'create index dateTimeStartIndex1 on tfts__1P(dateTimeStart)' )
    cur.execute( 'create index receiveDateIndex1 on tfts__1P(receiveDate)' )

    cur.execute( '''create table preProcFTS (
        preID            integer  PRIMARY KEY AUTOINCREMENT,
        ftsID            integer  NOT NULL,
        name             text     NOT NULL,
        path             text     NOT NULL,
        productVersion   char(16) NOT NULL,
        creationDate     datetime NOT NULL default '0000-00-00',
        FOREIGN KEY(ftsID) REFERENCES tfts__1P(ftsID) )''' ) 
    cur.close()
    con.commit()
    con.close()
    return

#--------------------------------------------------
def fill_sqlite_rootPaths( dbname ):
    list_paths = [
        { "host" : 'shogun', 
          "path" : '/array/slot2A/GOSAT_FTS', 
          "nfs" : '/GOSAT/LV1_01' },
        { "host" : 'shogun', 
          "path" : '/array/slot2C/GOSAT_FTS', 
          "nfs" : '/GOSAT/LV1_02' },
        { "host" : 'shogun', 
          "path" : '/array/slot2A/GOSAT_CAI/CAI_L2', 
          "nfs" : '/GOSAT/LV2_01/CAI_L2' },
        { "host" : 'shikken', 
          "path" : '/array/slot2A/GOSAT_CAI/CAI_L2', 
          "nfs" : '/GOSAT/LV2_02/CAI_L2' }
    ]
    str_sql = 'insert into rootPaths values' \
        '(NULL, \'%(host)s\',\'%(path)s\',\'%(nfs)s\')'

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    for dict_path in list_paths:
        cur.execute( str_sql % dict_path )
    cur.close()
    con.commit()
    con.close()

#--------------------------------------------------
def read_gosat_cai( flname ):
    from time import gmtime, strftime

    dict_gosat = {}
    dict_gosat['fileName'] = os.path.basename( flname )
    dict_gosat['filePath'] = os.path.dirname( flname )
    buff = dict_gosat['fileName'][9:]
    dict_gosat['acquisitionDate'] = \
        strftime("%F %T", gmtime(os.path.getmtime( flname )))
    dict_gosat['passNumber'] = int(dict_gosat['fileName'][21:24])
    dict_gosat['frameNumber'] = int(dict_gosat['fileName'][24:27])
    dict_gosat['productVersion'] = dict_gosat['fileName'][35:41]
    dict_gosat['receiveDate'] = \
        strftime("%F %T", gmtime(os.path.getctime( flname )))
    dict_gosat['fileSize'] = os.path.getsize( flname )

    with h5py.File( flname, mode='r' ) as fid:
        grp = fid['/Global/MD_Metadata']
        dset = grp['dateStamp']
        dict_gosat['creationDate'] = dset[0]

        grp = fid['/Global/metadata']
        dset = grp['sensorName']
        dict_gosat['sensorName'] = dset[0]
        dset = grp['productCode']
        dict_gosat['productCode'] = dset[0]

        grp = fid['/frameAttribute']
        dset = grp['frameCenterTime']
        dict_gosat['dateTimeStart'] = dset[0]
        dset = grp['missingPixelRate']
        dict_gosat['missingPixelRate'] = dset[0]
        dset = grp['numLine']
        dict_gosat['numLine'] = dset[0]
        dset = grp['numPixel']
        dict_gosat['numPixel'] = dset[0]

    return dict_gosat

#--------------------------------------------------
def read_gosat_fts( flname ):
    from time import gmtime, strftime

    dict_gosat = {}
    dict_gosat['fileName'] = os.path.basename( flname )
    dict_gosat['filePath'] = os.path.dirname( flname )
    buff = dict_gosat['fileName'][9:]
    dict_gosat['acquisitionDate'] = \
        strftime("%F %T", gmtime(os.path.getmtime( flname )))
    dict_gosat['passNumber'] = int(dict_gosat['fileName'][21:24])
    dict_gosat['frameNumber'] = int(dict_gosat['fileName'][24:27])
    dict_gosat['observationMode'] = dict_gosat['fileName'][31:35]
    dict_gosat['productVersion'] = dict_gosat['fileName'][35:41]
    dict_gosat['receiveDate'] = \
        strftime("%F %T", gmtime(os.path.getctime( flname )))
    dict_gosat['fileSize'] = os.path.getsize( flname )

    with h5py.File( flname, mode='r' ) as fid:
        if '/Global' in fid:
            grp = fid['/Global/MD_Metadata']
            dset = grp['dateStamp']
            dict_gosat['creationDate'] = dset[:].tostring().decode('ascii')

            grp = fid['/Global/metadata']
            dset = grp['sensorName']
            dict_gosat['sensorName'] = dset[:].tostring().decode('ascii')
            dset = grp['algorithmName']
            dict_gosat['algorithmName'] = dset[:].tostring().decode('ascii')
            dset = grp['algorithmVersion']
            dict_gosat['algorithmVersion'] = dset[:].tostring().decode('ascii')
            dset = grp['parameterVersion']
            dict_gosat['paramVersion'] = dset[:].tostring().decode('ascii')
            dset = grp['observationMode']
            dict_gosat['observationMode'] = dset[:].tostring().decode('ascii')

            grp = fid['/ancillary/OrbitData']
            dset = grp['startDate']
            dict_gosat['dateTimeStart'] = \
                    "%04d-%02d-%02d %02d:%02d:%09.6f" % dset[0].tolist()
        elif '/globalAttribute' in fid:
            grp = fid['/globalAttribute/metadata']
            dset = grp['dateStamp']
            dict_gosat['creationDate'] = np.string_(dset[...]).decode('ascii')
            
            grp = fid['/globalAttribute/extensionMetadata']
            dset = grp['algorithmName']
            dict_gosat['algorithmName'] = np.string_(dset[...]).decode('ascii')
            dset = grp['algorithmVersion']
            dict_gosat['algorithmVersion'] = np.string_(dset[...]).decode('ascii')
            dset = grp['parameterVersion']
            dict_gosat['paramVersion'] = np.string_(dset[...]).decode('ascii')
            dset = grp['sensorName']
            dict_gosat['sensorName'] = np.string_(dset[...]).decode('ascii')

            grp = fid['/ancillary/orbitData']
            dset = grp['startDate']
            dict_gosat['dateTimeStart'] = \
                    "%04d-%02d-%02d %02d:%02d:%09.6f" % dset[0].tolist()
        else:
            return {}

        grp = fid['/exposureAttribute']
        dset = grp['numPoints_SWIR']
        dict_gosat['numPoints_SWIR'] = dset[0]
        dset = grp['numPoints_TIR']
        dict_gosat['numPoints_TIR'] = dset[0]

    return dict_gosat

#--------------------------------------------------
def check_sqlite_gosat( dbname, gosatfl ):
    if gosatfl[0:9] == 'GOSATTCAI':
        table = 'tcai__2P'
        query_str = 'select caiID from %s where name=\'%s\''
    elif gosatfl[0:9] == 'GOSATTFTS':
        table = 'tfts__1P'
        query_str = 'select ftsID from %s where name=\'%s\''
    else:
        return True

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    cur.execute( query_str % (table, gosatfl) )
    row = cur.fetchone()
    cur.close()
    con.close()
    if row == None: 
        return False
    else:
        return True

#--------------------------------------------------
def del_sqlite_gosat( dbname, gosatfl ):
    if gosatfl[0:9] == 'GOSATTCAI':
        table = 'tcai__2P'
        query_str = 'select caiID from %s where name=\'%s\''
        remove_str = 'delete from %s where caiID=%d'
    elif gosatfl[0:9] == 'GOSATTFTS':
        table = 'tfts__1P'
        query_str = 'select ftsID from %s where name=\'%s\''
        remove_str = 'delete from %s where ftsID=%d'
    else:
        return

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    cur.execute( 'PRAGMA foreign_keys = ON' )
    cur.execute( query_str % (table, gosatfl) )
    row = cur.fetchone()
    if row is not None:
        cur.execute( remove_str % (table, row[0]) )

    cur.close()
    con.commit()
    con.close()

#--------------------------------------------------
def add_sqlite_gosat( dbname, dict_gosat ):
    str_path_sql = 'select pathID from rootPaths where'\
        ' localPath == \'%s\' or nfsPath == \'%s\''
        
    if dict_gosat['sensorName'] == 'TANSO-FTS':
        buffer = dict_gosat["filePath"]
        indx = buffer.find(dict_gosat["observationMode"])
        rootPath = buffer[0:indx-1]

        str_sql = 'insert into tfts__1P values' \
            '(NULL,\'%(fileName)s\',%(pathID)d'\
            ',%(passNumber)d,%(frameNumber)d'\
            ',\'%(productVersion)s\',\'%(algorithmName)s\''\
            ',\'%(algorithmVersion)s\',\'%(paramVersion)s\''\
            ',\'%(observationMode)s\',\'%(dateTimeStart)s\''\
            ',\'%(acquisitionDate)s\',\'%(creationDate)s\',\'%(receiveDate)s\''\
            ',\'{%(numPoints_SWIR)d,%(numPoints_TIR)d}\',%(fileSize)d)'
    elif dict_gosat['sensorName'] == 'TANSO-CAI':
        buffer = dict_gosat["filePath"]
        indx = buffer.find(dict_gosat["dateTimeStart"][0:4])
        rootPath = buffer[0:indx-1]

        str_sql = 'insert into tcai__2P values' \
            '(NULL,\'%(fileName)s\',%(pathID)d'\
            ',%(passNumber)d,%(frameNumber)d'\
            ',\'%(productCode)s\',\'%(productVersion)s\''\
            ',\'%(dateTimeStart)s\''\
            ',\'%(acquisitionDate)s\',\'%(creationDate)s\',\'%(receiveDate)s\''\
            ',%(missingPixelRate)f,%(numLine)d,%(numPixel)d,%(fileSize)d)'
    else:
        print( 'Invalid sensor name: ', dict_gosat['sensorName'] )
        return

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    cur.execute( 'PRAGMA foreign_keys = ON' )

    ## obtain pathID from table rootPaths
    cur.execute( str_path_sql % (rootPath,rootPath) )
    row = cur.fetchone()
    if row is not None:
        dict_gosat['pathID'] = row[0]
    else:
        dict_gosat['pathID'] = 0

    ## do actual query
    cur.execute( str_sql % dict_gosat )

    cur.close()
    con.commit()
    con.close()
    return

#- main code -------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument( '--debug', action='store_true', default=False,
                         help='show what will be done, but do nothing' )
    parser.add_argument( '--remove', action='store_true', default=False,
                         help='remove SQL data of INPUT_FILE from database' )
    parser.add_argument( '--replace', action='store_true', default=False,
                         help='replace SQL data of INPUT_FILE in database' )
    parser.add_argument( '--dbname', dest='dbname', default=b'sron_gosat.db', 
                         help='name of GOSAT/SQLite database' )
    parser.add_argument( 'input_file', nargs='?', type=str,
                         help='read from INPUT_FILE' )
    args = parser.parse_args()

    if not h5py.is_hdf5( np.string_(args.input_file) ):
        print( 'Info: %s is not a HDF5/GOSAT product' % args.input_file )
        sys.exit(0)

    if not os.path.isfile( args.dbname ):
        cre_sqlite_gosat_db( args.dbname )
        fill_sqlite_rootPaths( args.dbname )

    # Check if product is already in database
    gosat_fl = os.path.basename( args.input_file )
    if not args.debug:
        if args.remove or args.replace:
            del_sqlite_gosat( args.dbname, gosat_fl )
            if args.remove: sys.exit(0)
        elif check_sqlite_gosat( args.dbname, gosat_fl ):
            print( 'Info: %s is already stored in database' % gosat_fl )
            sys.exit(0)
    
    if gosat_fl[0:9] == 'GOSATTCAI':
        dict_gosat = read_gosat_cai( args.input_file )
    elif gosat_fl[0:9] == 'GOSATTFTS':
        dict_gosat = read_gosat_fts( args.input_file )
    else:
        print( 'Info: %s is not a regular CAI/FTS product' % gosat_fl )
        sys.exit(0)

    if not dict_gosat:
        print( 'Info: %s is not a valid CAI/FTS product' % gosat_fl )
        os.remove( args.input_file )
        sys.exit(0)
        
    if args.debug:
        for item in dict_gosat.keys():
            print( item, '\t', dict_gosat[item] )
        sys.exit(0)

    add_sqlite_gosat( args.dbname, dict_gosat )
    sys.exit(0)
