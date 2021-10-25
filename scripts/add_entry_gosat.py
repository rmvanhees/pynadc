#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Defines class ArchiveGosat to add new entries to GOSAT SQLite database

Copyright (c) 2016-2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
import argparse
import sqlite3

from pathlib import Path
from time import gmtime, strftime

import numpy as np
import h5py


# --------------------------------------------------
def cre_sqlite_gosat_db(dbname):
    """
    function to define database for GOSAT database and tables
    """
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute('PRAGMA foreign_keys = ON')

    cur.execute(
        """create table rootPaths (
        pathID           integer  PRIMARY KEY AUTOINCREMENT,
        hostName         text     NOT NULL,
        localPath        text     NOT NULL,
        nfsPath          text     NOT NULL)
        """)

    cur.execute(
        """create table tcai__2P (
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
        FOREIGN KEY(pathID) REFERENCES rootPaths(pathID))
        """)
    cur.execute('create index dateTimeStartIndex2 on tcai__2P(dateTimeStart)')
    cur.execute('create index receiveDateIndex2 on tcai__2P(receiveDate)')

    cur.execute(
        """create table tfts__1P (
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
        FOREIGN KEY(pathID) REFERENCES rootPaths(pathID))
        """)
    cur.execute('create index dateTimeStartIndex1 on tfts__1P(dateTimeStart)')
    cur.execute('create index receiveDateIndex1 on tfts__1P(receiveDate)')
    cur.close()
    con.commit()
    con.close()


# --------------------------------------------------
def sql_write_basedirs(dbname):
    """
    write names of directories where to find the GOSAT products
    """
    list_paths = [
        {"host": 'poseidon',
         "path": '/array/slot2B/GOSAT/FTS/L1X',
         "nfs": '/nfs/GOSAT/FTS/L1X'},
        {"host": 'poseidon',
         "path": '/array/slot2B/GOSAT/FTS/L1B',
         "nfs": '/nfs/GOSAT/FTS/L1B'},
        {"host": 'poseidon',
         "path": '/array/slot2B/GOSAT/CAI/L2',
         "nfs": '/nfs/GOSAT/CAI/L2'}
    ]

    str_sql = ('insert into rootPaths values'
               '(NULL, \'%(host)s\',\'%(path)s\',\'%(nfs)s\')')

    con = sqlite3.connect(dbname)
    cur = con.cursor()
    for dict_path in list_paths:
        cur.execute(str_sql % dict_path)
    cur.close()
    con.commit()
    con.close()


# --------------------------------------------------
class ArchiveGosat():
    """
    class to archive GOSAT products
    """
    def __init__(self, db_name='./sron_gosat.db'):
        """
        initialize the class
        """
        self.dbname = db_name

        if not Path(db_name).is_file():
            cre_sqlite_gosat_db(db_name)
            sql_write_basedirs(db_name)

    # -------------------------
    @staticmethod
    def rd_cai(flname):
        """
        read meta data from a GOSAT CAI level 2 product
        """
        gosatfl = Path(flname)
        stat = gosatfl.stat()

        dict_gosat = {}
        dict_gosat['fileName'] = gosatfl.name
        dict_gosat['filePath'] = str(gosatfl.parent)
        dict_gosat['acquisitionDate'] = strftime("%F %T",
                                                 gmtime(stat.st_mtime))
        dict_gosat['passNumber'] = int(dict_gosat['fileName'][21:24])
        dict_gosat['frameNumber'] = int(dict_gosat['fileName'][24:27])
        dict_gosat['productVersion'] = dict_gosat['fileName'][35:41]
        dict_gosat['receiveDate'] = strftime("%F %T",
                                             gmtime(stat.st_ctime))
        dict_gosat['fileSize'] = stat.st_size

        with h5py.File(flname, mode='r') as fid:
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

    # -------------------------
    @staticmethod
    def rd_fts(flname):
        """
        read meta data from a GOSAT FTS level 1(X) product
        """
        gosatfl = Path(flname)
        stat = gosatfl.stat()

        dict_gosat = {}
        dict_gosat['fileName'] = gosatfl.name
        dict_gosat['filePath'] = str(gosatfl.parent)
        dict_gosat['acquisitionDate'] = strftime("%F %T",
                                                 gmtime(stat.st_mtime))
        dict_gosat['passNumber'] = int(dict_gosat['fileName'][21:24])
        dict_gosat['frameNumber'] = int(dict_gosat['fileName'][24:27])
        dict_gosat['observationMode'] = dict_gosat['fileName'][31:35]
        dict_gosat['productVersion'] = dict_gosat['fileName'][35:41]
        dict_gosat['receiveDate'] = strftime("%F %T",
                                             gmtime(stat.st_ctime))
        dict_gosat['fileSize'] = stat.st_size

        with h5py.File(flname, mode='r') as fid:
            if '/Global' in fid:
                grp = fid['/Global/MD_Metadata']
                dset = grp['dateStamp']
                dict_gosat['creationDate'] = dset[:].tobytes().decode('ascii')

                grp = fid['/Global/metadata']
                dset = grp['sensorName']
                dict_gosat['sensorName'] = dset[:].tobytes().decode('ascii')
                dset = grp['algorithmName']
                dict_gosat['algorithmName'] = \
                    dset[:].tobytes().decode('ascii')
                dset = grp['algorithmVersion']
                dict_gosat['algorithmVersion'] = \
                    dset[:].tobytes().decode('ascii')
                dset = grp['parameterVersion']
                dict_gosat['paramVersion'] = dset[:].tobytes().decode('ascii')
                dset = grp['observationMode']
                dict_gosat['observationMode'] = \
                    dset[:].tobytes().decode('ascii')

                grp = fid['/ancillary/OrbitData']
                dset = grp['startDate']
                dict_gosat['dateTimeStart'] = \
                    "%04d-%02d-%02d %02d:%02d:%09.6f" % dset[0].tolist()
            elif '/globalAttribute' in fid:
                grp = fid['/globalAttribute/metadata']
                dset = grp['dateStamp']
                dict_gosat['creationDate'] = \
                    np.string_(dset[...]).decode('ascii')
                grp = fid['/globalAttribute/extensionMetadata']
                dset = grp['algorithmName']
                dict_gosat['algorithmName'] = \
                    np.string_(dset[...]).decode('ascii')
                dset = grp['algorithmVersion']
                dict_gosat['algorithmVersion'] = \
                    np.string_(dset[...]).decode('ascii')
                dset = grp['parameterVersion']
                dict_gosat['paramVersion'] = \
                    np.string_(dset[...]).decode('ascii')
                dset = grp['sensorName']
                dict_gosat['sensorName'] = \
                    np.string_(dset[...]).decode('ascii')

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

    # -------------------------
    def check_entry(self, gosatfl, verbose=False) -> bool:
        """
        check if entry is already present in database
        """
        if gosatfl[0:9] == 'GOSATTCAI':
            table = 'tcai__2P'
            query_str = 'select caiID from %s where name=\'%s\''
        elif gosatfl[0:9] == 'GOSATTFTS':
            table = 'tfts__1P'
            query_str = 'select ftsID from %s where name=\'%s\''
        else:
            raise ValueError('Unknown GOSAT product')

        if verbose:
            print(query_str)

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute(query_str % (table, gosatfl))
        row = cur.fetchone()
        cur.close()
        con.close()
        if row is None:
            return False

        return True

    # -------------------------
    def remove_entry(self, gosatfl, verbose=False) -> None:
        """
        remove entry from database
        """
        if gosatfl[0:9] == 'GOSATTCAI':
            table = 'tcai__2P'
            query_str = 'select caiID from %s where name=\'%s\''
            remove_str = 'delete from %s where caiID=%d'
        elif gosatfl[0:9] == 'GOSATTFTS':
            table = 'tfts__1P'
            query_str = 'select ftsID from %s where name=\'%s\''
            remove_str = 'delete from %s where ftsID=%d'
        else:
            raise ValueError('Unknown GOSAT product')

        if verbose:
            print(query_str)

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute('PRAGMA foreign_keys = ON')
        cur.execute(query_str % (table, gosatfl))
        row = cur.fetchone()
        if row is not None:
            cur.execute(remove_str % (table, row[0]))

        cur.close()
        con.commit()
        con.close()

    # -------------------------
    def add_entry(self, gosatfl_str, debug=False):
        """
        add new entry to SQLite database
        """
        str_path_sql = ('select pathID from rootPaths where'
                        ' localPath == \'%s\' or nfsPath == \'%s\'')

        gosatfl = Path(gosatfl_str)
        if gosatfl.name[0:9] == 'GOSATTFTS':
            dict_gosat = self.rd_fts(gosatfl_str)
            buffer = str(gosatfl.parent)
            indx = buffer.find(dict_gosat['observationMode'])
            basedir = buffer[0:indx-1]

            str_sql = 'insert into tfts__1P values' \
                      '(NULL,\'%(fileName)s\',%(pathID)d'\
                      ',%(passNumber)d,%(frameNumber)d'\
                      ',\'%(productVersion)s\',\'%(algorithmName)s\''\
                      ',\'%(algorithmVersion)s\',\'%(paramVersion)s\''\
                      ',\'%(observationMode)s\',\'%(dateTimeStart)s\''\
                      ',\'%(acquisitionDate)s\',\'%(creationDate)s\''\
                      ',\'%(receiveDate)s\',\'{%(numPoints_SWIR)d'\
                      ',%(numPoints_TIR)d}\',%(fileSize)d)'
        elif gosatfl.name[0:9] == 'GOSATTCAI':
            dict_gosat = self.rd_cai(gosatfl_str)
            buffer = str(gosatfl.parent)
            indx = buffer.find(dict_gosat['dateTimeStart'][0:4])
            basedir = buffer[0:indx-1]

            str_sql = 'insert into tcai__2P values' \
                      '(NULL,\'%(fileName)s\',%(pathID)d'\
                      ',%(passNumber)d,%(frameNumber)d'\
                      ',\'%(productCode)s\',\'%(productVersion)s\''\
                      ',\'%(dateTimeStart)s\',\'%(acquisitionDate)s\''\
                      ',\'%(creationDate)s\',\'%(receiveDate)s\''\
                      ',%(missingPixelRate)f,%(numLine)d,%(numPixel)d'\
                      ',%(fileSize)d)'
        else:
            raise ValueError('Invalid sensor name: ', dict_gosat['sensorName'])

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute('PRAGMA foreign_keys = ON')

        # obtain pathID from table base-dirs
        cur.execute(str_path_sql % (basedir, basedir))
        row = cur.fetchone()
        if row is not None:
            dict_gosat['pathID'] = row[0]
        else:
            dict_gosat['pathID'] = 0

        if debug:
            print(str_sql % dict_gosat)
            return

        # do actual query
        cur.execute(str_sql % dict_gosat)
        cur.close()
        con.commit()
        con.close()


# - main code --------------------------------------------------
def main():
    """
    main function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', default=False,
                        help='show what will be done, but do nothing')
    parser.add_argument('--remove', action='store_true', default=False,
                        help='remove SQL data of INPUT_FILE from database')
    parser.add_argument('--replace', action='store_true', default=False,
                        help='replace SQL data of INPUT_FILE in database')
    parser.add_argument('--dbname', dest='dbname', default=b'sron_gosat.db',
                        help='name of GOSAT/SQLite database')
    parser.add_argument('input_file', nargs='?', type=str,
                        help='read from INPUT_FILE')
    args = parser.parse_args()

    if not h5py.is_hdf5(args.input_file):
        print('Info: %s is not a HDF5/GOSAT product' % args.input_file)
        return

    gosatdb = ArchiveGosat(args.dbname)
    gosatfl = Path(args.input_file).name

    # Check if product is already in database
    if not args.debug:
        if args.remove or args.replace:
            gosatdb.remove_entry(gosatfl)
        if args.remove:
            print('Info: {} is removed from database'.format(gosatfl))
            return

        if gosatdb.check_entry(gosatfl):
            print('Info: {} is already stored in database'.format(gosatfl))
            return

    gosatdb.add_entry(args.input_file, debug=args.debug)


if __name__ == '__main__':
    main()
