#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Defines class ArchiveGosat2L2 to add new entries to GOSAT-2 SQLite database

Copyright (c) 2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
import argparse
import sqlite3

from pathlib import Path
from time import gmtime, strftime

import h5py


def cleanup_string(dset) -> str:
    """
    Returns bytes as string
    """
    if len(dset) == 1:
        return dset[0].decode('ascii').rstrip('\0')

    return dset[:].tobytes().decode('ascii').rstrip('\0')


def check_product_name(flname) -> str:
    """
    Validate the product name
    """
    if flname[0:11] != 'GOSAT2TFTS2':
        raise ValueError('not an official GOSAT-2 FTS-2 product')

    if flname[20:22] != '02':
        raise ValueError('not an official GOSAT-2 Level-2 product')

    if flname[22:26] not in ('SWPR',):
        raise ValueError('not a known GOSAT-2 Level-2 algorithm')

    return flname[22:26].lower()


# --------------------------------------------------
def cre_sqlite_gosat2_db(dbname) -> None:
    """
    function to define database for GOSAT-2 Level-2 tables
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
        """create table swpr (
        swprID           integer  PRIMARY KEY AUTOINCREMENT,
        name             char(50) NOT NULL UNIQUE,
        pathID           integer  NOT NULL,
        productAlgo      char(4)  NOT NULL,
        productVersion   char(10) NOT NULL,
        algorithmVersion char(6)  NOT NULL,
        inputDataVersion char(4)  NOT NULL,
        dateTimeStart    datetime NOT NULL default '0000-00-00T00:00:00',
        dateTimeEnd      datetime NOT NULL default '0000-00-00T00:00:00',
        acquisitionDate  datetime NOT NULL default '0000-00-00T00:00:00',
        creationDate     datetime NOT NULL default '0000-00-00T00:00:00',
        receiveDate      datetime NOT NULL default '0000-00-00T00:00:00',
        numSoundings     smallint NOT NULL,
        fileSize         integer  NOT NULL,
        FOREIGN KEY(pathID) REFERENCES rootPaths(pathID))
        """)
    cur.execute('create index dateTimeStartIndex1 on swpr(dateTimeStart)')
    cur.execute('create index receiveDateIndex1 on swpr(receiveDate)')

    cur.close()
    con.commit()
    con.close()


# --------------------------------------------------
def sql_write_basedirs(dbname) -> None:
    """
    write names of directories where to find the GOSAT-2 products
    """
    list_paths = [
        {"host": 'shogun',
         "path": '/array/slot2B/GOSAT-2/FTS/L2',
         "nfs": '/nfs/GOSAT2/FTS/L2'}
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
class ArchiveGosat2L2():
    """
    class to archive GOSAT-2 Level-2 products
    """
    def __init__(self, db_name='./sron_gosat2l2.db'):
        """
        initialize the class
        """
        self.dbname = db_name

        if not Path(db_name).is_file():
            cre_sqlite_gosat2_db(db_name)
            sql_write_basedirs(db_name)

    # -------------------------
    @staticmethod
    def rd_fts(flname) -> dict:
        """
        read meta data from a GOSAT-2 FTS-2 L2 product
        """
        gosatfl = Path(flname)
        stat = gosatfl.stat()

        dict_gosat = {}
        dict_gosat['fileName'] = gosatfl.name
        dict_gosat['filePath'] = str(gosatfl.parent)
        dict_gosat['acquisitionDate'] = strftime("%FT%T",
                                                 gmtime(stat.st_mtime))
        dict_gosat['productAlgo'] = dict_gosat['fileName'][22:26]
        dict_gosat['productVersion'] = dict_gosat['fileName'][27:37]
        dict_gosat['receiveDate'] = strftime("%FT%T",
                                             gmtime(stat.st_ctime))
        dict_gosat['fileSize'] = stat.st_size

        with h5py.File(flname, mode='r') as fid:
            if '/Metadata' in fid:
                grp = fid['/Metadata']
                dset = grp['processingDate']
                dict_gosat['creationDate'] = cleanup_string(dset)
                dset = grp['sensorName']
                dict_gosat['sensorName'] = cleanup_string(dset)
                dset = grp['algorithmVersion']
                dict_gosat['algorithmVersion'] = cleanup_string(dset)
                dset = grp['inputDataVersion']
                dict_gosat['inputDataVersion'] = cleanup_string(dset)

                dset = grp['startDate']
                dict_gosat['dateTimeStart'] = cleanup_string(dset)
                dset = grp['endDate']
                dict_gosat['dateTimeEnd'] = cleanup_string(dset)
            else:
                return {}

            grp = fid['/SoundingGeometry']
            dict_gosat['numSoundings'] = len(grp['latitude'])

        return dict_gosat

    # -------------------------
    def check_entry(self, gosatfl, verbose=False) -> bool:
        """
        check if entry is already present in database
        """
        try:
            table = check_product_name(gosatfl)
        except ValueError as exc:
            raise RuntimeError('invalid product name') from exc

        query_str = 'select swprID from %s where name=\'%s\''
        if verbose:
            print(query_str)

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute(query_str % (table, gosatfl))
        row = cur.fetchone()
        cur.close()
        con.close()

        return row is not None

    # -------------------------
    def remove_entry(self, gosatfl, verbose=False) -> None:
        """
        remove entry from database
        """
        try:
            table = check_product_name(gosatfl)
        except ValueError as exc:
            raise RuntimeError('invalid product name') from exc

        query_str = 'select swprID from %s where name=\'%s\''
        remove_str = 'delete from %s where swidID=%d'
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
    def add_entry(self, flname, debug=False):
        """
        add new entry to SQLite database
        """
        str_path_sql = ('select pathID from rootPaths where'
                        ' localPath == \'%s\' or nfsPath == \'%s\'')

        gosatfl = Path(flname)
        try:
            table = check_product_name(gosatfl.name)
        except ValueError as exc:
            raise RuntimeError('invalid product name') from exc

        dict_gosat = self.rd_fts(flname)
        basedir = str(gosatfl.parent)
        indx = basedir.find(dict_gosat['productAlgo'])
        basedir = basedir[0:indx-1]

        str_sql = 'insert into {} values'.format(table)
        str_sql += '(NULL,\'%(fileName)s\',%(pathID)d'\
                   ',\'%(productAlgo)s\',\'%(productVersion)s\''\
                   ',\'%(algorithmVersion)s\',\'%(inputDataVersion)s\''\
                   ',\'%(dateTimeStart)s\',\'%(dateTimeEnd)s\''\
                   ',\'%(acquisitionDate)s\',\'%(creationDate)s\''\
                   ',\'%(receiveDate)s\',%(numSoundings)d'\
                   ',%(fileSize)d)'

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute('PRAGMA foreign_keys = ON')

        # obtain pathID from table base-dirs
        cur.execute(str_path_sql % (basedir, basedir))
        row = cur.fetchone()
        if row is not None:
            dict_gosat['pathID'] = row[0]
        else:
            dict_gosat['pathID'] = 1

        if debug:
            print(repr(str_sql % dict_gosat))
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
    parser.add_argument('--dbname', dest='dbname', default=b'sron_gosat2.db',
                        help='name of GOSAT/SQLite database')
    parser.add_argument('input_file', nargs='?', type=str,
                        help='read from INPUT_FILE')
    args = parser.parse_args()
    if not h5py.is_hdf5(args.input_file):
        print('Info: %s is not a HDF5/GOSAT-2 product' % args.input_file)
        return

    gosatdb = ArchiveGosat2L2(args.dbname)
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
