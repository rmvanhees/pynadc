#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Defines class ArchiveGosat2 to add new entries to GOSAT-2 SQLite database

Copyright (c) 2019-2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
import argparse
import sqlite3

from pathlib import Path
from time import gmtime, strftime

import h5py


def cleanup_string(dset):
    """
    Returns bytes as string
    """
    return dset[:].tobytes().decode('ascii').rstrip('\0')


# --------------------------------------------------
def cre_sqlite_gosat2_db(dbname):
    """
    function to define database for GOSAT-2 database and tables
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
        """create table swir_l1b (
        swirID           integer  PRIMARY KEY AUTOINCREMENT,
        name             char(50) NOT NULL UNIQUE,
        pathID           integer  NOT NULL,
        passNumber	 smallint NOT NULL,
        sceneNumber	 smallint NOT NULL,
        operationMode    char(4)  NOT NULL,
        algorithmVersion char(3)  NOT NULL,
        paramVersion     char(3)  NOT NULL,
        dateTimeStart    datetime NOT NULL default '0000-00-00T00:00:00',
        dateTimeEnd      datetime NOT NULL default '0000-00-00T00:00:00',
        acquisitionDate  datetime NOT NULL default '0000-00-00T00:00:00',
        creationDate     datetime NOT NULL default '0000-00-00T00:00:00',
        receiveDate      datetime NOT NULL default '0000-00-00T00:00:00',
        numSoundings     smallint NOT NULL,
        fileSize         integer  NOT NULL,
        FOREIGN KEY(pathID) REFERENCES rootPaths(pathID))
        """)
    cur.execute('create index dateTimeStartIndex1 on swir_l1b(dateTimeStart)')
    cur.execute('create index receiveDateIndex1 on swir_l1b(receiveDate)')

    cur.execute(
        """create table tir_l1b (
        tirID            integer  PRIMARY KEY AUTOINCREMENT,
        name             char(50) NOT NULL UNIQUE,
        pathID           integer  NOT NULL,
        passNumber	 smallint NOT NULL,
        sceneNumber	 smallint NOT NULL,
        operationMode    char(4)  NOT NULL,
        algorithmVersion char(3)  NOT NULL,
        paramVersion     char(3)  NOT NULL,
        dateTimeStart    datetime NOT NULL default '0000-00-00T00:00:00',
        dateTimeEnd      datetime NOT NULL default '0000-00-00T00:00:00',
        acquisitionDate  datetime NOT NULL default '0000-00-00T00:00:00',
        creationDate     datetime NOT NULL default '0000-00-00T00:00:00',
        receiveDate      datetime NOT NULL default '0000-00-00T00:00:00',
        numSoundings     smallint NOT NULL,
        fileSize         integer  NOT NULL,
        FOREIGN KEY(pathID) REFERENCES rootPaths(pathID))
        """)
    cur.execute('create index dateTimeStartIndex2 on tir_l1b(dateTimeStart)')
    cur.execute('create index receiveDateIndex2 on tir_l1b(receiveDate)')

    cur.execute(
        """create table common_l1b (
        commonID         integer  PRIMARY KEY AUTOINCREMENT,
        name             char(50) NOT NULL UNIQUE,
        pathID           integer  NOT NULL,
        passNumber	 smallint NOT NULL,
        sceneNumber	 smallint NOT NULL,
        operationMode    char(4)  NOT NULL,
        algorithmVersion char(3)  NOT NULL,
        paramVersion     char(3)  NOT NULL,
        dateTimeStart    datetime NOT NULL default '0000-00-00T00:00:00',
        dateTimeEnd      datetime NOT NULL default '0000-00-00T00:00:00',
        acquisitionDate  datetime NOT NULL default '0000-00-00T00:00:00',
        creationDate     datetime NOT NULL default '0000-00-00T00:00:00',
        receiveDate      datetime NOT NULL default '0000-00-00T00:00:00',
        numSoundings     smallint NOT NULL,
        fileSize         integer  NOT NULL,
        FOREIGN KEY(pathID) REFERENCES rootPaths(pathID))
        """)
    cur.execute('create index dateTimeStartIndex3 on tir_l1b(dateTimeStart)')
    cur.execute('create index receiveDateIndex3 on tir_l1b(receiveDate)')

    cur.close()
    con.commit()
    con.close()


# --------------------------------------------------
def sql_write_basedirs(dbname):
    """
    write names of directories where to find the GOSAT-2 products
    """
    list_paths = [
        {"host": 'shogun',
         "path": '/array/slot2B/GOSAT-2/FTS/L1B',
         "nfs": '/nfs/GOSAT2/FTS/L1B'}
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
class ArchiveGosat2():
    """
    class to archive GOSAT-2 products
    """
    def __init__(self, db_name='./sron_gosat2.db'):
        """
        initialize the class
        """
        self.dbname = db_name

        if not Path(db_name).is_file():
            cre_sqlite_gosat2_db(db_name)
            sql_write_basedirs(db_name)

    # -------------------------
    @staticmethod
    def rd_fts(flname):
        """
        read meta data from a GOSAT-2 FTS L1B product
        """
        gosatfl = Path(flname)
        stat = gosatfl.stat()

        dict_gosat = {}
        dict_gosat['fileName'] = gosatfl.name
        dict_gosat['filePath'] = str(gosatfl.parent)
        dict_gosat['acquisitionDate'] = strftime("%FT%T",
                                                 gmtime(stat.st_mtime))
        dict_gosat['passNumber'] = int(dict_gosat['fileName'][23:26])
        dict_gosat['sceneNumber'] = int(dict_gosat['fileName'][26:28])
        dict_gosat['operationMode'] = dict_gosat['fileName'][36:40]
        dict_gosat['productVersion'] = dict_gosat['fileName'][40:46]
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
                dset = grp['parameterVersion']
                dict_gosat['paramVersion'] = cleanup_string(dset)
                dset = grp['operationMode']
                dict_gosat['operationMode'] = cleanup_string(dset)

                if 'startDate' in grp:
                    dset = grp['startDate']
                    dict_gosat['dateTimeStart'] = cleanup_string(dset)
                    dset = grp['endDate']
                    dict_gosat['dateTimeEnd'] = cleanup_string(dset)
                else:
                    if grp['startDateSWIR'][:] == b'-':
                        dset = grp['startDateTIR']
                    elif grp['startDateTIR'][:] == b'-':
                        dset = grp['startDateSWIR']
                    elif grp['startDateSWIR'][:] < grp['startDateTIR'][:]:
                        dset = grp['startDateSWIR']
                    else:
                        dset = grp['startDateTIR']
                    dict_gosat['dateTimeStart'] = cleanup_string(dset)

                    if grp['endDateSWIR'][:] == b'-':
                        dset = grp['endDateTIR']
                    elif grp['endDateTIR'][:] == b'-':
                        dset = grp['endDateSWIR']
                    elif grp['endDateSWIR'][:] > grp['endDateTIR'][:]:
                        dset = grp['endDateSWIR']
                    else:
                        dset = grp['endDateTIR']
                    dict_gosat['dateTimeEnd'] = cleanup_string(dset)
            else:
                return {}

            if '/SoundingGeometry' in grp:
                grp = fid['/SoundingGeometry']
                dict_gosat['numSoundings'] = len(grp['latitude'])
            else:
                grp = fid['Telemetry_CAM']
                dict_gosat['numSoundings'] = grp['numSoundings'][0]

        return dict_gosat

    # -------------------------
    def check_entry(self, gosatfl, verbose=False) -> bool:
        """
        check if entry is already present in database
        """
        if gosatfl[6:11] != 'TFTS2':
            raise ValueError('expect an FTS L1B product')

        if gosatfl[29:32] == '1BS':
            table = 'swir_l1b'
            query_str = 'select swirID from %s where name=\'%s\''
        elif gosatfl[29:32] == '1BT':
            table = 'tir_l1b'
            query_str = 'select tirID from %s where name=\'%s\''
        elif gosatfl[29:32] == '1BC':
            table = 'common_l1b'
            query_str = 'select commonID from %s where name=\'%s\''
        else:
            raise ValueError('expect GOSAT-2 band SWIR, TIR or COMMON')

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
        if gosatfl[6:11] != 'TFTS2':
            raise ValueError('expect an FTS L1B product')

        if gosatfl[29:32] == '1BS':
            table = 'swir_l1b'
            query_str = 'select swirID from %s where name=\'%s\''
            remove_str = 'delete from %s where swirID=%d'
        elif gosatfl[29:32] == '1BT':
            table = 'tir_l1b'
            query_str = 'select tirID from %s where name=\'%s\''
            remove_str = 'delete from %s where tirID=%d'
        elif gosatfl[29:32] == '1BC':
            table = 'common_l1b'
            query_str = 'select commonID from %s where name=\'%s\''
            remove_str = 'delete from %s where commonID=%d'
        else:
            raise ValueError('expect GOSAT-2 band SWIR, TIR or COMMON')

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
        if gosatfl.name[0:11] != 'GOSAT2TFTS2':
            raise ValueError('Invalid sensor name: ', gosatfl.name[6:11])

        obs_name = None
        if gosatfl.name[29:32] == '1BS':
            table = 'swir_l1b'
            obs_name = 'SWIR_{}'
        elif gosatfl.name[29:32] == '1BT':
            table = 'tir_l1b'
            obs_name = 'TIR_{}'
        elif gosatfl.name[29:32] == '1BC':
            table = 'common_l1b'
            obs_name = 'COMMON_{}'
        else:
            raise ValueError('expect GOSAT-2 band SWIR, TIR and COMMON')

        dict_gosat = self.rd_fts(gosatfl_str)
        buffer = str(gosatfl.parent)
        if dict_gosat['operationMode'][3] == 'D':
            obs_name = obs_name.format('DAY')
        elif dict_gosat['operationMode'][3] == 'N':
            obs_name = obs_name.format('NIGHT')
        else:
            raise ValueError('expect GOSAT-2 band to contain DAY or NIGHT')
        indx = buffer.find(obs_name)
        basedir = buffer[0:indx-1]

        str_sql = 'insert into {} values'.format(table)
        str_sql += '(NULL,\'%(fileName)s\',%(pathID)d'\
                   ',%(passNumber)d,%(sceneNumber)d'\
                   ',\'%(operationMode)s\''\
                   ',\'%(algorithmVersion)s\',\'%(paramVersion)s\''\
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
            dict_gosat['pathID'] = 0

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

    gosatdb = ArchiveGosat2(args.dbname)
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
