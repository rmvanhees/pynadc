#!/usr/bin/env python3
"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Defines class ArchiveScia to add new entries to Sciamachy SQLite database

Copyright (c) 2016-2021 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
import argparse
import gzip
import sqlite3

from datetime import datetime
from pathlib import Path
from random import randint
from time import gmtime, strftime


# --------------------------------------------------
def cre_sqlite_scia_db(dbname):
    """
    function to define database for Sciamachy database and tables
    """
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute(
        """create table meta__0P (
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
        q_flag          integer NOT NULL)
        """)
    cur.execute('create index absOrbitIndex0 on meta__0P(absOrbit)')

    cur.execute(
        """create table meta__1P (
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
        fileSize	integer NOT NULL)
        """)
    cur.execute('create index absOrbitIndex1 on meta__1P(absOrbit)')

    cur.execute(
        """create table meta__2P (
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
        fileSize	integer NOT NULL)
        """)
    cur.execute('create index absOrbitIndex2 on meta__2P(absOrbit)')

    cur.execute(
        """create table quality_definition (
        flag    integer PRIMARY KEY,
        id      text NOT NULL UNIQUE,
        descr   text NOT NULL)
        """)
    cur.execute("""insert into quality_definition values(0, 'UNKNOWN',
       'Default value should not be used in database')""")
    cur.execute("""insert into quality_definition values(1, 'REJECT',
       'Obsolete product will be removed from archive')""")
    cur.execute("""insert into quality_definition values(2, '2SHORT',
       'Product contains less MDS than expected')""")
    cur.execute("""insert into quality_definition values(3, 'ORB_OFF',
       'Orbit number is not correct')""")
    cur.execute("""insert into quality_definition values(4, 'GOOD',
       'Default flag for NRT products')""")
    cur.execute("""insert into quality_definition values(5, '2LONG',
       'Product contains more MDS than expected')""")
    cur.execute("""insert into quality_definition values(6, 'ERROR',
       'Product errors detected - use with care, cl0 only')""")
    cur.execute("""insert into quality_definition values(7, 'SOLOMON',
       'Too many Reed Solomon corrections - use with care, cl0 only')""")
    cur.execute("""insert into quality_definition values(8, 'NOT_USED8',
       'Flag not yet used in database')""")
    cur.execute("""insert into quality_definition values(9, 'SODAP',
       'Consolidated SODAP product - no verification')""")
    cur.execute("""insert into quality_definition values(10, 'CONS',
       'File checked and passed succesfully')""")
    cur.close()
    con.commit()
    con.close()


# --------------------------------------------------
class ArchiveScia():
    """
    class to archive Sciamachy products
    """
    def __init__(self, db_name='./sron_scia.db'):
        """
        initialize the class
        """
        self.dbname = db_name

        if not Path(db_name).is_file():
            cre_sqlite_scia_db(db_name)

    # -------------------------
    @staticmethod
    def rd_lv0(sciafl_str):
        """
        read meta data from a Sciamachy level 0 product
        """
        sciafl = Path(sciafl_str)

        dict_meta = {}
        dict_meta['filePath'] = str(sciafl.parent)
        dict_meta['receiveDate'] = strftime("%F %T",
                                            gmtime(sciafl.stat().st_ctime))
        if sciafl.suffix == '.gz':
            dict_meta['fileName'] = sciafl.stem
            dict_meta['compress'] = True
            fp = gzip.open(sciafl_str, "rb")
        else:
            dict_meta['fileName'] = sciafl.name
            dict_meta['compress'] = False
            fp = open(sciafl_str, "rb")

        for _ in range(50):
            line = fp.readline()
            if not line:
                break

            words = line.decode('ascii').split('=')
            if len(words) < 2:
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
                _dt = datetime.strptime(words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f')
                if _dt.microsecond == 0:
                    _dt = _dt.replace(microsecond=randint(0, 99999))
                dict_meta['procTime'] = _dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif words[0] == "SOFTWARE_VER":
                dict_meta['softVersion'] = words[1][1:-2].rstrip()
            elif words[0] == "SENSING_START":
                _dt1 = datetime.strptime(words[1][1:-2],
                                         '%d-%b-%Y %H:%M:%S.%f')
                dict_meta['dateTimeStart'] = _dt1.strftime('%Y-%m-%d %H:%M:%S')
                dict_meta['muSeconds'] = _dt1.microsecond
            elif words[0] == "SENSING_STOP":
                _dt2 = datetime.strptime(words[1][1:-2],
                                         '%d-%b-%Y %H:%M:%S.%f')
            elif words[0] == "REL_ORBIT":
                dict_meta['relOrbit'] = int(words[1])
            elif words[0] == "ABS_ORBIT":
                dict_meta['absOrbit'] = int(words[1])
            elif words[0] == "TOT_SIZE":
                dict_meta['fileSize'] = int(words[1][0:21])
            elif words[0] == "NUM_DATA_SETS":
                dict_meta['numDataSets'] = int(words[1])
            elif words[0] == "SPH_DESCRIPTOR":
                break

        fp.close()

        _dt = _dt2 - _dt1
        dict_meta['duration'] = _dt.seconds + _dt.microseconds / 1e6
        return dict_meta

    # -------------------------
    @staticmethod
    def rd_lv1(sciafl_str):
        """
        read meta data from a Sciamachy level 1B product
        """
        sciafl = Path(sciafl_str)

        dict_meta = {}
        dict_meta['filePath'] = str(sciafl.parent)
        dict_meta['receiveDate'] = strftime("%F %T",
                                            gmtime(sciafl.stat().st_ctime))
        if sciafl.suffix == '.gz':
            dict_meta['fileName'] = sciafl.stem
            dict_meta['compress'] = True
            fp = gzip.open(sciafl_str, "rb")
        else:
            dict_meta['fileName'] = sciafl.name
            dict_meta['compress'] = False
            fp = open(sciafl_str, "rb")

        for _ in range(100):
            line = fp.readline()
            if not line:
                break

            words = line.decode('ascii').split('=')
            if len(words) < 2:
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
                _dt = datetime.strptime(words[1][1:-2], '%d-%b-%Y %H:%M:%S.%f')
                if _dt.microsecond == 0:
                    _dt = _dt.replace(microsecond=randint(0, 99999))
                dict_meta['procTime'] = _dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif words[0] == "SOFTWARE_VER":
                dict_meta['softVersion'] = words[1][1:-2].rstrip()
            elif words[0] == "REL_ORBIT":
                dict_meta['relOrbit'] = int(words[1])
            elif words[0] == "ABS_ORBIT":
                dict_meta['absOrbit'] = int(words[1])
            elif words[0] == "TOT_SIZE":
                dict_meta['fileSize'] = int(words[1][0:21])
            elif words[0] == "NUM_DATA_SETS":
                dict_meta['numDataSets'] = int(words[1])
            elif words[0] == "START_TIME":
                _dt1 = datetime.strptime(words[1][1:-2],
                                         '%d-%b-%Y %H:%M:%S.%f')
                dict_meta['dateTimeStart'] = _dt1.strftime('%Y-%m-%d %H:%M:%S')
                dict_meta['muSeconds'] = _dt1.microsecond
            elif words[0] == "STOP_TIME":
                _dt2 = datetime.strptime(words[1][1:-2],
                                         '%d-%b-%Y %H:%M:%S.%f')
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
                dict_meta['nadirStates'] = int(words[1])
            elif words[0] == "NO_OF_LIMB_STATES":
                dict_meta['limbStates'] = int(words[1])
            elif words[0] == "NO_OF_OCCULTATION_STATES":
                dict_meta['occulStates'] = int(words[1])
            elif words[0] == "NO_OF_MONI_STATES":
                dict_meta['monitorStates'] = int(words[1])
            elif words[0] == "NO_OF_NOPROC_STATES":
                dict_meta['noProcStates'] = int(words[1])
            elif words[0] == "DS_NAME":
                break

        fp.close()

        _dt = _dt2 - _dt1
        dict_meta['duration'] = _dt.seconds + _dt.microseconds / 1e6
        return dict_meta

    # -------------------------
    @staticmethod
    def rd_lv2(sciafl_str):
        """
        read meta data from a Sciamachy level 2 product
        """
        sciafl = Path(sciafl_str)

        dict_meta = {}
        nadir_products = []
        limb_products = []
        dict_meta['filePath'] = str(sciafl.parent)
        dict_meta['receiveDate'] = strftime("%F %T",
                                            gmtime(sciafl.stat().st_ctime))
        if sciafl.suffix == '.gz':
            dict_meta['fileName'] = sciafl.stem
            dict_meta['compress'] = True
            fp = gzip.open(sciafl_str, "rb")
        else:
            dict_meta['fileName'] = sciafl.name
            dict_meta['compress'] = False
            fp = open(sciafl_str, "rb")

        for _ in range(150):
            line = fp.readline()
            if not line:
                break

            words = line.decode('ascii').split('=')
            if len(words) < 2:
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
                _dt = datetime.strptime(words[1][1:-2],
                                        '%d-%b-%Y %H:%M:%S.%f')
                if _dt.microsecond == 0:
                    _dt = _dt.replace(microsecond=randint(0, 99999))
                dict_meta['procTime'] = _dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif words[0] == "SOFTWARE_VER":
                dict_meta['softVersion'] = words[1][1:-2].rstrip()
            elif words[0] == "REL_ORBIT":
                dict_meta['relOrbit'] = int(words[1])
            elif words[0] == "ABS_ORBIT":
                dict_meta['absOrbit'] = int(words[1])
            elif words[0] == "TOT_SIZE":
                dict_meta['fileSize'] = int(words[1][0:21])
            elif words[0] == "NUM_DATA_SETS":
                dict_meta['numDataSets'] = int(words[1])
            elif words[0] == "START_TIME":
                _dt1 = datetime.strptime(words[1][1:-2],
                                         '%d-%b-%Y %H:%M:%S.%f')
                dict_meta['dateTimeStart'] = _dt1.strftime('%Y-%m-%d %H:%M:%S')
                dict_meta['muSeconds'] = _dt1.microsecond
            elif words[0] == "STOP_TIME":
                _dt2 = datetime.strptime(words[1][1:-2],
                                         '%d-%b-%Y %H:%M:%S.%f')
            elif words[0] == "FITTING_ERROR_SUM":
                dict_meta['fittingErrSum'] = words[1][1:-2].rstrip()
            elif (words[0].find("NAD_FIT_WINDOW_") != -1
                  and words[1].find("EMPTY") == -1):
                if nadir_products:
                    nadir_products.append(',')
                nadir_products.append(words[1][1:-2].replace('- ',
                                                             '-').strip())
            elif (words[0].find("LIM_FIT_WINDOW_") != -1
                  and words[1].find("EMPTY") == -1):
                if limb_products:
                    limb_products.append(',')
                limb_products.append(words[1][1:-2].replace('- ',
                                                            '-').strip())
            elif words[0] == "DS_NAME":
                break
        fp.close()

        _dt = _dt2 - _dt1
        dict_meta['duration'] = _dt.seconds + _dt.microseconds / 1e6
        dict_meta['nadirProducts'] = ''.join(nadir_products)
        dict_meta['limbProducts'] = ''.join(limb_products)
        return dict_meta

    # -------------------------
    def check_entry(self, sciafl, verbose=False) -> bool:
        """
        check if entry is already present in database
        """
        query_fmt = "select name from meta__{}P where name=\'{}\'"

        if sciafl[0:10] == 'SCI_NL__0P':
            query_str = query_fmt.format(0, sciafl)
        elif sciafl[0:10] == 'SCI_NL__1P':
            query_str = query_fmt.format(1, sciafl)
        elif sciafl[0:10] == 'SCI_OL__2P' or sciafl[0:10] == 'SCI_NL__2P':
            query_str = query_fmt.format(2, sciafl)
        else:
            raise ValueError('Level of Sciamachy product is unknown')

        if verbose:
            print(query_str)

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute(query_str)
        row = cur.fetchone()
        cur.close()
        con.close()
        if row is None:
            return False

        return True

    # -------------------------
    def remove_entry(self, sciafl, verbose=False) -> None:
        """
        remove entry from database
        """
        if not self.check_entry(sciafl):
            return

        query_fmt = "delete from meta__{}P where name=\'{}\'"
        if sciafl[0:10] == 'SCI_NL__0P':
            query_str = query_fmt.format(0, sciafl)
        elif sciafl[0:10] == 'SCI_NL__1P':
            query_str = query_fmt.format(1, sciafl)
        elif sciafl[0:10] == 'SCI_OL__2P' or sciafl[0:10] == 'SCI_NL__2P':
            query_str = query_fmt.format(2, sciafl)
        else:
            raise ValueError('Level of Sciamachy product is unknown')

        if verbose:
            print(query_str)

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute(query_str)
        cur.close()
        con.commit()
        con.close()

    # -------------------------
    def add_entry(self, sciafl_str, debug=False):
        """
        add new entry to SQLite database
        """
        sciafl = Path(sciafl_str).name
        if sciafl[0:10] == 'SCI_NL__0P':
            str_sql = 'insert into meta__0P values' \
                      '(\'%(product)s\',\'%(filePath)s\',%(compress)d'\
                      ',\'%(procStage)s\',\'%(procCenter)s\''\
                      ',\'%(softVersion)s\',\'%(qualityFlag)s\''\
                      ',\'%(receiveDate)s\',\'%(procTime)s\''\
                      ',\'%(dateTimeStart)s\''\
                      ',%(muSeconds)d,%(duration)f,%(absOrbit)d,%(relOrbit)d'\
                      ',%(numDataSets)d,%(fileSize)d,%(q_flag)d)'
            str_sql_meta = str_sql % self.rd_lv0(sciafl_str)
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
            str_sql_meta = str_sql % self.rd_lv1(sciafl_str)
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
            str_sql_meta = str_sql % self.rd_lv2(sciafl_str)
        else:
            raise ValueError('Level of Sciamachy product is unknown')

        if debug:
            print(str_sql_meta)
            return

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute(str_sql_meta)
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
    parser.add_argument('--dbname', default='/SCIA/share/db/sron_scia.db',
                        help='name of SCIA/SQLite database')
    parser.add_argument('input_file', nargs='?', type=str,
                        help='read from INPUT_FILE')
    args = parser.parse_args()

    if not Path(args.input_file).is_file():
        print('Info: \"%s\" is not a valid file' % args.input_file)
        return

    sciadb = ArchiveScia(args.dbname)
    sciafl = Path(args.input_file).name

    # Check if product is already in database
    if not args.debug:
        if args.remove or args.replace:
            sciadb.remove_entry(sciafl)
        if args.remove:
            print('Info: {} is removed from database'.format(sciafl))
            return

        if sciadb.check_entry(sciafl):
            print('Info: {} is already stored in database'.format(sciafl))
            return

    sciadb.add_entry(args.input_file, debug=args.debug)


if __name__ == '__main__':
    main()
