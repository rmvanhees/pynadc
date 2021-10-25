"""
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Methods to query the NADC GOSAT-2 SQLite database

Copyright (c) 2019 SRON - Netherlands Institute for Space Research
   All Rights Reserved

License:  BSD-3-Clause
"""
import platform
import sqlite3

from pathlib import Path


def subdir_from_product(name):
    """
    Define function to construct date sub-directories from FTS product-name:
     1) observationName: [COMMON|SWIR|TIR]_[DAY|NIGHT]
     2) productVersion: [xxxxxx]
     3) Year: [yyyy]
     4) Month: [mm]
     5) Day: [dd]
    """
    obs_name = None
    if name[29:32] == '1BS':
        obs_name = 'SWIR_{}'
    elif name[29:32] == '1BT':
        obs_name = 'TIR_{}'
    elif name[29:32] == '1BC':
        obs_name = 'COMMON_{}'
    else:
        raise ValueError('expect GOSAT-2 band COMMON, SWIR or TIR')

    if name[39] == 'D':
        obs_name = obs_name.format('DAY')
    elif name[39] == 'N':
        obs_name = obs_name.format('NIGHT')
    else:
        raise ValueError('expect GOSAT-2 Day or Night observations')

    return Path(obs_name, name[40:46], name[11:15], name[15:17], name[17:19],
                name)


# --------------------------------------------------
def get_product_by_name(args=None, dbname=None, product=None,
                        to_screen=False, dump=False, debug=False):
    """
    Query NADC GOSAT-2 SQLite database on product name

    Input
    -----
    args     : dictionary with keys dbname, product, to_screen, dump, debug
    dbname   : full path to GOSAT-2 SQLite database
    product  : name of product [value required]
    to_screen : print query result to standard output [default: False]
    dump     : return database content about product, instead of full-path
    debug    : do not query data base, but display SQL query [default: False]

    Output
    ------
    return full-path to product [default] or show database content about product

    """
    if args:
        dbname = args.dbname
        product = args.product
        dump = args.dump
        debug = args.debug

    if dbname is None:
        print('Fatal, SQLite database is not specified')
        return []

    if not Path(dbname).is_file():
        print('Fatal, can not find SQLite database: %s' % dbname)
        return []

    if product[0:11] != 'GOSAT2TFTS2':
        raise ValueError('expect an FTS L1B product')

    if product[29:32] == '1BS':
        table = 'swir_l1b'
    elif product[29:32] == '1BT':
        table = 'tir_l1b'
    elif product[29:32] == '1BC':
        table = 'common_l1b'
    else:
        raise ValueError('expect GOSAT-2 band SWIR, TIR or COMMON')

    if dump:
        select_str = '*'
    else:
        select_str = 'pathID, name'

    query_str = 'select {} from {} where name=\'{}\''.format(select_str,
                                                             table,
                                                             product)
    # perform query on database
    # pylint: disable=no-member
    conn = sqlite3.connect(dbname)
    if dump:
        conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if debug:
        print(query_str)
        row = (1, product)
    else:
        cur.execute(query_str)
        row = cur.fetchone()
        if row is None:
            conn.close()
            return []

    # obtain root directories (local or NFS)
    if not dump:
        case_str = 'case when hostName == \'{}\''\
            ' then localPath else nfsPath end'.format(platform.node())
        query_str = 'select {} from rootPaths where pathID={}'.format(case_str,
                                                                      row[0])
        if debug:
            print(query_str)
            root = '/fakePath/toProduct'
        else:
            cur.execute(query_str)
            root = cur.fetchone()[0]
    conn.close()

    if dump:
        if to_screen:
            for name in row.keys():
                print(name, '\t', row[name])
        return row

    full_path = Path(root, subdir_from_product(row[1]))
    if to_screen:
        print(full_path)

    return str(full_path)


# --------------------------------------------------
def get_product_by_type(args=None, dbname=None, prod_type=None,
                        date=None, rtime=None,
                        band=None, prod_version=None,
                        to_screen=False, debug=False):
    """
    Query NADC GOSAT-2 SQLite database on product type with data selections

    Input
    -----
    args      : dictionary with keys dbname, type, date, rtime, obs_mode,
                prod_version, to_screen, dump, debug
    dbname    : full path to GOSAT-2 SQLite database
    prod_type : type of product, supported 1B_FTS [value required]
    date      : select on dateTimeStart [default: None]
    rtime     : select on receiveTime [default: None]
    band      : select on band and observation mode, supported
                SWIR_DAY, TIR_DAY, TIR_NIGHT, COMMON_DAY, COMMON_NIGHT
    prod_version : select on product version:
                algorithmVersion + parameterVersion [default: None]
    to_screen  : print query result to standard output [default: False]
    debug     : do not query data base, but display SQL query [default: False]

    Output
    ------
    return full-path to selected products [default]
    """
    if args:
        dbname = args.dbname
        prod_type = args.type
        band = args.band
        prod_version = args.prod_version
        date = args.date
        rtime = args.rtime
        debug = args.debug

    if dbname is None:
        print('Fatal, SQLite database is not specified')
        return []

    if not Path(dbname).is_file():
        print('Fatal, can not find SQLite database: %s' % dbname)
        return []

    if prod_type != '1B_FTS':
        raise ValueError('not supported product type: {}'.format(prod_type))

    if band.startswith('SWIR'):
        table = 'swir_l1b'
    elif band.startswith('TIR'):
        table = 'tir_l1b'
    elif band.startswith('COMMON'):
        table = 'common_l1b'
    else:
        raise ValueError('expect GOSAT-2 band to be SWIR, TIR or COMMON')

    # define query on meta-Table
    query_str = ['select pathID, name from {}'.format(table)]

    # define query to obtain root directories (local or NFS)
    case_str = 'case when hostName == \'{}\''\
               ' then localPath else nfsPath end'.format(platform.node())
    query_str2 = 'select {} from rootPaths where pathID={}'

    # perform selection on datetime
    if date:
        if len(query_str) == 1:
            query_str.append('where')
        else:
            query_str.append('and')

        year = int(date[0:4])
        dtime = '+1 year'
        if len(date) >= 6:
            month = int(date[4:6])
            dtime = '+1 month'
        else:
            month = 1
        if len(date) >= 8:
            day = int(date[6:8])
            dtime = '+1 day'
        else:
            day = 1
        if len(date) >= 10:
            hour = int(date[8:10])
            dtime = '+1 hour'
        else:
            hour = 0
        if len(date) >= 12:
            minu = int(date[10:12])
            dtime = '+1 minute'
        else:
            minu = 0
        _d1 = '%04d-%02d-%02d %02d:%02d:%02d' % (year, month, day,
                                                 hour, minu, 0)

        mystr = 'dateTimeStart between \'{}\' and datetime(\'{}\',\'{}\')'
        query_str.append(mystr.format(_d1, _d1, dtime))

    if rtime:
        if len(query_str) == 1:
            query_str.append('where')
        else:
            query_str.append('and')

        mystr = 'receiveDate between datetime(\'now\',\'-{} {}\')' \
            + ' and datetime(\'now\')'
        if rtime[-1] == 'h':
            query_str.append(mystr.format(rtime[:-1], 'hour'))
        else:
            query_str.append(mystr.format(rtime[:-1], 'day'))

    if band:
        if len(query_str) == 1:
            query_str.append('where')
        else:
            query_str.append('and')

        if band.endswith('DAY'):
            query_str.append('operationMode == \'OB1D\'')
        elif band.endswith('NIGHT'):
            query_str.append('operationMode == \'OB1N\'')
        else:
            raise ValueError('expect GOSAT-2 band to contain DAY or NIGHT')

    if prod_version:
        if len(query_str) == 1:
            query_str.append('where')
        else:
            query_str.append('and')

        query_str.append('algorithmVersion == \'%s\'' % prod_version[:3])
        query_str.append('and paramVersion == \'%s\'' % prod_version[3:])

    # pylint: disable=no-member
    row_list = []
    if debug:
        print(' '.join(query_str))
        print(query_str2.format(case_str, 1))
    else:
        conn = sqlite3.connect(dbname)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(' '.join(query_str))

        cuu = conn.cursor()
        for row in cur:
            cuu.execute(query_str2.format(case_str, row[0]))
            root = cuu.fetchone()[0]

            full_path = Path(root, subdir_from_product(row[1]))
            if to_screen:
                print(full_path)

            row_list.append(str(full_path))

        conn.close()

    return row_list
