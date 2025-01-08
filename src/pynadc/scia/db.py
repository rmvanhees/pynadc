#
# This file is part of pynadc
#
# https://github.com/rmvanhees/pynadc
#
# Copyright (c) 2012-2021 SRON - Netherlands Institute for Space Research
#    All Rights Reserved
#
# License:  BSD-3-Clause
#
"""Methods to query the NADC Sciamachy SQLite database."""

from __future__ import annotations

import sqlite3
from pathlib import Path


# --------------------------------------------------
def get_product_by_name(
    args: dict | None = None,
    dbname: str | None = None,
    product: str | None = None,
    to_screen: bool = False,
    dump: bool = False,
    debug: bool = False,
) -> str | list[str, ...]:
    """Query NADC Sciamachy SQLite database on product name.

    Parameters
    ----------
    args : dict, optional
       dictionary with keys dbname, product, to_screen, dump, debug
    dbname : str, optional
       full path to Sciamachy SQLite database
    product : str
       name of product [value required]
    to_screen :  bool, default=False
       print query result to standard output [default: False]
    dump : bool, default=False
       return database content about product, instead of full-path
    debug :  bool, default=False
       do not query data base, but display SQL query [default: False]

    Returns
    -------
    return full-path to product [default]
           or show database content about product

    """
    if args:
        dbname = args.dbname
        product = args.product
        dump = args.dump
        debug = args.debug

    if dbname is None:
        print("Fatal, SQLite database is not specified")
        return []

    if not Path(dbname).is_file():
        print(f"Fatal, can not find SQLite database: {dbname}")
        return []

    if product[0:10] == "SCI_NL__0P":
        table = "meta__0P"
    elif product[0:10] == "SCI_NL__1P":
        table = "meta__1P"
    else:
        table = "meta__2P"

    select_str = "*" if dump else "path,name,compression"
    query_str = f"select {select_str} from {table} where name={product:r}"

    # pylint: disable=no-member
    conn = sqlite3.connect(dbname)
    if dump:
        conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if debug:
        print(query_str)
        conn.close()
        return []

    cur.execute(query_str)
    row = cur.fetchone()
    if row is None:
        conn.close()
        return []

    if to_screen:
        if dump:
            for name in row:
                print(name, "\t", row[name])
        else:
            if row[2] == 0:
                print(Path(*row[:-1]))
            else:
                print(Path(*row[:-1]).with_suffix(".gz"))

    if dump:
        return row

    if row[2] == 0:
        return str(Path(*row[:-1]))

    return str(Path(*row[:-1]).with_suffix(".gz"))


# --------------------------------------------------
def get_product_by_type(
    args: dict | None = None,
    dbname: str | None = None,
    prod_type: str | None = None,
    proc_stage: str | None = None,
    proc_best: str | None = None,
    orbits: list[int, ...] | None = None,
    date: str | None = None,
    rtime: str | None = None,
    to_screen: bool = False,
    dump: bool = False,
    debug: bool = False,
) -> str:
    """Query NADC Sciamachy SQLite database on product type with data selections.

    Input
    -----
    args :  dict
       dictionary with keys dbname, type, proc, best, orbit, date,
       rtime, to_screen, dump, debug
    dbname : str, optional
       full path to Sciamachy SQLite database
    prod_type :  int, optional
       level of product, available 0, 1, 2
    prod_stage :  str, optional
       baseline of product (PROC_STAGE): N, R, P, R, U, W, ...
    prod_best :  str, optional
       select highest available baseline
    orbit : list[int, ...] | None, optional
       select on absolute orbit number
    date :  str, optional
       select on dateTimeStart
    rtime :  str, optional
       select on receiveTime
    to_screen :  bool, default=False
       print query result to standard output
    dump : bool, default=False
       return database content about product, instead of full-path
    debug :  bool, default=False
       do not query data base, but display SQL query

    Returns
    -------
       full-path to selected products

    """
    if args:
        dbname = args.dbname
        prod_type = args.type
        proc_stage = args.proc
        proc_best = args.best
        orbits = args.orbit
        date = args.date
        rtime = args.rtime
        dump = args.dump
        debug = args.debug

    if dbname is None:
        print("Fatal, SQLite database is not specified")
        return []

    if not Path(dbname).is_file():
        print(f"Fatal, can not find SQLite database: {dbname}")
        return []

    if dump:
        query_str = [f"select * from meta__{prod_type}P"]
    else:
        query_str = [f"select path,name,compression from meta__{prod_type}P"]
    if proc_best:
        if prod_type == "0":
            query_str.append(" as s1 join (select absOrbit,MAX(q_flag)")
            query_str.append(f" as qflag from meta__{prod_type}P")
        else:
            query_str.append(" as s1 join (select absOrbit,MAX(procStage)")
            query_str.append(f" as proc from meta__{prod_type}P")

    if orbits:
        if " where" not in query_str:
            query_str.append(" where")
        else:
            query_str.append(" and")

        if len(orbits) == 1:
            mystr = " absOrbit={orbits[0]:-d}"
        else:
            mystr = f" absOrbit between {orbits[0]:-d} and {orbits[1]:-d}"
        query_str.append(mystr)

    if proc_stage:
        if " where" not in query_str:
            query_str.append(" where")
        else:
            query_str.append(" and")

        mystr = " procStage in ("
        for _c in proc_stage:
            if mystr[-1] != "(":
                mystr += ","
            mystr += "'" + _c + "'"
        mystr += ")"
        query_str.append(mystr)

    if date:
        if " where" not in query_str:
            query_str.append(" where")
        else:
            query_str.append(" and")

        dtime = "+1 second"
        year = int(date[0:4])
        dtime = "+1 year"

        if len(date) >= 6:
            month = int(date[4:6])
            dtime = "+1 month"
        else:
            month = 1

        if len(date) >= 8:
            day = int(date[6:8])
            dtime = "+1 day"
        else:
            day = 1

        if len(date) >= 10:
            hour = int(date[8:10])
            dtime = "+1 hour"
        else:
            hour = 0

        if len(date) >= 12:
            minu = int(date[10:12])
            dtime = "+1 minute"
        else:
            minu = 0
        _d1 = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minu:02d}:{0:02d}"

        mystr = " dateTimeStart between '%s' and datetime('%s','%s')"
        query_str.append(mystr % (_d1, _d1, dtime))

    if rtime:
        if " where" not in query_str:
            query_str.append(" where")
        else:
            query_str.append(" and")

        mystr = " receiveDate between datetime('now','-%-d %s') and datetime('now')"
        if rtime[-1] == "h":
            query_str.append(mystr % (int(rtime[0:-1]), "hour"))
        else:
            query_str.append(mystr % (int(rtime[0:-1]), "day"))

    if proc_best:
        query_str.append(" GROUP by absOrbit) as s2 on")
        query_str.append(" s1.absOrbit=s2.absOrbit")
        if prod_type == "0":
            query_str.append(" and s1.q_flag=s2.qflag")
        else:
            query_str.append(" and s1.procStage=s2.proc")
    else:
        query_str.append(" order by absOrbit ASC, procStage DESC")

    if debug:
        print("".join(query_str))
        return []

    # pylint: disable=no-member
    row_list = []
    conn = sqlite3.connect(dbname)
    if dump:
        conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("".join(query_str))
    for row in cur:
        if to_screen:
            if dump:
                print(row)
            else:
                if row[2] == 0:
                    print(Path(*row[:-1]))
                else:
                    print(Path(*row[:-1]).with_suffix(".gz"))
        else:
            if dump:
                row_list.append(row)
            else:
                if row[2] == 0:
                    row_list.append(str(Path(*row[:-1])))
                else:
                    row_list.append(str(Path(*row[:-1]).with_suffix(".gz")))

    conn.close()
    return row_list
