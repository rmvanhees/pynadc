#!/usr/bin/env python
#
from __future__ import print_function
from __future__ import division

import sys

#-------------------------SECTION ARGPARSE----------------------------------
def handleCmdParams():
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser( 
        formatter_class=RawDescriptionHelpFormatter,
        description= 'read Sciamachy level 1b product'
        )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument( '--orbit', nargs=1, type=int, 
                         help='select data from given orbit, preferably \'W\'' )
    group.add_argument( '--file', type=str, help='read data from given file' )
    return parser.parse_args()

#-------------------------SECTION MAIN--------------------------------------
if __name__ == '__main__':
    import os.path
    from pynadc.scia import db,lv0

    args = handleCmdParams()

    scia_fl = ""
    if args.orbit is not None:
        fileList = db.get_product_by_type( prod_type='0',
                                           proc_best=True, 
                                           orbits=args.orbit )
        if len(fileList) > 0 and os.path.isfile( fileList[0] ):
            scia_fl = fileList[0]
    elif args.file is not None:
        if os.path.isfile( args.file ):
            scia_fl = args.file
        else:
            fileList = db.get_product_by_name( product=args.file )
            if len(fileList) > 0 and os.path.isfile( fileList[0] ):
                scia_fl = fileList[0]

    if not scia_fl:
        print( "Failed: file not found on your system" )
        sys.exit(1)

    print( scia_fl )
    # create object and open Sciamachy level 0 product
    try:
        obj = lv0.File( scia_fl )
    except lv0.fmtError as e:
        print( e.msg )
        sys.exit(1)

    print( obj.mph, '\n' )
    print( obj.sph, '\n' )
    print( obj.dsd, '\n' )

    obj.getMDS()
#    for ni in range(obj.mds.size):
#        print( obj.mds[ni] )

    obj.__del__()
