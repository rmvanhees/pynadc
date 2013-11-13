#!/usr/bin/env python
#
from __future__ import print_function
from __future__ import division

import sys

import numpy as np
import h5py

class clusDB:
    def __init__( self, args=None, db_name='./nadc_clusDef.h5',
                  verbose=False ):
        if args:
            self.db_name  = args.db_name
            self.verbose  = args.verbose
        else:
            self.db_name  = db_name
            self.verbose  = verbose

    def create( self ):
        maxOrbit = 53000

        with h5py.File( self.db_name, 'w', libver='latest' ) as fid:
            mtbl = np.zeros( maxOrbit, dtype='uint16,uint8,uint8,uint16' )
            mtbl.dtype.names = ('absOrbit','num_cluster','indx_clusDef','bcps')
            mtbl[:]['absOrbit'] = np.arange( maxOrbit, dtype='uint16' )
            mtbl[:]['indx_clusDef'] = 2**8-1
            
            for ns in range(1,71):
                grp = fid.create_group( "State_%02d" % (ns) )
                ds = grp.create_dataset( 'metaTable', data=mtbl,
                                         chunks=(16384 // mtbl.dtype.itemsize,),
                                         compression='gzip', compression_opts=1,
                                         shuffle=True )
            
    def append( self, stateID, mtbl, clusDef ):
        with h5py.File( self.db_name, 'r+' ) as fid:
            grp = fid['State_%02d' % (stateID)]
            ds_mtbl = grp['metaTable']
            ds_mtbl[mtbl[0]] = mtbl

            # check if dataset "clusDef" exists, if not create
            if not "clusDef" in grp:
                ds = grp.create_dataset( 'clusDef', 
                                         data=clusDef.reshape(1,64),
                                         maxshape=(None,64) )
            else:
                ds = grp['clusDef']
                clusDef_db = ds[:]
                ax1 = ds.shape[0]
                for ni in range(ax1):
                    if (clusDef_db[ni,:] == clusDef).all():
                        ds_mtbl[mtbl[0],'indx_clusDef'] = ni
                        return

                # new cluster definition: extent dataset
                ds.resize(ax1+1, axis=0)
                ds[ax1,:] = clusDef
                ds_mtbl[mtbl[0],'indx_clusDef'] = ax1

    def fill_mtbl( self ):
        with h5py.File( self.db_name, 'r+' ) as fid:
            for ns in range(1,71):
                grp = fid['State_%02d' % (ns)]
                if "clusDef" in grp:
                    ds_mtbl = grp['metaTable']
                    num_cluster  = ds_mtbl[:,'num_cluster']
                    indx_clusDef = ds_mtbl[:,'indx_clusDef']
                    bcps        = ds_mtbl[:,'bcps']

                    # skip all undefined entries at the start
                    nj = 0
                    while nj < indx_clusDef.size and indx_clusDef[nj] == 255:
                        nj += 1

                    # replace undefined entries
                    while nj < indx_clusDef.size:
                        ni = nj
                        while ni < indx_clusDef.size \
                                and indx_clusDef[ni] != 255:
                            ni += 1

                        val_num  = num_cluster[ni-1]
                        val_indx = indx_clusDef[ni-1]
                        val_bcps = bcps[ni-1]

                        nj = ni + 1
                        while nj < indx_clusDef.size \
                                and indx_clusDef[nj] == 255:
                            nj += 1

                        if nj == indx_clusDef.size: break


                        if indx_clusDef[nj] == val_indx \
                                and bcps[nj] == val_bcps:

                            print( 'State_%02d: ' % (ns), ni, nj, val_indx )
                            num_cluster[ni:nj]  = val_num
                            indx_clusDef[ni:nj] = val_indx
                            bcps[ni:nj]         = val_bcps

                    ds_mtbl[:,'num_cluster']  = num_cluster
                    ds_mtbl[:,'indx_clusDef'] = indx_clusDef
                    ds_mtbl[:,'bcps']         = bcps
                elif ns == 65:
                    ds_mtbl = grp['metaTable']
                    ds_mtbl[2204:52867,'num_cluster']  = 40
                    ds_mtbl[2204:52867,'indx_clusDef'] = 0
                    ds_mtbl[2204:52867,'bcps']         = 320

                    grp_46 = fid['State_46']
                    ds_46 = grp_46['clusDef']
                    clusDef = ds_46[0,:]
                    ds = grp.create_dataset( 'clusDef', 
                                             data=clusDef.reshape(1,64),
                                             maxshape=(None,64) )
                else:
                    print( "Info: skipping state %d" % (ns) )
        
#-------------------------SECTION ARGPARSE----------------------------------
def handleCmdParams():
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser( 
        formatter_class=RawDescriptionHelpFormatter,
        description= 'read Sciamachy level 1b product'
        )
    parser.add_argument( '-v', '--verbose', action='store_true',
                         help='be verbose' )
    parser.add_argument( 'db_name', nargs='?', type=str,
                         default='./nadc_clusDef.h5',
                         help='write to hdf5 database' )    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument( '--orbit', nargs=1, type=int, 
                         help='select data from given orbit, preferably \'W\'' )
    group.add_argument( '--file', type=str, help='read data from given file' )
    group.add_argument( '--mtbl_fill', action='store_true',
                        help='update missing data to metaTable' )
    return parser.parse_args()

#-------------------------SECTION MAIN--------------------------------------
if __name__ == '__main__':
    import os.path
    import scia.db as db
    import scia.lv1 as lv1

    args = handleCmdParams()

    scia_fl = ""
    if args.orbit is not None:
        fileList = db.get_product_by_type( prod_type='1',
                                           proc_stage='W', 
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
    else:
        obj_db = clusDB( args )
        obj_db.fill_mtbl()
        sys.exit(0)

    if not scia_fl:
        print( "Failed: file not found on your system" )
        sys.exit(0)

    print( scia_fl )
    # create object and open Sciamachy level 1b product
    try:
        obj = lv1.File( scia_fl )
    except lv1.fmtError as e:
        print( e.msg )
        sys.exit(1)

    # check fileSize
    if obj.mph['TOTAL_SIZE'] != os.path.getsize(scia_fl):
        print( 'Fatal: file %s incomplete' % scia_fl)
        sys.exit(1)

    # read STATES GADS
    obj.getSTATES()

    if args.verbose:
        print( obj.states.dtype.names )
        print( obj.states['Clcon'].dtype.names )
        for xx in range( obj.states.size ):
            print( obj.states['state_id'][xx], obj.states['flag_attached'][xx], 
                   obj.states['flag_reason'][xx], obj.states['duration'][xx], 
                   obj.states['length'][xx],
                   obj.states['Clcon'][xx]['intg'][0:obj.states['num_clus'][xx]]
                   )

    # remove corrupted states
    states = obj.states[(obj.states['flag_reason'] != 1)
                        & (obj.states['duration'] != 0)] 

    # create clusterDef database object
    obj_db = clusDB( args )
    if not os.path.exists( obj_db.db_name ):
        obj_db.create()

    # loop over all ID of states
    for stateID in np.unique( states['state_id'] ):
        indx = np.where( states['state_id'] == stateID )[0]
        mtbl = ( obj.mph['ABS_ORBIT'], states['num_clus'][indx[0]],
                 0, states['duration'][indx[0]] )
        Clcon = states['Clcon'][indx[0],:]
        if args.verbose:
            print( stateID, ' - ', indx[0], Clcon.shape )
        obj_db.append( stateID, mtbl, Clcon )

    obj.__del__()

    
