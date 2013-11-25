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
            mtbl = np.zeros( maxOrbit, 
                             dtype='uint16,uint8,uint8,uint16,uint16' )
            mtbl.dtype.names = ('orbit','num_clus','indx_Clcon',
                                'duration', 'num_info')
            mtbl[:]['orbit'] = np.arange( maxOrbit, dtype='uint16' )
            mtbl[:]['indx_Clcon'] = 2**8-1
            
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
                ds_clus = grp.create_dataset( 'clusDef', 
                                              data=clusDef.reshape(1,64),
                                              maxshape=(None,64) )
            else:
                ds_clus = grp['clusDef']
                clusDef_db = ds_clus[:]
                ax1 = ds_clus.shape[0]
                for ni in range(ax1):
                    if (clusDef_db[ni,:] == clusDef).all():
                        ds_mtbl[mtbl[0],'indx_Clcon'] = ni
                        return

                # new cluster definition: extent dataset
                ds_clus.resize(ax1+1, axis=0)
                ds_clus[ax1,:] = clusDef
                ds_mtbl[mtbl[0],'indx_Clcon'] = ax1

    def add_lv0_states( self ):
        with h5py.File( self.db_name, 'r+' ) as fid:

            grp = fid['State_24']
            ds_mtbl  = grp['metaTable']
            mtbl_dim = ds_mtbl.size
            ds_clus  = grp['clusDef']
            clus_dim = ds_clus.shape[0]
            if np.all(ds_mtbl[3034,'indx_Clcon'] == 255):
                clusDef = ds_clus[0,:]
                clusDef['pet'] = 1.0
                clusDef['intg'] = 16
                clusDef['coaddf'] = 1
                clusDef['readouts'] = 10
                clusDef['clus_type'] = 1
                clusDef['readouts'][0:3] = 1
                clusDef['pet'][0:3] = 10
                clusDef['intg'][0:3] = 160
                indx = np.where(clusDef['chan_id'] == 6)[0]
                clusDef['pet'][indx] = 0.5
                indx = np.where(clusDef['chan_id'] == 6)[0][2:11]
                clusDef['intg'][indx] = 8
                clusDef['readouts'][indx] = 5
                indx = np.where(clusDef['chan_id'] == 6)[0][np.array([0,1,11])]
                clusDef['intg'][indx] = 16
                clusDef['coaddf'][indx] = 2
                clusDef['clus_type'][indx] = 2
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[3034,'num_clus'] = 56
                ds_mtbl[3034,'indx_Clcon'] = clus_dim
                ds_mtbl[3034,'duration'] = 1280
                ds_mtbl[3034,'num_info'] = 160
                clus_dim += 1

            if np.all(ds_mtbl[36873:38267,'indx_Clcon'] == 255):
                clusDef = ds_clus[2,:]
                clusDef['pet'] = 1.0
                clusDef['intg'] = 16
                clusDef['coaddf'] = 1
                clusDef['readouts'] = 1
                clusDef['clus_type'] = 1
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[36873:38267,'num_clus'] = 40
                ds_mtbl[36873:38267,'indx_Clcon'] = clus_dim
                ds_mtbl[36873:38267,'duration'] = 1600
                ds_mtbl[36873:38267,'num_info'] = 100
                clus_dim += 1

            if np.all(ds_mtbl[47994:48074,'indx_Clcon'] == 255):
                clusDef = ds_clus[2,:]
                clusDef['pet'] = 1.0
                clusDef['intg'] = 16
                clusDef['coaddf'] = 1
                clusDef['readouts'] = 1
                clusDef['clus_type'] = 1
                ds_clus.resize(clus_dim+1, axis=0)
                ds_clus[clus_dim,:] = clusDef
                ds_mtbl[47994:48074,'num_clus'] = 40
                ds_mtbl[47994:48074,'indx_Clcon'] = clus_dim
                ds_mtbl[47994:48074,'duration'] = 1440
                ds_mtbl[47994:48074,'num_info'] = 90
                clus_dim += 1

    def fill_mtbl( self ):
        with h5py.File( self.db_name, 'r+' ) as fid:
            for ns in range(1, 71):
                grp = fid['State_%02d' % (ns)]
                if "clusDef" in grp:
                    ds_mtbl  = grp['metaTable']
                    mtbl_dim = ds_mtbl.size

                    num_clus   = ds_mtbl[:,'num_clus']
                    indx_Clcon = ds_mtbl[:,'indx_Clcon']
                    duration   = ds_mtbl[:,'duration']
                    num_info   = ds_mtbl[:,'num_info']

                    # skip all undefined entries at the start
                    nj = 0
                    while nj < mtbl_dim and indx_Clcon[nj] == 255:
                        nj += 1

                    # replace undefined entries
                    while nj < mtbl_dim:
                        ni = nj
                        while ni < mtbl_dim and indx_Clcon[ni] != 255:
                            ni += 1

                        val_num_clus = num_clus[ni-1]
                        val_indx     = indx_Clcon[ni-1]
                        val_duration = duration[ni-1]
                        val_num_info = num_info[ni-1]

                        nj = ni + 1
                        while nj < mtbl_dim and indx_Clcon[nj] == 255:
                            nj += 1

                        if nj == mtbl_dim: break

                        if indx_Clcon[nj] == val_indx \
                                and duration[nj] == val_duration:

                            print( 'State_%02d: ' % (ns), ni, nj, val_indx )
                            num_clus[ni:nj]   = val_num_clus
                            indx_Clcon[ni:nj] = val_indx
                            duration[ni:nj]   = val_duration
                            num_info[ni:nj]   = val_num_info

                    ds_mtbl[:,'num_clus']   = num_clus
                    ds_mtbl[:,'indx_Clcon'] = indx_Clcon
                    ds_mtbl[:,'duration']   = duration
                    ds_mtbl[:,'num_info']   = num_info
                elif ns == 65:
                    ds_mtbl = grp['metaTable']
                    ds_mtbl[2204:52867,'num_clus']   = 40
                    ds_mtbl[2204:52867,'indx_Clcon'] = 0
                    ds_mtbl[2204:52867,'duration']   = 320
                    ds_mtbl[2204:52867,'num_info']   = 40

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
        obj_db.add_lv0_states()
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
                 0, states['duration'][indx[0]], states['num_geo'][indx[0]] )
        Clcon = states['Clcon'][indx[0],:]
        if args.verbose:
            print( stateID, ' - ', indx[0], Clcon.shape )
        obj_db.append( stateID, mtbl, Clcon )

    obj.__del__()
