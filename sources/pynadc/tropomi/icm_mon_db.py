# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to create and fill ICM monitoring databases

-- Adding new entries to the database --
 Public methods to add new entries to the monitoring databases are:
 * h5_write_hk
 * h5_write_frames
 * sql_write_meta

-- Updating existing entries in the database --
 Public methods to update existing entries in the monitoring databases are:
 * h5_update_hk
 * h5_update_frames
 * sql_update_meta

-- Query the database --
 Public methods to query the monitoring databases are:
 * ...

-- Configuration management --
* ICM product version [KNMI]
  The version of the ICM processor is visible in the SQLite monitoring database,
  reprocessed L1b products will initiate a reprocessing of the results in the
  monitoring database

* Monitor algorithm version [SRON]
  Major version updates of the Monitoring algorithms should indicate that a 
  reprocessing of the monitoring results is necessary, otherwise no reprocessing
  is necessary

* Monitor database format [SRON]
  Major version updates of the Monitoring databases should indicate that a 
  reprocessing of the monitoring results is necessary, otherwise no reprocessing
  is necessary
'''

import os.path
import sqlite3

from importlib import util

import numpy as np
import h5py

from pynadc.stats import biweight

class ICM_mon( object ):
    '''
    '''
    def __init__( self, dbname, orbit_window=15 ):
        '''
        Perform the following tasts:
        1) if databases do not exist then create them
        2) check versions of databases and this S/W
        3) set class attributes
        '''
        self.dbname = dbname
        
        if not os.path.exists( dbname+'.h5' ) \
           and not os.path.exists( dbname+'.db' ):
            self.__init = True
            self.__fid = h5py.File( dbname+'.h5', 'w' )
            self.__fid.attrs['orbit_window'] = orbit_window
            ## write database meta-data
        else:
            self.__init = False
            self.__fid = h5py.File( dbname+'.h5', 'r+' )
            ## check versions

    def __repr__( self ):
        pass
    
    def __del__( self ):
        '''
        first check if SQLite and HDF5 are consistent
        then close access to databases
        '''
        self.__fid.close()

    ## ---------- WRITE HOUSE-KEEPING DATA ----------
    def __h5_cre_hk( self, hk ):
        '''
        Create datasets for house-keeping data.
        seperated datasets for  biweight medium and scale

        Todo: only include hk-data when the instrument is really performing 
              the requested measurements. 
              Reject entries during start-up or at the end
        '''
        # SWIR related house-keeping data
        nameList = ('temp_det4','temp_obm_swir','temp_cu_sls_stim',
                    'temp_obm_swir_grating','temp_obm_swir_if',
                    'temp_pelt_cu_sls1','temp_pelt_cu_sls2',
                    'temp_pelt_cu_sls3','temp_pelt_cu_sls4',
                    'temp_pelt_cu_sls5','swir_vdet_bias',
                    'difm_status', 'det4_led_status','wls_status',
                    'common_led_status','sls1_status','sls2_status',
                    'sls3_status','sls4_status','sls5_status')
        dtypeList = ()
        for name in nameList:
            dtypeList += (hk[name].dtype.name,)

        hk_buff = np.empty( 1, dtype=','.join(dtypeList) )
        hk_buff.dtype.names = nameList
        dset = self.__fid.create_dataset( "hk_medium", (0,), maxshape=(None,),
                                          dtype=hk_buff.dtype )
        dset = self.__fid.create_dataset( "hk_scale", (0,), maxshape=(None,),
                                          dtype=hk_buff.dtype )
                
    def h5_write_hk( self, hk ):
        '''
        Create datasets for house-keeping data.
        seperated datasets for  biweight medium and scale

        Todo: only include hk-data when the instrument is really performing 
              the requested measurements. 
              Reject entries during start-up or at the end
        '''
        if self.__init:
            self.__h5_cre_hk( hk )

        hk_median = np.empty( 1, dtype=self.__fid['hk_medium'].dtype )
        hk_scale = np.empty( 1, dtype=self.__fid['hk_medium'].dtype )
        for name in self.__fid['hk_medium'].dtype.names:
            if hk[name].dtype.name.find( 'float' ) >= 0:
                (mx, sx) = biweight(hk[name], scale=True)
                hk_median[name] = mx
                hk_scale[name]  = sx
            elif hk[name].dtype.name.find( 'int' ) >= 0:
                hk_median[name] = np.median(hk[name])
                hk_scale[name]  = np.all(hk[name])
            else:
                print( name )
        dset = self.__fid['hk_medium']
        dset.resize( (dset.shape[0]+1,) )
        dset[-1] = hk_median

        dset = self.__fid['hk_scale']
        dset.resize( (dset.shape[0]+1,) )
        dset[-1] = hk_scale

    ## ---------- WRITE DATA (frames averaged in time) ----------
    def __h5_cre_frames( self, frames, method='std',
                         rows=False, cols=False ):
        '''
        create datasets for measurement data
        '''
        dset = self.__fid.create_dataset( "signal_avg",
                                          (0,) + frames.shape,
                                          chunks=(1,) + frames.shape,
                                          maxshape=(None,) + frames.shape,
                                          dtype=frames.dtype )
        dset = self.__fid.create_dataset( "signal_avg_{}".format(method),
                                          (0,) + frames.shape,
                                          chunks=(1,) + frames.shape,
                                          maxshape=(None,) + frames.shape,
                                          dtype=frames.dtype )
        if rows:
            nrows = (frames.shape[1],)
            dset = self.__fid.create_dataset( "signal_avg_row",
                                              (0,) + nrows,
                                              chunks=(1,) + nrows,
                                              maxshape=(None,) + nrows,
                                              dtype=frames.dtype )
            dset = self.__fid.create_dataset( "signal_avg_row_{}".format(method),
                                              (0,) + nrows,
                                              chunks=(1,) + nrows,
                                              maxshape=(None,) + nrows,
                                              dtype=frames.dtype )

        if cols:
            ncols = (frames.shape[0],)
            dset = self.__fid.create_dataset( "signal_avg_col",
                                              (0,) + ncols,
                                              chunks=(1,) + ncols,
                                              maxshape=(None,) + ncols,
                                              dtype=frames.dtype )
            dset = self.__fid.create_dataset( "signal_avg_col_{}".format(method),
                                              (0,) + ncols,
                                              chunks=(1,) + ncols,
                                              maxshape=(None,) + ncols,
                                              dtype=frames.dtype )

    def h5_write_frames( self, values, errors, statistics=None ):
        '''
        parameter statistics may contain the following strings (comma-separated)
         - error, noise or std  : description of parameter errors (first field)
         - rows : when row average/median should be calculated from frames
         - cols : when column average/median should be calculated from frames
        '''
        if self.__init:
            if statistics is None:
                self.__fid.attrs['method'] = 'std'
                self.__fid.attrs['rows'] = False
                self.__fid.attrs['cols'] = False
            else:
                stat_list = statistics.split(',')
                self.__fid.attrs['method'] = stat_list[0]
                self.__fid.attrs['rows'] = 'rows' in stat_list
                self.__fid.attrs['cols'] = 'cols' in stat_list
            self.__h5_cre_frames( values,
                                  method=self.__fid.attrs['method'],
                                  rows=self.__fid.attrs['rows'],
                                  cols=self.__fid.attrs['cols'] )

        dset = self.__fid['signal_avg']
        shape = (dset.shape[0]+1,) + dset.shape[1:]
        dset.resize( shape )
        dset[-1,:,:] = values

        ext = self.__fid.attrs['method']
        dset = self.__fid['signal_avg_{}'.format(ext)]
        shape = (dset.shape[0]+1,) + dset.shape[1:]
        dset.resize( shape )
        dset[-1,:,:] = values

        if self.__fid.attrs['rows']:
            dset = self.__fid['signal_avg_row']
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:] = np.nanmedian( values, axis=0 )

            dset = self.__fid['signal_avg_row_{}'.format(ext)]
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:] = np.nanmedian( errors, axis=0 )
 
        if self.__fid.attrs['cols']:
            dset = self.__fid['signal_avg_col']
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:] = np.nanmedian( values, axis=1 )

            dset = self.__fid['signal_avg_col_{}'.format(ext)]
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:] = np.nanmedian( errors, axis=1 )
 
            
   ## ---------- WRITE META-DATA TO SQL database ----------
    def __sql_cre_meta( self ):
        dbname = self.dbname + '.db'
        
        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( '''create table icm_meta (
           referenceOrbit  integer     PRIMARY KEY,
           orbitsUsed      integer     NOT NULL,
           entryDateTime   datetime    NOT NULL default '0000-00-00 00:00:00',
           startDateTime   datetime    NOT NULL default '0000-00-00 00:00:00',
           icmVersion      text        NOT NULL,
           algVersion      text        NOT NULL,
           dbVersion       text        NOT NULL,
           q_detTemp       integer     NOT NULL,
           q_obmTemp       integer     NOT NULL
        )''' )
        cur.execute( 'create index entryIndx on icm_meta(entryDateTime)' )
        cur.execute( 'create index startIndx on icm_meta(startDateTime)' )
        cur.close()
        con.commit()
        con.close()

    def sql_write_meta( self, meta_dict ):
        '''
        Append monitoring meta-data to SQLite database
        '''
        dbname = self.dbname + '.db'
        if self.__init:
            self.__sql_cre_meta()

        str_sql = 'insert into icm_meta values' \
                  '({orbit},{orbit_used},{entry_date!r},{start_time!r}'\
                  ',{icm_version!r},{alg_version!r},{db_version!r}'\
                  ',{q_det_temp},{q_obm_temp})'
        print( str_sql.format(**meta_dict) )

        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql.format(**meta_dict) )
        cur.close()
        con.commit()
        con.close()

#--------------------------------------------------
def test( num_orbits=1 ):
    '''
    Perform some simple test to check the ICM_mon_db class
    '''
    import os
    import shutil
    
    from datetime import datetime

    from pynadc.tropomi.icm_io import ICM_io

    DBNAME = 'mon_test'
    ORBIT_WINDOW = 16
    if os.path.exists( DBNAME + '.h5' ):
        os.remove( DBNAME + '.h5' )
    if os.path.exists( DBNAME + '.db' ):
        os.remove( DBNAME + '.db' )

    if os.path.isdir('/Users/richardh'):
        fl_path = '/Users/richardh/Data/S5P_ICM_CA_SIR/001000/2012/09/19'
    else:
        fl_path = '/nfs/TROPOMI/ical/S5P_ICM_CA_SIR/001000/2012/09/19'
    icm_file = 'S5P_ICM_CA_SIR_20120919T051721_20120919T065655_01939_01_001000_20151002T140000.h5'

    for ii in range( num_orbits ):
        print( ii )
        
        ## open access to ICM product
        fp = ICM_io( os.path.join(fl_path, icm_file), readwrite=False )

        ## select measurement and collect its meta-data 
        fp.select( 'BACKGROUND_RADIANCE_MODE_0005' )
        meta_dict = {}
        meta_dict['orbit'] = fp.orbit + ii
        meta_dict['orbit_window'] = ORBIT_WINDOW
        meta_dict['orbit_used'] = ORBIT_WINDOW - (ii % 3) ## placeholder
        meta_dict['entry_date'] = datetime.utcnow().isoformat(' ')
        meta_dict['start_time'] = fp.start_time
        meta_dict['icm_version'] = fp.creator_version
        meta_dict['alg_version'] = '0.1.0.0'              ## placeholder
        version_spec = util.find_spec( "pynadc.version" )
        if version_spec is not None:
            from pynadc import version
            meta_dict['db_version'] = version.__version__
        else:
            meta_dict['db_version'] = '0.1.0.0'           ## placeholder
        meta_dict['q_det_temp'] = 0                       ## placeholder
        meta_dict['q_obm_temp'] = 0                       ## placeholder
        hk_data = fp.housekeeping_data
    
        ## read data from ICM product and combine band 7 & 8
        (values, errors) = fp.get_data( 'avg' )
        values = np.hstack((values[0][:-1,:], values[1][:-1,:]))
        errors = np.hstack((errors[0][:-1,:], errors[1][:-1,:]))
        del( fp )

        ## then add information to monitoring database
        mon = ICM_mon( DBNAME, orbit_window=ORBIT_WINDOW )
        mon.h5_write_hk( hk_data  )
        mon.h5_write_frames( values, errors, statistics='std,rows,cols'  )
        mon.sql_write_meta( meta_dict )
        del( mon )

#--------------------------------------------------
if __name__ == '__main__':
    test( 5475 )
