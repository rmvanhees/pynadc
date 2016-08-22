# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to create and fill ICM monitoring databases

-- Adding new entries to the database --
 Public methods to add new entries to the monitoring databases are:
 * h5_write_attr
 * h5_write_hk
 * h5_write_frames
 * h5_write_dpqf     ## not yet defined nor implemented
 * h5_write_isrf     ## not yet defined nor implemented
 * h5_write_stray    ## not yet defined nor implemented
 * sql_write_meta

-- Updating existing entries in the database --
 Public methods to update existing entries in the monitoring databases are:
 * h5_update_hk      ## not yes implemented
 * h5_update_frames  ## not yes implemented
 * sql_update_meta   ## not yes implemented

-- Query the database --
 Public methods to query the monitoring databases are:
 * sql_check_orbit
 *...

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
    def __init__( self, dbname, mode='r' ):
        '''
        Perform the following tasts:
        * if database does not exist 
        then 
         1) create HDF5 database (mode='w')
         2) set attribute 'orbit_window'
         3) set attributes with 'algo_version' and 'db_version'
        else
         1) open HDF5 database, read only when orbit is already present
         1) check versions of 'algo_version' and 'db_version'
         3) set class attributes
        '''
        assert( (mode == 'r') or (mode =='r+') )
        self.dbname = dbname
        
        if not os.path.exists( dbname+'.h5' ) \
           and not os.path.exists( dbname+'.db' ):
            self.__init = True
            self.__mode = 'w'
            self.__fid = h5py.File( dbname+'.h5', 'w' )
        else:
            self.__init = False
            self.__mode = mode
            self.__fid = h5py.File( dbname+'.h5', mode )
            ## check versions 

    def __repr__( self ):
        pass
    
    def __del__( self ):
        '''
        first check if SQLite and HDF5 are consistent
        then close access to databases
        '''
        self.__fid.close()

    ## ---------- READ/WRITE (USER) ATTRIBUTES ----------
    def h5_set_attr( self, name, value ):
        '''
        Add attributes to HDF5 database, during definition phase.
        Otherwise the call is silently ignored

        Please use standard names for attributes:
         - 'orbit_window' : [scalar] size of the orbit window 
                           Note, defined at creation of HDF5 database
         - 'icid_list' : [array] list of ic_id and ic_version used by algorithm
         - ...
        '''
        if self.__init:
            self.__fid.attrs[name] = value

    def h5_get_attr( self, name ):
        '''
        Obtain value of an HDF5 database attribute
        '''
        return self.__fid.attrs[name]
        
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
        assert(self.__mode != 'r')

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
    def __h5_cre_frames( self, frames,
                         rows=False, cols=False, method='none' ):
        '''
        create datasets for measurement data
        '''
        chunk_sz = (16, frames.shape[0] // 4, frames.shape[1] // 4)
        dset = self.__fid.create_dataset( "signal",
                                          (0,) + frames.shape,
                                          chunks=chunk_sz,
                                          maxshape=(None,) + frames.shape,
                                          dtype=frames.dtype )
        if rows:
            nrows = (frames.shape[1],)
            dset = self.__fid.create_dataset( "signal_row",
                                              (0,) + nrows,
                                              chunks=(16,) + nrows,
                                              maxshape=(None,) + nrows,
                                              dtype=frames.dtype )

        if cols:
            ncols = (frames.shape[0],)
            dset = self.__fid.create_dataset( "signal_col",
                                              (0,) + ncols,
                                              chunks=(16,) + ncols,
                                              maxshape=(None,) + ncols,
                                              dtype=frames.dtype )

        if method == 'none':
            return
        
        dset = self.__fid.create_dataset( "signal_{}".format(method),
                                          (0,) + frames.shape,
                                          chunks=chunk_sz,
                                          maxshape=(None,) + frames.shape,
                                          dtype=frames.dtype )
        if rows:
            nrows = (frames.shape[1],)
            dset = self.__fid.create_dataset( "signal_row_{}".format(method),
                                              (0,) + nrows,
                                              chunks=(16,) + nrows,
                                              maxshape=(None,) + nrows,
                                              dtype=frames.dtype )

        if cols:
            ncols = (frames.shape[0],)
            dset = self.__fid.create_dataset( "signal_col_{}".format(method),
                                              (0,) + ncols,
                                              chunks=(16,) + ncols,
                                              maxshape=(None,) + ncols,
                                              dtype=frames.dtype )

    def h5_write_frames( self, values, errors, statistics=None ):
        '''
        parameter statistics may contain the following strings (comma-separated)
         - error, noise or std  : description of parameter errors (first field)
         - rows : when row average/median should be calculated from frames
         - cols : when column average/median should be calculated from frames
        '''
        assert(self.__mode != 'r')

        if self.__init:
            if statistics is None:
                self.__fid.attrs['rows'] = False
                self.__fid.attrs['cols'] = False
                if errors is None:
                    ext = 'none'
                else:
                    ext = 'std'
            else:
                stat_list = statistics.split(',')
                self.__fid.attrs['rows'] = 'rows' in stat_list
                self.__fid.attrs['cols'] = 'cols' in stat_list
                if errors is None:
                    ext = 'none'
                else:
                    ext = stat_list[0]

            self.__fid.attrs['method'] = ext
            self.__h5_cre_frames( values, method=ext,
                                  rows=self.__fid.attrs['rows'],
                                  cols=self.__fid.attrs['cols'] )
        else:
            ext = self.__fid.attrs['method']

        dset = self.__fid['signal']
        shape = (dset.shape[0]+1,) + dset.shape[1:]
        dset.resize( shape )
        dset[-1,:,:] = values

        if ext != 'none':
            dset = self.__fid['signal_{}'.format(ext)]
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:,:] = errors

        if self.__fid.attrs['rows']:
            dset = self.__fid['signal_row']
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:] = np.nanmedian( values, axis=0 )

            if ext != 'none':
                dset = self.__fid['signal_row_{}'.format(ext)]
                shape = (dset.shape[0]+1,) + dset.shape[1:]
                dset.resize( shape )
                dset[-1,:] = np.nanmedian( errors, axis=0 )
 
        if self.__fid.attrs['cols']:
            dset = self.__fid['signal_col']
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:] = np.nanmedian( values, axis=1 )

            if ext != 'none':
                dset = self.__fid['signal_col_{}'.format(ext)]
                shape = (dset.shape[0]+1,) + dset.shape[1:]
                dset.resize( shape )
                dset[-1,:] = np.nanmedian( errors, axis=1 )
 
            
   ## ---------- WRITE META-DATA TO SQL database ----------
    def __sql_cre_meta( self ):
        dbname = self.dbname + '.db'
        
        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( '''create table icm_meta (
           rowID           integer     PRIMARY KEY AUTOINCREMENT,
           referenceOrbit  integer     NOT NULL UNIQUE,
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
        The dictionary "meta_dict" should contain the following fields:
         "orbit_ref"    : column referenceOrbit [integer]
         "orbit_used"   : column orbitsUsed     [integer]
         "entry_date"   : column entryDateTime  [text format YY-MM-DD hh:mm:ss]
         "start_time"   : column startDateTime  [text format YY-MM-DD hh:mm:ss]
         "icm_version"  : column icmVersion     [text format xx.xx.xx]
         "algo_version" : column algVersion     [text format xx.xx.xx]
         "db_version"   : column dbVersion      [text format xx.xx.xx]
         "q_det_temp"   : column q_detTemp      [integer]
         "q_obm_temp"   : column q_obmTemp      [integer]
        '''
        dbname = self.dbname + '.db'
        if self.__init:
            self.__sql_cre_meta()

        str_sql = 'insert into icm_meta values' \
                  '(NULL,{orbit_ref},{orbit_used},{entry_date!r}' \
                  ',{start_time!r}'\
                  ',{icm_version!r},{algo_version!r},{db_version!r}'\
                  ',{q_det_temp},{q_obm_temp})'
        #print( str_sql.format(**meta_dict) )

        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql.format(**meta_dict) )
        cur.close()
        con.commit()
        con.close()

    def sql_check_orbit( self, orbit ):
        '''
        Check if data from referenceOrbit is already present in database

        Returns rowID equals -1 when row is not present, else rowID > 0
        '''
        dbname = self.dbname + '.db'
        if self.__init:
            return -1
        
        str_sql = 'select rowID from icm_meta where referenceOrbit={}'
        
        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql.format(orbit) )
        row = cur.fetchone()
        cur.close()
        con.close()
        if row == None: 
            return -1
        else:
            return row[0]

    def sql_select_orbit( self, orbit, orbit_used=None,
                          q_det_temp=None, q_obm_temp=None ):
        '''
        Obtain list of rowIDs for given orbit(range)
        Parameters:
           orbit      : scalar or range orbit_min, orbit_max
           orbit_used : minimal number of orbits used to calculate results 
           q_det_temp : select only entries with stable detector temperature
           q_obm_temp : select only entries with stable OBM temperature
        '''
        dbname = self.dbname + '.db'
        row_list = ()
        if self.__init:
            return row_list

        str_sql = 'select rowID from icm_meta'
        if len(orbit) == 1:
            str_sql += ' where referenceOrbit={}'.format(orbit)
        else:
            str_sql += ' where referenceOrbit between {} and {}'.format(*orbit)

        if orbit_used is not None:
            str_sql += ' and orbitsUsed >= {}'.format(orbit_used)

        if q_det_temp is not None:
            str_sql += ' and q_det_temp = 0'

        if q_obm_temp is not None:
            str_sql += ' and q_obm_temp = 0'
        str_sql += ' order by referenceOrbit'

        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql )
        for row in cur:
            row_list += (row[0],)
        cur.close()
        con.close()

        return row_list

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
        meta_dict['orbit_ref'] = fp.orbit + ii
        meta_dict['orbit_window'] = ORBIT_WINDOW
        meta_dict['orbit_used'] = ORBIT_WINDOW - (ii % 3) ## placeholder
        meta_dict['entry_date'] = datetime.utcnow().isoformat(' ')
        meta_dict['start_time'] = fp.start_time
        meta_dict['icm_version'] = fp.creator_version
        meta_dict['algo_version'] = '0.1.0.0'             ## placeholder
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
        ## Note that ingesting results twice leads to database corruption!
        ##   Please use 'mon.sql_check_orbit'
        mon = ICM_mon( DBNAME, mode='r+' )
        if mon.sql_check_orbit( meta_dict['orbit_ref'] ) >= 0:
            continue
        mon.h5_set_attr( 'orbit_window', ORBIT_WINDOW )
        mon.h5_write_hk( hk_data  )
        mon.h5_write_frames( values, errors, statistics='std,rows,cols'  )
        mon.sql_write_meta( meta_dict )
        del( mon )

    ## select rows from database given an orbit range
    mon = ICM_mon( DBNAME, mode='r' )
    print( mon.sql_select_orbit( [1940,1950] ) )
    del(mon)

        
#--------------------------------------------------
if __name__ == '__main__':
    #test( 5475 )
    test( 75 )
