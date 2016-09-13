# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to create and fill ICM monitoring databases

-- Adding new entries to the database --
 Public methods to add new entries to the monitoring databases are:
 * h5_set_attr
 * h5_write_hk
 * h5_write_frames
 * h5_set_frame_attr
 * h5_write_dpqf     ## not yet defined nor implemented
 * h5_write_isrf     ## not yet defined nor implemented
 * h5_write_stray    ## not yet defined nor implemented
 * sql_write_meta

-- Updating existing entries in the database --
 Public methods to update existing entries in the monitoring databases are:
 * h5_update_hk      ## not yes implemented
 * h5_update_frames  ## not yes implemented
 * sql_update_meta   ## not yes implemented

-- Miscellaneous functions --
 * pynadc_version

-- Query the database --
 Public methods to query the monitoring databases are:
 * get_orbit_latest
 * h5_get_attr
 * h5_get_frame_attr
 * h5_read_frames
 * h5_get_trend_colum
 * h5_get_trend_row
 * h5_get_trend_pixel

 * sql_num_entries
 * sql_check_orbit
 * sql_select_orbit
 * sql_get_row_list
 * sql_get_trend_frames

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

import os
import sqlite3

import numpy as np
import h5py

from pynadc.stats import biweight

##--------------------------------------------------
class ICM_mon( object ):
    '''
    Defines methods to create ICM monitoring databases and ingest new entries
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

        self.__mode = mode
        self.__fid = None
        if not os.path.exists( dbname+'.h5' ) \
           and not os.path.exists( dbname+'.db' ):
            assert mode == 'r+', \
                "*** Fatal: databse {} does not exist in read-mode".format(dbname)
            self.__mode = 'w'
            self.__fid = h5py.File( dbname+'.h5', 'w' )
            self.__fid.attrs['dbVersion'] = self.pynadc_version()
        else:
            self.__fid = h5py.File( dbname+'.h5', mode )
            ## check versions
            db_version = self.__fid.attrs['dbVersion'].split('.')
            sw_version = self.pynadc_version().split('.')
            assert( (db_version[0] == sw_version[0])
                    and (db_version[1] == sw_version[1]) )

#    def __repr__( self ):
#        pass

    def __del__( self ):
        '''
        first check if SQLite and HDF5 are consistent
        then close access to databases
        '''
        if self.__mode != 'r':
            ## obtain number of row in SQLite database
            num_sql_rows = self.sql_num_entries()
            
            ## check number of entries in HDF5 datasets
            ds_list = ['hk_median', 'hk_scale', 'signal',
                       'signal_row', 'signal_col']
            for ds_name in ds_list:
                if ds_name in self.__fid:
                    num_ds_rows = self.__fid[ds_name].shape[0]
                    msg = "integrity test faild on {}, which has not {} rows".format(ds_name, num_sql_rows)
                    assert (num_sql_rows == num_ds_rows), msg

        if self.__fid is not None:
            self.__fid.close()

    ## ---------- RETURN VERSION of the S/W ----------
    def pynadc_version( self ):
        '''
        Return S/W version
        '''
        from importlib import util

        version_spec = util.find_spec( "pynadc.version" )
        assert (version_spec is not None)

        from pynadc import version
        return version.__version__

    def get_method( self ):
        return self.__fid.attrs['method']

    ## ---------- READ/WRITE GOBAL ATTRIBUTES ----------
    def h5_set_attr( self, name, value ):
        '''
        Add global attributes to HDF5 database, during definition phase.
        Otherwise the call is silently ignored

        Please use standard names for attributes:
         - 'orbit_window' : [scalar] size of the orbit window 
                           Note, defined at creation of HDF5 database
         - 'icid_list' : [array] list of ic_id and ic_version used by algorithm
         - ...
        '''
        if self.__mode == 'r' or name in self.__fid.attrs.keys():
            return
        
        self.__fid.attrs[name] = value

    def h5_get_attr( self, name ):
        '''
        Obtain value of an HDF5 database attribute
        '''
        if name in self.__fid.attrs.keys():
            return self.__fid.attrs[name]
        else:
            return None
        
    ## ---------- WRITE HOUSE-KEEPING DATA ----------
    def __h5_cre_hk( self, hk ):
        '''
        Create datasets for house-keeping data.
        seperated datasets for biweight median and scale

        Todo: only include hk-data when the instrument is really performing 
              the requested measurements. 
              Reject entries during start-up or at the end
        '''
        # SWIR related house-keeping data
        name_list = ('temp_det4','temp_obm_swir','temp_cu_sls_stim',
                     'temp_obm_swir_grating','temp_obm_swir_if',
                     'temp_pelt_cu_sls1','temp_pelt_cu_sls2',
                     'temp_pelt_cu_sls3','temp_pelt_cu_sls4',
                     'temp_pelt_cu_sls5','swir_vdet_bias',
                     'difm_status', 'det4_led_status','wls_status',
                     'common_led_status','sls1_status','sls2_status',
                     'sls3_status','sls4_status','sls5_status')
        dtype_list = ()
        for name in name_list:
            dtype_list += (hk[name].dtype.name,)

        hk_buff = np.empty( 1, dtype=','.join(dtype_list) )
        hk_buff.dtype.names = name_list
        self.__fid.create_dataset( "hk_median", (0,), maxshape=(None,),
                                   dtype=hk_buff.dtype )
        self.__fid["hk_median"].attrs["comment"] = \
                                "biweight median of hous-keeping data (SWIR)"
        self.__fid["hk_median"].attrs["fields"] = \
                                    np.array([np.string_(n) for n in name_list])
        self.__fid.create_dataset( "hk_scale", (0,), maxshape=(None,),
                                   dtype=hk_buff.dtype )
        self.__fid["hk_scale"].attrs["comment"] = \
                                "biweight scale of hous-keeping data (SWIR)"
        self.__fid["hk_scale"].attrs["fields"] = \
                                    np.array([np.string_(n) for n in name_list])
                
    def h5_write_hk( self, hk ):
        '''
        Create datasets for house-keeping data.
        seperated datasets for biweight median and scale

        Todo: only include hk-data when the instrument is really performing 
              the requested measurements. 
              Reject entries during start-up or at the end
        '''
        assert(self.__mode != 'r')

        if not ('hk_median' in self.__fid and 'hk_scale' in self.__fid):
            self.__h5_cre_hk( hk )

        hk_median = np.empty( 1, dtype=self.__fid['hk_median'].dtype )
        hk_scale = np.empty( 1, dtype=self.__fid['hk_median'].dtype )
        for name in self.__fid['hk_median'].dtype.names:
            if hk[name].dtype.name.find( 'float' ) >= 0:
                (mx, sx) = biweight(hk[name], scale=True)
                hk_median[name] = mx
                hk_scale[name]  = sx
            elif hk[name].dtype.name.find( 'int' ) >= 0:
                hk_median[name] = np.median(hk[name])
                hk_scale[name]  = np.all(hk[name])
            else:
                print( name )
        dset = self.__fid['hk_median']
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
        self.__fid.create_dataset( "signal",
                                   (0,) + frames.shape,
                                   chunks=chunk_sz,
                                   maxshape=(None,) + frames.shape,
                                   dtype=frames.dtype )
        self.__fid["signal"].attrs["comment"] = \
                                    "values in detector-pixel coordinates"
        if rows:
            nrows = (frames.shape[1],)
            self.__fid.create_dataset( "signal_row",
                                       (0,) + nrows,
                                       chunks=(16,) + nrows,
                                       maxshape=(None,) + nrows,
                                       dtype=frames.dtype )
            self.__fid["signal_row"].attrs["comment"] = \
                                    "medians along the first axis of signal"

        if cols:
            ncols = (frames.shape[0],)
            self.__fid.create_dataset( "signal_col",
                                       (0,) + ncols,
                                       chunks=(16,) + ncols,
                                       maxshape=(None,) + ncols,
                                       dtype=frames.dtype )
            self.__fid["signal_col"].attrs["comment"] = \
                                    "medians along the second axis of signal"

        if method == 'none':
            return

        ds_name = "signal_{}".format(method)
        self.__fid.create_dataset( ds_name,
                                   (0,) + frames.shape,
                                   chunks=chunk_sz,
                                   maxshape=(None,) + frames.shape,
                                   dtype=frames.dtype )
        self.__fid[ds_name].attrs["comment"] = \
                                    "errors in detector-pixel coordinates"
        if rows:
            nrows = (frames.shape[1],)
            ds_name = "signal_row_{}".format(method)
            self.__fid.create_dataset( ds_name,
                                       (0,) + nrows,
                                       chunks=(16,) + nrows,
                                       maxshape=(None,) + nrows,
                                       dtype=frames.dtype )
            self.__fid[ds_name].attrs["comment"] = \
                    "medians along the first axis of signal_{}".format(method)

        if cols:
            ds_name = "signal_col_{}".format(method)
            ncols = (frames.shape[0],)
            self.__fid.create_dataset( ds_name,
                                       (0,) + ncols,
                                       chunks=(16,) + ncols,
                                       maxshape=(None,) + ncols,
                                       dtype=frames.dtype )
            self.__fid[ds_name].attrs["comment"] = \
                    "medians along the second axis of signal_{}".format(method)

    def h5_write_frames( self, values, errors, statistics=None ):
        '''
        parameter statistics may contain the following strings (comma-separated)
         - error, noise or std  : description of parameter errors (first field)
         - rows : when row average/median should be calculated from frames
         - cols : when column average/median should be calculated from frames
        '''
        assert(self.__mode != 'r')

        if not 'signal' in self.__fid:
            ext = 'none'
            if statistics is None:
                self.__fid.attrs['rows'] = False
                self.__fid.attrs['cols'] = False
                if errors is not None:
                    ext = 'std'
            else:
                stat_list = statistics.split(',')
                self.__fid.attrs['rows'] = 'rows' in stat_list
                self.__fid.attrs['cols'] = 'cols' in stat_list
                if errors is not None:
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
 
    def h5_read_frames( self, row_id, statistics=None ):
        '''
        Read data given row_id

        parameter statistics may contain the following strings (comma-separated)
         - rows  : return row-averaged results
         - cols  : return column-averaged results
         - error : return also error estimates
        '''
        res = {}
        ext = self.__fid.attrs['method']
        stat_list = statistics.split(',')

        dset = self.__fid['signal']
        res['signal'] = dset[row_id-1,:,:]

        if ext != 'none' and 'error' in stat_list:
            dset = self.__fid['signal_{}'.format(ext)]
            res['signal_{}'.format(ext)] = dset[row_id-1,:,:]
            
        if self.__fid.attrs['rows'] and 'rows' in stat_list:
            dset = self.__fid['signal_row']
            res['signal_row'] = dset[row_id-1,:]

            if ext != 'none' and 'error' in stat_list:
                dset = self.__fid['signal_row_{}'.format(ext)]
                res['signal_row_{}'.format(ext)] = dset[row_id-1,:]
               
        if self.__fid.attrs['cols'] and 'cols' in stat_list:
            dset = self.__fid['signal_col']
            res['signal_col'] = dset[row_id-1,:]

            if ext != 'none' and 'error' in stat_list:
                dset = self.__fid['signal_col_{}'.format(ext)]
                res['signal_col_{}'.format(ext)] = dset[row_id-1,:]
               
        return res
    
    def h5_get_trend_cols( self, orbit_list=None, orbit_range=None ):
        pass

    def h5_get_trend_rows( self, orbit_list=None, orbit_range=None ):
        pass

    def h5_get_trend_pixel( self, pixel_id, orbit_list=None, orbit_range=None ):
        pass

    ## ---------- READ/WRITE DATASET ATTRIBUTES ----------
    def h5_set_frame_attr( self, attr, value ):
        '''
        Add attributes to HDF5 datasets, during definition phase.
        Otherwise the call is silently ignored

        Please use names according to the CF conventions:
         - 'standard_name' : required CF-conventions attribute
         - 'long_name' : descriptive name may be used for labeling plots
         - 'units' : unit of the dataset values
        '''
        if self.__mode == 'r':
            return

        ds_list = [ 'signal', 'signal_row', 'signal_col' ]
        ext = self.__fid.attrs['method']
        
        for ds_name in ds_list:
            if not attr in self.__fid[ds_name].attrs.keys():
                self.__fid[ds_name].attrs[attr] = value
            if ext != 'none':
                ds_ext = ds_name + '_{}'.format(ext)
                if not attr in self.__fid[ds_ext].attrs.keys():
                    self.__fid[ds_ext].attrs[attr] = value

    def h5_get_frame_attr( self, ds_name, attr ):
        '''
        Obtain value of an HDF5 dataset attribute
        '''
        if attr in self.__fid[ds_name].attrs.keys():
            return self.__fid[ds_name].attrs[attr]
        else:
            return None
        
    ## ---------- WRITE META-DATA TO SQL database ----------
    def __sql_cre_meta( self ):
        '''
        Create SQLite database for ICM monitoring
        '''
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
           dataMedian      float       NOT NULL,
           dataScale       float       NOT NULL,
           errorMedian     float       NOT NULL,
           errorScale      float       NOT NULL,
           q_detTemp       integer     NOT NULL,
           q_obmTemp       integer     NOT NULL
        )''' )
        cur.execute( 'create index entryIndx on icm_meta(entryDateTime)' )
        cur.execute( 'create index startIndx on icm_meta(startDateTime)' )
        cur.close()
        con.commit()
        con.close()

    def sql_write_meta( self, meta_dict, verbose=False ):
        '''
        Append monitoring meta-data to SQLite database
        The dictionary "meta_dict" should contain the following fields:
         "orbit_ref"    : column referenceOrbit [integer]
         "orbit_used"   : column orbitsUsed     [integer]
         "entry_date"   : column entryDateTime  [text format YY-MM-DD hh:mm:ss]
         "start_time"   : column startDateTime  [text format YY-MM-DD hh:mm:ss]
         "icm_version"  : column icmVersion     [text free-format]
         "algo_version" : column algVersion     [text free-format]
         "db_version"   : column dbVersion      [text free-format]
         "data_median"  : column dataMedian     [float]
         "data_scale"   : column dataScale      [float]
         "error_median" : column errorMedian    [float]
         "error_scale"  : column errorScale     [float]
         "q_det_temp"   : column q_detTemp      [integer]
         "q_obm_temp"   : column q_obmTemp      [integer]
        '''
        dbname = self.dbname + '.db'
        if not os.path.exists( dbname ):
            self.__sql_cre_meta()

        str_sql = 'insert into icm_meta values' \
                  '(NULL,{orbit_ref},{orbit_used},{entry_date!r}' \
                  ',{start_time!r}'\
                  ',{icm_version!r},{algo_version!r},{db_version!r}'\
                  ',{data_median},{data_scale},{error_median},{error_scale}'\
                  ',{q_det_temp},{q_obm_temp})'
        if verbose:
            print( str_sql.format(**meta_dict) )

        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql.format(**meta_dict) )
        cur.close()
        con.commit()
        con.close()

    def sql_num_entries( self ):
        '''
        Returns number of rows
        '''
        dbname = self.dbname + '.db'
        if not os.path.exists( dbname ):
            return 0
        
        str_sql = 'select count(*) from icm_meta'
        
        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql )
        row = cur.fetchone()
        cur.close()
        con.close()
        if row is None:
            return -1
        else:
            return row[0]

    def get_orbit_latest( self ):
        '''
        Returns number of rows
        '''
        dbname = self.dbname + '.db'
        if not os.path.exists( dbname ):
            return 0
        
        str_sql = 'select max(referenceOrbit) from icm_meta'
        
        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql )
        row = cur.fetchone()
        cur.close()
        con.close()
        if row is None:
            return -1
        else:
            return row[0]

    def sql_check_orbit( self, orbit ):
        '''
        Check if data from referenceOrbit is already present in database

        Returns rowID equals -1 when row is not present, else rowID > 0
        '''
        dbname = self.dbname + '.db'
        if not os.path.exists( dbname ):
            return -1
        
        str_sql = 'select rowID from icm_meta where referenceOrbit={}'
        
        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql.format(orbit) )
        row = cur.fetchone()
        cur.close()
        con.close()
        if row is None:
            return -1
        else:
            return row[0]

    def sql_select_orbit( self, orbit, full=False, orbit_used=None,
                          q_det_temp=None, q_obm_temp=None ):
        '''
        Obtain list of rowIDs and orbit numbers for given orbit(range)
        Parameters:
           orbit      : scalar or range orbit_min, orbit_max
           orbit_used : minimal number of orbits used to calculate results 
           q_det_temp : select only entries with stable detector temperature
           q_obm_temp : select only entries with stable OBM temperature
           full       : add data of all columns to output dictionary

        Returns dictionary with keys: 'rowID' and 'referenceOrbit'
        '''
        dbname = self.dbname + '.db'
        row_list = {}
        if not os.path.exists( dbname ):
            return row_list

        if full:
            str_sql = 'select * from icm_meta'
        else:
            str_sql = 'select rowID,referenceOrbit from icm_meta'

        if isinstance( orbit, int ):
            str_sql += ' where referenceOrbit={}'.format(orbit)
        elif len(orbit) == 1:
            str_sql += ' where referenceOrbit={}'.format(orbit[0])
        else:
            str_sql += ' where referenceOrbit between {} and {}'.format(*orbit)

        if orbit_used is not None:
            str_sql += ' and orbitsUsed >= {}'.format(orbit_used)

        if q_det_temp is not None:
            str_sql += ' and q_det_temp = 0'

        if q_obm_temp is not None:
            str_sql += ' and q_obm_temp = 0'
        str_sql += ' order by referenceOrbit'

        conn = sqlite3.connect( dbname )
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute( str_sql )
        for row in cur:
            if len(row_list) == 0:
                for key_name in row.keys():
                    row_list[key_name] = ()
            for key_name in row.keys():
                row_list[key_name] += (row[key_name],)
        cur.close()
        conn.close()

        return row_list
    
    def sql_get_row_list( self, orbit_list=None, orbit_range=None,
                          frame_stats=False ):
        '''
        Obtain list of rowIDs and orbit numbers for given orbit(range)
        Parameters:
           orbit_list  : list with orbit numbers
           orbit_range : list with orbit range: [orbit_mn, orbit_mx]
           frame_stats : add statistics of selected frames as
                           dataMedian, dataScale, errorMedian, errorScale

        Returns dictionary with keys: 'rowID' and 'referenceOrbit'
        '''
        dbname = self.dbname + '.db'
        row_list = {}
        if not os.path.exists( dbname ):
            return row_list

        if frame_stats:
            str_sql = 'select rowID,referenceOrbit,dataMedian,dataScale,errorMedian,errorScale from icm_meta'
        else:
            str_sql = 'select rowID,referenceOrbit from icm_meta'
            
        if orbit_list is not None:
            str_sql += ' where referenceOrbit in ('
            str_sql += ','.join(str(x) for x in orbit_list)
            str_sql += ')'
        elif orbit_range is not None:
            assert( len(orbit_range) == 2 )

            str_sql += ' where referenceOrbit between {} and {}'.format(*orbit_range)
        str_sql += ' order by referenceOrbit'

        conn = sqlite3.connect( dbname )
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute( str_sql )
        for row in cur:
            if len(row_list) == 0:
                for key_name in row.keys():
                    row_list[key_name] = ()
            for key_name in row.keys():
                row_list[key_name] += (row[key_name],)
        cur.close()
        conn.close()
 
        return row_list
           

    def sql_get_trend_frames( self, orbit_list=None, orbit_range=None ):
        '''
        Obtain list of rowIDs and orbit numbers for given orbit(range)
        Parameters:
           orbit_list  : list with orbit numbers
           orbit_range : list with orbit range: [orbit_mn, orbit_mx]

        Returns dictionary with keys: 'rowID', 'referenceOrbit', 'dataMedian', 
             'dataScale', 'errorMedian', 'errorScale'
        '''
        return self.sql_get_row_list( orbit_list=orbit_list,
                                      orbit_range=orbit_range,
                                      frame_stats=True )

#--------------------------------------------------
def test_background( num_orbits=365, rebuild=False ):
    '''
    Perform some simple test to check the ICM_mon_db class
    '''
    from datetime import datetime, timedelta

    from pynadc.tropomi.icm_io import ICM_io

    DBNAME = 'mon_background_0005_test'
    if rebuild:
        ORBIT_WINDOW = 15
    
        if os.path.exists( DBNAME + '.h5' ):
            os.remove( DBNAME + '.h5' )
        if os.path.exists( DBNAME + '.db' ):
            os.remove( DBNAME + '.db' )

        if os.path.isdir('/Users/richardh'):
            fl_path = '/Users/richardh/Data/S5P_ICM_CA_SIR/001000/2012/09/19'
        elif os.path.isdir('/nfs/TROPOMI/ical/'):
            fl_path = '/nfs/TROPOMI/ical/S5P_ICM_CA_SIR/001000/2012/09/19'
        else:
            fl_path = '/data/richardh/Tropomi/ical/S5P_ICM_CA_SIR/001000/2012/09/19'
        icm_file = 'S5P_ICM_CA_SIR_20120919T051721_20120919T065655_01939_01_001000_20151002T140000.h5'

        for ii in range( num_orbits ):
            print( ii )
        
            ## open access to ICM product
            fp = ICM_io( os.path.join(fl_path, icm_file), readwrite=False )

            ## select measurement and collect its meta-data 
            fp.select( 'BACKGROUND_RADIANCE_MODE_0005' )

            ## read data from ICM product and combine band 7 & 8
            (values, errors) = fp.get_data( 'avg' )
            values = np.hstack((values[0][:-1,:], values[1][:-1,:]))
            errors = np.hstack((errors[0][:-1,:], errors[1][:-1,:]))

            meta_dict = {}
            meta_dict['orbit_ref'] = fp.orbit + ii * ORBIT_WINDOW
            meta_dict['orbit_window'] = ORBIT_WINDOW
            meta_dict['orbit_used'] = ORBIT_WINDOW - (ii % 3) ## placeholder
            meta_dict['entry_date'] = datetime.utcnow().isoformat(' ')
            start_time = datetime.strptime( fp.start_time,
                                            '%Y-%m-%d %H:%M:%S' ) \
                + timedelta(days=ii)
            meta_dict['start_time'] = start_time.isoformat(' ')
            meta_dict['icm_version'] = fp.creator_version

            (mx, sx) = biweight(values, scale=True)
            meta_dict['data_median'] = mx
            meta_dict['data_scale'] = sx
            (mx, sx) = biweight(errors, scale=True)
            meta_dict['error_median'] = mx
            meta_dict['error_scale'] = sx

            ## Tim: how to obtain the actual version of your S/W?
            meta_dict['algo_version'] = '00.01.00'
            meta_dict['q_det_temp'] = 0                 ## obtain from hk
            meta_dict['q_obm_temp'] = 0                 ## obtain from hk
            meta_dict['q_algo'] = 0                     ## obtain from algo?!
            hk_data = fp.housekeeping_data
    
            ## then add information to monitoring database
            ## Note that ingesting results twice leads to database corruption!
            ##   Please use 'mon.sql_check_orbit'
            mon = ICM_mon( DBNAME, mode='r+' )
            meta_dict['db_version'] = mon.pynadc_version()
            if mon.sql_check_orbit( meta_dict['orbit_ref'] ) < 0:
                mon.h5_set_attr( 'title',
                                 'Tropomi SWIR thermal background monitoring' )
                mon.h5_set_attr( 'institution',
                                 'SRON Netherlands Institute for Space Research' )
                mon.h5_set_attr( 'source',
                                 'Copernicus Sentinel-5 Precursor Tropomi Inflight Calibration and Monitoring product' )
                mon.h5_set_attr( 'references', 'https://www.sron.nl/Tropomi' ) 
                mon.h5_set_attr( 'comment',
                                 'ICID {} ($t_{{exp}}={:.3f}$)'.format(fp.instrument_settings['ic_id'], float(fp.instrument_settings['exposure_time'])) )
                mon.h5_set_attr( 'orbit_window', ORBIT_WINDOW )
                mon.h5_set_attr( 'icid_list', [fp.instrument_settings['ic_id'],] )
                mon.h5_set_attr( 'ic_version', [fp.instrument_settings['ic_version'],] )
                mon.h5_write_hk( hk_data  )
                mon.h5_write_frames( values, errors,
                                     statistics='std,rows,cols'  )
                mon.h5_set_frame_attr( 'long_name', 'signal' )
                mon.h5_set_frame_attr( 'units', 'electron' )
                mon.sql_write_meta( meta_dict )
            del( fp )
            del( mon )

    ## select rows from database given an orbit range
    mon = ICM_mon( DBNAME, mode='r' )
    print( mon.sql_select_orbit( [1940,2050] ) )

    print( mon.sql_get_row_list( orbit_range=[1940,2050] ) )

    print( mon.sql_get_row_list( orbit_list=[1954,1969,1999,2044] ) )
        
    print( mon.sql_get_row_list( orbit_list=[2044] ) )
        
    print( mon.sql_get_trend_frames( orbit_range=[2010,2050] ) )

    del(mon)
        
#--------------------------------------------------
def test( num_orbits=1 ):
    '''
    Perform some simple test to check the ICM_mon_db class
    '''
    from datetime import datetime

    from pynadc.tropomi.icm_io import ICM_io

    DBNAME = 'mon_quick_test'
    ORBIT_WINDOW = 16
    if os.path.exists( DBNAME + '.h5' ):
        os.remove( DBNAME + '.h5' )
    if os.path.exists( DBNAME + '.db' ):
        os.remove( DBNAME + '.db' )

    if os.path.isdir('/Users/richardh'):
        fl_path = '/Users/richardh/Data/S5P_ICM_CA_SIR/001000/2012/09/19'
    elif os.path.isdir('/nfs/TROPOMI/ical/'):
        fl_path = '/nfs/TROPOMI/ical/S5P_ICM_CA_SIR/001000/2012/09/19'
    else:
        fl_path = '/data/richardh/Tropomi/ical/S5P_ICM_CA_SIR/001000/2012/09/19'
    icm_file = 'S5P_ICM_CA_SIR_20120919T051721_20120919T065655_01939_01_001000_20151002T140000.h5'

    for ii in range( num_orbits ):
        print( ii )
        
        ## open access to ICM product
        fp = ICM_io( os.path.join(fl_path, icm_file), readwrite=False )

        ## select measurement and collect its meta-data 
        fp.select( 'BACKGROUND_RADIANCE_MODE_0005' )
    
        ## read data from ICM product and combine band 7 & 8
        (values, errors) = fp.get_data( 'avg' )
        values = np.hstack((values[0][:-1,:], values[1][:-1,:]))
        errors = np.hstack((errors[0][:-1,:], errors[1][:-1,:]))

        meta_dict = {}
        meta_dict['orbit_ref'] = fp.orbit + ii
        meta_dict['orbit_window'] = ORBIT_WINDOW
        meta_dict['orbit_used'] = ORBIT_WINDOW - (ii % 3) ## placeholder
        meta_dict['entry_date'] = datetime.utcnow().isoformat(' ')
        meta_dict['start_time'] = fp.start_time
        meta_dict['icm_version'] = fp.creator_version

        (mx, sx) = biweight(values, scale=True)
        meta_dict['data_median'] = mx
        meta_dict['data_scale'] = sx
        (mx, sx) = biweight(errors, scale=True)
        meta_dict['error_median'] = mx
        meta_dict['error_scale'] = sx

        ## Tim: how to obtain the actual version of your S/W?
        meta_dict['algo_version'] = '00.01.00'
        meta_dict['q_det_temp'] = 0                       ## obtain from hk
        meta_dict['q_obm_temp'] = 0                       ## obtain from hk
        meta_dict['q_algo'] = 0                           ## obtain from algo?!
        hk_data = fp.housekeeping_data

        ## then add information to monitoring database
        ## Note that ingesting results twice leads to database corruption!
        ##   Please use 'mon.sql_check_orbit'
        mon = ICM_mon( DBNAME, mode='r+' )
        meta_dict['db_version'] = mon.pynadc_version()
        if mon.sql_check_orbit( meta_dict['orbit_ref'] ) < 0:
            mon.h5_set_attr( 'title', 'Tropomi SWIR dark-flux monitoring results' )
            mon.h5_set_attr( 'institution', 'SRON Netherlands Institute for Space Research' )
            mon.h5_set_attr( 'source', 'Copernicus Sentinel-5 Precursor Tropomi Inflight Calibration and Monitoring product' )
            mon.h5_set_attr( 'references', 'https://www.sron.nl/Tropomi' ) 
            mon.h5_set_attr( 'comment',
                             'ICID {} ($t_{{exp}}={:.3f}$)'.format(fp.instrument_settings['ic_id'], float(fp.instrument_settings['exposure_time'])) )
            mon.h5_set_attr( 'orbit_window', ORBIT_WINDOW )
            mon.h5_set_attr( 'icid_list', [fp.instrument_settings['ic_id'],] )
            mon.h5_set_attr( 'ic_version', [fp.instrument_settings['ic_version'],] )
            mon.h5_write_hk( hk_data  )
            mon.h5_write_frames( values, errors, statistics='std,rows,cols'  )
            mon.h5_set_frame_attr( 'long_name', 'background signal' )
            mon.h5_set_frame_attr( 'units', 'electron' )
            mon.sql_write_meta( meta_dict, verbose=True )
        del( fp )
        del( mon )

    ## select rows from database given an orbit range
    mon = ICM_mon( DBNAME, mode='r' )
    print( mon.sql_select_orbit( [1940,1950], full=True ) )
    del(mon)
        
#--------------------------------------------------
def test2():
    '''
    Perform some simple test to check the ICM_mon_db class
    '''
    from datetime import datetime

    from pynadc.tropomi.ocm_io import OCM_io

    DBNAME = 'mon_sun_isrf_test'
    ORBIT_WINDOW = 1
    if os.path.exists( DBNAME + '.h5' ):
        os.remove( DBNAME + '.h5' )
    if os.path.exists( DBNAME + '.db' ):
        os.remove( DBNAME + '.db' )

    if os.path.isdir('/Users/richardh'):
        fl_path = '/Users/richardh/Data/S5P_ICM_CA_SIR/001000/2012/09/19'
    elif os.path.isdir('/nfs/TROPOMI/ocal/'):
        fl_path = '/nfs/TROPOMI/ocal/proc_knmi/2015_05_02T10_28_44_SwirlsSunIsrf'
    else:
        fl_path = '/data/richardh/Tropomi/ical/S5P_ICM_CA_SIR/001000/2012/09/19'
    ocm_msm = 'after_et_l1bavg_004_block-004-004'

    dirList = [d for d in os.listdir( fl_path ) 
               if os.path.isdir(os.path.join(fl_path, d))]
    dirList.sort()
    ii = 0
    for msm in dirList:
        fp = OCM_io( os.path.join(fl_path, msm), verbose=True )
        if fp.select( 31623 ) == 0:
            continue
        
        print( fp )
        (values, errors) = fp.get_data()
        print( 'dimensions of values: ', len(values), values[0].shape )
        values = np.hstack((values[0], values[1]))
        errors = np.hstack((errors[0], errors[1]))
        print( 'dimensions of values: ', values.shape )
        
        meta_dict = {}
        meta_dict['orbit_ref'] = ii
        meta_dict['orbit_window'] = ORBIT_WINDOW
        meta_dict['orbit_used'] = fp.num_msm
        meta_dict['entry_date'] = datetime.utcnow().isoformat(' ')
        meta_dict['start_time'] = fp.start_time
        meta_dict['icm_version'] = fp.creator_version
        
        (mx, sx) = biweight(values, scale=True)
        meta_dict['data_median'] = mx
        meta_dict['data_scale'] = sx
        (mx, sx) = biweight(errors, scale=True)
        meta_dict['error_median'] = mx
        meta_dict['error_scale'] = sx

        ## Tim: how to obtain the actual version of your S/W?
        meta_dict['algo_version'] = '00.01.00'
        meta_dict['db_version'] = fp.pynadc_version()
        meta_dict['q_det_temp'] = 0                       ## obtain from hk
        meta_dict['q_obm_temp'] = 0                       ## obtain from hk
        meta_dict['q_algo'] = 0                           ## obtain from algo?!
        hk_data = fp.housekeeping_data
        ii += 1
        
        mon = ICM_mon( DBNAME, mode='r+' )
        mon.h5_set_attr( 'title', 'Tropomi SWIR OCAL SunISRF backgrounds' )
        mon.h5_set_attr( 'institution', 'SRON Netherlands Institute for Space Research' )
        mon.h5_set_attr( 'source', 'Copernicus Sentinel-5 Precursor Tropomi On-ground Calibration and Monitoring product' )
        mon.h5_set_attr( 'references', 'https://www.sron.nl/Tropomi' )
        mon.h5_set_attr( 'comment',
                         'ICID {} ($t_{{exp}}={:.3f}$)'.format(fp.instrument_settings['ic_id'], float(fp.instrument_settings['exposure_time'])) )
        mon.h5_set_attr( 'orbit_window', ORBIT_WINDOW )
        mon.h5_set_attr( 'icid_list', [fp.instrument_settings['ic_id'],] )
        mon.h5_set_attr( 'ic_version', [fp.instrument_settings['ic_version'],] )
        mon.h5_write_hk( hk_data  )
        mon.h5_write_frames( values, errors, statistics='std,rows,cols'  )
        mon.h5_set_frame_attr( 'long_name', 'signal' )
        mon.h5_set_frame_attr( 'units', 'electron / s' )
        mon.sql_write_meta( meta_dict, verbose=True )

        del( fp )
        del( mon )
        
        
#--------------------------------------------------
if __name__ == '__main__':
    #test_background(rebuild=True)
    #test( 25 )
    test2()
