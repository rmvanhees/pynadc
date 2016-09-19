# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to create and fill ICM monitoring databases

-- Adding new entries to the database --
 Public methods to add new entries to the monitoring databases are:
 * h5_set_attr
 * h5_write_hk
 * h5_write_frame
 * h5_set_frame_attr
 * h5_write_dpqf     ## not yet defined nor implemented
 * h5_write_isrf     ## not yet defined nor implemented
 * h5_write_stray    ## not yet defined nor implemented
 * sql_write_meta

-- Updating existing entries in the database --
 Public methods to update existing entries in the monitoring databases are:
 * h5_update_hk      ## not yes implemented
 * h5_update_frame   ## not yes implemented
 * sql_update_meta   ## not yes implemented

-- Miscellaneous functions --
 * pynadc_version

-- Query the database --
 Public methods to query the monitoring databases are:
 * get_orbit_latest
 * h5_get_attr
 * h5_get_frame_attr
 * h5_read_frame
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
    def __init__( self, dbname, readwrite=False ):
        '''
        Perform the following tasts:
        * if database does not exist 
        then
         1) create HDF5 database (readwrite=True)
         3) set attributes with 'db_version'
        else
         1) open HDF5 database, read only when orbit is already present
         1) check versions of 'algo_version' and 'db_version'
         3) set class attributes
        '''
        assert os.path.exists(dbname+'.h5') == os.path.exists(dbname+'.db') ,\
            "*** Fatal: lonely SQLite or HDF5 database exists (corruption?)"

        self.dbname = dbname
        self.__rw = readwrite

        self.__fid = None
        if not (os.path.exists(dbname+'.h5') or os.path.exists(dbname+'.db')):
            assert readwrite, \
                "*** Fatal: attempt to create databse {} in read-mode".format(dbname)
            self.__fid = h5py.File( dbname+'.h5', 'w' )
            self.__fid.attrs['institution'] = \
                            'SRON Netherlands Institute for Space Research'
            self.__fid.attrs['references'] = 'https://www.sron.nl/Tropomi'
            self.__fid.attrs['dbVersion'] = self.pynadc_version()
            self.__sql_init( dbname+'.db' )
        else:
            if self.__rw:
                self.__fid = h5py.File( dbname+'.h5', 'r+' )
            else:
                self.__fid = h5py.File( dbname+'.h5', 'r' )
            ## check versions
            db_version = self.__fid.attrs['dbVersion'].split('.')
            sw_version = self.pynadc_version().split('.')
            assert( (db_version[0] == sw_version[0])
                    and (db_version[1] == sw_version[1]) )

    def __repr__( self ):
        class_name = type(self).__name__
        return '{}({!r}, readwrite={!r})'.format( class_name,
                                             self.dbname, self.__rw )

    def __del__( self ):
        '''
        first check if SQLite and HDF5 are consistent
        then close access to databases
        '''
        if self.__rw:
            ## obtain number of row in SQLite database
            num_sql_rows = self.sql_num_entries()
            
            ## check number of entries in HDF5 datasets
            ds_list = ['hk_median', 'hk_spread', 'signal',
                       'signal_row', 'signal_col']
            for ds_name in ds_list:
                if ds_name in self.__fid:
                    num_ds_rows = self.__fid[ds_name].shape[0]
                    msg = "integrity test faild on {}, which has not {} rows".format(ds_name, num_sql_rows)
                    assert (num_sql_rows == num_ds_rows), msg

        if self.__fid is not None:
            self.__fid.close()

    ## ---------- initialize SQLite database ----------
    def __sql_init( self, dbname ):
        '''
        Create SQLite database for ICM monitoring
        '''
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
           dataSpread      float       NOT NULL,
           errorMedian     float       NOT NULL,
           errorSpread     float       NOT NULL,
           q_detTemp       integer     NOT NULL,
           q_obmTemp       integer     NOT NULL
        )''' )
        cur.execute( 'create index entryIndx on icm_meta(entryDateTime)' )
        cur.execute( 'create index startIndx on icm_meta(startDateTime)' )
        cur.close()
        con.commit()
        con.close()

    ## ---------- initialize HDF5 datasets ----------
    def h5_init( self, frame_shape, frame_dtype, statistics=None ):
        '''
        create datasets for measurement data

        parameter statistics may contain the following strings (comma-separated)
         - hk   : store house-keeping data
         - error, noise or std  : store errors of given type
         - rows : when row average/median should be calculated from frames
         - cols : when column average/median should be calculated from frames
        '''
        if statistics is None:
            self.__fid.attrs['hk'] = False
            self.__fid.attrs['rows'] = False
            self.__fid.attrs['cols'] = False
            self.__fid.attrs['method'] = 'none'
        else:
            stat_list = statistics.split(',')
            self.__fid.attrs['hk'] = 'hk' in stat_list
            self.__fid.attrs['rows'] = 'row' in stat_list
            self.__fid.attrs['cols'] = 'col' in stat_list
            if 'std' in stat_list:
                self.__fid.attrs['method'] = 'std'
            elif 'error' in stat_list:
                self.__fid.attrs['method'] = 'error'
            elif 'noise' in stat_list:
                self.__fid.attrs['method'] = 'noise'
            else:
                self.__fid.attrs['method'] = 'none'
        ext = self.__fid.attrs['method']
        
        id_dtype = np.dtype( [('sql_rowid', np.int),
                              ('orbit_ref', np.int)] )
        id_names = np.array([np.string_(n) for n in id_dtype.names])
        self.__fid.create_dataset( "id", (0,), maxshape=(None,),
                                   dtype=id_dtype )
        self.__fid["id"].attrs["comment"] = "entry identifier"
        self.__fid["id"].attrs["fields"] = id_names

        # SWIR related house-keeping data
        if self.__fid.attrs['hk']:
            hk_dtype = np.dtype( [('temp_det4',             np.float32),
                                  ('temp_obm_swir',         np.float32),
                                  ('temp_cu_sls_stim',      np.float32),
                                  ('temp_obm_swir_grating', np.float32),
                                  ('temp_obm_swir_if',      np.float32),
                                  ('temp_pelt_cu_sls1',     np.float32),
                                  ('temp_pelt_cu_sls2',     np.float32),
                                  ('temp_pelt_cu_sls3',     np.float32),
                                  ('temp_pelt_cu_sls4',     np.float32),
                                  ('temp_pelt_cu_sls5',     np.float32),
                                  ('swir_vdet_bias',        np.float32),
                                  ('difm_status',           np.float32),
                                  ('det4_led_status',   np.uint8),
                                  ('wls_status',        np.uint8),
                                  ('common_led_status', np.uint8),
                                  ('sls1_status',       np.uint8),
                                  ('sls2_status',       np.uint8),
                                  ('sls3_status',       np.uint8),
                                  ('sls4_status',       np.uint8),
                                  ('sls5_status',       np.uint8)] )
            hk_names = np.array([np.string_(n) for n in hk_dtype.names])
                                
            self.__fid.create_dataset( "hk_median", (0,), maxshape=(None,),
                                       dtype=hk_dtype )
            self.__fid["hk_median"].attrs["comment"] = \
                                "biweight median of hous-keeping data (SWIR)"
            self.__fid["hk_median"].attrs["fields"] = hk_names
                                                      
            self.__fid.create_dataset( "hk_spread", (0,), maxshape=(None,),
                                       dtype=hk_dtype )
            self.__fid["hk_spread"].attrs["comment"] = \
                                "biweight spread of hous-keeping data (SWIR)"
            self.__fid["hk_spread"].attrs["fields"] = hk_names

        # Detector datasets
        chunk_sz = (16, frame_shape[0] // 4, frame_shape[1] // 4)
        self.__fid.create_dataset( "signal",
                                   (0,) + frame_shape,
                                   chunks=chunk_sz,
                                   maxshape=(None,) + frame_shape,
                                   dtype=frame_dtype )
        self.__fid["signal"].attrs["comment"] = \
                                    "values in detector-pixel coordinates"
        if self.__fid.attrs['rows']:
            nrows = (frame_shape[1],)
            self.__fid.create_dataset( "signal_row",
                                       (0,) + nrows,
                                       chunks=(16,) + nrows,
                                       maxshape=(None,) + nrows,
                                       dtype=frame_dtype )
            self.__fid["signal_row"].attrs["comment"] = \
                                    "medians along the first axis of signal"

        if self.__fid.attrs['cols']:
            ncols = (frame_shape[0],)
            self.__fid.create_dataset( "signal_col",
                                       (0,) + ncols,
                                       chunks=(16,) + ncols,
                                       maxshape=(None,) + ncols,
                                       dtype=frame_dtype )
            self.__fid["signal_col"].attrs["comment"] = \
                                    "medians along the second axis of signal"

        if ext == 'none':
            return

        ds_name = "signal_{}".format(ext)
        self.__fid.create_dataset( ds_name,
                                   (0,) + frame_shape,
                                   chunks=chunk_sz,
                                   maxshape=(None,) + frame_shape,
                                   dtype=frame_dtype )
        self.__fid[ds_name].attrs["comment"] = \
                                    "errors in detector-pixel coordinates"
        if self.__fid.attrs['rows']:
            nrows = (frame_shape[1],)
            ds_name = "signal_row_{}".format(ext)
            self.__fid.create_dataset( ds_name,
                                       (0,) + nrows,
                                       chunks=(16,) + nrows,
                                       maxshape=(None,) + nrows,
                                       dtype=frame_dtype )
            self.__fid[ds_name].attrs["comment"] = \
                    "medians along the first axis of signal_{}".format(ext)

        if self.__fid.attrs['cols']:
            ds_name = "signal_col_{}".format(ext)
            ncols = (frame_shape[0],)
            self.__fid.create_dataset( ds_name,
                                       (0,) + ncols,
                                       chunks=(16,) + ncols,
                                       maxshape=(None,) + ncols,
                                       dtype=frame_dtype )
            self.__fid[ds_name].attrs["comment"] = \
                    "medians along the second axis of signal_{}".format(ext)

    ## ---------- RETURN VERSION of the S/W ----------
    @staticmethod
    def pynadc_version():
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
        if not self.__rw or name in self.__fid.attrs.keys():
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
        if not self.__rw:
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
        
    ## ---------- WRITE HOUSE-KEEPING DATA ----------
    def h5_write_hk( self, id, hk ):
        '''
        Create datasets for house-keeping data.
        seperated datasets for biweight median and spread

        Todo: only include hk-data when the instrument is really performing 
              the requested measurements. 
              Reject entries during start-up or at the end
        '''
        assert self.__rw

        ## write entry identifier
        id_entry = np.empty( 1, dtype=self.__fid['id'].dtype )
        id_entry[0] = id
        dset = self.__fid['id']
        dset.resize( (dset.shape[0]+1,) )
        dset[-1] = id_entry

        ## write SWIR house-keeping data
        if self.__fid.attrs['hk']:
            hk_median = np.empty( 1, dtype=self.__fid['hk_median'].dtype )
            hk_spread = np.empty( 1, dtype=self.__fid['hk_median'].dtype )
            for name in self.__fid['hk_median'].dtype.names:
                if hk[name].dtype.name.find( 'float' ) >= 0:
                    (mx, sx) = biweight(hk[name], spread=True)
                    hk_median[name] = mx
                    hk_spread[name] = sx
                elif hk[name].dtype.name.find( 'int' ) >= 0:
                    hk_median[name] = np.median(hk[name])
                    hk_spread[name] = np.all(hk[name])
                else:
                    print( name )
            dset = self.__fid['hk_median']
            dset.resize( (dset.shape[0]+1,) )
            dset[-1] = hk_median

            dset = self.__fid['hk_spread']
            dset.resize( (dset.shape[0]+1,) )
            dset[-1] = hk_spread

    ## ---------- WRITE DATA (frames averaged in time) ----------
    def h5_write_frame( self, values, errors ):
        '''
        '''
        assert self.__rw

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
 
    def h5_read_frame( self, row_id, statistics=None ):
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
    
    ## --------------------------------------------------
    def h5_get_trend_cols( self, orbit_list=None, orbit_range=None ):
        pass

    def h5_get_trend_rows( self, orbit_list=None, orbit_range=None ):
        pass

    def h5_get_trend_pixel( self, pixel_id, orbit_list=None, orbit_range=None ):
        '''
        '''
        ext = self.__fid.attrs['method']

        row = pixel_id // 1000
        col = pixel_id - row * 1000
        
        if orbit_list is not None:
            z_list = self.sql_get_rowid( orbit_list=orbit_list )
        elif orbit_range is not None:
            z_list = self.sql_get_rowid( orbit_range=orbit_range )
        else:
            z_list = self.sql_get_rowid()

        res = {}
        dset = self.__fid['id']
        id = dset[:]
        indx = np.nonzero(np.in1d(id['sql_rowid'],z_list))[0].tolist()
        res['orbit_ref'] = id['orbit_ref'][indx]
        
        dset = self.__fid['signal']
        res['signal'] = dset[indx, col, row]

        dset = self.__fid['signal_{}'.format(ext)]
        res['signal_{}'.format(ext)] = dset[indx, col, row]
        
        return res
        

    ## ---------- WRITE META-DATA TO SQL database ----------
    def sql_write( self, meta_dict, verbose=False ):
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
         "data_spread"  : column dataSpread     [float]
         "error_median" : column errorMedian    [float]
         "error_spread" : column errorSpread    [float]
         "q_det_temp"   : column q_detTemp      [integer]
         "q_obm_temp"   : column q_obmTemp      [integer]
        '''
        dbname = self.dbname + '.db'
        str_sql = 'insert into icm_meta values' \
                  '(NULL,{orbit_ref},{orbit_used},{entry_date!r}' \
                  ',{start_time!r}'\
                  ',{icm_version!r},{algo_version!r},{db_version!r}'\
                  ',{data_median},{data_spread},{error_median},{error_spread}'\
                  ',{q_det_temp},{q_obm_temp})'
        if verbose:
            print( str_sql.format(**meta_dict) )

        con = sqlite3.connect( dbname )
        cur = con.cursor()
        cur.execute( str_sql.format(**meta_dict) )
        con.commit()
        cur.execute( 'select last_insert_rowid()' )
        row = cur.fetchone()
        cur.close()
        con.close()
        if row is None:
            return -1
        else:
            return row[0]
        

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
    
    def sql_get_rowid( self, orbit_list=None, orbit_range=None ):
        '''
        Obtain rowIDs of selected entries in the SQLite or HDF5 databases

        Parameters:
           orbit_list  : list with orbit numbers
           orbit_range : list with orbit range: [orbit_mn, orbit_mx]

        Returns boolean array
        '''
        dbname = self.dbname + '.db'
        id_list = []
        if not os.path.exists( dbname ):
            return id_list

        str_sql = 'select rowID from icm_meta'
        if orbit_list is not None:
            str_sql += ' where referenceOrbit in ('
            str_sql += ','.join(str(x) for x in orbit_list)
            str_sql += ') order by referenceOrbit'
        elif orbit_range is not None:
            assert( len(orbit_range) == 2 )

            str_sql += ' where referenceOrbit between'
            str_sql += ' {} and {}'.format(*orbit_range)
            str_sql += ' order by referenceOrbit'
        else:
            str_sql += ' order by referenceOrbit'
            
        conn = sqlite3.connect( dbname )
        cur = conn.cursor()
        cur.execute( str_sql )
        for row in cur:
            id_list += [row[0],]
        cur.close()
        conn.close()
        
        return id_list

    def sql_get_row_list( self, orbit_list=None, orbit_range=None,
                          frame_stats=False ):
        '''
        Obtain list of rowIDs and orbit numbers for given orbit(range)
        Parameters:
           orbit_list  : list with orbit numbers
           orbit_range : list with orbit range: [orbit_mn, orbit_mx]
           frame_stats : add statistics of selected frames as
                           dataMedian, dataSpread, errorMedian, errorSpread

        Returns dictionary with keys: 'rowID' and 'referenceOrbit'
        '''
        dbname = self.dbname + '.db'
        row_list = {}
        if not os.path.exists( dbname ):
            return row_list

        if frame_stats:
            str_sql = 'select rowID,referenceOrbit,dataMedian,dataSpread,errorMedian,errorSpread from icm_meta'
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
             'dataSpread', 'errorMedian', 'errorSpread'
        '''
        return self.sql_get_row_list( orbit_list=orbit_list,
                                      orbit_range=orbit_range,
                                      frame_stats=True )

#--------------------------------------------------
def test_sun_isrf():
    '''
    Create test database from OCAL Sun-ISRF background measurements
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
        
        (mx, sx) = biweight(values, spread=True)
        meta_dict['data_median'] = mx
        meta_dict['data_spread'] = sx
        (mx, sx) = biweight(errors, spread=True)
        meta_dict['error_median'] = mx
        meta_dict['error_spread'] = sx

        ## Tim: how to obtain the actual version of your S/W?
        meta_dict['algo_version'] = '00.01.00'
        meta_dict['db_version'] = fp.pynadc_version()
        meta_dict['q_det_temp'] = 0                    ## obtain from hk
        meta_dict['q_obm_temp'] = 0                    ## obtain from hk
        meta_dict['q_algo'] = 0                        ## obtain from algo?!
        hk_data = fp.housekeeping_data

        mon = ICM_mon( DBNAME, readwrite=True )
        if ii == 0:
            mon.h5_set_attr( 'title', 'Tropomi SWIR OCAL SunISRF backgrounds' )
            mon.h5_set_attr( 'source',
                             'Copernicus Sentinel-5 Precursor Tropomi On-ground Calibration and Monitoring products' )
            mon.h5_set_attr( 'comment',
                             'ICID {} ($t_{{exp}}={:.3f}$ sec)'.format(fp.instrument_settings['ic_id'], float(fp.instrument_settings['exposure_time'])) )
            mon.h5_set_attr( 'orbit_window', ORBIT_WINDOW )
            mon.h5_set_attr( 'icid_list',
                             [fp.instrument_settings['ic_id'],] )
            mon.h5_set_attr( 'ic_version',
                             [fp.instrument_settings['ic_version'],] )

            ## initialize HDF5 datasets
            mon.h5_init( (256,1000), np.float64, statistics='hk,std,col,row' )
            mon.h5_set_frame_attr( 'long_name', 'signal' )
            mon.h5_set_frame_attr( 'units', 'electron / s' )

        sql_rowid = mon.sql_write( meta_dict )
        if sql_rowid > 0:
            mon.h5_write_hk( (sql_rowid,ii), hk_data )
            mon.h5_write_frame( values, errors )

        del( mon )
        del( fp )
        ii += 1
        
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
        fl_path = '/Users/richardh/Data/S5P_ICM_CA_SIR/001100/2012/09/18'
    elif os.path.isdir('/nfs/TROPOMI/ical/'):
        fl_path = '/nfs/TROPOMI/ical/S5P_ICM_CA_SIR/001100/2012/09/18'
    else:
        fl_path = '/data/richardh/Tropomi/ical/S5P_ICM_CA_SIR/001100/2012/09/18'
    icm_file = 'S5P_TEST_ICM_CA_SIR_20120918T131651_20120918T145629_01890_01_001100_20151002T140000.h5'

    ## ----- initialize ICM monitor databases -----
    ## Note that you do not need to open an product using ICM_io, but then
    ##      you need to provide some the required information yourself
    fp = ICM_io( os.path.join(fl_path, icm_file) )
    fp.select( 'BACKGROUND_RADIANCE_MODE_0005' )
    
    mon = ICM_mon( DBNAME, readwrite=True )
    mon.h5_set_attr( 'title', 'Tropomi SWIR dark-flux monitoring results' )
    mon.h5_set_attr( 'source',
                     'Copernicus Sentinel-5 Precursor Tropomi In-flight Calibration and Monitoring products' )
    mon.h5_set_attr( 'comment',
                     'ICID {} ($t_{{exp}}={:.3f}$ sec)'.format(fp.instrument_settings['ic_id'], float(fp.instrument_settings['exposure_time'])) )
    mon.h5_set_attr( 'orbit_window', ORBIT_WINDOW )
    mon.h5_set_attr( 'icid_list', [fp.instrument_settings['ic_id'],] )
    mon.h5_set_attr( 'ic_version', [fp.instrument_settings['ic_version'],] )

    ## initialize HDF5 datasets
    mon.h5_init( (256,1000), np.float64, statistics='hk,std,col,row' )
    mon.h5_set_frame_attr( 'long_name', 'background signal' )
    mon.h5_set_frame_attr( 'units', 'electron' )
    del(mon)
    del(fp)
    
    for ii in range( num_orbits ):
        print( ii )
        
        ## open access to ICM product
        fp = ICM_io( os.path.join(fl_path, icm_file) )

        ## select measurement and collect its meta-data 
        fp.select( 'BACKGROUND_RADIANCE_MODE_0005' )
    
        ## read data from ICM product and combine band 7 & 8
        res = fp.get_data()
        values = np.hstack((res['signal_avg'][0][:-1,:],
                            res['signal_avg'][1][:-1,:]))
        errors = np.hstack((res['signal_avg_std'][0][:-1,:], 
                            res['signal_avg_std'][1][:-1,:]))

        meta_dict = {}
        meta_dict['orbit_ref'] = fp.orbit + ii
        meta_dict['orbit_window'] = ORBIT_WINDOW
        meta_dict['orbit_used'] = ORBIT_WINDOW - (ii % 3) ## placeholder
        meta_dict['entry_date'] = datetime.utcnow().isoformat(' ')
        meta_dict['start_time'] = fp.start_time

        ## Tim: how to obtain the actual version of your S/W?
        meta_dict['algo_version'] = '00.01.00'
        meta_dict['icm_version'] = fp.creator_version
        meta_dict['db_version'] = fp.pynadc_version()

        (mx, sx) = biweight(values, spread=True)
        meta_dict['data_median'] = mx
        meta_dict['data_spread'] = sx
        (mx, sx) = biweight(errors, spread=True)
        meta_dict['error_median'] = mx
        meta_dict['error_spread'] = sx

        meta_dict['q_det_temp'] = 0                    ## obtain from hk
        meta_dict['q_obm_temp'] = 0                    ## obtain from hk
        meta_dict['q_algo'] = 0                        ## obtain from algo?!
        hk_data = fp.housekeeping_data

        ## then add information to monitoring database
        ## Note that ingesting results twice leads to database corruption!
        ##   Please use 'mon.sql_check_orbit'
        mon = ICM_mon( DBNAME, readwrite=True )
        sql_rowid = mon.sql_write( meta_dict )
        if sql_rowid > 0:
            mon.h5_write_hk( (sql_rowid, meta_dict['orbit_ref']), hk_data  )
            mon.h5_write_frame( values, errors )
        del( fp )
        del( mon )

    ## select rows from database given an orbit range
    mon = ICM_mon( DBNAME, readwrite=False )
    print( mon.sql_select_orbit( [1900,1910], full=True ) )
    del(mon)
        
#--------------------------------------------------
if __name__ == '__main__':
    test( 31 )
    #test_sun_isrf()
