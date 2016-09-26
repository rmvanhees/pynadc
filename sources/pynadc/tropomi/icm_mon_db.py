# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
The SRON monitor database consists of a SQLite database and a HDF5 database

SQLite Database layout
----------------------
 <mode=frame>
 Table icm_meta (
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
 )

 <mode=dpqm>
 Table icm_meta (
      rowID           integer     PRIMARY KEY AUTOINCREMENT,
      referenceOrbit  integer     NOT NULL UNIQUE,
      orbitsUsed      integer     NOT NULL,
      entryDateTime   datetime    NOT NULL default '0000-00-00 00:00:00',
      startDateTime   datetime    NOT NULL default '0000-00-00 00:00:00',
      icmVersion      text        NOT NULL,
      algVersion      text        NOT NULL,
      dbVersion       text        NOT NULL,
      dpqf_01         int         NOT NULL,
      dpqf_08         int         NOT NULL,
      dpqf_dark_01    int         NOT NULL,
      dpqf_dark_08    int         NOT NULL,
      dpqf_noise_01   int         NOT NULL,
      dpqf_noise_08   int         NOT NULL,
      q_detTemp       integer     NOT NULL,
      q_obmTemp       integer     NOT NULL
 )

HDF5 Database layout
---------------
-- datasets --
 * id : compound dataset
     Entry identifier, to match entries of the SQLite and HDF5 database.
     struct {
               "sql_rowid"        +0    native long
               "orbit_ref"        +8    native long
     } 16 bytes

 * hk_median : compound dataset
     Biweight median of the housekeeping data (non-SWIR information is removed).
     struct {
               "temp_det4"        +0    native float
               "temp_obm_swir"    +4    native float
               "temp_cu_sls_stim" +8    native float
               "temp_obm_swir_grating" +12   native float
               "temp_obm_swir_if" +16   native float
               "temp_pelt_cu_sls1" +20   native float
               "temp_pelt_cu_sls2" +24   native float
               "temp_pelt_cu_sls3" +28   native float
               "temp_pelt_cu_sls4" +32   native float
               "temp_pelt_cu_sls5" +36   native float
               "swir_vdet_bias"   +40   native float
               "difm_status"      +44   native float
               "det4_led_status"  +48   native unsigned char
               "wls_status"       +49   native unsigned char
               "common_led_status" +50   native unsigned char
               "sls1_status"      +51   native unsigned char
               "sls2_status"      +52   native unsigned char
               "sls3_status"      +53   native unsigned char
               "sls4_status"      +54   native unsigned char
               "sls5_status"      +55   native unsigned char
     } 56 bytes

 * hk_spread : compound dataset
     Biweight spread of the housekeeping data (non-SWIR information is removed).
     Same structure as for "hk_median"

 <mode=frame>
 * signal :  ndarray with same shape as the SWIR detector, row 257 removed
    Parameter derived for each detector pixel, described in the attributes
    "long_name" and "units"
 * signal_<method> :  ndarray like "signal", method={std|error|noise}
    Standard deviation/error/noise of the dataset "signal"
 * signal_col :  ndarray with same number of columns as the SWIR detector
    Biweight median of dataset "signal" over the rows
 * signal_col_<method> :  ndarray like "signal_col"
    Biweight median of dataset "signal_<method>" over the rows
 * signal_row : ndarray with same number of rows as the SWIR detector
    Biweight median of dataset "signal" over the columns
 * signal_row_<method> :  ndarray like "signal_row"
    Biweight median of dataset "signal_<method>" over the columns

 <mode=dpqm>
 * dpqf_map :  ndarray with same shape as the SWIR detector, row 257 removed
    Pixel quality
 * dpqm_dark_flux :  ndarray like "dpqf_map"
    Pixel quality derived from dark measurements
 * dpqm_noise :  ndarray like "dpqf_map"
    Pixel quality derived from noise statistics

-- attributes --
 * title        :  string
    Short description of the file contents
 * comment      :  string
    Short description of data used
 * institution  :  string
    Where the databases were produced
 * source       :  string
    Reference to the original data
 * references   :  string
    References that describe the data or methods used to produce it
 * algoVersion  :  string
    Version of the algorithms used to produce it
 * dbVersion    :  string
    Version of the database S/W used to produce it
 * icmVersion   :  string
    Version of the ICM products used as input
 * orbit_window :  integer
    Size of the orbit window of the ICM products used

 * mode    :  string
    Kind of data used, currently foreseen are: dpqm, isrf or frame (= default)
 * method  :  string
    Defines error estimate: standard deviation "std", error or noise
 * hk      :  boolean
    Presence of housekeeping data
 * cols    :  boolean
    Presence of column statistics
 * rows    :  boolean
    Presence of row statistics

 * ds_list :  list of strings
    Listing of dataset present in the HDF5 database
 * icid_list  : list if integers
    Listing of ICIDs used in the calculations
 * ic_version : list if integers
    Listing of instrument configuration version used in the calculations

Functions and class-methods defined in the class ICM_mon
--------------------------------------------------------
-- create a new database --
 * mon = ICM_mon( dbname, readwrite=True )

-- add/read HDF5 attributes:
 * h5_set_attr( attr_name, value )
 * h5_get_attr( attr_name )
 * h5_set_ds_attr( ds_name, attr_name, value )
 * h5_get_ds_attr( ds_name, attr_name )

-- add new entries to database --
 Public methods to add new entries to the monitor databases are:
 * sql_write( meta_dict )
 * h5_write_hk( hk_data )
 * h5_write( values, erros )
 * h5_write_dpqm( dict_dpqm )

-- read entries from database
 * h5_read( dict_frame )
 * h5_read_dpqm( dict_dpqm )
 * h5_get_trend( ds_name, data )   ## not yes implemented

-- update existing entries in database --
 Public methods to update existing entries in the monitor databases are:
 * h5_update_hk      ## not yes implemented
 * h5_update_frame   ## not yes implemented
 * sql_update_meta   ## not yes implemented

-- query database --
 Public methods to query the monitor databases are:
 * get_orbit_latest()
 * sql_num_entries()
 * sql_check_orbit( orbit )
 * sql_select_orbit( orbit )
 * sql_get_row_list()
 * sql_get_rowid()
 * sql_get_trend()

-- miscellaneous functions --
 * pynadc_version()
 * get_method()
 * get_ds_list()

Configuration management
------------------------
* ICM product version [KNMI]
  The version of the ICM processor is visible in the SQLite monitor database,
  reprocessed L1b products will initiate a reprocessing of the results in the
  monitor database

* Monitor algorithm version [SRON]
  Major version updates of the monitor algorithms should indicate that a 
  reprocessing of the monitor results is necessary, otherwise no reprocessing
  is necessary

* Monitor database format [SRON]
  Major version updates of the monitor databases should indicate that a 
  reprocessing of the monitor results is necessary, otherwise no reprocessing
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
    Defines methods to create ICM monitor databases and ingest new entries
    '''
    def __init__( self, dbname, readwrite=False,
                  mode='frame', statistics='std,hk,col,row' ):
        '''
        Perform the following tasks:
        * if database does not exist 
        then
         1) create HDF5 database (readwrite=True)
         3) set attributes with 'db_version'
        else
         1) open HDF5 database, read only when orbit is already present
         1) check versions of 'algo_version' and 'db_version'
         3) set class attributes

        Parameters
        ----------
        dbname :  string
              Name of the monitor database without extension '.db' or '.h5'
        readwrite :  boolean, optional
              Open HDF5 database read/append mode. The default is to open the
              HDF5 database in readonly mode
        mode   :  {'frame','dpqm','isrf'}, optional
              Differentiate between ICM monitor databases for 'frames': values 
              mapped on detector frames, 'dpqm': quality data between 0 and 1 
              mapped on detector frames, or 'isrf': ISRF data of detector 
              regions. The default is 'frame'
        statistics : {'none|std|error|noise','hk','col','row'}, optional
              Define which datasets are stored in the HDF5 database, except the
              values. Error estimates can be absent 'none', or standard 
              deviations 'std', or algorithm/calibration errors 'error', or 
              noise 'noise'. Presence of housekeeping data, column and/or row 
              statistics can be indicated by resp. 'hk', 'col' and 'row'. 
              Default is 'std,hk,col,row'
        The parameters 'mode' and 'statistics' are only applied when a new 
        database is created.
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
            self.__fid.attrs['mode'] = mode

            if statistics == '':
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

            self.__sql_init()
            self.__h5_init((256,1000), np.float)   ## works only for SWIR!!!
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

        self.__method = self.__fid.attrs['method']
        self.__mode   = self.__fid.attrs['mode']

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
            ds_list = ['hk_median', 'hk_spread'] + self.__fid.attrs['ds_list'].tolist()
            for ds_name in ds_list:
                if ds_name in self.__fid:
                    num_ds_rows = self.__fid[ds_name].shape[0]
                    msg = "integrity test faild on {}, which has not {} rows".format(ds_name, num_sql_rows)
                    assert (num_sql_rows == num_ds_rows), msg

        if self.__fid is not None:
            self.__fid.close()

    ## ---------- initialize SQLite database ----------
    def __sql_init( self ):
        '''
        Create SQLite monitor database
        '''
        con = sqlite3.connect( self.dbname+'.db' )
        cur = con.cursor()
        if self.__fid.attrs['mode'] == 'dpqm':
            cur.execute( '''create table icm_meta (
            rowID           integer     PRIMARY KEY AUTOINCREMENT,
            referenceOrbit  integer     NOT NULL UNIQUE,
            orbitsUsed      integer     NOT NULL,
            entryDateTime   datetime    NOT NULL default '0000-00-00 00:00:00',
            startDateTime   datetime    NOT NULL default '0000-00-00 00:00:00',
            icmVersion      text        NOT NULL,
            algVersion      text        NOT NULL,
            dbVersion       text        NOT NULL,
            dpqf_01         int         NOT NULL,
            dpqf_08         int         NOT NULL,
            dpqf_dark_01    int         NOT NULL,
            dpqf_dark_08    int         NOT NULL,
            dpqf_noise_01   int         NOT NULL,
            dpqf_noise_08   int         NOT NULL,
            q_detTemp       integer     NOT NULL,
            q_obmTemp       integer     NOT NULL
            )''' )
        else:
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
    def __h5_init( self, frame_shape, frame_dtype ):
        '''
        Create datasets in HDF5 monitor database

        Parameters
        ----------
        frame_shape :  tuple
           Shape of datasets
        frame_dtype :  data-type
           Desired data-type of dataset
        '''
        method = self.__fid.attrs['method']
        
        self.__fid.attrs['institution'] = \
                                'SRON Netherlands Institute for Space Research'
        self.__fid.attrs['references'] = 'https://www.sron.nl/Tropomi'
        self.__fid.attrs['dbVersion'] = self.pynadc_version()

        # Table with row identifiers
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
        ds_list = []
        chunk_sz = (16, frame_shape[0] // 4, frame_shape[1] // 4)
        if self.__fid.attrs['mode'] == 'dpqm':
            self.__fid.create_dataset( "dpqf_map",
                                       (0,) + frame_shape,
                                       chunks=chunk_sz,
                                       maxshape=(None,) + frame_shape,
                                       dtype=frame_dtype )
            self.__fid["dpqf_map"].attrs["comment"] = \
                                        "Quality in detector-pixel coordinates"
            ds_list.append("dpqf_map")
            self.__fid.create_dataset( "dpqm_dark_flux",
                                       (0,) + frame_shape,
                                       chunks=chunk_sz,
                                       maxshape=(None,) + frame_shape,
                                       dtype=frame_dtype )
            self.__fid["dpqm_dark_flux"].attrs["comment"] = \
                                        "Quality in detector-pixel coordinates"
            ds_list.append("dpqm_dark_flux")
            self.__fid.create_dataset( "dpqm_noise",
                                       (0,) + frame_shape,
                                       chunks=chunk_sz,
                                       maxshape=(None,) + frame_shape,
                                       dtype=frame_dtype )
            self.__fid["dpqm_noise"].attrs["comment"] = \
                                        "Quality in detector-pixel coordinates"
            ds_list.append("dpqm_noise")
        else:
            self.__fid.create_dataset( "signal",
                                       (0,) + frame_shape,
                                       chunks=chunk_sz,
                                       maxshape=(None,) + frame_shape,
                                       dtype=frame_dtype )
            self.__fid["signal"].attrs["comment"] = \
                                        "values in detector-pixel coordinates"
            ds_list.append("signal")

            if self.__fid.attrs['cols']:
                ncols = (frame_shape[0],)
                self.__fid.create_dataset( "signal_col",
                                           (0,) + ncols,
                                           chunks=(16,) + ncols,
                                           maxshape=(None,) + ncols,
                                           dtype=frame_dtype )
                self.__fid["signal_col"].attrs["comment"] = \
                                    "medians along the second axis of signal"
                ds_list.append("signal_col")

            if self.__fid.attrs['rows']:
                nrows = (frame_shape[1],)
                self.__fid.create_dataset( "signal_row",
                                           (0,) + nrows,
                                           chunks=(16,) + nrows,
                                           maxshape=(None,) + nrows,
                                           dtype=frame_dtype )
                self.__fid["signal_row"].attrs["comment"] = \
                                    "medians along the first axis of signal"
                ds_list.append("signal_row")

            ds_name = "signal_{}".format(method)
            self.__fid.create_dataset( ds_name,
                                       (0,) + frame_shape,
                                       chunks=chunk_sz,
                                       maxshape=(None,) + frame_shape,
                                       dtype=frame_dtype )
            self.__fid[ds_name].attrs["comment"] = \
                                        "errors in detector-pixel coordinates"
            ds_list.append(ds_name)
            
            if self.__fid.attrs['rows']:
                nrows = (frame_shape[1],)
                ds_name = "signal_row_{}".format(method)
                self.__fid.create_dataset( ds_name,
                                           (0,) + nrows,
                                           chunks=(16,) + nrows,
                                           maxshape=(None,) + nrows,
                                           dtype=frame_dtype )
                self.__fid[ds_name].attrs["comment"] = \
                    "medians along the first axis of signal_{}".format(method)
                ds_list.append(ds_name)
                
            if self.__fid.attrs['cols']:
                ds_name = "signal_col_{}".format(method)
                ncols = (frame_shape[0],)
                self.__fid.create_dataset( ds_name,
                                           (0,) + ncols,
                                           chunks=(16,) + ncols,
                                           maxshape=(None,) + ncols,
                                           dtype=frame_dtype )
                self.__fid[ds_name].attrs["comment"] = \
                    "medians along the second axis of signal_{}".format(method)
                ds_list.append(ds_name)

        self.__fid.attrs['ds_list'] = np.array([np.string_(n) for n in ds_list])
                
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

    def get_ds_list( self ):
        return [n.decode('ascii') for n in self.__fid.attrs['ds_list']]

    ## ---------- READ/WRITE GOBAL ATTRIBUTES ----------
    def h5_set_attr( self, name, value ):
        '''
        Add global attributes to HDF5 database, during definition phase.
        Otherwise the call is silently ignored

        Parameters
        ----------
        name  : string
        value : scalar or array-like

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

        Parameters
        ----------
        name  : string
        '''
        if name in self.__fid.attrs.keys():
            return self.__fid.attrs[name]
        else:
            return None
        
    ## ---------- READ/WRITE DATASET ATTRIBUTES ----------
    def h5_set_ds_attr( self, ds_name, attr, value ):
        '''
        Add attributes to HDF5 datasets, during definition phase.
        Otherwise the call is silently ignored

        Parameters
        ----------
        ds_name  : string
        attr     : string
        value : scalar or array-like
        
        Please use names according to the CF conventions:
         - 'standard_name' : required CF-conventions attribute
         - 'long_name' : descriptive name may be used for labeling plots
         - 'units' : unit of the dataset values
        '''
        if not self.__rw:
            return

        if len(ds_name ) == 0:
            for ds_name in self.get_ds_list():
                if not attr in self.__fid[ds_name].attrs.keys():
                    self.__fid[ds_name].attrs[attr] = value
        else:
            if not attr in self.__fid[ds_name].attrs.keys():
                self.__fid[ds_name].attrs[attr] = value

    def h5_get_ds_attr( self, ds_name, attr ):
        '''
        Obtain value of an HDF5 dataset attribute

        Parameters
        ----------
        ds_name  : string
        attr     : string

        '''
        if attr in self.__fid[ds_name].attrs.keys():
            return self.__fid[ds_name].attrs[attr]
        else:
            return None
        
    ## ---------- WRITE HOUSE-KEEPING DATA ----------
    def h5_write_hk( self, id, hk ):
        '''
        Write ID of entry and biweight median and spread of house-keeping data

        Parameters
        ----------
        id  : tuple
           Tuple with rowID and orbit-number obtained from SQLite database
        hk  : array of type housekeeping_data, optional
           All housekeeping data of the used measurements

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
    def h5_write( self, values, errors ):
        '''
        Write values and errors to HDF5 database with mode=frame

        Parameters
        ----------
        values :  ndarray
        errors :  ndarray
        '''
        assert self.__rw
        assert self.__mode == 'frame'

        dset = self.__fid['signal']
        shape = (dset.shape[0]+1,) + dset.shape[1:]
        dset.resize( shape )
        dset[-1,:,:] = values

        if self.__method != 'none':
            dset = self.__fid['signal_{}'.format(self.__method)]
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:,:] = errors

        if self.__fid.attrs['cols']:
            dset = self.__fid['signal_col']
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:] = np.nanmedian( values, axis=1 )

            if self.__method != 'none':
                dset = self.__fid['signal_col_{}'.format(self.__method)]
                shape = (dset.shape[0]+1,) + dset.shape[1:]
                dset.resize( shape )
                dset[-1,:] = np.nanmedian( errors, axis=1 )
 
        if self.__fid.attrs['rows']:
            dset = self.__fid['signal_row']
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            dset[-1,:] = np.nanmedian( values, axis=0 )

            if self.__method != 'none':
                dset = self.__fid['signal_row_{}'.format(self.__method)]
                shape = (dset.shape[0]+1,) + dset.shape[1:]
                dset.resize( shape )
                dset[-1,:] = np.nanmedian( errors, axis=0 )
 
    def h5_read( self, orbit ):
        '''
        Read signal data-sets given a reference orbit-number from 
        HDF5 database with mode=frame

        Parameters
        ----------
        orbit  :  integer
           Reference orbit-numer
           
        Returns
        -------
        dict_frame  :  dictionary
           Dictionary with ndarrays of all signal data-sets (only one entry)
        '''
        assert self.__mode == 'frame'

        res = {}
        dset = self.__fid['id']
        id = dset[:]
        indx = np.where( id['orbit_ref'] == orbit )[0]

        for name in self.get_ds_list():
            dset = self.__fid[name]
            res[name] = dset[indx,...]
               
        return res
    
    ## ---------- WRITE DATA (quality maps) ----------
    def h5_write_dpqm( self, dict_dpqm ):
        '''
        Write dynamic CKD DPQF_MAP to HDF5 database with mode=dpqm

        Parameters
        ----------
        dict_dpqm  :  dictionary
           Dictionary with ndarrays as return by icm_io.get_data()
        '''
        assert self.__rw
        assert self.__mode == 'dpqm'

        for name in self.get_ds_list():
            dset = self.__fid[name]
            shape = (dset.shape[0]+1,) + dset.shape[1:]
            dset.resize( shape )
            values = np.hstack((dict_dpqm[name][0][:-1,:],
                                dict_dpqm[name][1][:-1,:]))
            dset[-1,:,:] = values
 
    def h5_read_dpqm( self, orbit ):
        '''
        Read all DPQF_MAP datasets given a reference orbit-number from
        HDF5 database with mode=dpqm

        Parameters
        ----------
        orbit  :  integer
           Reference orbit-numer
           
        Returns
        -------
        dict_dpqm  :  dictionary
           Dictionary with ndarrays of all DPQF_MAP data-sets (only one entry)
        '''
        assert self.__mode == 'dpqm'

        dset = self.__fid['id']
        id = dset[:]
        indx = np.where( id['orbit_ref'] == orbit )[0]

        res = {}
        for name in self.get_ds_list():
            dset = self.__fid[name]
            res[name] = dset[indx,...]

        return res

    ## --------------------------------------------------
    def h5_get_trend_cols( self, orbit_list=None, orbit_range=None ):
        pass

    def h5_get_trend_rows( self, orbit_list=None, orbit_range=None ):
        pass

    def h5_get_trend_pixel( self, col, row, orbit_list=None, orbit_range=None ):
        '''
        '''
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

        dset = self.__fid['signal_{}'.format(self.__method)]
        res['signal_{}'.format(self.__method)] = dset[indx, col, row]
        
        return res

    ## ---------- WRITE META-DATA TO SQL database ----------
    def sql_write( self, meta_dict, verbose=False ):
        '''
        Append monitor meta-data to SQLite database

        Parameters
        ----------
        meta_dict : dictionary
           The dictionary "meta_dict" should contain all fields if table 
           ICM_META, except "rowID"
        '''
        dbname = self.dbname + '.db'
        if self.__mode == 'dpqm':
            str_sql = 'insert into icm_meta values' \
                      '(NULL,{orbit_ref},{orbit_used},{entry_date!r}' \
                      ',{start_time!r}'\
                      ',{icm_version!r},{algo_version!r},{db_version!r}'\
                      ',{dpqf_01},{dpqf_08}' \
                      ',{dpqf_dark_01},{dpqf_dark_08}' \
                      ',{dpqf_noise_01},{dpqf_noise_08}' \
                      ',{q_det_temp},{q_obm_temp})'
        else:
            str_sql = 'insert into icm_meta values' \
                      '(NULL,{orbit_ref},{orbit_used},{entry_date!r}' \
                      ',{start_time!r}'\
                      ',{icm_version!r},{algo_version!r},{db_version!r}'\
                      ',{data_median},{data_spread}' \
                      ',{error_median},{error_spread}'\
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

        Parameters
        ----------
        orbit  :  integer
           Reference orbit-numer

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

        Parameters
        ----------
        orbit      : scalar or list
           Reference orbit-numer or range by orbit_min, orbit_max
        orbit_used : integer, optional
           Minimal number of orbits used to calculate results 
        q_det_temp : float, optional
           Select only entries with stable detector temperature
        q_obm_temp : float, optional
           Select only entries with stable OBM temperature
        full       : boolean, optional
           Return all fields of the selected rows from table ICM_META,
           else only return rowID & referenceOrbit
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

        Parameters
        ----------
        orbit_list  : list
           Reference orbit-numbers
        orbit_range : list
           Reference orbit-number range: [orbit_mn, orbit_mx]

        Returns
        -------
        List with selected rowIDs
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
        ----------
        orbit_list  : list
           Reference orbit-numbers
        orbit_range : list
           Reference orbit-number range: [orbit_mn, orbit_mx]
        frame_stats : boolean
           If true then add statistics of selected entries, given are 
           dataMedian, dataSpread, errorMedian, errorSpread

        Returns
        -------
        Dictionary with keys: 'rowID' and 'referenceOrbit'
        '''
        assert self.__mode == 'frame'

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

    def sql_get_trend( self, orbit_list=None, orbit_range=None ):
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
        fl_path = '/data/richardh/Tropomi/ISRF/2015_05_02T10_28_44_SwirlsSunIsrf'

    dirList = [d for d in os.listdir( fl_path ) 
               if os.path.isdir(os.path.join(fl_path, d))]
    dirList.sort()
    for msm in dirList:
        fp = OCM_io( os.path.join(fl_path, msm), verbose=True )
        if fp.select( 31623 ) > 0:
            break
    mon = ICM_mon( DBNAME, readwrite=True )
    mon.h5_set_attr( 'title', 'Tropomi SWIR OCAL SunISRF backgrounds' )
    mon.h5_set_attr( 'source',
                     'Copernicus Sentinel-5 Precursor Tropomi On-ground Calibration and Monitoring products' )
    mon.h5_set_attr( 'comment',
                     'ICID {} ($t_{{exp}}={:.3f}$ sec)'.format(fp.instrument_settings['ic_id'],
                                                               float(fp.instrument_settings['exposure_time'])) )
    mon.h5_set_attr( 'orbit_window', ORBIT_WINDOW )
    mon.h5_set_attr( 'icid_list',
                     [fp.instrument_settings['ic_id'],] )
    mon.h5_set_attr( 'ic_version',
                     [fp.instrument_settings['ic_version'],] )
    for ds_name in mon.get_ds_list():
        mon.h5_set_ds_attr( ds_name, 'long_name', ds_name )
        mon.h5_set_ds_attr( ds_name, 'units', 'electron / s' )
    del(fp)
    del(mon)
    
    ii = 0
    for msm in dirList:
        fp = OCM_io( os.path.join(fl_path, msm), verbose=True )
        if fp.select( 31623 ) == 0:
            continue
        
        print( fp )
        res = fp.get_data()
        values = np.hstack((res['signal'][0][:-1,:],
                            res['signal'][1][:-1,:]))
        errors = np.hstack((res['signal_error'][0][:-1,:],
                            res['signal_error'][1][:-1,:]))
        
        meta_dict = {}
        meta_dict['orbit_ref'] = ii
        meta_dict['orbit_window'] = ORBIT_WINDOW
        meta_dict['orbit_used'] = fp.num_msm
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

        mon = ICM_mon( DBNAME, readwrite=True )
        sql_rowid = mon.sql_write( meta_dict )
        if sql_rowid > 0:
            mon.h5_write_hk( (sql_rowid,ii), hk_data )
            mon.h5_write( values, errors )
        ii += 1
        
    del( fp )
    del( mon )
    
#--------------------------------------------------
def test_quality():
    '''
    '''
    from datetime import datetime

    from pynadc.tropomi import db
    from pynadc.tropomi.icm_io import ICM_io

    DBNAME = 'mon_quality_test'
    ORBIT_WINDOW = 15
    if os.path.exists( DBNAME + '.h5' ):
        os.remove( DBNAME + '.h5' )
    if os.path.exists( DBNAME + '.db' ):
        os.remove( DBNAME + '.db' )

    res_list = db.get_product_by_type(dataset='DPQF_MAP')
    fp = ICM_io( os.path.join(res_list[0][0], res_list[0][1]) )
    fp.select( 'DPQF_MAP' )
    
    mon = ICM_mon( DBNAME, readwrite=True, mode='dpqm', statistics='hk')
    mon.h5_set_attr( 'title', 'Tropomi SWIR Quality (dynamic DPQF CKD)' )
    mon.h5_set_attr( 'source',
                     'Copernicus Sentinel-5 Precursor Tropomi In-flight Calibration and Monitoring products' )
    mon.h5_set_attr( 'comment',
                     'ICID {} ($t_{{exp}}={:.3f}$ sec)'.format(fp.instrument_settings['ic_id'], float(fp.instrument_settings['exposure_time'])) )
    mon.h5_set_attr( 'orbit_window', ORBIT_WINDOW )
    mon.h5_set_attr( 'icid_list', [fp.instrument_settings['ic_id'],] )
    mon.h5_set_attr( 'ic_version', [fp.instrument_settings['ic_version'],] )

    for ds_name in mon.get_ds_list():
        mon.h5_set_ds_attr( ds_name, 'long_name', ds_name )
        mon.h5_set_ds_attr( ds_name, 'units', '1' )
    del(mon)
    del(fp)
    
    for res in res_list:
        fp = ICM_io( os.path.join(res[0], res[1]) )
        print(fp)
        fp.select( 'DPQF_MAP' )
        msm = fp.get_data()

        meta_dict = {}
        meta_dict['orbit_ref'] = fp.orbit
        meta_dict['orbit_window'] = ORBIT_WINDOW
        meta_dict['orbit_used'] = ORBIT_WINDOW
        meta_dict['entry_date'] = datetime.utcnow().isoformat(' ')
        meta_dict['start_time'] = fp.start_time

        ## Tim: how to obtain the actual version of your S/W?
        meta_dict['algo_version'] = '00.01.00'
        meta_dict['icm_version'] = fp.creator_version
        meta_dict['db_version'] = fp.pynadc_version()

        thres_min = 1
        thres_max = 8
        dpqm = np.hstack((msm['dpqf_map'][0][:-1,:],
                          msm['dpqf_map'][1][:-1,:]))
        dpqf = (dpqm * 10).astype(np.byte)
        unused_cols = np.where(np.sum(dpqm, axis=0) < (256 // 4))
        if unused_cols[0].size > 0:
            dpqf[:,unused_cols[0]] = -1
        unused_rows = np.where(np.sum(dpqm, axis=1) < (1000 // 4))
        if unused_rows[0].size > 0:
            dpqf[:,unused_rows[0]] = -1
        meta_dict['dpqf_01'] = np.sum(((dpqf >= 0) & (dpqf < thres_min)))
        meta_dict['dpqf_08'] = np.sum(((dpqf >= 0) & (dpqf < thres_min)))
        dpqm = np.hstack((msm['dpqm_dark_flux'][0][:-1,:],
                          msm['dpqm_dark_flux'][1][:-1,:]))
        dpqf = (dpqm * 10).astype(np.byte)
        unused_cols = np.where(np.nansum(dpqm, axis=0) < (256 // 4))
        if unused_cols[0].size > 0:
            dpqf[:,unused_cols[0]] = -1
        unused_rows = np.where(np.nansum(dpqm, axis=1) < (1000 // 4))
        if unused_rows[0].size > 0:
            dpqf[:,unused_rows[0]] = -1
        meta_dict['dpqf_dark_01'] = np.sum(((dpqf >= 0) & (dpqf < thres_min)))
        meta_dict['dpqf_dark_08'] = np.sum(((dpqf >= 0) & (dpqf < thres_min)))
        dpqm = np.hstack((msm['dpqm_noise'][0][:-1,:],
                          msm['dpqm_noise'][1][:-1,:]))
        dpqf = (dpqm * 10).astype(np.byte)
        unused_cols = np.where(np.sum(dpqm, axis=0) < (256 // 4))
        if unused_cols[0].size > 0:
            dpqf[:,unused_cols[0]] = -1
        unused_rows = np.where(np.sum(dpqm, axis=1) < (1000 // 4))
        if unused_rows[0].size > 0:
            dpqf[:,unused_rows[0]] = -1
        meta_dict['dpqf_noise_01'] = np.sum(((dpqf >= 0) & (dpqf < thres_min)))
        meta_dict['dpqf_noise_08'] = np.sum(((dpqf >= 0) & (dpqf < thres_min)))

        meta_dict['q_det_temp'] = 0                    ## obtain from hk
        meta_dict['q_obm_temp'] = 0                    ## obtain from hk
        meta_dict['q_algo'] = 0                        ## obtain from algo?!
        hk_data = fp.housekeeping_data
        
        mon = ICM_mon( DBNAME, readwrite=True )
        sql_rowid = mon.sql_write( meta_dict )
        if sql_rowid > 0:
            mon.h5_write_hk( (sql_rowid, meta_dict['orbit_ref']), hk_data  )
            mon.h5_write_dpqm( msm )

    del( fp )
    del( mon )
            
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

    mon.h5_set_ds_attr( 'signal', 'long_name', 'background signal' )
    mon.h5_set_ds_attr( 'signal_std', 'long_name', 'background signal_std' )
    mon.h5_set_ds_attr( 'signal_col', 'long_name', 'background signal_col' )
    mon.h5_set_ds_attr( 'signal_row', 'long_name', 'background signal_row' )
    mon.h5_set_ds_attr( '', 'units', 'electron' )
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
            mon.h5_write( values, errors )
        del( fp )
        del( mon )

    ## select rows from database given an orbit range
    mon = ICM_mon( DBNAME, readwrite=False )
    print( mon.sql_select_orbit( [1900,1910], full=True ) )
    del(mon)
        
#--------------------------------------------------
if __name__ == '__main__':
    #test( 31 )
    test_sun_isrf()
    #test_quality()
