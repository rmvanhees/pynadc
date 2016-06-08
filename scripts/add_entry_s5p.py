#!/usr/bin/env python

# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.
'''
Defines class ArchiveSirICM to add new entries to S5p Tropomi database
'''
#
from __future__ import print_function
from __future__ import division

import os.path
import sqlite3

import numpy as np
import h5py

#--------------------------------------------------
def cre_sqlite_s5p_db( dbname ):
    '''
    function to define database for S5p ICM database and tables:
     - ICM_SIR_LOCATION
     - ICM_SIR_TBL_ICID
     - ICM_SIR_META
     - ICM_SIR_ANALYSIS
     - ICM_SIR_CALIBRATION
     - ICM_SIR_IRRADIANCE
     - ICM_SIR_RADIANCE
    '''
    con = sqlite3.connect( dbname )
    cur = con.cursor()
    cur.execute( 'PRAGMA foreign_keys = ON' )

    cur.execute( '''create table ICM_SIR_LOCATION (
       pathID           integer  PRIMARY KEY AUTOINCREMENT,
       hostName         text     NOT NULL,
       localPath        text     NOT NULL,
       nfsPath          text     NOT NULL )''' )
    
    cur.execute( '''create table ICM_SIR_TBL_ICID (
       ic_id                     integer  NOT NULL,
       ic_version                smallint NOT NULL,
       ic_set                    smallint NOT NULL,
       ic_idx                    smallint NOT NULL,
       processing_class          smallint NOT NULL,
       msmt_mcp_ft_offset        real     NOT NULL,
       reset_time                real     NOT NULL,
       master_cycle_period_us    integer  NOT NULL,
       coaddition_period_us      integer  NOT NULL,
       exposure_time_us          integer  NOT NULL,
       exposure_period_us        integer  NOT NULL,
       int_hold                  integer  NOT NULL,
       int_delay                 smallint NOT NULL,
       nr_coadditions            tinyint  NOT NULL,
       clipping                  tinyint  NOT NULL,
       PRIMARY KEY (ic_id, ic_version) )''' )

    cur.execute( '''create table ICM_SIR_META (
       metaID           integer  PRIMARY KEY AUTOINCREMENT,
       name             char(81) UNIQUE,
       pathID           integer  REFERENCES ICM_SIR_LOCATION(pathID),
       creator          text     NOT NULL,
       creatorVersion   text     NOT NULL,
       acquisitionDate  datetime NOT NULL default '0000-00-00 00:00:00',
       creationDate     datetime NOT NULL default '0000-00-00 00:00:00',
       receiveDate      datetime NOT NULL default '0000-00-00 00:00:00',
       referenceOrbit   integer  NOT NULL,
       fileSize         integer  NOT NULL,
       num_icm_analys   integer  default 0,
       num_icm_calib    integer  default 0,
       num_icm_irrad    integer  default 0,
       num_icm_rad      integer  default 0 )''' )
    #cur.execute( 'create index dateTimeStartIndex1 on ICM_SIR_META(dateTimeStart)' )
    cur.execute( 'create index receiveDateIndex1 on ICM_SIR_META(receiveDate)' )

    cur.execute( '''create table ICM_SIR_ANALYSIS (
       dckdID           integer  PRIMARY KEY AUTOINCREMENT,
       metaID           integer  REFERENCES ICM_SIR_META(metaID),
       name             text     NOT NULL,
       after_dn2v       boolean  DEFAULT 0,     
       svn_revision     integer  NOT NULL,
       scanline         integer  NOT NULL )''' )
    #cur.execute( 'create index dateTimeStartIndex2 on ICM_SIR_ANALYSIS(dateTimeStart)' )

    cur.execute( '''create table ICM_SIR_CALIBRATION (
       calibID          integer  PRIMARY KEY AUTOINCREMENT,
       metaID           integer  REFERENCES ICM_SIR_META(metaID),
       name             text     NOT NULL,
       after_dn2v       boolean  DEFAULT 0,     
       dateTimeStart    datetime NOT NULL default '0000-00-00T00:00:00',
       ic_id            integer  NOT NULL,
       ic_version       smallint NOT NULL,
       scanline         smallint NOT NULL,
       FOREIGN KEY (ic_id, ic_version) REFERENCES ICM_SIR_TBL_ICID (ic_id, ic_version) ON UPDATE CASCADE )''' )
    cur.execute( 'create index dateTimeStartIndex3 on ICM_SIR_CALIBRATION(dateTimeStart)' )

    cur.execute( '''create table ICM_SIR_IRRADIANCE (
       irradID          integer  PRIMARY KEY AUTOINCREMENT,
       metaID           integer  REFERENCES ICM_SIR_META(metaID),
       name             text     NOT NULL,
       after_dn2v       boolean  DEFAULT 0,     
       dateTimeStart    datetime NOT NULL default '0000-00-00T00:00:00',
       ic_id            integer  NOT NULL,
       ic_version       smallint NOT NULL,
       scanline         smallint NOT NULL,
       FOREIGN KEY (ic_id, ic_version) REFERENCES ICM_SIR_TBL_ICID (ic_id, ic_version) ON UPDATE CASCADE )''' )
    cur.execute( 'create index dateTimeStartIndex4 on ICM_SIR_IRRADIANCE(dateTimeStart)' )

    cur.execute( '''create table ICM_SIR_RADIANCE (
       radID            integer  PRIMARY KEY AUTOINCREMENT,
       metaID           integer  REFERENCES ICM_SIR_META(metaID),
       name             text     NOT NULL,
       after_dn2v       boolean  DEFAULT 0,     
       dateTimeStart    datetime NOT NULL default '0000-00-00T00:00:00',
       ic_id            integer  NOT NULL,
       ic_version       smallint NOT NULL,
       scanline         smallint NOT NULL,
       FOREIGN KEY (ic_id, ic_version) REFERENCES ICM_SIR_TBL_ICID (ic_id, ic_version) ON UPDATE CASCADE )''' )
    cur.execute( 'create index dateTimeStartIndex5 on ICM_SIR_RADIANCE(dateTimeStart)' )

    cur.close()
    con.commit()
    con.close()

#--------------------------------------------------
def fill_tbl_location( dbname ):
    '''
    function to fill table with possible paths to ICM products
    '''
    list_paths = [
        { "host" : 'electra', 
          "path" : '/array/slot6A/TROPOMI', 
          "nfs" : '/TROPOMI/ical_01' },
        { "host" : 'poseidon', 
          "path" : '/array/slot2C/TROPOMI', 
          "nfs" : '/TROPOMI/ical_02' },
    ]
    str_sql = 'insert into ICM_SIR_LOCATION values' \
        '(NULL, \'%(host)s\',\'%(path)s\',\'%(nfs)s\')'

    con = sqlite3.connect( dbname )
    cur = con.cursor()
    for dict_path in list_paths:
        cur.execute( str_sql % dict_path )
    cur.close()
    con.commit()
    con.close()

def delta_time2date( time, delta_time, time_reference=None ):
    '''
    convert S5p time/delta_time to SQL date-time string
    '''
    from datetime import datetime, timedelta

    if time_reference is None:
        return (datetime(2012,1,1,0,0,0)
                + timedelta(seconds=int(time))
                + timedelta(milliseconds=int(delta_time))).isoformat()[0:19]

#-------------------------
def ic_dtype():
    '''
    Summary of compound "instrument_settings" in the group INSTRUMENT
    It contains a selection of available fields
    The fields in this dtype need to be equal to the fields in the compound "instrument_settings"
    '''
    return np.dtype( [('ic_id', np.int32),
                      ('ic_version', np.int16),
                      ('ic_set', np.int16),
                      ('ic_idx', np.int16),
                      ('processing_class', np.int16),
                      ('msmt_mcp_ft_offset', np.float32),
                      ('reset_time', np.float32),
                      ('master_cycle_period_us', np.int32),
                      ('coaddition_period_us', np.int32),
                      ('exposure_time_us', np.int32),
                      ('exposure_period_us', np.int32),
                      ('int_hold', np.uint32),
                      ('int_delay', np.uint16),
                      ('nr_coadditions', np.int16),
                      ('clipping', np.uint8)] )

def grp_dtype():
    '''
    Defines info on each calibration state which needs to be stored in the database
    '''
    return np.dtype( [('metaID', np.int32),
                      ('name', np.str_, 64),
                      ('after_dn2v', np.uint16),
                      ('dateTimeStart', np.str_, 20),
                      ('instrumentConfig', ic_dtype()),
                      ('ic_id', np.int32),
                      ('ic_version', np.int16),
                      ('svn_revision', np.int32),
                      ('scanline', np.int16)] )

#--------------------------------------------------
class ArchiveSirICM( object ):
    '''
    Define methods to create an inventory of the summary data present in an ICM_CA_SIR product 
    '''
    def __init__( self, db_name='./sron_s5p_icm.db' ):
        '''
        '''
        self.dbname = db_name
        
        if not os.path.isfile( db_name ):
            cre_sqlite_s5p_db( db_name )
            fill_tbl_location( db_name )
        self.meta   = {}
        self.analys = np.array([])
        self.calib  = np.array([])
        self.irrad  = np.array([])
        self.rad    = np.array([])

    def rd_meta( self, flname, verbose=False ):
        '''
        Collect information for table ICM_SIR_META.
        '''
        from time import gmtime, strftime

        self.meta['fileName'] = os.path.basename( flname )
        self.meta['filePath'] = os.path.dirname( flname )
        self.meta['acquisitionDate'] = \
                                strftime("%F %T", gmtime(os.path.getmtime( flname )))
        self.meta['receiveDate'] = \
                                strftime("%F %T", gmtime(os.path.getctime( flname )))
        self.meta['fileSize'] = os.path.getsize( flname )

        with h5py.File( flname, mode='r' ) as fid:
            self.meta['referenceOrbit'] = fid.attrs['reference_orbit'][0]
            grp = fid['/METADATA/ESA_METADATA/earth_explorer_header/fixed_header']
            dset = grp['source']
            self.meta['creationDate'] = (dset.attrs['Creation_Date'].split(b'=')[1]).decode('ascii')
            self.meta['creator'] = (dset.attrs['Creator']).decode('ascii')
            self.meta['creatorVersion'] = (dset.attrs['Creator_Version']).decode('ascii')

        if verbose:
            print( self.meta )

    def rd_analys( self, flname, verbose=False ):
        '''
        Collect information for table ICM_SIR_ANALYSIS.
        Expected analysis results are:
         - ANALOG_OFFSET_SWIR
         - DPQF_MAP
         - LONG_TERM_SWIR
         - NOISE
        '''
        with h5py.File( flname, mode='r' ) as fid:
            if '/BAND7_ANALYSIS' in fid:
                grp7 = fid['/BAND7_ANALYSIS']
            else:
                grp7 = None

            if '/BAND8_ANALYSIS' in fid:
                grp8 = fid['/BAND8_ANALYSIS']
            else:
                grp8 = None

            ## Analysis data of both band should be present:
            if grp7 is None and grp8 is None:
                return
            
            if grp7 is None or grp8 is None:
                print( '*** Fatal analysis results of band 7 or 8 are not present' )
                return

            if len(grp7) != len(grp8):
                print( '*** Fatal analysis results of band 7 or 8 is incomplete' )
                return
            
            ii = 0
            buff = np.zeros( len(grp7), dtype=grp_dtype() )
            if 'ANALOG_OFFSET_SWIR' in grp7:
                buff[ii]['name'] = 'ANALOG_OFFSET_SWIR'
                dset = grp7['ANALOG_OFFSET_SWIR']['analog_offset_swir']
                buff[ii]['svn_revision'] = dset.attrs['svn_revision']
                buff[ii]['scanline'] = 1
                ii += 1
            if 'DPQF_MAP' in grp7:
                buff[ii]['name'] = 'DPQF_MAP'
                dset = grp7['DPQF_MAP']['dpqf_map']
                buff[ii]['svn_revision'] = dset.attrs['svn_revision']
                buff[ii]['scanline'] = 1
                ii += 1
            if 'LONG_TERM_SWIR' in grp7:
                buff[ii]['name'] = 'LONG_TERM_SWIR'
                dset = grp7['LONG_TERM_SWIR']['long_term_swir']
                buff[ii]['svn_revision'] = dset.attrs['svn_revision']
                buff[ii]['scanline'] = 1
                ii += 1
            if 'NOISE' in grp7:
                buff[ii]['name'] = 'NOISE'
                dset = grp7['NOISE']['noise']
                buff[ii]['svn_revision'] = dset.attrs['svn_revision']
                buff[ii]['scanline'] = grp7['NOISE']['scanline'].size
                ii += 1
            if ii < len(grp7):
                print( '*** Warning analysis results contains unexpected group' )

        self.analys = buff[0:ii]
        if verbose:
            print( self.analys )

    def rd_calib( self, flname, verbose=True ):
        '''
        Collect information for table ICM_SIR_CALIBRATION.
        '''
        with h5py.File( flname, mode='r' ) as fid:
            if '/BAND7_CALIBRATION' in fid:
                grp7 = fid['/BAND7_CALIBRATION']
            else:
                grp7 = None

            if '/BAND8_CALIBRATION' in fid:
                grp8 = fid['/BAND8_CALIBRATION']
            else:
                grp8 = None

            ## Calibration data of both band should be present:
            if grp7 is None and grp8 is None:
                return
            
            if grp7 is None or grp8 is None:
                print( '*** Fatal calibration results of band 7 or 8 are not present' )
                return

            if len(grp7) != len(grp8):
                print( '*** Fatal calibration results of band 7 or 8 is incomplete' )
                return

            self.calib = np.zeros( len(grp7), dtype=grp_dtype() )
            ii = 0
            for key in grp7:
                full_name = np.string_(key).decode('ascii')
                if full_name.endswith( 'AFTER_DN2V'):
                    self.calib[ii]['name'] = full_name[0:-11]
                    self.calib[ii]['after_dn2v'] = True
                else:
                    self.calib[ii]['name'] = full_name
                    self.calib[ii]['after_dn2v'] = False
                self.calib[ii]['scanline'] = grp7[key]['scanline'].size

                sgrp = grp7[key]['INSTRUMENT']
                dset = sgrp['instrument_settings']
                for parm in self.calib[ii]['instrumentConfig'].dtype.names:
                    self.calib[ii]['instrumentConfig'][parm] = dset[0][parm]
                dset = sgrp['instrument_configuration']
                self.calib[ii]['ic_id'] = dset[0,0]['ic_id']
                self.calib[ii]['ic_version'] = dset[0,0]['ic_version']
                
                sgrp = grp7[key]['OBSERVATIONS']
                self.calib[ii]['dateTimeStart'] = delta_time2date( sgrp['time'][0],
                                                                   sgrp['delta_time'][0,0] )
                ii += 1

        if verbose:
            print( self.calib )

    def rd_irrad( self, flname, verbose=True ):
        '''
        Collect information for table ICM_SIR_IRRADIANCE.
        '''
        with h5py.File( flname, mode='r' ) as fid:
            if '/BAND7_IRRADIANCE' in fid:
                grp7 = fid['/BAND7_IRRADIANCE']
            else:
                grp7 = None

            if '/BAND8_IRRADIANCE' in fid:
                grp8 = fid['/BAND8_IRRADIANCE']
            else:
                grp8 = None

            ## Irradiance data of both band should be present:
            if grp7 is None and grp8 is None:
                return
            
            if grp7 is None or grp8 is None:
                print( '*** Fatal irradiance results of band 7 or 8 are not present' )
                return

            if len(grp7) != len(grp8):
                print( '*** Fatal irradiance results of band 7 or 8 is incomplete' )
                return

            self.irrad = np.zeros( len(grp7), dtype=grp_dtype() )
            ii = 0
            for key in grp7:
                self.irrad[ii]['name'] = np.string_(key).decode('ascii')
                self.irrad[ii]['scanline'] = grp7[key]['scanline'].size

                sgrp = grp7[key]['INSTRUMENT']
                dset = sgrp['instrument_settings']
                for parm in self.irrad[ii]['instrumentConfig'].dtype.names:
                    self.irrad[ii]['instrumentConfig'][parm] = dset[0][parm]
                dset = sgrp['instrument_configuration']
                self.irrad[ii]['ic_id'] = dset[0,0]['ic_id']
                self.irrad[ii]['ic_version'] = dset[0,0]['ic_version']
                
                sgrp = grp7[key]['OBSERVATIONS']
                self.irrad[ii]['dateTimeStart'] = delta_time2date( sgrp['time'][0],
                                                                   sgrp['delta_time'][0,0] )
                ii += 1

        if verbose:
            print( self.irrad )

    def rd_rad( self, flname, verbose=True ):
        '''
        Collect information for table ICM_SIR_RADIANCE.
        '''
        with h5py.File( flname, mode='r' ) as fid:
            if '/BAND7_RADIANCE' in fid:
                grp7 = fid['/BAND7_RADIANCE']
            else:
                grp7 = None

            if '/BAND8_RADIANCE' in fid:
                grp8 = fid['/BAND8_RADIANCE']
            else:
                grp8 = None

            ## Radiance data of both band should be present:
            if grp7 is None and grp8 is None:
                return
            
            if grp7 is None or grp8 is None:
                print( '*** Fatal radiance results of band 7 or 8 are not present' )
                return

            if len(grp7) != len(grp8):
                print( '*** Fatal radiance results of band 7 or 8 is incomplete' )
                return

            self.rad = np.zeros( len(grp7), dtype=grp_dtype() )
            ii = 0
            for key in grp7:
                self.rad[ii]['name'] = np.string_(key).decode('ascii')
                self.rad[ii]['scanline'] = grp7[key]['scanline'].size

                sgrp = grp7[key]['INSTRUMENT']
                dset = sgrp['instrument_settings']
                for parm in self.rad[ii]['instrumentConfig'].dtype.names:
                    self.rad[ii]['instrumentConfig'][parm] = dset[0][parm]
                dset = sgrp['instrument_configuration']
                self.rad[ii]['ic_id'] = dset[0,0]['ic_id']
                self.rad[ii]['ic_version'] = dset[0,0]['ic_version']
                
                sgrp = grp7[key]['OBSERVATIONS']
                self.rad[ii]['dateTimeStart'] = delta_time2date( sgrp['time'][0],
                                                                 sgrp['delta_time'][0,0] )
                ii += 1

        if verbose:
            print( self.rad )

    def check_entry( self, flname, verbose=False ):
        '''
        check if product is already stored in database
        '''
        name = os.path.basename( flname )
        query_str = 'select metaID from ICM_SIR_META where name=\'{}\''.format( name )
        if verbose:
            print( query_str )
    
        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( query_str )
        row = cur.fetchone()
        cur.close()
        con.close()
        if row == None: 
            return False
        else:
            return True

    def remove_entry( self, flname, verbose=False ):
        '''
        remove entry from database
        '''
        name = os.path.basename( flname )
        query_str = 'select metaID from ICM_SIR_META where name=\'{}\''.format( name )
        remove_str = 'delete from ICM_SIR_META where caiID={}'
        if verbose:
            print( query_str )
    
        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( 'PRAGMA foreign_keys = ON' )
        cur.execute( query_str )
        row = cur.fetchone()
        if row is not None:
            cur.execute( remove_str.format(row[0]) )
        cur.close()
        con.commit()
        con.close()

    def add_entry( self, flname, verbose=True ):
        '''
        add new product to database
        '''
        name = os.path.basename( flname )
        query_id_str = 'select metaID from ICM_SIR_META where name=\'{}\''.format( name )
        
        query_icid_str = 'select ic_id from ICM_SIR_TBL_ICID where ic_id={} and ic_version={}'

        str_sql_meta = 'insert into ICM_SIR_META values' \
                       '(NULL,\'%(fileName)s\',%(pathID)d'\
                       ',\'%(creator)s\',\'%(creatorVersion)s\''\
                       ',\'%(acquisitionDate)s\',\'%(creationDate)s\',\'%(receiveDate)s\''\
                       ',%(referenceOrbit)d,%(fileSize)d,0,0,0,0)'

        str_sql_analys = 'insert into ICM_SIR_ANALYSIS values' \
                       '(NULL,%(metaID)d,\'%(name)s\',0,%(svn_revision)d,%(scanline)d)'

        str_sql_calib = 'insert into ICM_SIR_CALIBRATION values' \
                       '(NULL,%(metaID)d,\'%(name)s\',%(after_dn2v)d' \
                       ',\'%(dateTimeStart)s\''\
                       ',%(ic_id)d,%(ic_version)d,%(scanline)d)'

        str_sql_irrad = 'insert into ICM_SIR_IRRADIANCE values' \
                       '(NULL,%(metaID)d,\'%(name)s\',%(after_dn2v)d' \
                       ',\'%(dateTimeStart)s\''\
                       ',%(ic_id)d,%(ic_version)d,%(scanline)d)'

        str_sql_rad  = 'insert into ICM_SIR_RADIANCE values' \
                       '(NULL,%(metaID)d,\'%(name)s\',%(after_dn2v)d' \
                       ',\'%(dateTimeStart)s\''\
                       ',%(ic_id)d,%(ic_version)d,%(scanline)d)'

        str_sql_icid = 'insert into ICM_SIR_TBL_ICID values' \
                       '(%(ic_id)d,%(ic_version)d,%(ic_set)d,%(ic_idx)d'\
                       ',%(processing_class)d,%(msmt_mcp_ft_offset)f'\
                       ',%(reset_time)f,%(master_cycle_period_us)f'\
                       ',%(coaddition_period_us)f,%(exposure_time_us)f'\
                       ',%(exposure_period_us)f,%(int_hold)d,%(int_delay)d'\
                       ',%(nr_coadditions)d,%(clipping)d)'

        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( 'PRAGMA foreign_keys = ON' )
        cur.execute( 'BEGIN IMMEDIATE' )

        ## obtain pathID from table ICM_SIR_LOCATION
        #cur.execute( str_path_sql % (rootPath,rootPath) )
        #row = cur.fetchone()
        #if row is not None:
        #    self.meta['pathID'] = row[0]
        #else:
        #    self.meta['pathID'] = 0
        self.meta['pathID'] = 1            # ToDo: fix this

        ## add entry to meta-table
        if verbose:
            print( str_sql_meta % self.meta )
        cur.execute( str_sql_meta % self.meta )

        ## obtain metaID for foreign keys
        cur.execute( query_id_str )
        row = cur.fetchone()
        if row is None:
            cur.close()
            con.rollback()
            con.close()
            return
        meta_id = row[0]

        ## add entries to table ICM_SIR_ANALYSIS
        if self.analys[:].size > 0:
            str_sql = 'update ICM_SIR_META set num_icm_analys={}'.format(self.analys[:].size)
            cur.execute( str_sql )
            for ii in range(self.analys[:].size):
                self.analys[ii]['metaID'] = meta_id
                if verbose:
                    print( str_sql_analys % self.analys[ii] )
                cur.execute( str_sql_analys % self.analys[ii] )

        ## add entries to table ICM_SIR_CALIBRATION
        if self.calib[:].size > 0:
            str_sql = 'update ICM_SIR_META set num_icm_calib={}'.format(self.calib[:].size)
            cur.execute( str_sql )
            for ii in range(self.calib[:].size):
                cur.execute( query_icid_str.format(self.calib[ii]['ic_id'],self.calib[ii]['ic_version']) )
                row = cur.fetchone()
                if row is None:
                    cur.execute( str_sql_icid % self.calib[ii]['instrumentConfig'] )

                self.calib[ii]['metaID'] = meta_id
                if verbose:
                    print( str_sql_calib % self.calib[ii] )
                cur.execute( str_sql_calib % self.calib[ii] )
            
        ## add entries to table ICM_SIR_IRRADIANCE
        if self.irrad[:].size > 0:
            str_sql = 'update ICM_SIR_META set num_icm_irrad={}'.format(self.irrad[:].size)
            cur.execute( str_sql )
            for ii in range(self.irrad[:].size):
                cur.execute( query_icid_str.format(self.irrad[ii]['ic_id'],self.irrad[ii]['ic_version']) )
                row = cur.fetchone()
                if row is None:
                    cur.execute( str_sql_icid % self.irrad[ii]['instrumentConfig'] )

                self.irrad[ii]['metaID'] = meta_id
                if verbose:
                    print( str_sql_irrad % self.irrad[ii] )
                cur.execute( str_sql_irrad % self.irrad[ii] )
            
        ## add entries to table ICM_SIR_RADIANCE
        if self.rad[:].size > 0:
            str_sql = 'update ICM_SIR_META set num_icm_rad={}'.format(self.rad[:].size)
            cur.execute( str_sql )
            for ii in range(self.rad[:].size):
                cur.execute( query_icid_str.format(self.rad[ii]['ic_id'],self.rad[ii]['ic_version']) )
                row = cur.fetchone()
                if row is None:
                    cur.execute( str_sql_icid % self.rad[ii]['instrumentConfig'] )

                self.rad[ii]['metaID'] = meta_id
                if verbose:
                    print( str_sql_rad % self.rad[ii] )
                cur.execute( str_sql_rad % self.rad[ii] )
            
        cur.close()
        con.commit()
        con.close()
        
#-------------------------
def main( dbname, input_file, debug=False ):
    '''
    Store information of product in database
    '''
    db = ArchiveSirICM( dbname )
    if not db.check_entry( input_file, verbose=debug ):
        db.rd_meta( input_file, verbose=debug )
        db.rd_analys( input_file, verbose=debug )
        db.rd_calib( input_file, verbose=debug )
        db.rd_irrad( input_file, verbose=debug )
        db.rd_rad( input_file, verbose=debug )
        db.add_entry( input_file, verbose=debug )
    
#----- main code --------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument( '--debug', action='store_true', default=False,
                         help='show what will be done, but do nothing' )
    parser.add_argument( '--remove', action='store_true', default=False,
                         help='remove SQL data of INPUT_FILE from database' )
    parser.add_argument( '--replace', action='store_true', default=False,
                         help='replace SQL data of INPUT_FILE in database' )
    parser.add_argument( '--dbname', dest='dbname',
                         default=np.string_('sron_s5p_icm.db').decode('ascii'), 
                         help='name of S5P-ICM/SQLite database' )
    parser.add_argument( 'input_file', nargs='?', type=str,
                         help='read from INPUT_FILE' )
    args = parser.parse_args()

    if not h5py.h5f.is_hdf5( np.string_(args.input_file) ):
        print( 'Info: %s is not a HDF5/S5P-ICM product' % args.input_file )
        sys.exit(0)

    main( args.dbname, args.input_file, debug=args.debug )
