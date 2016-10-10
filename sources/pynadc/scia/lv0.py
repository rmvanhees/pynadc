'''
This file is part of pynadc

https://github.com/rmvanhees/pynadc

Methods to read Sciamachy level 0 data products

Copyright (c) 2012-2016 SRON - Netherlands Institute for Space Research 
   All Rights Reserved

License:  Standard 3-clause BSD

'''
from __future__ import print_function
from __future__ import division

import os.path

import traceback

import numpy as np
import h5py

class FormatError(Exception):
    '''
    Class to generate format related errors
    '''
    def __init__(self, msg):
        mytrace = traceback.extract_stack()[-2]
        self.msg = 'Fatal: %s at line %-d in module %s - %s' % mytrace

#-------------------------SECTION CHECK DATA--------------------------------
def size_mds( state_id, orbit, num_in_state, packet_length=0 ):
    '''
    '''
    if num_in_state == 1:
        if os.path.exists( '/SCIA/share/nadc_tools/nadc_clusDef.h5' ):
            db_name = '/SCIA/share/nadc_tools/nadc_clusDef.h5'
        else:
            db_name = '/Users/richardh/SCIA/CKD/nadc_clusDef.h5'
        with h5py.File( db_name, 'r' ) as fid:
            grp = fid['State_%02d' % (state_id)]
            ds = grp['metaTable']
            size_mds.mtbl = ds[orbit]
            ds = grp['clusDef']
            size_mds.clusDef = ds[size_mds.mtbl['indx_clusDef'],:]
            size_mds.clusDef = size_mds.clusDef[size_mds.clusDef['readouts'] > 0]

        max_n_read = size_mds.clusDef['readouts'][:].max()
        size_mds.repeat_read = max_n_read / size_mds.clusDef['readouts']

    sz = 65
    for nch in range(1,9):
        mask = (size_mds.clusDef['chan_id'] == nch) \
            & ((num_in_state % size_mds.repeat_read) == 0)
        if sum(mask) == 0:
            continue

        sz += 16
        coaddf = np.clip(2 * size_mds.clusDef['coaddf'][mask], 0, 3)
        buff = size_mds.clusDef['length'][mask] * coaddf
        buff[(buff % 2) == 1] += 1
        sz += sum(mask) * 10 + sum(buff)

    print( state_id, orbit, num_in_state, sz, packet_length-sz )
    return sz
        
#-------------------------SECTION READ DATA---------------------------------
class File( object ):
    '''
    Class to read Sciamachy level 0 products
    '''
    def __del__(self):
        if hasattr(self, "fp"):
            self.fp.close()

    def __init__(self, flname):
        import io

        self.mph = {}
        self.sph = {}
        self.dsd = None
        self.mds = None

        if flname[-3:] == '.gz':
            print( 'Fatal: can not read compressed file: ', flname )
            raise FormatError('fileCompressed')

        # open file in text mode and read text-headers
        try:
            self.fp = io.open( flname, 'rt', encoding='latin-1' )
        except IOError as e:
            print( "I/O error({0}): {1}".format(e.errno, e.strerror) )
            raise

        # read Main Product Header
        self.__get_mph__()
        # read Specific Product Header
        self.__get_sph__()
        # read Data Set Descriptors
        self.__get_dsd__()

        # check file size
        if self.mph['TOTAL_SIZE'] != os.path.getsize( flname ):
            print( 'Fatal: file %s incomplete' % flname )
            raise FormatError('fileSize')

        # re-open file in binary mode
        self.fp.close()
        self.fp = open( flname, 'rb' )

    def __get_mph__(self):
        '''
        '''
        words = self.fp.readline().split( '=' )
        if words[0] != 'PRODUCT':
            raise FormatError('PRODUCT')
        self.mph['PRODUCT'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'PROC_STAGE':
            raise FormatError('PROC_STAGE')
        self.mph['PROC_STAGE'] = words[1][0:-1]
        words = self.fp.readline().split( '=' )
        if words[0] != 'REF_DOC':
            raise FormatError('REF_DOC')
        self.mph['REF_DOC'] = words[1][1:-2]
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'ACQUISITION_STATION': 
            raise FormatError('ACQUISITION_STATION')
        self.mph['ACQUISITION_STATION'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'PROC_CENTER':
            raise FormatError('PROC_CENTER')
        self.mph['PROC_CENTER'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'PROC_TIME':
            raise FormatError('PROC_TIME')
        self.mph['PROC_TIME'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'SOFTWARE_VER':
            raise FormatError('SOFTWARE_VER')
        self.mph['SOFT_VERSION'] = words[1][1:-2].rstrip()
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'SENSING_START':
            raise FormatError('SENSING_START')
        self.mph['SENSING_START'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'SENSING_STOP':
            raise FormatError('SENSING_STOP')
        self.mph['SENSING_STOP'] = words[1][1:-2]
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'PHASE':
            raise FormatError('PHASE')
        self.mph['PHASE'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'CYCLE':
            raise FormatError('CYCLE')
        self.mph['CYCLE'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'REL_ORBIT':
            raise FormatError('REL_ORBIT')
        self.mph['REL_ORBIT'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'ABS_ORBIT':
            raise FormatError('ABS_ORBIT')
        self.mph['ABS_ORBIT'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'STATE_VECTOR_TIME':
            raise FormatError('STATE_VECTOR_TIME')
        self.mph['STATE_VECTOR_TIME'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'DELTA_UT1':
            raise FormatError('DELTA_UT1')
        self.mph['DELTA_UT1'] = float( words[1][:-4] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'X_POSITION':
            raise FormatError('X_POSITION')
        self.mph['X_POSITION'] = float( words[1][:-4] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'Y_POSITION':
            raise FormatError('Y_POSITION')
        self.mph['Y_POSITION'] = float( words[1][:-4] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'Z_POSITION':
            raise FormatError('Z_POSITION')
        self.mph['Z_POSITION'] = float( words[1][:-4] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'X_VELOCITY':
            raise FormatError('X_VELOCITY')
        self.mph['X_VELOCITY'] = float( words[1][:-6] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'Y_VELOCITY':
            raise FormatError('Y_VELOCITY')
        self.mph['Y_VELOCITY'] = float( words[1][:-6] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'Z_VELOCITY':
            raise FormatError('Z_VELOCITY')
        self.mph['Z_VELOCITY'] = float( words[1][:-6] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'VECTOR_SOURCE':
            raise FormatError('VECTOR_SOURCE')
        self.mph['VECTOR_SOURCE'] = words[1][1:-2]
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'UTC_SBT_TIME':
            raise FormatError('UTC_SBT_TIME')
        self.mph['UTC_SBT_TIME'] = words[1][:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'SAT_BINARY_TIME':
            raise FormatError('SAT_BINARY_TIME')
        self.mph['SAT_BINARY_TIME'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'CLOCK_STEP':
            raise FormatError('CLOCK_STEP')
        self.mph['CLOCK_STEP'] = int( words[1][:-5] )
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'LEAP_UTC':
            raise FormatError('LEAP_UTC')
        self.mph['LEAP_UTC'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'LEAP_SIGN':
            raise FormatError('LEAP_SIGN')
        self.mph['LEAP_SIGN'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'LEAP_ERR':
            raise FormatError('LEAP_ERR')
        self.mph['LEAP_ERR'] = int( words[1] )
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'PRODUCT_ERR':
            raise FormatError('PRODUCT_ERR')
        self.mph['PRODUCT_ERR'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'TOT_SIZE':
            raise FormatError('TOT_SIZE')
        self.mph['TOTAL_SIZE'] = int( words[1][0:21] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'SPH_SIZE':
            raise FormatError('SPH_SIZE')
        self.mph['SPH_SIZE'] = int( words[1][:-8] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_DSD':
            raise FormatError('NUM_DSD')
        self.mph['NUM_DSD'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'DSD_SIZE':
            raise FormatError('DSD_SIZE')
        self.mph['SIZE_DSD'] = int( words[1][:-8] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_DATA_SETS':
            raise FormatError('NUM_DATA_SETS')
        self.mph['NUM_DATA_SETS'] = int( words[1] )
        self.fp.readline()

    def __get_sph__(self):
        '''
        '''
        words = self.fp.readline().split( '=' )
        if words[0] != 'SPH_DESCRIPTOR':
            raise FormatError('SPH_DESCRIPTOR')
        self.sph['SPH_DESCRIPTOR'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'START_LAT':
            raise FormatError('START_LAT')
        self.sph['START_LAT'] = int(words[1][1:-11])
        words = self.fp.readline().split( '=' )
        if words[0] != 'START_LONG':
            raise FormatError('START_LONG')
        self.sph['START_LONG'] = int(words[1][1:-11])
        words = self.fp.readline().split( '=' )
        if words[0] != 'STOP_LAT':
            raise FormatError('STOP_LAT')
        self.sph['STOP_LAT'] = int(words[1][1:-11])
        words = self.fp.readline().split( '=' )
        if words[0] != 'STOP_LONG':
            raise FormatError('STOP_LONG')
        self.sph['STOP_LONG'] = int(words[1][1:-11])
        words = self.fp.readline().split( '=' )
        if words[0] != 'SAT_TRACK':
            raise FormatError('SAT_TRACK')
        self.sph['SAT_TRACK'] = float(words[1][1:-6])
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'ISP_ERRORS_SIGNIFICANT': 
            raise FormatError('ISP_ERRORS_SIGNIFICANT')
        self.sph['ISP_ERRORS_SIGNIFICANT'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'MISSING_ISPS_SIGNIFICANT': 
            raise FormatError('MISSING_ISPS_SIGNIFICANT')
        self.sph['MISSING_ISPS_SIGNIFICANT'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'ISP_DISCARDED_SIGNIFICANT': 
            raise FormatError('ISP_DISCARDED_SIGNIFICANT')
        self.sph['ISP_DISCARDED_SIGNIFICANT'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'RS_SIGNIFICANT':
            raise FormatError('RS_SIGNIFICANT')
        self.sph['RS_SIGNIFICANT'] = int(words[1])
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_ERROR_ISPS':
            raise FormatError('NUM_ERROR_ISPS')
        self.sph['NUM_ERROR_ISPS'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'ERROR_ISPS_THRESH':
            raise FormatError('ERROR_ISPS_THRESH')
        self.sph['ERROR_ISPS_THRESH'] = float(words[1][1:-4])
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_MISSING_ISPS':
            raise FormatError('NUM_MISSING_ISPS')
        self.sph['NUM_MISSING_ISPS'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'MISSING_ISPS_THRESH': 
            raise FormatError('MISSING_ISPS_THRESH')
        self.sph['MISSING_ISPS_THRESH'] = float(words[1][1:-4])
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_DISCARDED_ISPS': 
            raise FormatError('NUM_DISCARDED_ISPS')
        self.sph['NUM_DISCARDED_ISPS'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'DISCARDED_ISPS_THRESH': 
            raise FormatError('DISCARDED_ISPS_THRESH')
        self.sph['DISCARDED_ISPS_THRESH'] = float(words[1][1:-4])
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_RS_ISPS':
            raise FormatError('NUM_RS_ISPS')
        self.sph['NUM_RS_ISPS'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'RS_THRESH':
            raise FormatError('RS_THRESH')
        self.sph['RS_THRESH'] = float(words[1][1:-4])
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'TX_RX_POLAR':
            raise FormatError('TX_RX_POLAR')
        self.sph['TX_RX_POLAR'] = words[1][1:6].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'SWATH':
            raise FormatError('SWATH')
        self.sph['SWATH'] = words[1][1:4].rstrip()
        self.fp.readline()

    def __get_dsd__(self):
        '''
        '''
        self.dsd = {'DS_SIZE': [], 'NUM_DSR': [], 'FILENAME': [], 
                    'DS_NAME': [], 'DS_OFFSET': [], 'DS_TYPE': [], 
                    'DSR_SIZE': []}

        for ni in range(self.mph['NUM_DSD']-1):
            words = self.fp.readline().split( '=' )
            if words[0] != 'DS_NAME':
                raise FormatError('DS_NAME')
            self.dsd['DS_NAME'].append( words[1][1:-2].rstrip() )
            words = self.fp.readline().split( '=' )
            if words[0] != 'DS_TYPE':
                raise FormatError('DS_TYPE')
            self.dsd['DS_TYPE'].append(  words[1].rstrip() )
            words = self.fp.readline().split( '=' )
            if words[0] != 'FILENAME':
                raise FormatError('FILENAME')
            self.dsd['FILENAME'].append( words[1][1:-2].rstrip() )
            words = self.fp.readline().split( '=' )
            if words[0] != 'DS_OFFSET':
                raise FormatError('DS_OFFSET')
            self.dsd['DS_OFFSET'].append( int(words[1][:-8]) )
            words = self.fp.readline().split( '=' )
            if words[0] != 'DS_SIZE':
                raise FormatError('DS_SIZE')
            self.dsd['DS_SIZE'].append( int(words[1][:-8]) )
            words = self.fp.readline().split( '=' )
            if words[0] != 'NUM_DSR':
                raise FormatError('NUM_DSR')
            self.dsd['NUM_DSR'].append( int(words[1]) )
            words = self.fp.readline().split( '=' )
            if words[0] != 'DSR_SIZE':
                raise FormatError('DSR_SIZE')
            self.dsd['DSR_SIZE'].append( int(words[1][:-8]) )
            self.fp.readline()

    # read SCIAMACHY_SOURCE_PACKETS
    def get_mds(self):
        '''
        '''
        lv0hdr_dtype = np.dtype([
            ('isp', {'names':['days', 'secnds', 'musec'], 
                     'formats':['>i4','>u4', '>u4']}),
            ('mjd', {'names':['days', 'secnds', 'musec'],   # FEP
                     'formats':['>i4','>u4', '>u4']}),
            ('isp_length', '>u2'),
            ('num_crc_error', '>u2'),
            ('num_rs_error', '>u2'),
            ('spare', '>u1', (2)),
            ('packet_id', '>u2'),                   # packet header
            ('packet_control', '>u2'),
            ('packet_length', '>u2'),
            ('packet_data_length', '>u2'),          # data field header
            ('category', '>u1'),
            ('state_id', '>u1'),
            ('icu_time', '>u4'),
            ('rdv', '>u2'),
            ('packet_type', '>u1'),
            ('overflow', '>u1'),
        ])
        indx = self.dsd['DS_NAME'].index('SCIAMACHY_SOURCE_PACKETS')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.mds = np.empty( self.dsd['NUM_DSR'][indx], dtype=lv0hdr_dtype )

        state_id = 0
        num_in_state = 0
        for ni in range(self.dsd['NUM_DSR'][indx]):
            self.mds[ni] = np.fromfile( self.fp, dtype=lv0hdr_dtype, count=1 )
            self.fp.seek(self.mds[ni]['isp_length']-11, 1)

            if (self.mds[ni]['packet_type'] >> 4) == 1: 
                if state_id != self.mds[ni]['state_id']:
                    state_id = self.mds[ni]['state_id']
                    num_in_state = 1
                else:
                    num_in_state += 1
                sz = size_mds( state_id, self.mph['ABS_ORBIT'], num_in_state,
                               packet_length=self.mds[ni]['packet_length'] )
                print( ni, sz )
