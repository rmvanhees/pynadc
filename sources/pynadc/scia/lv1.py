# (c) SRON - Netherlands Institute for Space Research (2014).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

"""
Methods to read Sciamachy level 1b data products
"""

from __future__ import print_function
from __future__ import division

import os.path

import traceback
import warnings

import numpy as np

class fmtError(Exception):
    def __init__(self, msg):
        mytrace = traceback.extract_stack()[-2]
        self.msg = 'Fatal: %s at line %-d in module %s - %s' % mytrace

#-------------------------SECTION READ DATA---------------------------------
class File:
    def __del__(self):
        if hasattr(self, "fp"): self.fp.close()

    def __init__(self, flname):
        import io

        if flname[-3:] == '.gz':
            print( 'Fatal: can not read compressed file: ', flname )
            raise fmtError('fileCompressed')

        # open file in text mode and read text-headers
        try:
            self.fp = io.open( flname, 'rt', encoding='latin-1' )
        except IOError as e:
            print( "I/O error({0}): {1}".format(e.errno, e.strerror) )
            raise

        # read Main Product Header
        self._getMPH()
        # read Specific Product Header
        self._getSPH()
        # read Data Set Descriptors
        self._getDSD()

        # check file size
        if self.mph['TOTAL_SIZE'] != os.path.getsize( flname ):
            print( 'Fatal: file %s incomplete' % flname )
            raise fmtError('fileSize')

        # re-open file in binary mode
        self.fp.close()
        self.fp = open( flname, 'rb' )

    def _getMPH(self):
        self.mph = {}
        words = self.fp.readline().split( '=' )
        if words[0] != 'PRODUCT': raise fmtError('PRODUCT')
        self.mph['PRODUCT'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'PROC_STAGE': raise fmtError('PROC_STAGE')
        self.mph['PROC_STAGE'] = words[1][0:-1]
        words = self.fp.readline().split( '=' )
        if words[0] != 'REF_DOC': raise fmtError('REF_DOC')
        self.mph['REF_DOC'] = words[1][1:-2]
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'ACQUISITION_STATION': 
            raise fmtError('ACQUISITION_STATION')
        self.mph['ACQUISITION_STATION'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'PROC_CENTER': raise fmtError('PROC_CENTER')
        self.mph['PROC_CENTER'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'PROC_TIME': raise fmtError('PROC_TIME')
        self.mph['PROC_TIME'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'SOFTWARE_VER': raise fmtError('SOFTWARE_VER')
        self.mph['SOFT_VERSION'] = words[1][1:-2].rstrip()
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'SENSING_START': raise fmtError('SENSING_START')
        self.mph['SENSING_START'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'SENSING_STOP': raise fmtError('SENSING_STOP')
        self.mph['SENSING_STOP'] = words[1][1:-2]
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'PHASE': raise fmtError('PHASE')
        self.mph['PHASE'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'CYCLE': raise fmtError('CYCLE')
        self.mph['CYCLE'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'REL_ORBIT': raise fmtError('REL_ORBIT')
        self.mph['REL_ORBIT'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'ABS_ORBIT': raise fmtError('ABS_ORBIT')
        self.mph['ABS_ORBIT'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'STATE_VECTOR_TIME': raise fmtError('STATE_VECTOR_TIME')
        self.mph['STATE_VECTOR_TIME'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'DELTA_UT1': raise fmtError('DELTA_UT1')
        self.mph['DELTA_UT1'] = float( words[1][:-4] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'X_POSITION': raise fmtError('X_POSITION')
        self.mph['X_POSITION'] = float( words[1][:-4] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'Y_POSITION': raise fmtError('Y_POSITION')
        self.mph['Y_POSITION'] = float( words[1][:-4] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'Z_POSITION': raise fmtError('Z_POSITION')
        self.mph['Z_POSITION'] = float( words[1][:-4] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'X_VELOCITY': raise fmtError('X_VELOCITY')
        self.mph['X_VELOCITY'] = float( words[1][:-6] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'Y_VELOCITY': raise fmtError('Y_VELOCITY')
        self.mph['Y_VELOCITY'] = float( words[1][:-6] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'Z_VELOCITY': raise fmtError('Z_VELOCITY')
        self.mph['Z_VELOCITY'] = float( words[1][:-6] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'VECTOR_SOURCE': raise fmtError('VECTOR_SOURCE')
        self.mph['VECTOR_SOURCE'] = words[1][1:-2]
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'UTC_SBT_TIME': raise fmtError('UTC_SBT_TIME')
        self.mph['UTC_SBT_TIME'] = words[1][:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'SAT_BINARY_TIME': raise fmtError('SAT_BINARY_TIME')
        self.mph['SAT_BINARY_TIME'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'CLOCK_STEP': raise fmtError('CLOCK_STEP')
        self.mph['CLOCK_STEP'] = int( words[1][:-5] )
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'LEAP_UTC': raise fmtError('LEAP_UTC')
        self.mph['LEAP_UTC'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'LEAP_SIGN': raise fmtError('LEAP_SIGN')
        self.mph['LEAP_SIGN'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'LEAP_ERR': raise fmtError('LEAP_ERR')
        self.mph['LEAP_ERR'] = int( words[1] )
        self.fp.readline()
        words = self.fp.readline().split( '=' )
        if words[0] != 'PRODUCT_ERR': raise fmtError('PRODUCT_ERR')
        self.mph['PRODUCT_ERR'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'TOT_SIZE': raise fmtError('TOT_SIZE')
        self.mph['TOTAL_SIZE'] = int( words[1][0:21] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'SPH_SIZE': raise fmtError('SPH_SIZE')
        self.mph['SPH_SIZE'] = int( words[1][:-8] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_DSD': raise fmtError('NUM_DSD')
        self.mph['NUM_DSD'] = int( words[1] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'DSD_SIZE': raise fmtError('DSD_SIZE')
        self.mph['SIZE_DSD'] = int( words[1][:-8] )
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_DATA_SETS': raise fmtError('NUM_DATA_SETS')
        self.mph['NUM_DATA_SETS'] = int( words[1] )
        self.fp.readline()

    def _getSPH(self):
        self.sph = {}
        words = self.fp.readline().split( '=' )
        if words[0] != 'SPH_DESCRIPTOR': raise fmtError('SPH_DESCRIPTOR')
        self.sph['SPH_DESCRIPTOR'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'STRIPLINE_CONTINUITY_INDICATOR': 
            raise fmtError('STRIPLINE_CONTINUITY_INDICATOR')
        self.sph['STRIPLINE_CONTINUITY_INDICATOR'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'SLICE_POSITION': raise fmtError('SLICE_POSITION')
        self.sph['SLICE_POSITION'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'NUM_SLICES': raise fmtError('NUM_SLICES')
        self.sph['NUM_SLICES'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'START_TIME': raise fmtError('START_TIME')
        self.sph['START_TIME'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'STOP_TIME': raise fmtError('STOP_TIME')
        self.sph['STOP_TIME'] = words[1][1:-2]
        words = self.fp.readline().split( '=' )
        if words[0] != 'START_LAT': raise fmtError('START_LAT')
        self.sph['START_LAT'] = int(words[1][1:-11])
        words = self.fp.readline().split( '=' )
        if words[0] != 'START_LONG': raise fmtError('START_LONG')
        self.sph['START_LONG'] = int(words[1][1:-11])
        words = self.fp.readline().split( '=' )
        if words[0] != 'STOP_LAT': raise fmtError('STOP_LAT')
        self.sph['STOP_LAT'] = int(words[1][1:-11])
        words = self.fp.readline().split( '=' )
        if words[0] != 'STOP_LONG': raise fmtError('STOP_LONG')
        self.sph['STOP_LONG'] = int(words[1][1:-11])
        words = self.fp.readline().split( '=' )
        if words[0] == 'INIT_VERSION':
            if words[1].find('DECONT'):
                self.sph['INIT_VERSION'] = words[1][:-6].strip()
                self.sph['DECONT'] = words[2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'KEY_DATA_VERSION': raise fmtError('KEY_DATA_VERSION')
        self.sph['KEY_DATA_VERSION'] = words[1][1:-2].strip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'M_FACTOR_VERSION': raise fmtError('M_FACTOR_VERSION')
        self.sph['M_FACTOR_VERSION'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'SPECTRAL_CAL_CHECK_SUM': 
            raise fmtError('SPECTRAL_CAL_CHECK_SUM')
        self.sph['SPECTRAL_CAL_CHECK_SUM'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'SATURATED_PIXEL': raise fmtError('SATURATED_PIXEL')
        self.sph['SATURATED_PIXEL'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'DEAD_PIXEL': raise fmtError('DEAD_PIXEL')
        self.sph['DEAD_PIXEL'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'DARK_CHECK_SUM': raise fmtError('DARK_CHECK_SUM')
        self.sph['DARK_CHECK_SUM'] = words[1][1:-2].rstrip()
        words = self.fp.readline().split( '=' )
        if words[0] != 'NO_OF_NADIR_STATES': 
            raise fmtError('NO_OF_NADIR_STATES')
        self.sph['NO_OF_NADIR_STATES'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'NO_OF_LIMB_STATES': 
            raise fmtError('NO_OF_LIMB_STATES')
        self.sph['NO_OF_LIMB_STATES'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'NO_OF_OCCULTATION_STATES': 
            raise fmtError('NO_OF_OCCULTATION_STATES')
        self.sph['NO_OF_OCCULTATION_STATES'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'NO_OF_MONI_STATES': raise fmtError('NO_OF_MONI_STATES')
        self.sph['NO_OF_MONI_STATES'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'NO_OF_NOPROC_STATES': 
            raise fmtError('NO_OF_NOPROC_STATES')
        self.sph['NO_OF_NOPROC_STATES'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'COMP_DARK_STATES': raise fmtError('COMP_DARK_STATES')
        self.sph['COMP_DARK_STATES'] = int(words[1])
        words = self.fp.readline().split( '=' )
        if words[0] != 'INCOMP_DARK_STATES': 
            raise fmtError('INCOMP_DARK_STATES')
        self.sph['INCOMP_DARK_STATES'] = int(words[1])
        self.fp.readline()

    def _getDSD(self):
        self.dsd = {'DS_SIZE': [], 'NUM_DSR': [], 'FILENAME': [], 
                    'DS_NAME': [], 'DS_OFFSET': [], 'DS_TYPE': [], 
                    'DSR_SIZE': []}

        for ni in range(self.mph['NUM_DSD']-1):
            words = self.fp.readline().split( '=' )
            if words[0] != 'DS_NAME': raise fmtError('DS_NAME')
            self.dsd['DS_NAME'].append( words[1][1:-2].rstrip() )
            words = self.fp.readline().split( '=' )
            if words[0] != 'DS_TYPE': raise fmtError('DS_TYPE')
            self.dsd['DS_TYPE'].append(  words[1].rstrip() )
            words = self.fp.readline().split( '=' )
            if words[0] != 'FILENAME': raise fmtError('FILENAME')
            self.dsd['FILENAME'].append( words[1][1:-2].rstrip() )
            words = self.fp.readline().split( '=' )
            if words[0] != 'DS_OFFSET': raise fmtError('DS_OFFSET')
            self.dsd['DS_OFFSET'].append( int(words[1][:-8]) )
            words = self.fp.readline().split( '=' )
            if words[0] != 'DS_SIZE': raise fmtError('DS_SIZE')
            self.dsd['DS_SIZE'].append( int(words[1][:-8]) )
            words = self.fp.readline().split( '=' )
            if words[0] != 'NUM_DSR': raise fmtError('NUM_DSR')
            self.dsd['NUM_DSR'].append( int(words[1]) )
            words = self.fp.readline().split( '=' )
            if words[0] != 'DSR_SIZE': raise fmtError('DSR_SIZE')
            self.dsd['DSR_SIZE'].append( int(words[1][:-8]) )
            self.fp.readline()

    # read Summary of Quality Flags per State (SQADS)
    def getSQADS(self):
        record_dtype = np.dtype([
            ('mjd', {'names':['days', 'secnds', 'musec'], 
                     'formats':['>i4','>u4', '>u4']}),
            ('flag_attached', 'i1'),
            ('mean_wv_diff', '>f4', (8)),
            ('sdev_wv_diff', '>f4', (8)),
            ('spare1', '>u2'),
            ('mean_lc_diff', '>f4', (15)),
            ('flag_sunglint', 'u1'),
            ('flag_rainbow', 'u1'),
            ('flag_saa', 'u1'),
            ('num_hot', '>u2', (15)),
            ('spare', '>u1', (10))
        ])
        indx = self.dsd['DS_NAME'].index('SUMMARY_QUALITY')
        self.fp.seek( self.dsd['DS_OFFSET'][indx] )
        self.sqads = np.fromfile( self.fp, dtype=record_dtype, 
                                  count=self.dsd['NUM_DSR'][indx] )

    # read Geolocation of the States (LADS)
    def getLADS(self):
        record_dtype = np.dtype([
            ('mjd', {'names':['days', 'secnds', 'musec'], 
                     'formats':['>i4','>u4', '>u4']}),
            ('flag_attached', 'i1'),
            ('corners', {'names': ['lat', 'lon'],
                         'formats': ['>i4', '>i4']}, (4))            
        ])
        indx = self.dsd['DS_NAME'].index('GEOLOCATION')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.lads = np.fromfile( self.fp, dtype=record_dtype, 
                                  count=self.dsd['NUM_DSR'][indx] )

    # read Static Instrument Parameters (SIP)
    def getSIP(self):
        record_dtype = np.dtype([
            ('n_lc_min', 'u1'),
            ('ds_n_phase', 'u1'),
            ('ds_phase_boundaries', '>f4', (13)),
            ('lc_stray_index', '>f4', (2)),
            ('lc_harm_order', 'u1'),
            ('ds_poly_order', 'u1'),
            ('do_var_lc_cha', '4a', (3)),
            ('do_stray_lc_cha', '4a', (8)),
            ('do_var_lc_pmd', '4a', (2)),
            ('do_stray_lc_pmd', '4a', (7)),
            ('electron_bu', '>f4', (8)),
            ('ppg_error', '>f4'),
            ('stray_error', '>f4'),
            ('sp_n_phases', 'u1'),
            ('sp_phase_boundaries', '>f4', (13)),
            ('startpix_6', '>u2'),
            ('startpix_8', '>u2'),
            ('h_toa', '>f4'),
            ('lambda_end_gdf', '>f4'),
            ('do_pol_point', 'c', (12)),
            ('sat_level', '>u2', (8)),
            ('pmd_saturation_limit', '>u2'),
            ('do_use_limb_dark', 'c'),
            ('do_pixelwise', 'c', (8)),
            ('alpha0_asm', '>f4'),
            ('alpha0_ems', '>f4'),
            ('do_fraunhofer', '5a', (8)),
            ('do_etalon', '3a', (8)),
            ('do_IB_SD_ETN', 'c', (7)),
            ('do_IB_OC_ETN', 'c', (7)),
            ('level_2_SMR', 'u1', (8))
        ])
        indx = self.dsd['DS_NAME'].index('INSTRUMENT_PARAMS')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.sip = np.fromfile( self.fp, dtype=record_dtype, 
                                count=self.dsd['NUM_DSR'][indx] )

    # read Leakage Current Parameters (CLCP)
    def getCLCP(self):
        record_dtype = np.dtype([
            ('fpn', '>f4', (8192)),
            ('fpn_error', '>f4', (8192)),
            ('lc', '>f4', (8192)),
            ('lc_error', '>f4', (8192)),
            ('pmd_dark', '>f4', (14)),
            ('pmd_dark_error', '>f4', (14)),
            ('mean_noise', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('LEAKAGE_CONSTANT')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.clcp = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read Leakage Current Parameters (VLCP)
    def getVLCP(self):
        record_dtype = np.dtype([
            ('orbit_phase', '>f4'),
            ('temperatures', '>f4', (10)),
            ('var_lc', '>f4', (3072)),
            ('var_lc_error', '>f4', (3072)),
            ('stray', '>f4', (8192)),
            ('stray_error', '>f4', (8192)),
            ('pmd_stray', '>f4', (7)),
            ('pmd_stray_error', '>f4', (7)),
            ('pmd_var_lc', '>f4', (2)),
            ('pmd_var_lc_error', '>f4', (2))
        ])
        indx = self.dsd['DS_NAME'].index('LEAKAGE_VARIABLE')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.vlcp = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read PPG/Etalon Parameters (PPG)
    def getPPG(self):
        record_dtype = np.dtype([
            ('ppg_fact', '>f4', (8192)),
            ('etalon_fact', '>f4', (8192)),
            ('etalon_resid', '>f4', (8192)),
            ('wls_deg_fact', '>f4', (8192)),
            ('bdpm', 'u1', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('PPG_ETALON')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.ppg = np.fromfile( self.fp, dtype=record_dtype, 
                                count=self.dsd['NUM_DSR'][indx] )

    # read Precise Basis for Spectral Calibration Parameters (BASE)
    def getBASE(self):
        record_dtype = np.dtype([
            ('wavelen_grid', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('SPECTRAL_BASE')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.base = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read Spectral Calibration Parameters (SCP)
    def getSCP(self):
        record_dtype = np.dtype([
            ('orbit_phase', '>f4'),
            ('coeffs', '>f8', (8,5)),
            ('num_lines', '>u2', (8)),
            ('wavelen_error', '>f4', (8)),
        ])
        indx = self.dsd['DS_NAME'].index('SPECTRAL_CALIBRATION')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.scp = np.fromfile( self.fp, dtype=record_dtype, 
                                count=self.dsd['NUM_DSR'][indx] )

    # read Sun Reference Spectrum (SRS)
    def getSRS(self):
        record_dtype = np.dtype([
            ('spec_id', 'a2'),
            ('wavelength', '>f4', (8192)),
            ('smr', '>f4', (8192)),
            ('smr_precision', '>f4', (8192)),
            ('smr_accuracy', '>f4', (8192)),
            ('etalon', '>f4', (8192)),
            ('avg_azi', '>f4'),
            ('avg_ele', '>f4'),
            ('avg_sun_ele', '>f4'),
            ('mean_pmd', '>f4', (7)),
            ('pmd_nd_out', '>f4', (7)),
            ('pmd_nd_in', '>f4', (7)),
            ('doppler', '>f4')
        ])
        indx = self.dsd['DS_NAME'].index('SUN_REFERENCE')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.srs = np.fromfile( self.fp, dtype=record_dtype, 
                                count=self.dsd['NUM_DSR'][indx] )

    # read Polarisation Sensitivity Parameters Nadir (PSPN)
    def getPSPN(self):
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('mu2', '>f4', (8192)),
            ('mu3', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('POL_SENS_NADIR')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.pspn = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read Polarisation Sensitivity Parameters Limb (PSPL)
    def getPSPL(self):
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('ang_asm', '>f4'),
            ('mu2', '>f4', (8192)),
            ('mu3', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('POL_SENS_LIMB')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.pspl = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read Polarisation Sensitivity Parameters Occultation (PSPO)
    def getPSPO(self):
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('ang_asm', '>f4'),
            ('mu2', '>f4', (8192)),
            ('mu3', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('POL_SENS_OCC')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.pspo = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read Radiance Sensitivity Parameters Nadir (RSPN)
    def getRSPN(self):
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('sensitivity', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('RAD_SEND_NADIR')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.rspn = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read Radiance Sensitivity Parameters Limb (RSPL)
    def getRSPL(self):
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('ang_asm', '>f4'),
            ('sensitivity', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('RAD_SENS_LIMB')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.rspl = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read Radiance Sensitivity Parameters Occultation (RSPO)
    def getRSPO(self):
        record_dtype = np.dtype([
            ('ang_esm', '>f4'),
            ('ang_asm', '>f4'),
            ('sensitivity', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('RAD_SENS_OCC')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.rspo = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read Errors on Key Data (EKD)
    def getEKD(self):
        record_dtype = np.dtype([
            ('mu2_nadir', '>f4', (8192)),
            ('mu3_nadir', '>f4', (8192)),
            ('mu2_limb', '>f4', (8192)),
            ('mu3_limb', '>f4', (8192)),
            ('sensitivity_obm', '>f4', (8192)),
            ('sensitivity_nadir', '>f4', (8192)),
            ('sensitivity_limb', '>f4', (8192)),
            ('sensitivity_sun', '>f4', (8192)),
            ('bsdf', '>f4', (8192))
        ])
        indx = self.dsd['DS_NAME'].index('ERRORS_ON_KEY_DATA')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.ekd = np.fromfile( self.fp, dtype=record_dtype, 
                                count=self.dsd['NUM_DSR'][indx] )

    # read Slit Function Parameters (SFP)
    def getSFP(self):
        record_dtype = np.dtype([
            ('pixel_slit', '>u2'),
            ('type_slit', 'u1'),
            ('fwhm_slit', '>f4'),
            ('fwhm_lorenz', '>f4'),
        ])
        indx = self.dsd['DS_NAME'].index('SLIT_FUNCTION')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.sfp = np.fromfile( self.fp, dtype=record_dtype, 
                                count=self.dsd['NUM_DSR'][indx] )

    # read Small Aperture Function Parameters (ASFP)
    def getASFP(self):
        record_dtype = np.dtype([
            ('pixel_slit', '>u2'),
            ('type_slit', 'u1'),
            ('fwhm_slit', '>f4'),
            ('fwhm_lorenz', '>f4'),
        ])
        indx = self.dsd['DS_NAME'].index('SMALL_AP_SLIT_FUNCTION')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.asfp = np.fromfile( self.fp, dtype=record_dtype, 
                                 count=self.dsd['NUM_DSR'][indx] )

    # read States of the Product
    def getSTATES(self):
        record_dtype = np.dtype([
            ('mjd', {'names':['days', 'secnds', 'musec'], 
                     'formats':['>i4','>u4', '>u4']}),
            ('flag_attached', 'i1'),
            ('flag_reason', 'i1'),
            ('orbit_phase', '>f4'),
            ('category', '>u2'),
            ('state_id', '>u2'),
            ('duration', '>u2'),
            ('intg_max', '>u2'),
            ('num_clus', '>u2'),
            ('Clcon', {'names':['clus_id','chan_id','start','length','pet',
                                'intg','coaddf','readouts','clus_type'], 
                       'formats':['u1', 'u1', '>u2', '>u2', '>f4',
                                  '>u2', '>u2', '>u2', 'u1']}, (64)),
            ('mds_type', 'u1'),
            ('num_geo', '>u2'),
            ('num_pmd', '>u2'),
            ('num_intg', '>u2'),
            ('intg', '>u2', (64)),
            ('polv', '>u2', (64)),
            ('num_polv', '>u2'),
            ('number', '>u2'),
            ('length', '>u4')
        ])
        indx = self.dsd['DS_NAME'].index('STATES')
        self.fp.seek(self.dsd['DS_OFFSET'][indx])
        self.states = np.fromfile( self.fp, dtype=record_dtype, 
                                   count=self.dsd['NUM_DSR'][indx] )
