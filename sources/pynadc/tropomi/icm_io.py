# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Access to data-sets in a Tropomi ICM_CA_SIR product, including read and alter

'''
from __future__ import print_function
from __future__ import division

import os.path

import numpy as np
import h5py

#--------------------------------------------------
class ICM_io( object ):
    '''
    This class should offer all the necessary functionality to read and patch
    dataset in a Tropomi ICM_CA_SIR product

    Usage:
    1) open file (new class initiated)
    2) select group of a particular measurement
    3) read data
    .
    . <user actions>
    .
    4) write patched data
    .
    . back to step 2) or
    .
    5) close access to file
    '''
    def __init__( self, icm_product, readwrite=False, verbose=False ):
        '''
        Initialize access to an ICM product
        '''
        assert os.path.isfile( icm_product ), \
            '*** Fatal, can not find ICM_CA_SIR file: {}'.format(icm_product)

        # initialize class-attributes
        self.__verbose = verbose
        self.__product = icm_product
        self.__rw = readwrite
        self.__h5_path = None
        self.__h5_name = None
        self.__msm_mode = None
        self.__patched_msm = []

        # open ICM product as HDF5 file
        if readwrite:
            self.__fid = h5py.File( icm_product, "r+" )
        else:
            self.__fid = h5py.File( icm_product, "r" )

        # initialize public class-attributes
        self.orbit = int(self.__fid.attrs['reference_orbit'])
        self.start_time = self.__fid.attrs['time_coverage_start'].decode('ascii').strip('Z').replace('T',' ')
        grp = self.__fid['/METADATA/ESA_METADATA/earth_explorer_header/fixed_header']
        dset = grp['source']
        self.creation_date = (dset.attrs['Creation_Date'].split(b'=')[1]).decode('ascii').replace('T',' ')
        self.creator_version = (dset.attrs['Creator_Version']).decode('ascii')

        self.bands = ''
        self.ref_time = None
        self.delta_time = None
        self.instrument_settings = None
        self.housekeeping_data = None
        
    def __repr__( self ):
        class_name = type(self).__name__
        return '{}({!r}, readwrite={!r})'.format( class_name,
                                                  self.__product, self.__rw )

    def __del__( self ):
        '''
        Before closing the product, we make sure that the output product
        describes what has been altered by the S/W. To keep any change 
        traceable.
        '''
        if len(self.__patched_msm) > 0:
            '''
             as attributes of this group, we write:
             - dateStamp ('now')
             - Git-version of S/W
             - list of patched datasets
             - auxiliary datasets used by patch-routines
            '''
            from datetime import datetime
            
            sgrp = self.__fid.create_group( "METADATA/SRON_METADATA" )
            sgrp.attrs['dateStamp'] = datetime.utcnow().isoformat()
            sgrp.attrs['git_tag'] = self.pynadc_version()
            dt = h5py.special_dtype(vlen=str)
            ds = sgrp.create_dataset( 'patched_datasets',
                                      (len(self.__patched_msm),), dtype=dt)
            ds[:] = np.asarray(self.__patched_msm)
             
        self.__fid.close()
    
    # ---------- RETURN VERSION of the S/W ----------
    @staticmethod
    def pynadc_version():
        '''
        Return S/W version
        '''
        from importlib import util

        version_spec = util.find_spec( "pynadc.version" )
        assert version_spec is not None

        from pynadc import version
        return version.__version__

    #-------------------------
    def select( self, h5_name, h5_path=None ):
        '''
        Select a measurement as <processing class>_<ic_id>

        Parameters
        ----------
        h5_name :  string
          name of measurement group
        h5_path : {'BAND%_ANALYSIS', 'BAND%_CALIBRATION', 
                   'BAND%_IRRADIANCE', 'BAND%_RADIANCE'}
          name of path in HDF5 file to measurement group

        Returns
        -------
        String with spectral bands found in product

        Updated object attributes:
         - bands               : available spectral bands
         - ref_time            : reference time of measurement (datetime-object)
         - delta_time          : offset w.r.t. reference time (milli-seconds)
         - instrument_settings : copy of instrument settings
         - housekeeping_data   : copy of housekeeping data
        '''
        from datetime import datetime, timedelta

        self.bands = ''
        self.ref_time   = None
        self.delta_time = None
        self.instrument_settings = None
        self.housekeeping_data   = None

        # if path is given, then only determine avaialble spectral bands
        # else determine path and avaialble spectral bands
        if h5_path is not None:
            assert h5_path.find('%') > 0, \
                '*** Fatal: h5_path should start with BAND%'

            for ib in '12345678':
                grp_path = os.path.join( h5_path.replace('%', ib), h5_name )
                if grp_path in self.__fid:
                    self.bands += ib
                
        else:
            grp_list = [ 'ANALYSIS', 'CALIBRATION', 'IRRADIANCE', 'RADIANCE' ]
            for ib in '12345678':
                for name in grp_list:
                    grp_path = os.path.join( 'BAND{}_{}'.format(ib, name),
                                             h5_name )
                    if grp_path in self.__fid:
                        if self.__verbose:
                            print( grp_path, grp_path in self.__fid )
                        h5_path = 'BAND{}_{}'.format('%', name)
                        self.bands += ib

        # return in case no data was found
        if len(self.bands) == 0:
            return self.bands
        self.__h5_path = h5_path
        self.__h5_name = h5_name

        # NOTE it is assumed that the instrument settings and housekeeping data
        # are equal for the bands available
        ib = str(self.bands[0])
        if h5_name == 'ANALOG_OFFSET_SWIR' or h5_name == 'LONG_TERM_SWIR':
            grp_path = os.path.join( h5_path.replace('%', ib), h5_name )
            grp = self.__fid[grp_path]
            dset = grp[h5_name.lower() + '_group_keys']
            group_keys = dset['group'][:]
            for name in group_keys:
                grp_path = os.path.join( 'BAND{}_CALIBRATION'.format(ib),
                                         name.decode('ascii') )
                grp = self.__fid[grp_path]
                sgrp = grp['INSTRUMENT']
                is_data = np.squeeze(sgrp['instrument_settings'])
                hk_data = np.squeeze(sgrp['housekeeping_data'])
                if self.instrument_settings is None:
                    self.instrument_settings = is_data
                    self.housekeeping_data   = hk_data
                else:
                    self.instrument_settings = np.append(self.instrument_settings,
                                                         is_data)
                    self.housekeeping_data   = np.append(self.housekeeping_data,
                                                         hk_data)
                sgrp = grp['OBSERVATIONS']
                if self.ref_time is None:
                    self.ref_time = (datetime(2010,1,1,0,0,0) \
                                     + timedelta(seconds=int(sgrp['time'][0])))
                if self.delta_time is None:
                    self.delta_time = sgrp['delta_time'][0,:].astype(int)
                else:
                    self.delta_time = np.append(self.delta_time,
                                                sgrp['delta_time'][0,:].astype(int))
        elif h5_name == 'DPQF_MAP' or h5_name == 'NOISE':
            grp_path = os.path.join( h5_path.replace('%', ib),
                                     'ANALOG_OFFSET_SWIR' )
            grp = self.__fid[grp_path]
            dset = grp['analog_offset_swir_group_keys']
            group_keys = dset['group'][:]
            for name in group_keys:
                grp_path = os.path.join( 'BAND{}_CALIBRATION'.format(ib),
                                         name.decode('ascii') )
                grp = self.__fid[grp_path]
                sgrp = grp['INSTRUMENT']
                is_data = np.squeeze(sgrp['instrument_settings'])
                hk_data = np.squeeze(sgrp['housekeeping_data'])
                if self.instrument_settings is None:
                    self.instrument_settings = is_data
                    self.housekeeping_data   = hk_data
                else:
                    self.instrument_settings = np.append(self.instrument_settings,
                                                         is_data)
                    self.housekeeping_data   = np.append(self.housekeeping_data,
                                                         hk_data)
                sgrp = grp['OBSERVATIONS']
                if self.ref_time is None:
                    self.ref_time = (datetime(2010,1,1,0,0,0) \
                                     + timedelta(seconds=int(sgrp['time'][0])))
                if self.delta_time is None:
                    self.delta_time = sgrp['delta_time'][0,:].astype(int)
                else:
                    self.delta_time = np.append(self.delta_time,
                                                sgrp['delta_time'][0,:].astype(int))
        else:
            grp_path = os.path.join( h5_path.replace('%', ib), h5_name )
            grp = self.__fid[grp_path]
            sgrp = grp['INSTRUMENT']
            self.instrument_settings = np.squeeze(sgrp['instrument_settings'])
            self.housekeeping_data   = np.squeeze(sgrp['housekeeping_data'])
            sgrp = grp['OBSERVATIONS']
            self.ref_time = (datetime(2010,1,1,0,0,0) \
                             + timedelta(seconds=int(sgrp['time'][0])))
            self.delta_time = sgrp['delta_time'][0,:].astype(int)
            
        return self.bands

    #-------------------------
    def get_msm_names( self, msm_mode ):
        '''
        Parameters
        ----------
        msm_mode : {None, 'biweight', 'sls'}

        Returns
        -------
        List with names of the measurement datasets

        The names of the measurement datasets are different for
          1) datasets under BAND%_ANALYSIS, BAND%_CALIBRATION, BAND%_IRRADIANCE 
            and BAND%_RADIANCE
          2) datasets under ANALYSIS of BAND%_CALIBRATION
          3) dataset of dynamic CKD
        '''
        dset_msm = []
        if self.__h5_path.find('ANALYSIS') >= 0:
            dset_grp = ''
            if self.__h5_name == 'ANALOG_OFFSET_SWIR' \
               or self.__h5_name == 'LONG_TERM_SWIR':
                dset_msm.append(self.__h5_name.lower() + '_value')
                dset_msm.append(self.__h5_name.lower() + '_error')
            elif self.__h5_name == 'DPQF_MAP':
                dset_msm.append('dpqf_map')
                dset_msm.append('dpqm_dark_flux')
                dset_msm.append('dpqm_noise')
            elif self.__h5_name == 'NOISE':
                dset_msm.append('noise')
                dset_msm.append('noise_estimated_background')
        elif self.__h5_path.find('CALIBRATION') >= 0:
            if msm_mode == 'biweight':
                dset_grp = 'ANALYSIS'
                dset_msm.append('biweight_value')
                dset_msm.append('biweight_error')
            elif msm_mode == 'sls':
                dset_grp = 'ANALYSIS'
                dset_msm.append('det_lit_area_signal')
                dset_msm.append('det_lit_area_error')
            else:
                dset_grp = 'OBSERVATIONS'
                dset_msm.append('signal_avg')
                dset_msm.append('signal_avg_col')
                dset_msm.append('signal_avg_row')
                dset_msm.append('signal_avg_std')
        elif self.__h5_path.find('IRRADIANCE') >= 0:
            dset_grp = 'OBSERVATIONS'
            dset_msm.append('irradiance_avg')
            dset_msm.append('irradiance_avg_col')
            dset_msm.append('irradiance_avg_row')
            dset_msm.append('irradiance_avg_std')
        elif self.__h5_path.find('RADIANCE') >= 0:
            dset_grp = 'OBSERVATIONS'
            dset_msm.append('radiance_avg')
            dset_msm.append('radiance_avg_col')
            dset_msm.append('radiance_avg_row')
            dset_msm.append('radiance_avg_std')

        return (dset_grp, dset_msm)
        
    #-------------------------
    def get_data( self, msm_mode=None ):
        '''
        Read datasets from a measurement selected by class-method "select"

        Parameters
        ---------- 
        msm_mode :  {None, 'biweight', 'sls'}

        Returns
        -------
        Python dictionary with msm_names as keys and their values

        - these values are stored as a list of ndarrays (one array per band)
        - (float) FillValues are set to NaN
        '''
        FILLVALUE = float.fromhex('0x1.ep+122')

        (dset_grp, msm_list) = self.get_msm_names( msm_mode )
        
        res = {}
        for ib in self.bands:
            sgrp = self.__fid[os.path.join( self.__h5_path.replace('%', ib),
                                            self.__h5_name, dset_grp )]
            for msm in msm_list:
                data = np.squeeze(sgrp[msm])
                if sgrp[msm].attrs['_FillValue'] == FILLVALUE:
                    data[(data == FILLVALUE)] = np.nan
                if msm not in res.keys():
                    res[msm] = [data]
                else:
                    res[msm].append(data)
        
        return res

    #-------------------------
    def set_data( self, res ):
        '''
        Alter datasets from a measurement selected by class-method "select"

        Requires a dictionary alike the one returned by class-method "get_data"
        '''
        grp_list = ['OBSERVATIONS', 'ANALYSIS', '']

        ii = 0
        for ib in self.bands:
            for dset_grp in grp_list:
                ds_path = os.path.join( self.__h5_path.replace('%', ib),
                                        self.__h5_name, dset_grp )
                if ds_path not in self.__fid:
                    continue
                
                sgrp = self.__fid[ds_path]
                for key in res.keys():
                    if key not in sgrp:
                        continue

                    if key[0:12] == 'det_lit_area':
                        sgrp[key][...] = res[key]
                    else:
                        sgrp[key][...] = res[key][ii][...]
            ii += 1

        for dset_grp in grp_list:
            ds_path = os.path.join( self.__h5_path.replace('%', self.bands[0]),
                                    self.__h5_name, dset_grp )
            if ds_path not in self.__fid:
                continue
                
            sgrp = self.__fid[ds_path]
            for key in res.keys():
                if key in sgrp:
                    self.__patched_msm.append( os.path.join( self.__h5_path,
                                                             self.__h5_name,
                                                             key ) )

#--------------------------------------------------
def test():
    '''
    Perform some simple test to check the ICM_io class
    '''
    import shutil
    
    if os.path.isdir('/Users/richardh'):
        fl_path = '/Users/richardh/Data/S5P_ICM_CA_SIR/001000/2012/09/18'
    elif os.path.isdir('/nfs/TROPOMI/ical/'):
        fl_path = '/nfs/TROPOMI/ical/S5P_ICM_CA_SIR/001100/2012/09/18'
    else:
        fl_path = '/data/richardh/Tropomi/ical/S5P_ICM_CA_SIR/001100/2012/09/18'
    fl_name = 'S5P_TEST_ICM_CA_SIR_20120918T131651_20120918T145629_01890_01_001100_20151002T140000.h5'

    fp = ICM_io( os.path.join(fl_path, fl_name), verbose=True )
    fp.select( 'ANALOG_OFFSET_SWIR' )
    print( fp )
    print( fp.ref_time )
    print( fp.delta_time )
    res= fp.get_data()
    for key in res.keys():
        print( key, len(res[key]), res[key][0].shape )

    fp.select( 'BACKGROUND_MODE_1063', h5_path='BAND%_CALIBRATION' )
    print( fp )
    res= fp.get_data()
    for key in res.keys():
        print( key, len(res[key]), res[key][0].shape )

    res = fp.get_data( msm_mode='biweight' )
    for key in res.keys():
        print( key, len(res[key]), res[key][0].shape )

    fp.select( 'SOLAR_IRRADIANCE_MODE_0202' )
    print( fp )
    res= fp.get_data()
    for key in res.keys():
        print( key, len(res[key]), res[key][0].shape )

    fp.select( 'EARTH_RADIANCE_MODE_0004' )
    print( fp )
    res= fp.get_data()
    for key in res.keys():
        print( key, len(res[key]), res[key][0].shape )

    if os.path.isdir('/Users/richardh'):
        fl_path2 = '/Users/richardh/Data/S5P_ICM_CA_SIR/001000/2012/09/18'
    else:
        fl_path2 = '/data/richardh/Tropomi'
    fl_name2 = 'S5P_TEST_ICM_CA_SIR_20120918T131651_20120918T145629_01890_01_001101_20151002T140000.h5'
    shutil.copy( os.path.join(fl_path, fl_name),
                 os.path.join(fl_path2, fl_name2) )
    fp = ICM_io( os.path.join(fl_path2, fl_name2),
                 verbose=True, readwrite=True )
    fp.select( 'BACKGROUND_MODE_1063' )
    print( fp )
    res = fp.get_data()
    del res['signal_avg_col']
    del res['signal_avg_row']
    res['signal_avg'][0][:,:] = 2
    res['signal_avg'][1][:,:] = 3
    res['signal_avg_std'][0][:,:] = 0.25
    fp.set_data( res )
   
    del fp

#--------------------------------------------------
if __name__ == '__main__':
    test()
    
