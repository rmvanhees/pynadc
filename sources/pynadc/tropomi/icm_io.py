# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to read and write to a Tropomi ICM_CA_SIR product

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
    a Tropomi ICM_CA_SIR product

    Usage:
    1) open file (new class initiated)
    2) select group of a particular measurement
    3) read data (median/averaged full frame(s), only)
    .
    . <user actions>
    .
    4) write patched data
    * back to step 2) or
    5) close file
    '''
    def __init__( self, icm_product, readwrite=False, verbose=False ):
        assert os.path.isfile( icm_product ), \
            '*** Fatal, can not find ICM_CA_SIR file: {}'.format(icm_product)

        self.__product = icm_product
        if readwrite:
            self.__fid = h5py.File( icm_product, "r+" )
        else:
            self.__fid = h5py.File( icm_product, "r" )

        self.__verbose = verbose
        self.__h5_path = None 
        self.__h5_name = None 
        self.__msm_mode = None
        self.__patched_msm = []

        self.orbit = self.__fid.attrs['reference_orbit'][0]
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
        return "ICM_io: {}/{}/{} of bands {}".format( self.__product,
                                                    self.__h5_path,
                                                    self.__h5_name,
                                                    self.bands )

    def __del__( self ):
        '''
        Before closing the product, we make sure that the output product
        describes what has been altered by the S/W. 
        To keep any change traceable.
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
            from importlib import util
            
            sgrp = self.__fid.create_group( "METADATA/SRON_METADATA" )
            sgrp.attrs['dateStamp'] = datetime.utcnow().isoformat()
            version_spec = util.find_spec( "pynadc.version" )
            if version_spec is not None:
                from pynadc import version
                sgrp.attrs['git_tag'] = version.__version__
            dt = h5py.special_dtype(vlen=str)
            ds = sgrp.create_dataset( 'patched_datasets',
                                      (len(self.__patched_msm),), dtype=dt)
            ds[:] = np.asarray(self.__patched_msm)
             
        self.__fid.close()
    
    #-------------------------
    def select( self, h5_name, h5_path=None ):
        '''
        Parameters:
         - h5_name : name of measurement group
         - h5_path : name of path in HDF5 file to measurement group
                     use "BAND%_.../..."

        Updated object attributes:
         - ref_time            : reference time of measurement (datetime-object)
         - delta_time          : offset w.r.t. reference time (milli-seconds)
         - instrument_settings : copy of instrument settings
         - housekeeping_data   : copy of housekeeping data
        '''
        from datetime import datetime, timedelta

        self.bands = ''
        if h5_path is not None:
            assert h5_path.find('%') > 0, \
                print( '*** Fatal: h5_path should start with BAND%' )

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

        if len(self.bands) > 0:
            ib = str(self.bands[0])
            grp_path = os.path.join( h5_path.replace('%', ib), h5_name )
            grp = self.__fid[grp_path]
            sgrp = grp['INSTRUMENT']
            self.instrument_settings = np.squeeze(sgrp['instrument_settings'])
            self.housekeeping_data = np.squeeze(sgrp['housekeeping_data'])
            sgrp = grp['OBSERVATIONS']
            self.ref_time = (datetime(2010,1,1,0,0,0) \
                             + timedelta(seconds=int(sgrp['time'][0])))
            self.delta_time = sgrp['delta_time'][0,:].astype(int)
            self.__h5_path = h5_path
            self.__h5_name = h5_name
            
        return self.bands

    #-------------------------
    def get_data( self, msm_mode ):
        '''
        Pull averaged frame-data from dataset
        - msm_mode must be one of 'biweight', 'sls', 'avg', 'avg_col',
                   'avg_error', 'avg_noise', 'avg_quality_level', 'avg_row', 
                   'avg_std'

        The function returns a tuple with the data values and their errors.
        - these values and errors are stored as a list of ndarrays (one array
          per band)
        '''
        if msm_mode == 'biweight':
            dset_grp = 'ANALYSIS'
            dset_values = 'biweight_value'
            dset_errors = 'biweight_error'
        else:
            dset_grp = 'OBSERVATIONS'
            if self.__h5_path.find('CALIBRATION') >= 0:
                dset_values = 'signal_{}'.format(msm_mode)
                dset_errors = None
                if msm_mode == 'avg':
                    dset_errors = 'signal_{}_std'.format(msm_mode)
            elif self.__h5_path.find('IRRADIANCE') >= 0:
                dset_values = 'irradiance_{}'.format(msm_mode)
                dset_errors = None
                if msm_mode == 'avg':
                    dset_errors = 'irradiance_{}_std'.format(msm_mode)
            elif self.__h5_path.find('RADIANCE') >= 0:
                dset_values = 'radiance_{}'.format(msm_mode)
                dset_errors = None
                if msm_mode == 'avg':
                    dset_errors = 'radiance_{}_std'.format(msm_mode)

        values = None
        errors = None
        for ib in self.bands:
            sgrp = self.__fid[os.path.join( self.__h5_path.replace('%', ib),
                                            self.__h5_name, dset_grp )]
            if values is None:
                values = [np.squeeze(sgrp[dset_values])]
            else:
                values.append(np.squeeze(sgrp[dset_values]))
                
            if dset_errors is not None:
                if errors is None:
                    errors = [np.squeeze(sgrp[dset_errors])]
                else:
                    errors.append(np.squeeze(sgrp[dset_errors]))
        
        return (values, errors)

    #-------------------------
    def set_data( self, msm_mode, values, errors=None ):
        '''
        Push (patched) averaged frame-data to dataset
        - values and errors should be provided as a list of ndarrays 
          (one array per band)
        '''
        if msm_mode == 'biweight':
            dset_grp = 'ANALYSIS'
            dset_values = 'biweight_value'
            dset_errors = 'biweight_error'
        elif msm_mode == 'sls':
            dset_grp = 'ANALYSIS'
            dset_values = 'det_lit_area_signal'            
        elif msm_mode == 'sls_background':
            dset_grp = 'ANALYSIS'
            dset_values = 'det_lit_area_signal'            
        else:
            dset_grp = 'OBSERVATIONS'
            if self.__h5_path.find('CALIBRATION') >= 0:
                dset_values = 'signal_{}'.format(msm_mode)
                dset_errors = None
                if msm_mode == 'avg':
                    dset_errors = 'signal_{}_std'.format(msm_mode)
            elif self.__h5_path.find('IRRADIANCE') >= 0:
                dset_values = 'irradiance_{}'.format(msm_mode)
                dset_errors = None
                if msm_mode == 'avg':
                    dset_errors = 'irradiance_{}_std'.format(msm_mode)
            elif self.__h5_path.find('RADIANCE') >= 0:
                dset_values = 'radiance_{}'.format(msm_mode)
                dset_errors = None
                if msm_mode == 'avg':
                    dset_errors = 'radiance_{}_std'.format(msm_mode)

        ii = 0
        for ib in self.bands:
            ds_path = os.path.join( self.__h5_path.replace('%', ib),
                                    self.__h5_name, dset_grp )
            if not ds_path in self.__fid:
                continue
            
            sgrp = self.__fid[ds_path]
            if msm_mode[0:3] != 'sls':
                if values is not None:
                    sgrp[dset_values][...] = values[ii][...]

                if errors is not None:
                    sgrp[dset_errors][...] = errors[ii][...]
            else:
                if values is not None:
                    sgrp[dset_values][...] = values

                if errors is not None:
                    sgrp[dset_errors][...] = errors
            ii += 1

        self.__patched_msm.append( os.path.join( self.__h5_path,
                                                 self.__h5_name,
                                                 dset_values ) )

#--------------------------------------------------
def test():
    '''
    Perform some simple test to check the ICM_io class
    '''
    import shutil
    
    if os.path.isdir('/Users/richardh'):
        fl_path = '/Users/richardh/Data/S5P_ICM_CA_SIR/001000/2012/09/19'
    else:
        fl_path = '/nfs/TROPOMI/ical/S5P_ICM_CA_SIR/001000/2012/09/19'
    fl_name = 'S5P_ICM_CA_SIR_20120919T051721_20120919T065655_01890_01_001000_20151002T140000.h5'

    print( fl_name )
    fp = ICM_io( os.path.join(fl_path, fl_name), verbose=True )
    print( fp )
    fp.select( 'BACKGROUND_MODE_1063' )
    print( fp )
    fp.select( 'BACKGROUND_MODE_1063', h5_path='BAND%_CALIBRATION' )
    print( fp.ref_time )
    print( fp.delta_time )
    print( fp )
    (values, error) = fp.get_data( msm_mode='biweight' )
    print( 'biweight: ', len(values), values[0].shape )
    
    (values, error) = fp.get_data( msm_mode='avg' )
    print( 'avg: ', len(values), values[0].shape )

    (values, error) = fp.get_data( msm_mode='avg_col' )
    print( 'avg_col: ', len(values), values[0].shape )

    (values, error) = fp.get_data( msm_mode='avg_row' )
    print( 'avg_row: ', len(values), values[0].shape )

    if os.path.isdir('/Users/richardh'):
        fl_path2 = '/Users/richardh/Data/S5P_ICM_CA_SIR/001000/2012/09/19'
    else:
        fl_path2 = '/data/richardh/Tropomi'
    fl_name2 = 'S5P_ICM_CA_SIR_20120919T051721_20120919T065655_01890_02_001000_20151002T140000.h5'
    shutil.copy( os.path.join(fl_path, fl_name),
                 os.path.join(fl_path2, fl_name2) )
    fp = ICM_io( os.path.join(fl_path2, fl_name2),
                 verbose=True, readwrite=True )
    fp.select( 'BACKGROUND_MODE_1063' )
    print( fp )
    (values, error) = fp.get_data( msm_mode='avg' )
    values[0][:,:] = 2
    values[1][:,:] = 3
    fp.set_data( 'avg', values )
    
    del fp

#--------------------------------------------------
if __name__ == '__main__':
    test()
    
