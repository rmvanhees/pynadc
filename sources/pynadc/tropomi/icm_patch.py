# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to simulate SWIR calibration data for patching of ICM_CA_SIR product

Simulated products are derived as follows:
 a) Background:
     - derive from offset and dark CKD
     - requires  : exposure time & co-adding factor
     - TBD: what to do with orbital variation?
 b) DLED:
     - derive from OCAL measurements (email Paul 22-Juni-2016 14:30)
     - requires  : exposure time & co-adding factor     
 c) WLS:
     - derive from OCAL measurements (email Paul 22-Juni-2016 14:30)
     - requires  : exposure time & co-adding factor
 d) SLS (ISRF):
     - derive from OCAL measurements
 e) Irradiance:   [low priority]
     - using level 2 simulations?
 f) Radiance:     [low priority]
     - using level 2 simulations?

'''
import os.path

import numpy as np
import h5py

class ICM_patch( object ):
    '''
    '''
    def background( self, exposure_time, coadding_factor ):
        ckd_dir = '/nfs/TROPOMI/ocal/ckd/ckd_release_swir'

        # read v2c CKD
        ckd_file = os.path.join(ckd_dir, 'v2c', 'ckd.v2c_factor.detector4.nc')
        with h5py.File( ckd_file, 'r' ) as fid:
            dset = fid['/BAND7/v2c_factor_swir']
            v2c_b7 = dset[...]

        v2c_swir = v2c_b7[0]['value']

        # read offset CKD
        ckd_file = os.path.join(ckd_dir, 'offset', 'ckd.offset.detector4.nc')
        with h5py.File( ckd_file, 'r' ) as fid:
            dset = fid['/BAND7/analog_offset_swir']
            offs_b7 = dset[:-1,:]
            dset = fid['/BAND8/analog_offset_swir']
            offs_b8 = dset[:-1,:]

        offset_swir = np.hstack((offs_b7, offs_b8))
    
        # read dark CKD
        ckd_file = os.path.join(ckd_dir, 'darkflux', 'ckd.dark.detector4.nc')
        with h5py.File( ckd_file, 'r' ) as fid:
            dset = fid['/BAND7/long_term_swir']
            dark_b7 = dset[:-1,:]
            dset = fid['/BAND8/long_term_swir']
            dark_b8 = dset[:-1,:]

        dark_swir = np.hstack((dark_b7, dark_b8))

        background = offset_swir['value'] * v2c_swir
        background += dark_swir['value'] * exposure_time
        background *= coadding_factor

        error = 0.0 * offset_swir['error'] + 135  # fixed value at +/- 3 BU

        return (background, error)
        

    def dark( self, parms ):
        pass

    def dled( self, exposure_time, coadding_factor ):
        '''
        The DLED signal can be aproximated by the background signal and the
        signal-current of the DLED. Kindly provided by Paul Tol
        '''
        (signal, error) = self.background( exposure_time, 1 )

        dled_dir = '/data/richardh/Tropomi'
        dled_file = os.path.join(dled_dir, 'DledlinSw_signalcurrent_approx.h5')
        with h5py.File( dled_file, 'r' ) as fid:
            dset = fid['dled_signalcurrent_epers']
            dled_current = dset[:-1,:]

        signal += dled_current * exposure_time
        signal *= coadding_factor

        return (signal, error)

    def sls( self, parms ):
        pass

    def wls( self, exposure_time, coadding_factor ):
        '''
        The WLS signal is appriximated as the DLED signal, therefore, 
        it can be aproximated by the background signal and the
        signal-current of the DLED. Kindly provided by Paul Tol
        '''
        (signal, error) = self.background( exposure_time, 1 )

        dled_dir = '/data/richardh/Tropomi'
        dled_file = os.path.join(dled_dir, 'DledlinSw_signalcurrent_approx.h5')
        with h5py.File( dled_file, 'r' ) as fid:
            dset = fid['dled_signalcurrent_epers']
            dled_current = dset[:-1,:]

        signal += dled_current * exposure_time
        signal *= coadding_factor

        return (signal, error)

    def irradiance( self, parms ):
        pass

    def radiance( self, parms ):
        pass

#--------------------------------------------------
def test():
    patch = ICM_patch()
    res = patch.background( 1., 1 )
    print( res[0][110,8], res[1][110,8] )
    res = patch.background( 0.098, 1 )
    print( res[0][110,8], res[1][110,8] )
    res = patch.background( 0.0098, 1 )
    print( res[0][110,8], res[1][110,8] )
    res = patch.dled( 0.25, 1 )
    print( res[0][110,8], res[1][110,8] )
    res = patch.wls( 0.025, 1 )
    print( res[0][110,8], res[1][110,8] )

if __name__ == '__main__':
    test()
    
