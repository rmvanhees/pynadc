# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to read Tropomi OCAL products

'''
from __future__ import print_function
from __future__ import division

import os.path

import numpy as np
import h5py

from pynadc.stats import biweight

#--------------------------------------------------
class OCM_io( object ):
    '''
    This class should offer all the necessary functionality to read Tropomi
    on-ground calibration and products

    Usage:
    1) open file (new class initiated)
    2) select group of a particular measurement
    3) read data (median/averaged full frame(s), only)
    .
    . <user actions>
    .
    * back to step 2) or
    5) close file
    '''
    def __init__( self, ocm_msm, verbose=False ):
        # import numbers
        assert os.path.isdir( ocm_msm ), \
            '*** Fatal, can not find OCAL measurement: {}'.format(ocm_msm)

        self.__msm = ocm_msm
        self.__fid_b7 = h5py.File(os.path.join(ocm_msm, 'trl1brb7g.lx.nc'), "r")
        self.__fid_b8 = h5py.File(os.path.join(ocm_msm, 'trl1brb8g.lx.nc'), "r")

        self.__verbose = verbose
        self.__icid = None 
        self.__msm_mode = None
        self.__patched_msm = []

        self.orbit   = -1
        self.num_msm = 0
        self.start_time = self.__fid_b7.attrs['time_coverage_start'].decode('ascii').strip('Z').replace('T',' ')
        self.creator_version = self.__fid_b7.attrs['processor_version'].decode('ascii')

        self.ref_time = ()
        self.delta_time = ()
        self.instrument_settings = None
        self.housekeeping_data = None
        
    def __repr__( self ):
        return "OCM_io: {} - ICID: {}".format( self.__msm,
                                                      self.__icid )

    def __del__( self ):
        '''
        Before closing the product, we make sure that the output product
        describes what has been altered by the S/W. 
        To keep any change traceable.
        '''
        if self.__fid_b7 is not None:
            self.__fid_b7.close()
        if self.__fid_b8 is not None:
            self.__fid_b8.close()
    
    # ---------- RETURN VERSION of the S/W ----------
    def pynadc_version( self ):
        '''
        Return S/W version
        '''
        from importlib import util

        version_spec = util.find_spec( "pynadc.version" )
        assert (version_spec is not None)

        from pynadc import version
        return version.__version__

    #-------------------------
    def select( self, ic_id ):
        '''
        Parameters:
         - ic_id  : "BAND%/ICID_{}_GROUP_%".format(ic_id)

        Updated object attributes:
         - ref_time            : reference time of measurement (datetime-object)
         - delta_time          : offset w.r.t. reference time (milli-seconds)
         - instrument_settings : copy of instrument settings
         - housekeeping_data   : copy of housekeeping data
        '''
        from datetime import datetime, timedelta

        self.__icid = ic_id
        if self.__fid_b7 is not None:
            if not 'BAND7' in self.__fid_b7:
                return 0
            
            gid = self.__fid_b7['BAND7']
            grp_name = 'ICID_{:05}_GROUP'.format(ic_id)
            for grp in [s for s in gid.keys() if s.startswith(grp_name)]:
                self.num_msm += 1
                sgid = gid[os.path.join(grp,'INSTRUMENT')]
                self.instrument_settings = \
                                    np.squeeze(sgid['instrument_settings'])
                self.housekeeping_data = \
                                    np.squeeze(sgid['housekeeping_data'])
                sgid = gid[os.path.join(grp,'GEODATA')]
                self.ref_time += ((datetime(2010,1,1,0,0,0) \
                                   + timedelta(seconds=int(sgid['time'][0]))),)
                self.delta_time += (sgid['delta_time'][:].astype(int),)
            
        return self.num_msm

    #-------------------------
    def get_data( self, skip_first_frame=True ):
        '''
        Pull averaged frame-data from dataset

        The function returns a tuple with the data values and their errors.
        - these values and errors are stored as a list of ndarrays (one array
          per band)
        - FillValue are set to NaN
        '''
        if skip_first_frame:
            offs = 1
        else:
            offs = 0
            
        values = None
        errors = None
        fillvalue = float.fromhex('0x1.ep+122')
        if self.__fid_b7 is not None:
            gid = self.__fid_b7['BAND7']
            grp_name = 'ICID_{:05}_GROUP'.format(self.__icid)
            for grp in [s for s in gid.keys() if s.startswith(grp_name)]:
                sgid = gid[os.path.join(grp,'OBSERVATIONS')]
                data = sgid['signal'][offs:,:-1,:]
                if sgid['signal'].attrs['_FillValue'] == fillvalue:
                    data[(data == fillvalue)] = np.nan
                (mx, sx) = biweight( data, axis=0, spread=True )
                if values is None:
                    values = [mx]
                    errors = [sx]
                else:
                    values.append(mx)
                    errors.append(sx)

        if self.__fid_b8 is not None:
            gid = self.__fid_b8['BAND8']
            grp_name = 'ICID_{:05}_GROUP'.format(self.__icid)
            for grp in [s for s in gid.keys() if s.startswith(grp_name)]:
                sgid = gid[os.path.join(grp,'OBSERVATIONS')]
                data = sgid['signal'][offs:,:-1,:]
                if sgid['signal'].attrs['_FillValue'] == fillvalue:
                    data[(data == fillvalue)] = np.nan
                (mx, sx) = biweight( data, axis=0, spread=True )
                if values is None:
                    values = [mx]
                    errors = [sx]
                else:
                    values.append(mx)
                    errors.append(sx)
                    
        return (values, errors)

#--------------------------------------------------
def test():
    '''
    Perform some simple test to check the OCM_io class
    '''
    import shutil
    
    if os.path.isdir('/Users/richardh'):
        fl_path = '/Users/richardh/Data/S5P_OCM_CA_SIR/001000/2012/09/19'
    else:
        fl_path = '/nfs/TROPOMI/ocal/proc_knmi/2015_05_02T10_28_44_SwirlsSunIsrf'
    ocm_msm = 'after_et_l1bavg_004_block-004-004'

    print( ocm_msm )
    fp = OCM_io( os.path.join(fl_path, ocm_msm), verbose=True )
    print( fp )
    if fp.select( 31623 ) > 0:
        print( fp )
        print( fp.num_msm )
        print( fp.ref_time )
        print( fp.delta_time )
        (values, error) = fp.get_data()
        print( 'dimensions of values: ', len(values), values[0].shape )
    
    del fp

#--------------------------------------------------
if __name__ == '__main__':
    test()
    
