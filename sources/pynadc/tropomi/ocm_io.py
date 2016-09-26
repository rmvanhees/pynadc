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
    def __init__( self, ocm_dir, verbose=False ):
        '''
        Initialize access to an ICM products

        Parameters
        ----------
        ocm_dir :  string
           Patch to on-ground calibration measurement

        Note that each band is stored in a seperate product: trl1brb?g.lx.nc
        '''
        assert os.path.isdir( ocm_dir ), \
            '*** Fatal, can not find OCAL measurement: {}'.format(ocm_dir)

        # initialize class-attributes
        self.__product = ocm_dir
        self.__verbose = verbose
        self.__icid = None 
        self.__msm_mode = None
        self.__patched_msm = []

        # open OCM products as HDF5 file
        self.__fid_b7 = h5py.File(os.path.join(ocm_dir, 'trl1brb7g.lx.nc'), "r")
        self.__fid_b8 = h5py.File(os.path.join(ocm_dir, 'trl1brb8g.lx.nc'), "r")

        # initialize public class-attributes
        self.orbit   = -1
        self.num_msm = 0
        self.start_time = self.__fid_b7.attrs['time_coverage_start'].decode('ascii').strip('Z').replace('T',' ')
        self.creator_version = self.__fid_b7.attrs['processor_version'].decode('ascii')

        self.ref_time = ()
        self.delta_time = ()
        self.instrument_settings = None
        self.housekeeping_data = None
        
    def __repr__( self ):
        class_name = type(self).__name__
        return '{}({!r})'.format( class_name, self.__product )

    def __del__( self ):
        if self.__fid_b7 is not None:
            self.__fid_b7.close()
        if self.__fid_b8 is not None:
            self.__fid_b8.close()
    
    # ---------- RETURN VERSION of the S/W ----------
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

    #-------------------------
    def select( self, ic_id ):
        '''
        Parameters
        ----------
        ic_id  :  integer
          used as "BAND%/ICID_{}_GROUP_%".format(ic_id)

        Returns
        -------
        Number of measurements found

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

        Parameters
        ---------- 
        skip_first_frame :  boolean
           skip first frame because its memory effect is unkown. Default is True

        Returns
        -------
        Python dictionary with msm_names as keys and their values

        - these values are stored as a list of ndarrays (one array per band)
        - (float) FillValues are set to NaN
        '''
        FILLVALUE = float.fromhex('0x1.ep+122')
        if skip_first_frame:
            offs = 1
        else:
            offs = 0
            
        res = {}
        val_b7 = None
        err_b7 = None
        if self.__fid_b7 is not None:
            gid = self.__fid_b7['BAND7']
            grp_name = 'ICID_{:05}_GROUP'.format(self.__icid)
            for grp in [s for s in gid.keys() if s.startswith(grp_name)]:
                sgid = gid[os.path.join(grp,'OBSERVATIONS')]
                data = sgid['signal'][offs:,:,:]
                if sgid['signal'].attrs['_FillValue'] == FILLVALUE:
                    data[(data == FILLVALUE)] = np.nan
                (mx, sx) = biweight( data, axis=0, spread=True )
                if val_b7 is None:
                    val_b7 = mx
                    err_b7 = sx
                elif val_b7.shape == mx.shape:
                    val_b7 = np.vstack( (np.expand_dims(val_b7, axis=0),
                                         np.expand_dims(mx, axis=0)) )
                    err_b7 = np.vstack( (np.expand_dims(err_b7, axis=0),
                                         np.expand_dims(sx, axis=0)) )
                else:
                    val_b7 = np.vstack( (val_b7, np.expand_dims(mx, axis=0)) )
                    err_b7 = np.vstack( (err_b7, np.expand_dims(sx, axis=0)) )
               
        val_b8 = None
        err_b8 = None
        if self.__fid_b8 is not None:
            gid = self.__fid_b8['BAND8']
            grp_name = 'ICID_{:05}_GROUP'.format(self.__icid)
            for grp in [s for s in gid.keys() if s.startswith(grp_name)]:
                sgid = gid[os.path.join(grp,'OBSERVATIONS')]
                data = sgid['signal'][offs:,:,:]
                if sgid['signal'].attrs['_FillValue'] == FILLVALUE:
                    data[(data == FILLVALUE)] = np.nan
                (mx, sx) = biweight( data, axis=0, spread=True )
                if val_b8 is None:
                    val_b8 = mx
                    err_b8 = sx
                elif val_b8.shape == mx.shape:
                    val_b8 = np.vstack( (np.expand_dims(val_b8, axis=0),
                                         np.expand_dims(mx, axis=0)) )
                    err_b8 = np.vstack( (np.expand_dims(err_b8, axis=0),
                                         np.expand_dims(sx, axis=0)) )
                else:
                    val_b8 = np.vstack( (val_b8, np.expand_dims(mx, axis=0)) )
                    err_b8 = np.vstack( (err_b8, np.expand_dims(sx, axis=0)) )

        if val_b7 is None and val_b8 is None:
            return res
        elif val_b7 is None:
            res['signal'] = [val_b8]
            res['signal_error'] = [err_b8]
            return res
        elif val_b8 is None:
            res['signal'] = [val_b7]
            res['signal_error'] = [err_b7]
            return res
        else: 
            res['signal'] = [val_b7, val_b8]
            res['signal_error'] = [err_b7, err_b8]
            return res

#--------------------------------------------------
def test2():
    '''
    Perform some simple test to check the OCM_io class
    '''
    import shutil
    
    if os.path.isdir('/Users/richardh'):
        fl_path = '/Users/richardh/Data/'
    elif os.path.isdir('/nfs/TROPOMI/ocal/proc_knmi'):
        fl_path = '/nfs/TROPOMI/ocal/proc_knmi/2015_02_23T01_36_51_svn4709_CellEarth_CH4'
    else:
        fl_path = '/data/richardh/Tropomi/ISRF/2015_02_23T01_36_51_svn4709_CellEarth_CH4'
    ocm_dir = 'after_strayl_l1b_val_SWIR_2'

    fp = OCM_io( os.path.join(fl_path, ocm_dir), verbose=True )
    print( fp )
    if fp.select( 31524 ) > 0:
        print( fp.num_msm )
        print( fp.ref_time )
        print( fp.delta_time.shape )
        res = fp.get_data()
        for key in res.keys():
            print( key, len(res[key]), res[key][0].shape )
    
    del fp

#--------------------------------------------------
def test():
    '''
    Perform some simple test to check the OCM_io class
    '''
    import shutil
    
    if os.path.isdir('/Users/richardh'):
        fl_path = '/Users/richardh/Data/S5P_OCM_CA_SIR/001000/2012/09/19'
    elif os.path.isdir('/nfs/TROPOMI/ocal/proc_knmi'):
        fl_path = '/nfs/TROPOMI/ocal/proc_knmi/2015_05_02T10_28_44_SwirlsSunIsrf'
    else:
        fl_path = '/data/richardh/Tropomi/ISRF/2015_05_02T10_28_44_SwirlsSunIsrf'
    ocm_dir = 'after_et_l1bavg_003_block-003-003'

    fp = OCM_io( os.path.join(fl_path, ocm_dir), verbose=True )
    print( fp )
    if fp.select( 31624 ) > 0:
        print( fp.num_msm )
        print( fp.ref_time )
        print( fp.delta_time.shape )
        res = fp.get_data()
        for key in res.keys():
            print( key, len(res[key]), res[key][0].shape )
    
    del fp

#--------------------------------------------------
if __name__ == '__main__':
    test()
    
