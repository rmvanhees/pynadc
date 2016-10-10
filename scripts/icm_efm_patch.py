'''
This file is part of pynadc

https://github.com/rmvanhees/pynadc

.. ADD DESCRIPTION ..


Copyright (c) 2016 SRON - Netherlands Institute for Space Research 
   All Rights Reserved

License:  Standard 3-clause BSD

'''
import os
import shutil

import numpy as np
import h5py

ORBIT_SPECIAL=1500

def delta_time2date( time, delta_time, iso=False ):
    '''
    convert S5p time/delta_time to SQL date-time string
    '''
    from datetime import datetime, timedelta

    dt = datetime(2010,1,1,0,0,0) \
         + timedelta(seconds=int(time)) \
         + timedelta(milliseconds=int(delta_time))
    if iso:
        return np.string_(dt.isoformat()[0:19] + 'Z')
    else:
        return dt.strftime('%Y%m%dT%H%M%S')

def get_orbit_time( h5_name ):
    global ORBIT_SPECIAL
    
    orbit = -1
    delta_time_mn = 2**32-1
    delta_time_mx = -delta_time_mn
    ref_time = 0

    dir_name = os.path.dirname( h5_name )
    icm_name = os.path.basename( h5_name )
    shutil.copy( h5_name, h5_name + ".patch" )
    
    with h5py.File( h5_name + ".patch", 'r+' ) as fid:
        orbit = fid.attrs['reference_orbit']
        if orbit == 65535:
            orbit = ORBIT_SPECIAL
            fid.attrs['reference_orbit'] = orbit
            ORBIT_SPECIAL += 1
        offs_time = (orbit - 1900) * 5760000
        
        grp_list = ['BAND7_CALIBRATION', 'BAND8_CALIBRATION',
                    'BAND7_IRRADIANCE', 'BAND8_IRRADIANCE',
                    'BAND7_RADIANCE', 'BAND8_RADIANCE']
        for grp in grp_list:
            if not grp in fid.keys():
                continue
            
            gid = fid[grp]
            for sgrp in gid.keys():
                ref_time = gid[ sgrp + '/OBSERVATIONS']['time'][0]
                delta_time = gid[ sgrp + '/OBSERVATIONS']['delta_time'][0,:]
                
                delta_time += offs_time
                gid[ sgrp + '/OBSERVATIONS']['delta_time'][0,:] = delta_time
                if delta_time[0] < delta_time_mn:
                    delta_time_mn = delta_time[0]
                if delta_time[-1] > delta_time_mx:
                    delta_time_mx = delta_time[-1]

        fid.attrs['time_coverage_start'] = \
                            delta_time2date(ref_time, delta_time_mn, iso=True)
        fid.attrs['time_coverage_end'] = \
                            delta_time2date(ref_time, delta_time_mx, iso=True)
    icm_patch = '{}_{}_{}_{:05}_{}'.format(icm_name[0:19],
                                      delta_time2date(ref_time, delta_time_mn),
                                      delta_time2date(ref_time, delta_time_mx),
                                      orbit, 
                                      icm_name[58:]) 
    shutil.move( h5_name + ".patch",
                 os.path.join(dir_name, icm_patch) )
    print( icm_patch )
##
##--------------------------------------------------
##
if __name__ == '__main__':
    import sys
    from subprocess import run
    
    base_dir='/nfs/TROPOMI/ical/test_data/efm_e1_test_orbits'
    patch_dir='/data/richardh/Tropomi/efm_e1_test_orbits_patch'
    if not os.path.exists(base_dir):
        print( '*** Fatal directory {} does not exist'.format(base_dir) )
        sys.exit(1)
    
    if not os.path.exists(patch_dir):
        os.makedirs(patch_dir)
    run(["rsync", "-a", "--delete-after", base_dir+'/', patch_dir],
        check=True)
    
    dirList = os.listdir(patch_dir)
    dirList.sort()
    for sub_dir in dirList:
        h5_dir = os.path.join(patch_dir, sub_dir)
        fileList = [s for s in os.listdir(h5_dir) if s.endswith('.h5')]
        for h5_name in fileList:
            print( os.path.join(h5_dir, h5_name) )
            get_orbit_time( os.path.join(h5_dir, h5_name) )
