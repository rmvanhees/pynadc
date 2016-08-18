# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

"""Implement some slighty fancier statistics than numpy offers."""

from __future__ import division

import numpy as np

def biweight(data, axis=None, scale=False):
    """
    Calculate Tukey's biweight.
    Implementation based on Eqn. 7.6 and 7.7 in the SWIR OCAL ATBD.

    """
    if axis is None:
        xx = data[np.isfinite(data)]
        mm = np.median(xx)
        deltas = xx - mm
        dd = np.median(np.abs(deltas))
        if dd == 0:
            xb = mm
            if scale:
                sb = 0.
        else:
            w = np.maximum(0, 1 - (deltas / (6 * dd)) ** 2) ** 2
            xb = mm + np.sum(w * deltas) / np.sum(w)
            if scale:
                u = np.minimum(1, (deltas / (9 * dd)) ** 2)
                sb = np.sqrt(len(xx) * np.sum(deltas ** 2 * ( 1 - u) ** 4)
                             / np.sum((1 - u) *  (1 - 5 * u)) ** 2)
    else:
        mm = np.nanmedian(data, axis=axis)
        xb = mm
        sb = np.zeros( mm.shape, dtype=np.float64 )

        deltas = data - np.expand_dims( mm, axis=axis )

        dd = np.nanmedian(np.abs(deltas), axis=axis)        
        indices = (dd == 0)
        if not np.all(indices):
            dd[indices] = 1.  # dummy value
            dd = np.expand_dims(dd, axis=axis)
            w = np.maximum(0, 1 - (deltas / (6 * dd)) ** 2) ** 2
            xb[~indices] = (mm + np.sum(w * deltas, axis=axis)
                            / np.sum(w, axis=axis))[~indices]
            if scale:
                u = np.minimum(1, (deltas / (9 * dd)) ** 2)
                len_xx = np.sum(np.isfinite(data), axis=axis)
                sb[~indices] = np.sqrt(len_xx
                                       * np.sum(deltas ** 2
                                                * ( 1 - u) ** 4, axis=axis)
                                       / np.sum((1 - u) *  (1 - 5 * u), axis=axis) ** 2)[~indices]

    if scale:
        return (xb, sb)
    else:
        return xb

#--------------------------------------------------
def test():
    import os.path
    import h5py
    import matplotlib.pyplot as plt

    light_icid = 32096
    if os.path.isdir('/Users/richardh'):
        ocal_dir = '/Users/richardh/Data/proc_raw'
    else:
        ocal_dir = '/nfs/TROPOMI/ocal/proc_raw'
    data_dir = os.path.join( ocal_dir,
                             '2015_02_25T05_16_36_LaserDiodes_LD1_100',
                             'proc_raw' )
    data_fl = 'trl1brb7g.lx.nc'

    with h5py.File( os.path.join(data_dir, data_fl), 'r' ) as fid:
        path = 'BAND{}/ICID_{}_GROUP_00000'.format(7, light_icid-1)
        dset = fid[path + '/OBSERVATIONS/signal']
        frames = dset[1:,:,:]
            
    (background, background_std) = biweight( frames,
                                             axis=2, scale=True )
    print( background.shape, background_std.shape )
    (background, background_std) = biweight( frames,
                                             axis=1, scale=True )
    print( background.shape, background_std.shape )
    (background, background_std) = biweight( frames,
                                             axis=0, scale=True )
    print( background.shape, background_std.shape )
    print( np.amin( background ), np.amax( background ) )
    print( np.amin( background_std ), np.amax( background_std ) )
    
    check_xb = np.empty_like( background )
    check_sb = np.empty_like( background )
    for yy in range(check_xb.shape[0]):
        for xx in range(check_xb.shape[1]):
            res = biweight( frames[:,yy,xx], scale=True )
            check_xb[yy, xx] = res[0]
            check_sb[yy, xx] = res[1]

    print( check_xb.shape, check_sb.shape )
    print( np.nanmin( check_xb ), np.nanmax( check_xb ) )
    print( np.nanmin( check_sb ), np.nanmax( check_sb ) )
    print( np.nanmax( np.abs( background - check_xb ) ),
           np.nanmax( np.abs( background_std - check_sb ) ) )

    fig = plt.figure( figsize=(15,10) )
    a1 = fig.add_subplot(221)
    im = plt.imshow( background, 
                     interpolation='none', origin='lower',
                     cmap=plt.get_cmap('gnuplot2'), aspect='auto' )
    plt.colorbar( im, orientation='vertical' )
    a2 = fig.add_subplot(222)
    im = plt.imshow( background_std, vmin=0., vmax=10,
                     interpolation='none', origin='lower',
                     cmap=plt.get_cmap('gnuplot2'), aspect='auto' )
    plt.colorbar( im, orientation='vertical' )
    a3 = fig.add_subplot(223)
    im = plt.imshow( np.abs( background - check_xb ), vmin=0., vmax=1e-4, 
                     interpolation='none', origin='lower',
                     cmap=plt.get_cmap('gnuplot2'), aspect='auto' )
    plt.colorbar( im, orientation='vertical' )
    a4 = fig.add_subplot(224)
    im = plt.imshow( np.abs( background_std - check_sb), vmin=0., vmax=1e-5,
                     interpolation='none', origin='lower',
                     cmap=plt.get_cmap('gnuplot2'), aspect='auto' )
    plt.colorbar( im, orientation='vertical' )
    plt.show()

#--------------------------------------------------
if __name__ == '__main__':
    test()
