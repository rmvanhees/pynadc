# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
'''
import numpy as np
import h5py

import matplotlib as mpl
import matplotlib.pyplot as plt

from matplotlib import gridspec
from matplotlib.backends.backend_pdf import PdfPages

from pynadc import extendedrainbow_with_outliers
from pynadc.tropomi.icm_mon_db import ICM_mon

cmap = "Rainbow"

def plot_icm_info( fig, mon ):
    '''
    '''
    from datetime import datetime

    cre_date = datetime.utcnow().isoformat(' ')[0:19]
    
    info_str = 'date : {}'.format(cre_date) \
               + '\nicm_version  : {}'.format(mon.h5_get_attr('icm_version')) \
               + '\nalgo_version : {}'.format(mon.h5_get_attr('algo_version')) \
               + '\ndb_version   : {}'.format(mon.h5_get_attr('db_version'))
    
    fig.text( 0.025, 0.225, info_str,
              verticalalignment='top', horizontalalignment='left',
              bbox={'facecolor':'white', 'pad':10},
              fontsize=10, style='italic' )
    fig.text( 0.025, 0.875,
              r'$\copyright$ {}'.format(mon.h5_get_attr('institute')) )
    
    
def plot_icm_data( pdf, mon, signal, signal_col, signal_row ):
    '''
    '''
    fig = plt.figure(figsize=(24,10.5)) 
    fig.suptitle( mon.h5_get_attr('title' ), fontsize=24 )
    gs = gridspec.GridSpec(6,16) 

    ax0 = plt.subplot(gs[1:4,0:2])
    ax0.plot(signal_row, np.arange(signal_row.size))
    ax0.set_ylim( [0, signal_row.size-1] )
    ax0.locator_params(axis='x', nbins=3)
    ax0.set_xlabel( 'signal', fontsize=18 )
    ax0.set_ylabel( 'row', fontsize=18 )

    ax1 = plt.subplot(gs[1:4,2:14])
    ax1.imshow( signal, cmap=cmap, aspect=1,
                interpolation='none', origin='lower' )
    ax1.set_title( mon.h5_get_attr('comment' ) )

    ax3 = plt.subplot(gs[4:6,2:14])
    ax3.plot(np.arange(signal_col.size), signal_col)
    ax3.set_ylabel( 'signal', fontsize=18 )
    ax3.set_xlabel( 'column', fontsize=18 )

    ax4 = plt.subplot(gs[1:6,14])
    norm = mpl.colors.Normalize(vmin=-1, vmax=1)
    cb1 = mpl.colorbar.ColorbarBase( ax4, cmap=cmap, norm=norm,
                                     orientation='vertical' )
    cb1.set_label( 'electron / s', fontsize=18 )

    plot_icm_info( fig, mon )
    plt.tight_layout()
    pdf.savefig()
    plt.close()

def plot_icm_hist():
    '''
    '''
    fig = plt.figure(figsize=(24,10.5))
    gs = gridspec.GridSpec(6,16) 

    ax0 = plt.subplot(gs[1:4,0:8])
    ax0.hist( signal.reshape(-1), 20 )
    ax0.set_ylabel( 'count', fontsize=18 )
    ax0.set_xlabel( 'signal', fontsize=18 )

    ax1 = plt.subplot(gs[1:4,8:16])
    ax1.hist( signal_std.reshape(-1), 20 )
    ax1.set_ylabel( 'count', fontsize=18 )
    ax1.set_xlabel( 'error', fontsize=18 )

    ax3 = plt.subplot(gs[4:6,2:16])
    ax3.plot(15 * np.arange(signal_col_std.size), signal_col_std)
    ax3.set_xlim( [0, np.max(15 * np.arange(signal_col_std.size))] )
    ax3.set_ylabel( 'average signal', fontsize=18 )
    ax3.set_xlabel( 'orbit', fontsize=18 )

    fig.text( 0.025, 0.875, r'$\copyright$ {}'.format(institute) )
    plt.tight_layout()
    pdf.savefig()
    plt.close()
    
## read data of latest entry
def test():
    DBNAME = 'mon_dark'
    mon = ICM_mon( DBNAME, mode='r' )
    orbit = mon.get_orbit_latest()
    print( orbit )
    res_sql = mon.sql_select_orbit( orbit, full=True )
    print( res_sql )
    res_h5 = mon.h5_read_frames( res_sql['rowID'][0], statistics='rows,cols' )
    print( res_h5.keys() )

    ## plot latest entry
    with PdfPages( DBNAME + '_{}.pdf'.format(orbit) ) as pdf:
        plot_icm_data( pdf, mon, res_h5['signal'],
                       res_h5['signal_col'], res_h5['signal_row'] )

    ## read data of penultimate entry
    ## plot difference with penultimate result
    ## read data for time-series
    ## plot time-series
#--------------------------------------------------
if __name__ == '__main__':
    test()
