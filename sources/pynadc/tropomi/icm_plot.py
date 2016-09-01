# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
'''
import numpy as np
import h5py

import matplotlib as mpl
mpl.use('TkAgg')
import matplotlib.pyplot as plt

from matplotlib import gridspec
from matplotlib.backends.backend_pdf import PdfPages

from pynadc import extendedrainbow_with_outliers
from pynadc.tropomi.icm_mon_db import ICM_mon

class ICM_plot(object):
    def __init__( self, dbname, res_sql, cmap="Rainbow" ):
        self.__algo_version = res_sql['algVersion'][0]
        self.__db_version   = res_sql['dbVersion'][0]
        self.__icm_version  = res_sql['icmVersion'][0]
        self.__sign_median  = res_sql['dataMedian'][0]
        self.__error_median  = res_sql['errorMedian'][0]
        date = res_sql['startDateTime'][0][0:10].replace('-','')
        orbit = res_sql['referenceOrbit'][0]
        self.__pdf = PdfPages( dbname + '_{}_{:05}.pdf'.format(date, orbit) )
        self.__cmap = cmap
        
    def __repr__( self ):
        pass

    def __del__( self ):
        self.__pdf.close()

    def __icm_info( self, fig ):
        '''
        '''
        from datetime import datetime

        cre_date = datetime.utcnow().isoformat(' ')[0:19]
    
        info_str = 'date : {}'.format(cre_date) \
                   + '\nicm_version   : {}'.format(self.__icm_version) \
                   + '\nalgo_version  : {}'.format(self.__algo_version) \
                   + '\ndb_version    : {}'.format(self.__db_version) \
                   + '\nsignal_median : {:.3f}'.format(self.__sign_median) \
                   + '\nerror_median  : {:.3f}'.format(self.__error_median)
    
        fig.text( 0.015, 0.075, info_str,
                  verticalalignment='bottom', horizontalalignment='left',
                  bbox={'facecolor':'white', 'pad':10},
                  fontsize=10, style='italic' )
        fig.text( 0.025, 0.875,
                  r'$\copyright$ SRON, Netherlands Institute for Space Research' )

    def __frame( self, data_label, mon, signal, signal_col, signal_row ):
        '''
        '''
        fig = plt.figure(figsize=(24,10.5)) 
        fig.suptitle( mon.h5_get_attr('title' ), fontsize=24 )
        gs = gridspec.GridSpec(6,16) 

        ax0 = plt.subplot(gs[1:4,0:2])
        ax0.plot(signal_col, np.arange(signal_col.size))
        ax0.set_xlabel( data_label, fontsize=18 )
        ax0.set_ylim( [0, signal_col.size-1] )
        ax0.locator_params(axis='x', nbins=3)
        ax0.set_ylabel( 'row', fontsize=18 )

        ax1 = plt.subplot(gs[1:4,2:14])
        (p_10, p_90) = np.percentile( signal[np.isfinite(signal)], (10,90) )
        ax1.imshow( signal, cmap=self.__cmap, aspect=1, vmin=p_10, vmax=p_90,
                    interpolation='none', origin='lower' )
        ax1.set_title( mon.h5_get_attr('comment' ) )

        ax3 = plt.subplot(gs[4:6,2:14])
        ax3.plot(np.arange(signal_row.size), signal_row)
        ax3.set_ylabel( data_label, fontsize=18 )
        ax3.set_xlim( [0, signal_row.size-1] )
        ax3.set_xlabel( 'column', fontsize=18 )

        ax4 = plt.subplot(gs[1:6,14])
        norm = mpl.colors.Normalize(vmin=p_10, vmax=p_90)
        cb1 = mpl.colorbar.ColorbarBase( ax4, cmap=self.__cmap, norm=norm,
                                         orientation='vertical' )
        cb1.set_label( 'electron / s', fontsize=18 )

        self.__icm_info( fig )
        plt.tight_layout()
        self.__pdf.savefig()
        plt.close()


    def draw_signal( self, mon, signal, signal_col, signal_row ):
        '''
        '''
        self.__frame( 'signal', mon, signal, signal_col, signal_row )

    def draw_errors( self, mon, signal, signal_col, signal_row ):
        '''
        '''
        self.__frame( 'errors', mon, signal, signal_col, signal_row )

    def __hist( self ):
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
    
## 
## --------------------------------------------------
## 
def test():
    import os
    
    DBNAME = 'mon_background_0005_test'
        
    mon = ICM_mon( DBNAME, mode='r' )
    orbit = mon.get_orbit_latest()
    print( orbit )
    res_sql = mon.sql_select_orbit( orbit, full=True )
    print( res_sql )
    res_h5 = mon.h5_read_frames( res_sql['rowID'][0],
                                 statistics='error,rows,cols' )
    print( res_h5.keys() )
    for name in res_h5.keys():
        print( name, res_h5[name].shape )

    ## plot latest entry
    plot = ICM_plot( DBNAME, res_sql )
    plot.draw_signal( mon, res_h5['signal'],
                      res_h5['signal_col'], res_h5['signal_row'] )
    plot.draw_errors( mon, res_h5['signal_std'],
                      res_h5['signal_col_std'], res_h5['signal_row_std'] )
    del(mon)
    del(plot)
    ## read data of penultimate entry
    ## plot difference with penultimate result
    ## read data for time-series
    ## plot time-series
#--------------------------------------------------
if __name__ == '__main__':
    test()
