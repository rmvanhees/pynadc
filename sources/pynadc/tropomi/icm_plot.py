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
        self.__sign_scale   = res_sql['dataScale'][0]
        self.__error_median = res_sql['errorMedian'][0]
        self.__error_scale  = res_sql['errorScale'][0]
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
                   + '\nsignal_scale  : {:.3f}'.format(self.__sign_scale) \
                   + '\nerror_median  : {:.3f}'.format(self.__error_median) \
                   + '\nerror_scale   : {:.3f}'.format(self.__error_scale)
    
        fig.text( 0.015, 0.075, info_str,
                  verticalalignment='bottom', horizontalalignment='left',
                  bbox={'facecolor':'white', 'pad':10},
                  fontsize=8, style='italic' )
        fig.text( 0.025, 0.875,
                  r'$\copyright$ SRON, Netherlands Institute for Space Research' )

    def __frame( self, signal, signal_col, signal_row,
                 title=None, sub_title=None, data_label=None, data_unit=None ):
        '''
        '''
        print( data_unit )
        (p_10, p_90) = np.percentile( signal[np.isfinite(signal)], (10,90) )
        max_value = max(abs(p_10), abs(p_90))
        if max_value > 1000000000:
            signal /= 1000000000
            signal_col /= 1000000000
            signal_row /= 1000000000
            p_10 /= 1000000000
            p_90 /= 1000000000
            label = '{} [{}]'.format(data_label,
                                     data_unit.replace('electron', 'Ge'))
        elif max_value > 1000000:
            signal /= 1000000
            signal_col /= 1000000
            signal_row /= 1000000
            p_10 /= 1000000
            p_90 /= 1000000
            label = '{} [{}]'.format(data_label,
                                     data_unit.replace('electron', 'me'))
        elif max_value > 1000:
            signal /= 1000
            signal_col /= 1000
            signal_row /= 1000
            p_10 /= 1000
            p_90 /= 1000
            label = '{} [{}]'.format(data_label,
                                     data_unit.replace('electron', 'ke'))
        else:
            label = '{} [{}]'.format(data_label,
                                     data_unit.replace('electron', 'e'))
        
        fig = plt.figure(figsize=(18, 7.875)) 
        if title is not None:
            fig.suptitle( title, fontsize=24 )
        gs = gridspec.GridSpec(6,16) 

        ax0 = plt.subplot(gs[1:4,0:2])
        ax0.plot(signal_col, np.arange(signal_col.size))
        ax0.set_xlabel( label )
        ax0.set_ylim( [0, signal_col.size-1] )
        ax0.locator_params(axis='x', nbins=3)
        ax0.set_ylabel( 'row' )

        ax1 = plt.subplot(gs[1:4,2:14])
        ax1.imshow( signal, cmap=self.__cmap, aspect=1, vmin=p_10, vmax=p_90,
                    interpolation='none', origin='lower' )
        if sub_title is not None:
            ax1.set_title( sub_title )

        ax3 = plt.subplot(gs[4:6,2:14])
        ax3.plot(np.arange(signal_row.size), signal_row)
        ax3.set_ylabel( label )
        ax3.set_xlim( [0, signal_row.size-1] )
        ax3.set_xlabel( 'column' )

        ax4 = plt.subplot(gs[1:6,14])
        norm = mpl.colors.Normalize(vmin=p_10, vmax=p_90)
        cb1 = mpl.colorbar.ColorbarBase( ax4, cmap=self.__cmap, norm=norm,
                                         orientation='vertical' )
        if data_unit is not None:
            cb1.set_label( label )

        self.__icm_info( fig )
        plt.tight_layout()
        self.__pdf.savefig()
        #plt.close()


    def __hist( self, mon, res_sql, signal, signal_std, num_sigma=3 ):
        '''
        '''
        fig = plt.figure(figsize=(18, 7.875))
        fig.suptitle( mon.h5_get_attr('title' ), fontsize=24 )
        gs = gridspec.GridSpec(6,8) 

        buff = np.copy(signal).reshape(-1)
        buff = buff[np.isfinite(buff)] - res_sql['dataMedian'][0]
        ax0 = plt.subplot(gs[1:3,1:])
        ax0.hist( buff, range=[-num_sigma * res_sql['dataScale'][0],
                               num_sigma * res_sql['dataScale'][0]], bins=15 )
        ax0.set_title( r'Histogram is centered at the median with range of $\pm 3 \sigma$' )
        ax0.set_ylabel( 'count' )
        ax0.set_xlabel( 'signal' )

        buff = np.copy(signal_std).reshape(-1)
        buff = buff[np.isfinite(buff)] - res_sql['errorMedian'][0]
        ax1 = plt.subplot(gs[3:5,1:])
        ax1.hist( buff, range=[-num_sigma * res_sql['errorScale'][0],
                               num_sigma * res_sql['errorScale'][0]], bins=15 )
        ax1.set_ylabel( 'count' )
        ax1.set_xlabel( 'error' )

        self.__icm_info( fig )
        plt.tight_layout()
        self.__pdf.savefig()
        #plt.close()
    
    def __dpqm( self, mon, res_sql, dpqm, low_thres=0.1, high_thres=0.8,
                cmap="RainbowBands" ):
        '''
        '''
        fig = plt.figure(figsize=(18, 7.875)) 
        fig.suptitle( mon.h5_get_attr('title' ), fontsize=24 )
        gs = gridspec.GridSpec(6,16) 

        dpqf_col_01 = np.sum( (dpqm <= low_thres), axis=1 )
        dpqf_col_08 = np.sum( (dpqm <= high_thres), axis=1 )
        
        ax0 = plt.subplot(gs[1:4,0:2])
        ax0.step(dpqf_col_01, np.arange(dpqf_col_01.size), 'r-' )
        ax0.step(dpqf_col_08, np.arange(dpqf_col_08.size), 'b-' )
        ax0.set_xlim( [15, 45] )
        ax0.set_xlabel( 'bad (count)' )
        ax0.set_ylim( [0, dpqf_col_01.size-1] )
        ax0.set_ylabel( 'row' )
        ax0.grid(True)

        dpqf = np.copy(dpqm)
        dpqf[np.where( (dpqm == .0) )] = 1.
        dpqf[np.where( (dpqm > 0.) & (dpqm <= low_thres) )] = 0.8
        dpqf[np.where( (dpqm > low_thres) & (dpqm <= high_thres) )] = 0.5
        dpqf[np.where( (dpqm > high_thres) )] = 0.

        ax1 = plt.subplot(gs[1:4,2:14])
        ax1.imshow( dpqf, cmap=cmap, aspect=1, vmin=0, vmax=1,
                    interpolation='none', origin='lower' )

        dpqf_row_01 = np.sum( (dpqm <= low_thres), axis=0 )
        dpqf_row_08 = np.sum( (dpqm <= high_thres), axis=0 )
        
        ax3 = plt.subplot(gs[4:6,2:14])
        ax3.step(np.arange(dpqf_row_01.size), dpqf_row_01, 'r-')
        ax3.step(np.arange(dpqf_row_08.size), dpqf_row_08, 'b-')
        ax3.set_ylim( [0, 10] )
        ax3.set_ylabel( 'bad (count)' )
        ax3.set_xlim( [0, dpqf_row_01.size-1] )
        ax3.set_xlabel( 'column' )
        ax3.grid(True)

        plt.tight_layout()
        self.__pdf.savefig()
        
    def draw_signal( self, mon, data, data_col, data_row ):
        '''
        '''
        ds_name = 'signal'
        self.__frame( data, data_col, data_row ,
                      title=mon.h5_get_attr('title' ),
                      sub_title=mon.h5_get_attr('comment' ),
                      data_label=ds_name,
                      data_unit=mon.h5_get_frame_attr(ds_name, 'units') )

    def draw_errors( self, mon, data, data_col, data_row ):
        '''
        '''
        ds_name = 'signal_{}'.format(mon.get_method())
        self.__frame( data, data_col, data_row,
                      title=mon.h5_get_attr('title' ),
                      sub_title=mon.h5_get_attr('comment' ),
                      data_label=ds_name,
                      data_unit=mon.h5_get_frame_attr(ds_name, 'units') )

    def draw_hist( self, mon, res_sql, signal, signal_std ):
        '''
        '''
        self.__hist( mon, res_sql, signal, signal_std )

    def draw_dpqm( self, mon, res_sql, dpqm ):
        '''
        '''
        self.__dpqm( mon, res_sql, dpqm )
## 
## --------------------------------------------------
## 
def test_dpqm( ):
    import os
    
    dpqm_fl='/nfs/TROPOMI/ocal/ckd/ckd_release_swir/dpqf/ckd.dpqf.detector4.nc'
    with h5py.File( dpqm_fl, 'r' ) as fid:
        b7 = fid['BAND7/dpqf_map'][:-1,:]
        b8 = fid['BAND8/dpqf_map'][:-1,:]
        dpqm = np.hstack( (b7, b8) )

    DBNAME = 'mon_background_0005_test'
        
    mon = ICM_mon( DBNAME, mode='r' )
    orbit = mon.get_orbit_latest()
    print( orbit )
    res_sql = mon.sql_select_orbit( orbit, full=True )
    print( res_sql )

    plot = ICM_plot( DBNAME, res_sql )
    plot.draw_dpqm( mon, res_sql, dpqm )
    del(plot)
    del(mon)
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
    plot.draw_hist( mon, res_sql, res_h5['signal'], res_h5['signal_std'] )
    del(mon)
    del(plot)
    ## read data of penultimate entry
    ## plot difference with penultimate result
    ## read data for time-series
    ## plot time-series
#--------------------------------------------------
if __name__ == '__main__':
    test()
