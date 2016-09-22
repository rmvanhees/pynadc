# (c) SRON - Netherlands Institute for Space Research (2016).
# All Rights Reserved.
# This software is distributed under the BSD 2-clause license.

'''
Methods to create PDF plots from SRON monitoring data

-- generate figures --
 Public functions a page in the output PDF
 * draw_signal
 * draw_errors
 * draw_hist
 * draw_quality
'''
import numpy as np
import h5py

import matplotlib as mpl

from pynadc.tropomi.icm_mon_db import ICM_mon

mpl.use('TkAgg')
'''
Alternative Scheme for Qualitative Data by Paul Tol (SRON)

Palette of colour-blind safe, print-friendly colours for qualitative data

Reference:  https://personal.sron.nl/~pault
'''
LINE_COLORS=('#4477AA',   # blue
             '#66CCEE',   # cyan
             '#228833',   # green
             '#CCBB44',   # yellow
             '#EE6677',   # red
             '#AA3377',   # pink
             '#BBBBBB',   # grey
             '#000000')   # black

class ICM_plot(object):
    '''
    Generate figure(s) for the SRON Tropomi SWIR monitor website or MPC reports

    Usage:
    1) Generate an ICM_plot object and access to an PdfPages object
    2) Add pages to the PdfPages object, using ICM_plot method
        * draw_signal
        * draw_errors
        * draw_hist
        * draw_quality
    3) close the ICM_plot object

    The PDF will have the following name:
        <dbname>_<startDateTime of monitor entry>_<orbit of monitor entry>.pdf
    '''
    def __init__( self, dbname, res_sql, cmap="Rainbow" ):
        from matplotlib.backends.backend_pdf import PdfPages

        self.__algo_version = res_sql['algVersion'][0]
        self.__db_version   = res_sql['dbVersion'][0]
        self.__icm_version  = res_sql['icmVersion'][0]
        self.__sign_median  = res_sql['dataMedian'][0]
        self.__sign_spread  = res_sql['dataSpread'][0]
        self.__error_median = res_sql['errorMedian'][0]
        self.__error_spread = res_sql['errorSpread'][0]
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
                   + '\nsignal_spread : {:.3f}'.format(self.__sign_spread) \
                   + '\nerror_median  : {:.3f}'.format(self.__error_median) \
                   + '\nerror_spread  : {:.3f}'.format(self.__error_spread)
    
        fig.text( 0.015, 0.075, info_str,
                  verticalalignment='bottom', horizontalalignment='left',
                  bbox={'facecolor':'white', 'pad':10},
                  fontsize=8, style='italic' )
        fig.text( 0.025, 0.875,
                  r'$\copyright$ SRON Netherlands Institute for Space Research' )

    def __frame( self, signal_in, signal_col_in, signal_row_in,
                 title=None, sub_title=None, data_label=None, data_unit=None ):
        '''
        '''
        from matplotlib import pyplot as plt
        from matplotlib import gridspec

        from pynadc import extendedrainbow_with_outliers
       
        signal = np.copy(signal_in)
        signal_col = np.copy(signal_col_in)
        signal_row = np.copy(signal_row_in)
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
        ax0.plot(signal_col, np.arange(signal_col.size),
                 lw=0.5, color=LINE_COLORS[0])
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
        ax3.plot(np.arange(signal_row.size), signal_row,
                 lw=0.5, color=LINE_COLORS[0])
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
        plt.close()

    def __hist( self, signal_in, error_in, num_sigma=3,
                title=None, data_label=None, data_unit=None,
                error_label=None, error_unit=None ):
        '''
        '''
        from matplotlib import pyplot as plt
        from matplotlib import gridspec
        
        buff = np.copy(signal_in).reshape(-1)
        buff = buff[np.isfinite(buff)] - self.__sign_median
        buff_std = np.copy(error_in).reshape(-1)
        buff_std = buff_std[np.isfinite(buff_std)] - self.__error_median

        d_label = '{} [{}]'.format(data_label,
                                   data_unit.replace('electron', 'e'))
        e_label = '{} [{}]'.format(error_label,
                                   error_unit.replace('electron', 'e'))

        fig = plt.figure(figsize=(18, 7.875))
        if title is not None:
            fig.suptitle( title, fontsize=24 )
        gs = gridspec.GridSpec(6,8) 

        ax0 = plt.subplot(gs[1:3,1:])
        ax0.hist( buff, range=[-num_sigma * self.__sign_spread,
                               num_sigma * self.__sign_spread], 
                  bins=15, color=LINE_COLORS[0] )
        ax0.set_title( r'Histogram is centered at the median with range of ' \
                       r'$\pm 3 \sigma$' )
        ax0.set_xlabel( d_label )
        ax0.set_ylabel( 'count' )

        ax1 = plt.subplot(gs[3:5,1:])
        ax1.hist( buff_std, range=[-num_sigma * self.__error_spread,
                                   num_sigma * self.__error_spread],
                  bins=15, color=LINE_COLORS[0] )
        ax1.set_xlabel( e_label )
        ax1.set_ylabel( 'count' )

        self.__icm_info( fig )
        plt.tight_layout()
        self.__pdf.savefig()
        plt.close()
    
    def __quality( self, dpqm, low_thres=0.1, high_thres=0.8,
                   title=None, cmap=None ):
        '''
        '''
        from matplotlib import pyplot as plt
        from matplotlib import gridspec
        
        thres_min = 10 * low_thres
        thres_max = 10 * high_thres
        dpqf = (dpqm * 10).astype(np.byte)
        unused_cols = np.where(np.sum(dpqm, axis=0) < (256 // 4))
        if unused_cols[0].size > 0:
            dpqf[:,unused_cols[0]] = -1
        unused_rows = np.where(np.sum(dpqm, axis=1) < (1000 // 4))
        if unused_rows[0].size > 0:
            dpqf[:,unused_rows[0]] = -1

        fig = plt.figure(figsize=(18, 7.875))
        if title is not None:
            fig.suptitle( title, fontsize=24 )
        gs = gridspec.GridSpec(6,16) 

        clist = ['#BBBBBB', '#EE6677','#CCBB44','w']
        cmap = mpl.colors.ListedColormap(clist)
        bounds=[-1, 0, thres_min, thres_max, 10]
        norm = mpl.colors.BoundaryNorm(bounds, cmap.N)

        dpqf_col_01 = np.sum( ((dpqf >= 0) & (dpqf < thres_min)), axis=1 )
        dpqf_col_08 = np.sum( ((dpqf >= 0) & (dpqf < thres_max)), axis=1 )
        
        ax0 = plt.subplot(gs[1:4,0:2])
        ax0.step(dpqf_col_08, np.arange(dpqf_col_08.size),
                 lw=0.5, color=clist[2] )
        ax0.step(dpqf_col_01, np.arange(dpqf_col_01.size),
                 lw=0.6, color=clist[1] )
        ax0.set_xlim( [0, 30] )
        ax0.set_xlabel( 'bad (count)' )
        ax0.set_ylim( [0, dpqf_col_01.size-1] )
        ax0.set_ylabel( 'row' )
        ax0.grid(True)

        ax1 = plt.subplot(gs[1:4,2:14])
        ax1.imshow( dpqf, cmap=cmap, norm=norm,
                    aspect=1, vmin=-1, vmax=10,
                    interpolation='none', origin='lower' )

        dpqf_row_01 = np.sum( ((dpqf >= 0) & (dpqf < thres_min)), axis=0 )
        dpqf_row_08 = np.sum( ((dpqf >= 0) & (dpqf < thres_max)), axis=0 )
        
        ax3 = plt.subplot(gs[4:6,2:14])
        ax3.step(np.arange(dpqf_row_08.size), dpqf_row_08,
                 lw=0.5, color=clist[2] )
        ax3.step(np.arange(dpqf_row_01.size), dpqf_row_01,
                 lw=0.6, color=clist[1] )
        ax3.set_ylim( [0, 10] )
        ax3.set_ylabel( 'bad (count)' )
        ax3.set_xlim( [0, dpqf_row_01.size-1] )
        ax3.set_xlabel( 'column' )
        ax3.grid(True)

        plt.tight_layout()
        self.__pdf.savefig()
        plt.close()
        
    def draw_signal( self, mon, data, data_col, data_row ):
        '''
        '''
        from matplotlib import pyplot as plt

        ds_name = 'signal'
        self.__frame( data, data_col, data_row ,
                      title=mon.h5_get_attr('title' ),
                      sub_title=r'{}'.format(mon.h5_get_attr('comment' )),
                      data_label=ds_name,
                      data_unit=mon.h5_get_frame_attr(ds_name, 'units') )
        
    def draw_errors( self, mon, data, data_col, data_row ):
        '''
        '''
        ds_name = 'signal_{}'.format(mon.get_method())
        self.__frame( data, data_col, data_row,
                      title=mon.h5_get_attr('title' ),
                      sub_title=r'{}'.format(mon.h5_get_attr('comment' )),
                      data_label=ds_name,
                      data_unit=mon.h5_get_frame_attr(ds_name, 'units') )

    def draw_hist( self, mon, signal, error ):
        '''
        '''
        data_label = 'signal'
        error_label = 'signal_{}'.format(mon.get_method())
        self.__hist( signal, error, title=mon.h5_get_attr('title'),
                     data_label=data_label, error_label=error_label,
                     data_unit=mon.h5_get_frame_attr(data_label, 'units'),
                     error_unit=mon.h5_get_frame_attr(error_label, 'units') )

    def draw_quality( self, mon, dpqm ):
        '''
        '''
        self.__quality( dpqm, title=mon.h5_get_attr('title') )
## 
## --------------------------------------------------
## 
def test_dpqm( ):
    '''
    '''
    import os
    
    if os.path.isdir('/Users/richardh'):
        data_dir = '/Users/richardh/Data'
    else:
        data_dir ='/nfs/TROPOMI/ocal/ckd/ckd_release_swir/dpqf' 
    dpqm_fl=os.path.join(data_dir, 'ckd.dpqf.detector4.nc')
    
    with h5py.File( dpqm_fl, 'r' ) as fid:
        b7 = fid['BAND7/dpqf_map'][:-1,:]
        b8 = fid['BAND8/dpqf_map'][:-1,:]
        dpqm = np.hstack( (b7, b8) )

    DBNAME = 'mon_ocal_ckd_dpqf_test'
        
    mon = ICM_mon( DBNAME )
    plot = ICM_plot( DBNAME, res_sql )
    plot.draw_quality( mon, dpqm )
    del(plot)
    del(mon)
## 
## --------------------------------------------------
## 
def test():
    '''
    '''
    DBNAME = 'mon_sun_isrf_test'
        
    mon = ICM_mon( DBNAME )
    orbit = mon.get_orbit_latest()
    print( orbit )
    res_sql = mon.sql_select_orbit( orbit, full=True )
    print( res_sql )
    res_h5 = mon.h5_read_frame( res_sql['rowID'][0],
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
    plot.draw_hist( mon, res_h5['signal'], res_h5['signal_std'] )
    del(plot)
    del(mon)
    ## read data of penultimate entry
    ## plot difference with penultimate result
    ## read data for time-series
    ## plot time-series
#--------------------------------------------------
if __name__ == '__main__':
    test()
    test_dpqm()
