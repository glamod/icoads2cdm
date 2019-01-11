#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 20 11:43:22 2018

Script to reprocess and format ICOADS release 3.0. imma1 files to C3S-CDS CDM tables.

To run from command line:

    python(v) ICOADS3_to_CDS.py data_file YYYY m config_file

Inargs:
    data_file: path to ICOADS imma1 data file
    YYYY: year of data records
    m: month of data records
    config_file: path to script configuration json file with processing options

INPUT:
   

OUTPUT:
   


@author: iregon
"""
from __future__ import absolute_import
from __future__ import division
import future        # pip install future
import builtins      # pip install future
import past          # pip install future
import six           # pip install six
import datetime
import os
import sys
import __main__ as main
sys.path.append('/Users/iregon/C3S/dessaps/CDSpy')
import logging
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import glob
import io
import json
import holoviews as hv
from holoviews.operation import histogram as hv_histogram
from common.file_functions import *

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False

this_script = main.__file__

# PROCESS INPUT PARAMETERS AND INITIALIZE SOME STUFF ==========================
# 1. From command line. Create log
inDir='/Users/iregon/C3S/dessaps/CDSpy/test_data/ICOADS3_to_CDS'
y_ini = 1980
y_end = 1981
vars_in = ['SST','AT']
vars_file_prefix = {'SST':'observations-sst','AT':'observations-at'}
#try:
#    deck = sys.argv[1]
#    inDir = sys.argv[2]
#    y_ini = sys.argv[3]
#    y_end = sys.argv[4]
#except Exception as e:
#    print("Error processing line argument input to script: ", e)
#    exit(1)

file_path = inDir
logfile = os.path.join(file_path,".".join([this_script.split(".")[0],'log']))

float_precision = .2

 # Beware: 'w' might not work in python 2!!!!!
#logging.info("{0} on deck {1} files ({2})".format(this_script, deck, file_path))

# END PARAMS ==================================================================

# FUNCTIONS ===================================================================

def quantile_from_cumsum(df,q):    
    n = len(df)
    if n > 0:
        try:
            return df.loc[ df <=  df.iloc[-1]*q ].index[-1]
        except:
            return df.index[-1]
    else:
        return np.nan
    
    

# END FUNCTIONS ===============================================================


# MAIN ========================================================================
setup_log()
ts_stats = dict()
ts_sst_histos = dict()
ts_at_histos = dict()
glo_sst_hist = dict()
glo_ts_sst_stats = dict()
basics = dict()
basics['max'] = dict()
kdims=['primary_station_id']
vdims = ['sst','at','latitude','longitude']
for yr in range(y_ini,y_end + 1):
    for mo in range(1,13):
        dt = datetime.datetime(yr,mo,1)
        mm = '{:02d}'.format(mo)
        logging.info('{0}-{1}'.format(yr,mm))
        file_header = os.path.join(file_path,'-'.join(['header',str(yr),mm]) + '.psv')
        files_vars = dict(zip(vars_in,[ os.path.join(file_path,'-'.join([vars_file_prefix.get(vari),str(yr),'{:02d}'.format(mo)]) + '.psv') for vari in vars_in ])) 
        df_header = pd.read_csv(file_header,usecols=[0,10,13,14],sep="|",skiprows=0,na_values=["NULL"])
        df_header.set_index('report_id',drop=True, inplace=True)
        df_varis = dict()
        for vari in vars_in:
            df_varis[vari] = pd.read_csv(files_vars.get(vari),usecols=[1,14],sep="|",skiprows=0,na_values=["NULL"])  
            df_varis[vari].set_index('report_id',drop=True, inplace=True)
            df_varis[vari].rename({'observation_value':vari},axis='columns', inplace=True)    
        for vari in vars_in:
            df_header = pd.concat([df_header,df_varis.get(vari)],axis=1)
        
        vdims = ['sst','at','latitude','longitude']
        kdims=['primary_station_id']
        ds = hv.Dataset(df_header, kdims = kdims,vdims = vdims)
        # Check countGoodFloat; is not the one we want
        ts_stats[dt] = ds.aggregate(function = [np.nanmean,np.nanmax,np.nanmin,countGoodFloat]).data.set_index('primary_station_id')
       
        sst_bin_range = (int(ts_stats[dt]['sst']['nanmin'].min()),int(ts_stats[dt]['sst']['nanmax'].max())+1)
        sst_bin_edges = np.arange(sst_bin_range[0],sst_bin_range[1] + float_precision,float_precision)
        sst_bin_centres = (sst_bin_edges[:-1] + sst_bin_edges[1:]) / 2
        sst_n_bins = len(np.arange(sst_bin_range[0],sst_bin_range[1],float_precision))
        sst_plot_bin_width = 0.85 * (sst_bin_edges[1] - sst_bin_edges[0])
        
        at_bin_range = (int(ts_stats[dt]['at']['nanmin'].min()),int(ts_stats[dt]['at']['nanmax'].max())+1)
        at_bin_edges = np.arange(at_bin_range[0],at_bin_range[1] + float_precision,float_precision)
        at_bin_centres = (at_bin_edges[:-1] + at_bin_edges[1:]) / 2
        at_n_bins = len(np.arange(at_bin_range[0],at_bin_range[1],float_precision))
        at_plot_bin_width = 0.85 * (at_bin_edges[1] - at_bin_edges[0])
        ts_sst_histos[dt] = hv_histogram(ds, bin_range=sst_bin_range,num_bins=sst_n_bins,normed=False, dimension = 'sst',groupby = 'primary_station_id')
        ts_at_histos[dt] = hv_histogram(ds, bin_range=at_bin_range,num_bins=at_n_bins,normed=False, dimension = 'at',groupby = 'primary_station_id')


stations0 = [ ts_sst_histos[key].data.keys() for key in ts_sst_histos.keys() ]
stations = list(set([item for sublist in stations0 for item in sublist]))


for station in stations:
    basics[station] = dict()
    glo_sst_hist[station] = pd.DataFrame() 
    glo_ts_sst_stats[station] = pd.DataFrame()
    glo_ts_sst_stats[station]
    #glo_ts_sst_stats[station] = pd.DataFrame(index = pd.date_range(start = datetime.datetime(y_ini,1,1),end = datetime.datetime(y_end,12,1),freq='MS'))
    
    dates = [ x for x in ts_sst_histos.keys() if ts_sst_histos[x].data.get(station) ]
    glo_ts_sst_stats[station]['mean'] = pd.Series(index = dates, data = [ ts_stats[date]['sst']['nanmean'].loc[station[0]] for date in dates ])
    glo_ts_sst_stats[station]['min'] = pd.Series(index = dates, data = [ ts_stats[date]['sst']['nanmin'].loc[station[0]] for date in dates ])
    glo_ts_sst_stats[station]['max'] = pd.Series(index = dates, data = [ ts_stats[date]['sst']['nanmax'].loc[station[0]] for date in dates ])
    glo_ts_sst_stats[station]['counts'] = pd.Series(index = dates, data = [ ts_stats[date]['sst']['countGoodFloat'].loc[station[0]] for date in dates ])
    for date in dates:
        glo_sst_hist[station] = pd.concat([ glo_sst_hist[station], pd.DataFrame(columns=[date], index= ts_sst_histos[date].data.get(station)['sst'],data=ts_sst_histos[date].data.get(station)['sst_frequency'] )], axis=0,join='outer')
    glo_sst_hist[station].sort_index(inplace=True)
    glo_sst_hist[station]['total'] = glo_sst_hist[station].sum(axis=1)
    glo_sst_hist[station]['cum_sum'] = glo_sst_hist[station]['total'].cumsum(axis=0)
    basics[station]['p01'] = quantile_from_cumsum(glo_sst_hist[station]['cum_sum'],0.1)
    basics[station]['p99'] = quantile_from_cumsum(glo_sst_hist[station]['cum_sum'],0.99)
    basics[station]['median'] = quantile_from_cumsum(glo_sst_hist[station]['cum_sum'],0.5)


#'IC30-46010'
station = [ x for x in stations if 'IC30-46010' in x[0] ][0]
print(station)
print(basics[station])
  
#station = stations_flat[30]
for yr in range(y_ini,y_ini + 1):
    [fig, axs] = plt.subplots(nrows=3, ncols=4, sharex=True, figsize=(10, 10), dpi=100)
    axs = axs.flatten()
    i = 0
    for mo in range(1,13):
        date = datetime.datetime(yr,mo,1)
        mm = '{:02d}'.format(mo)  
        if date in ts_sst_histos.keys():
            if ts_sst_histos[date].data.get(station):
                print('plot:   ',date)
                axs[i].bar(glo_sst_hist[station].index,glo_sst_hist[station][date], align='center', width=sst_plot_bin_width,color='grey')
        i += 1
            
[fig, axs] = plt.subplots(nrows=2, ncols=1, figsize=(10, 10), dpi=100)  
axs = axs.flatten()
axs[0].bar(glo_sst_hist[station].index,glo_sst_hist[station]['total'], align='center', width=sst_plot_bin_width,color='grey')
axs[0].axvline(x=basics[station]['p01'], label='p01', color = 'DarkGreen')
axs[0].axvline(x=basics[station]['p99'], label='p99', color = 'DarkGreen')
axs[0].axvline(x=basics[station]['median'], label='median', color = 'Orange')

glo_ts_sst_stats[station][['max','min','mean']].plot(marker='o',linestyle = '', ax = axs[1] )
axs[1].set_ylabel('sst', color='k')
ax2 = axs[1].twinx()
ax2.bar(glo_ts_sst_stats[station].index,glo_ts_sst_stats[station]['counts'], align='center', width=4,color='grey')
ax2.set_ylabel('counts', color='k')

#for station in stations:
#    histograms[station]=pd.concat([],axis=0,join='outer')
#histograms = dict()
#basics = dict()
#for yr in range(y_ini,y_end + 1):
#    for mo in range(1,13):
#        dt = datetime.datetime(yr,mo,1)
#        mm = '{:02d}'.format(mo)
#        logging.info('{0}-{1}'.format(yr,mm))
#        file_header = os.path.join(file_path,'-'.join(['header',str(yr),mm]) + '.psv')
#        file_sst = os.path.join(file_path,'-'.join(['observations-sst',str(yr),mm]) + '.psv')
#        file_at = os.path.join(file_path,'-'.join(['observations-at',str(yr),mm]) + '.psv')
#        df_header = pd.read_csv(file_header,usecols=[0,10,13,14],sep="|",skiprows=0,na_values=["NULL"])
#        df_sst = pd.read_csv(file_sst,usecols=[1,14],sep="|",skiprows=0,na_values=["NULL"])  
#        df_at = pd.read_csv(file_at,usecols=[1,14],sep="|",skiprows=0,na_values=["NULL"]) 
#        df_header.set_index('report_id',drop=True, inplace=True)
#        df_sst.set_index('report_id',drop=True, inplace=True)
#        df_sst.rename({'observation_value':'sst'},axis='columns', inplace=True)
#        df_at.set_index('report_id',drop=True, inplace=True)
#        df_at.rename({'observation_value':'at'},axis='columns',inplace=True)
#
#        if len(df_header) != len(df_sst):
#            logging.error('Malformed files: unequal number of records')
#            exit(1)
#            
#        df = pd.concat([df_header,df_sst,df_at],axis=1)
#        stations= list(df['primary_station_id'].unique())
#
#        sst_bin_edges = np.arange(int(df['sst'].min()),int(df['sst'].max())+1,float_precision)
#        sst_bin_centres = (sst_bin_edges[:-1] + sst_bin_edges[1:]) / 2 
#        
#        at_bin_edges = np.arange(int(df['at'].min()),int(df['at'].max())+1,float_precision)
#        at_bin_centres = (at_bin_edges[:-1] + at_bin_edges[1:]) / 2 
#
#        points = hv.Points(df, kdims=['longitude','latitude'],vdims=['sst','primary_station_id'])
#        bin_range = (int(points.range(dim=2)[0]),int(points.range(dim=2)[1])+1)
#        sst_hist = hv_histogram(points, bin_range=bin_range,num_bins=n_bins,normed=False, dimension = 2,groupby = 'primary_station_id')
#
#        for station in stations:
#            if station not in histograms.keys():
#                histograms[station] = pd.DataFrame()
#                basics[station] = pd.DataFrame(index=pd.date_range(start=datetime.datetime(y_ini,1,1),end = datetime.datetime(y_end,12,1),freq='MS'),columns=['max','min','mean','p01','p99','median'])
#            try:
#                if len(df['sst'].loc[df['primary_station_id'] ==station].dropna()) > 0:
#                    histogram = np.histogram(df['sst'].loc[df['primary_station_id'] ==station].dropna(),bins = sst_bin_edges)
#                    histo_df = pd.DataFrame(columns=[datetime.datetime(yr,mo,1)], index= sst_bin_centres,data=histogram[0])
#                    histograms[station]=pd.concat([histograms[station],histo_df],axis=0,join='outer')
#                    basics[station]['max'].loc[dt] = df['sst'].loc[df['primary_station_id'] ==station].max()
#                    basics[station]['min'].loc[dt] = df['sst'].loc[df['primary_station_id'] ==station].min()
#                    basics[station]['mean'].loc[dt] = df['sst'].loc[df['primary_station_id'] ==station].mean()
#                    logging.info('Histogram at station {0}: ok'.format(station))
#            except Exception as e:
#                logging.error('Computing histogram at station {0}, date {1}-{2}: {3}'.format(station,yr,mm,e))
#
#i = 0
#for station in stations:
#    histograms[station].sort_index(inplace=True)
#    histograms[station]['total'] = histograms[station].sum(axis=1)
#    histograms[station]['cum_sum'] = histograms[station]['total'].cumsum(axis=0)
#    basics[station]['max_global'] = basics[station]['max'].max()
#    basics[station]['min_global'] = basics[station]['min'].min()
#    basics[station]['mean_global'] = basics[station]['mean'].mean()
#    basics[station]['p01'] = quantile_from_cumsum(histograms[station]['cum_sum'],0.1)
#    basics[station]['p99'] = quantile_from_cumsum(histograms[station]['cum_sum'],0.99)
#    basics[station]['median'] = quantile_from_cumsum(histograms[station]['cum_sum'],0.5)
#    plot_bin_width = 0.85 * (sst_bin_edges[1] - sst_bin_edges[0])
#    if i == 1:
#        fig = plt.figure(i, figsize=(10, 10), dpi=100)
#        ax = fig.add_subplot(111)
#        plt.bar(histograms[station].index, histograms[station]['total'], align='center', width=plot_bin_width,color='grey')
#        ax.axvline(x=basics[station]['min_global'].iloc[0], label='min', color = 'red')
#        ax.axvline(x=basics[station]['max_global'].iloc[0], label='max', color = 'red')
#        ax.axvline(x=basics[station]['mean_global'].iloc[0], label='mean', color = 'red')
#        ax.axvline(x=basics[station]['p01'].iloc[0], label='p01', color = 'DarkGreen')
#        ax.axvline(x=basics[station]['p99'].iloc[0], label='p99', color = 'DarkGreen')
#        fig = plt.figure(i + 1, figsize=(10, 10), dpi=100)
#        ax = fig.add_subplot(111)
#        plt.plot(histograms[station].index, histograms[station]['cum_sum'])
#        ax.axvline(x=basics[station]['p01'].iloc[0], label='p01', color = 'DarkGreen')
#        ax.axvline(x=basics[station]['p99'].iloc[0], label='p99', color = 'DarkGreen')
#        ax.axvline(x=basics[station]['median'].iloc[0], label='median', color = 'Orange')
#    i += 1
#


logging.info('END')