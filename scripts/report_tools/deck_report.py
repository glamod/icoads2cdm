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
from common.file_functions import *

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False

this_script = main.__file__

# PROCESS INPUT PARAMETERS AND INITIALIZE SOME STUFF ==========================
# 1. From command line. Create log
inDir='/Users/iregon/C3S/dessaps/CDSpy/test_data/ICOADS3_to_CDS'
y_ini = 2014
y_end = 2014
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

float_precision = .1

 # Beware: 'w' might not work in python 2!!!!!
#logging.info("{0} on deck {1} files ({2})".format(this_script, deck, file_path))

# END PARAMS ==================================================================

# FUNCTIONS ===================================================================

def quantile_from_cumsum(df,q):    
    n = len(df)
    if n > 0:
        return df.loc[ df <=  df.iloc[-1]*q ].index[-1]
    else:
        return np.nan
    
    

# END FUNCTIONS ===============================================================


# MAIN ========================================================================

import holoviews as hv
from holoviews.operation import histogram as hv_histogram

file_header = os.path.join(file_path,'-'.join(['header',str(yr),mm]) + '.psv')
df_header = pd.read_csv(file_header,usecols=[0,10,13,14],sep="|",skiprows=0,na_values=["NULL"])
df_header.set_index('report_id',drop=True, inplace=True)

file_sst = os.path.join(file_path,'-'.join(['observations-sst',str(yr),mm]) + '.psv')
df_sst = pd.read_csv(file_sst,usecols=[1,14],sep="|",skiprows=0,na_values=["NULL"])
df_sst.set_index('report_id',drop=True, inplace=True)
df_sst.rename({'observation_value':'sst'},axis='columns', inplace=True)

df = pd.concat([df_header,df_sst],axis=1)

points = hv.Points(df, kdims=['longitude','latitude'],vdims=['sst','primary_station_id'])
bin_range = (int(points.range(dim=2)[0]),int(points.range(dim=2)[1])+1)
sst_hist = hv_histogram(points, bin_range=bin_range,num_bins=n_bins,normed=False, dimension = 2,groupby = 'primary_station_id')
i = 0
for key in sst_hist.data.keys():
    if i < 10:
        fig = plt.figure(i, figsize=(10, 10), dpi=100)
        ax = fig.add_subplot(111)
        plt.bar(bin_centres, sst_hist.data.get(key)['2_frequency'], align='center', width=plot_bin_width,color='grey')
    i += 1
    
# Holoviews
points = hv.Points(df_sst['sst'].values)
bin_range = (int(points.range(dim=1)[0]),int(points.range(dim=1)[1])+1)
bin_edges = np.arange(bin_range[0],bin_range[1] + 0.2,0.2)
bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2
n_bins = len(np.arange(bin_range[0],bin_range[1],0.2))
plot_bin_width = 0.85 * (bin_edges[1] - bin_edges[0])

sst_hist = hv_histogram(points, bin_range=bin_range,num_bins=n_bins,normed=False, dimension = 1)

fig = plt.figure(1, figsize=(10, 10), dpi=100)
ax = fig.add_subplot(111)
plt.bar(bin_centres, sst_hist.data['1_frequency'], align='center', width=plot_bin_width,color='grey')

# numpy, matplotlib, etc...
histogram = np.histogram(df_sst['sst'].dropna(),bins = bin_edges)
fig = plt.figure(2, figsize=(10, 10), dpi=100)
ax = fig.add_subplot(111)
plt.bar(bin_centres, histogram[0], align='center', width=plot_bin_width,color='grey')


histograms = dict()
basics = dict()
for yr in range(y_ini,y_end + 1):
    for mo in range(1,13):
        dt = datetime.datetime(yr,mo,1)
        mm = '{:02d}'.format(mo)
        logging.info('{0}-{1}'.format(yr,mm))
        file_header = os.path.join(file_path,'-'.join(['header',str(yr),mm]) + '.psv')
        file_sst = os.path.join(file_path,'-'.join(['observations-sst',str(yr),mm]) + '.psv')
        file_at = os.path.join(file_path,'-'.join(['observations-at',str(yr),mm]) + '.psv')
        df_header = pd.read_csv(file_header,usecols=[0,10,13,14],sep="|",skiprows=0,na_values=["NULL"])
        df_sst = pd.read_csv(file_sst,usecols=[1,14],sep="|",skiprows=0,na_values=["NULL"])  
        df_at = pd.read_csv(file_at,usecols=[1,14],sep="|",skiprows=0,na_values=["NULL"]) 
        df_header.set_index('report_id',drop=True, inplace=True)
        df_sst.set_index('report_id',drop=True, inplace=True)
        df_sst.rename({'observation_value':'sst'},axis='columns', inplace=True)
        df_at.set_index('report_id',drop=True, inplace=True)
        df_at.rename({'observation_value':'at'},axis='columns',inplace=True)

        if len(df_header) != len(df_sst):
            logging.error('Malformed files: unequal number of records')
            exit(1)
            
        df = pd.concat([df_header,df_sst,df_at],axis=1)
        stations= list(df['primary_station_id'].unique())

        sst_bin_edges = np.arange(int(df['sst'].min()),int(df['sst'].max())+1,float_precision)
        sst_bin_centres = (sst_bin_edges[:-1] + sst_bin_edges[1:]) / 2 
        
        at_bin_edges = np.arange(int(df['at'].min()),int(df['at'].max())+1,float_precision)
        at_bin_centres = (at_bin_edges[:-1] + at_bin_edges[1:]) / 2 

        for station in stations:
            if station not in histograms.keys():
                histograms[station] = pd.DataFrame()
                basics[station] = pd.DataFrame(index=pd.date_range(start=datetime.datetime(y_ini,1,1),end = datetime.datetime(y_end,12,1),freq='MS'),columns=['max','min','mean','p01','p99','median'])
            try:
                if len(df['sst'].loc[df['primary_station_id'] ==station].dropna()) > 0:
                    histogram = np.histogram(df['sst'].loc[df['primary_station_id'] ==station].dropna(),bins = sst_bin_edges)
                    histo_df = pd.DataFrame(columns=[datetime.datetime(yr,mo,1)], index= sst_bin_centres,data=histogram[0])
                    histograms[station]=pd.concat([histograms[station],histo_df],axis=0,join='outer')
                    basics[station]['max'].loc[dt] = df['sst'].loc[df['primary_station_id'] ==station].max()
                    basics[station]['min'].loc[dt] = df['sst'].loc[df['primary_station_id'] ==station].min()
                    basics[station]['mean'].loc[dt] = df['sst'].loc[df['primary_station_id'] ==station].mean()
                    logging.info('Histogram at station {0}: ok'.format(station))
            except Exception as e:
                logging.error('Computing histogram at station {0}, date {1}-{2}: {3}'.format(station,yr,mm,e))

i = 0
for station in stations:
    histograms[station].sort_index(inplace=True)
    histograms[station]['total'] = histograms[station].sum(axis=1)
    histograms[station]['cum_sum'] = histograms[station]['total'].cumsum(axis=0)
    basics[station]['max_global'] = basics[station]['max'].max()
    basics[station]['min_global'] = basics[station]['min'].min()
    basics[station]['mean_global'] = basics[station]['mean'].mean()
    basics[station]['p01'] = quantile_from_cumsum(histograms[station]['cum_sum'],0.1)
    basics[station]['p99'] = quantile_from_cumsum(histograms[station]['cum_sum'],0.99)
    basics[station]['median'] = quantile_from_cumsum(histograms[station]['cum_sum'],0.5)
    plot_bin_width = 0.85 * (sst_bin_edges[1] - sst_bin_edges[0])
    if i == 1:
        fig = plt.figure(i, figsize=(10, 10), dpi=100)
        ax = fig.add_subplot(111)
        plt.bar(histograms[station].index, histograms[station]['total'], align='center', width=plot_bin_width,color='grey')
        ax.axvline(x=basics[station]['min_global'].iloc[0], label='min', color = 'red')
        ax.axvline(x=basics[station]['max_global'].iloc[0], label='max', color = 'red')
        ax.axvline(x=basics[station]['mean_global'].iloc[0], label='mean', color = 'red')
        ax.axvline(x=basics[station]['p01'].iloc[0], label='p01', color = 'DarkGreen')
        ax.axvline(x=basics[station]['p99'].iloc[0], label='p99', color = 'DarkGreen')
        fig = plt.figure(i + 1, figsize=(10, 10), dpi=100)
        ax = fig.add_subplot(111)
        plt.plot(histograms[station].index, histograms[station]['cum_sum'])
        ax.axvline(x=basics[station]['p01'].iloc[0], label='p01', color = 'DarkGreen')
        ax.axvline(x=basics[station]['p99'].iloc[0], label='p99', color = 'DarkGreen')
        ax.axvline(x=basics[station]['median'].iloc[0], label='median', color = 'Orange')
    i += 1



logging.info('END')