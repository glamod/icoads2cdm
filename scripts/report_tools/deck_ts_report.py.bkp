#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 20 11:43:22 2018


Have to add something to do with the fill_between when gaps in series

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
from common.file_functions import *
import matplotlib.dates as mdates

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False

this_script = main.__file__

# YOUR ATTENTION PLEASE!
# TO GROUP BY RANGES:
# test = code_keys.groupby(pd.cut(code_keys["key"], np.arange(0, 200, 50),labels=['1','2','3'])).quantile(q=0.75)
#type(test)
#<class 'pandas.core.frame.DataFrame'>
#>>> test.index
#IntervalIndex([(0, 50], (50, 100], (100, 150]]
#              closed='right',
#              name='key',
#              dtype='interval[int64]')
#>>> test
#0.75     key
#key         
#1      37.75
#2      87.75
#3     137.75
    
# PROCESS INPUT PARAMETERS AND INITIALIZE SOME STUFF ==========================
# 1. From command line
#try:
#    deck = sys.argv[1]
#    inDir = sys.argv[2]
#    y_ini = sys.argv[3]
#    y_end = sys.argv[4]
#except Exception as e:
#    print("Error processing line argument input to script: ", e)
#    exit(1)
deck='714'
inDir='/Users/iregon/C3S/dessaps/CDSpy/test_data/ICOADS3_to_CDS'
y_ini = 1980
y_end = 1984
vars_in = ['SST','AT']
# 2. Some options for processing and plotting
font_size_legend = 12
figsize=(70, 10)
stat_ini = ['median','p25','p75','count','min','max']

# 3. Some properties to use about the params. Will have to edit if adding others
# And probably add to a properties_cmd.py
vars_file_prefix = {'SST':'observations-sst','AT':'observations-at'}
vars_offset = {'SST':-273.15,'AT':-273.15}
vars_units = {'SST':'$^\circ$C','AT':'degrees ($^\circ$C)'}
# 4. Now process and initialize
file_path = inDir
out = '/Users/iregon/C3S/dessaps/CDSpy/L1a/ts'
out_path = dict()
for vari in vars_in:
    out_path[vari] = os.path.join(out,"_".join(['deck',deck,vari,'global','stats','ts']) + '.png')
# END PARAMS ==================================================================

# MAIN ========================================================================
setup_log()
stats = stat_ini.copy()
# 1. Set up monthly indexed DF where stats are stored
vars_idx = [vars_in[0]]*len(stat_ini)
for vari in vars_in[1:]:
    for stat in stat_ini: # Otherwise ugly flattening of string lists....
        vars_idx.append(vari)
        stats.append(stat)
        
tuples = list(zip(*[vars_idx,stats]))
columns = pd.MultiIndex.from_tuples(tuples)
ts_stats = dict()
for vari in vars_in:
    ts_stats[vari] = pd.DataFrame(index = pd.date_range(start = datetime.datetime(y_ini,1,1),end = datetime.datetime(2010,12,1),freq='MS'),columns = stat_ini)
    
# 2. Parse and compute file by file
for yr in range(y_ini,y_end + 1):
    for mo in range(1,13):
        dt = [ datetime.datetime(x,mo,1) for x in range(yr,2011,5) ]
        mm = '{:02d}'.format(mo)
        logging.info('{0}-{1}'.format(yr,mm))
        try:
            file_header = os.path.join(file_path,'-'.join(['header',str(yr),mm]) + '.psv')
            files_vars = dict(zip(vars_in,[ os.path.join(file_path,'-'.join([vars_file_prefix.get(vari),str(yr),'{:02d}'.format(mo)]) + '.psv') for vari in vars_in ])) 
            df_header = pd.read_csv(file_header,usecols=[0,10,13,14],sep="|",skiprows=0,na_values=["NULL"])
            df_header.set_index('report_id',drop=True, inplace=True)
            df_varis = dict()
            for vari in vars_in:
                df_varis[vari] = pd.read_csv(files_vars.get(vari),usecols=[1,14],sep="|",skiprows=0,na_values=["NULL"])  
                df_varis[vari].set_index('report_id',drop=True, inplace=True)
                df_varis[vari].rename({'observation_value':vari},axis='columns', inplace=True)  
                df_varis[vari] = df_varis[vari] + vars_offset.get(vari)
            for vari in vars_in:
                df_header = pd.concat([df_header,df_varis.get(vari)],axis=1)
            # See below for considetions on NANs in the pd functions used
            for vari in vars_in:
                ts_stats[vari]['median'].loc[dt] = df_header[vari].median()
                ts_stats[vari]['p75'].loc[dt] = df_header[vari].quantile(q=0.75)
                ts_stats[vari]['p25'].loc[dt] = df_header[vari].quantile(q=0.25)
                ts_stats[vari]['count'].loc[dt]  = df_header[vari].count()
                ts_stats[vari]['min'].loc[dt] = df_header[vari].min()
                ts_stats[vari]['max'].loc[dt] = df_header[vari].max()
        except Exception as e:
            logging.warning('Could not process date {0}. Error issued: {1}'.format("-".join([str(yr),mm]),e))
# 3. Plot       
plt.rc('legend',**{'fontsize':font_size_legend})        
for vari in vars_in:
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=figsize, dpi=150) 
    ax.fill_between(ts_stats[vari].index, ts_stats[vari]['p25'].astype('float'),  ts_stats[vari]['p75'].astype('float'),
                    facecolor='DarkBlue', alpha=0.25, interpolate=True, label='IQR')
    ax.plot(ts_stats[vari].index,ts_stats[vari]['max'],marker = '.',linestyle=' ',color='OrangeRed',label='_nolegend_',linewidth = 0.7)
    ax.plot(ts_stats[vari].index,ts_stats[vari]['min'],marker = '.',linestyle = ' ',color='OrangeRed',label='extremes',linewidth = 0.7)
    ax.plot(ts_stats[vari].index,ts_stats[vari]['median'],linestyle ='-',color='black',label='median')
    ax.legend(loc='center', bbox_to_anchor=(0.5, -0.08),ncol=3)
    ax.set_ylabel(vars_units.get(vari), color='k')
    ax2 = ax.twinx()
    ax2.fill_between(ts_stats[vari].index,0,ts_stats[vari]['count'], facecolor='grey',alpha=0.15,interpolate=True,label='nObs')
    ax2.set_yscale("log")
    ax2.set_ylabel('counts', color='k')
    ax2.legend(loc=1)
    # Now send the histogram to the back
    ax.set_zorder(ax2.get_zorder()+1) # put ax in front of ax2 
    ax.patch.set_visible(False) # hide the 'canvas'
    xmin = datetime.datetime(y_ini,1,1) - datetime.timedelta(days=15)
    xmax = datetime.datetime(2010,12,1) + datetime.timedelta(days=15) 
    ax.set_xlim([xmin, xmax])
    ax_y_span = ax.get_ylim()[1] - ax.get_ylim()[0]
    ax_y_lim = [ts_stats[vari]['min'].min() - 0.3*ax_y_span,ts_stats[vari]['max'].max() +  0.3*ax_y_span ]
    ax.set_ylim(ax_y_lim)
    ax2_y_span = ax2.get_ylim()[1] - ax2.get_ylim()[0]
    ax2.set_ylim([ax2.get_ylim()[0], ts_stats[vari]['count'].max()+  0.4*ax2_y_span ])
    plt.title('-'.join([deck,vari]) + '\n' + "-".join([str(y_ini),str(y_end)]))
    plt.tight_layout();
    plt.savefig(out_path.get(vari),bbox_inches='tight')

logging.info('END')

#pd.DF.count()
#Count non-NA cells for each column or row.
#The values None, NaN, NaT, and optionally numpy.inf (depending on pandas.options.mode.use_inf_as_na) are considered NA.
#pd.DF.mean(), max(),min(),median()
#skipna : boolean, default True
#Exclude NA/null values when computing the result.
#pd.DF.quantile() ????
#DataFrame.quantile(q=0.5, axis=0, numeric_only=True, interpolation='linear')[source]
