#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 20 11:43:22 2018


! matplotlib not happy with xaxis in py3!
Currently only running in py2...  %%£##@%^!!!!
Does not recognize datetime values.......



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
os.path.join('/Users/iregon/C3S/dessaps/CDSpy/py_tools')
import sys
import __main__ as main
import logging
import pandas as pd
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import numpy as np
import matplotlib.dates as mdates
from common.var_properties import *

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False

this_script = main.__file__

# PROCESS INPUT PARAMETERS AND INITIALIZE SOME STUFF ==========================
# 1. From command line
try:
    sid_deck = sys.argv[1]
    inDir = sys.argv[2]
    y_ini = int(sys.argv[3])
    y_end = int(sys.argv[4])
    out = sys.argv[5]
    mode = sys.argv[6] # 'qc' or whatever
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)

# 2. Some plotting options
font_size_legend = 11
axis_label_size = 11
tick_label_size = 9
title_label_size = 14
markersize = 2
figsize=(9, 14)

# 3. Processing options
vars_in = ['dpt','wbt','sst','at','slp','wd','ws']
vari_qc=['dpt','sst','at','slp']
stat_ini = ['median','p25','p75','count','min','max']
if mode=='qc':
    stat_ini = ['median','p25','p75','count','count_qc0','count_qc01','count_qc012','min','max']
lat_band_edges = [[-90,-60],[-60,-30],[-30,30],[30,60],[60,90]]
lat_band_tags = ['60-90 S','30-60 S','30 S-30 N','30-60 N','60-90 N']



# 4. Now process and initialize
file_path = inDir
out_path = dict()
ext='.png'
for vari in vars_in:
    out_path[vari] = os.path.join(out,"_".join([sid_deck,vari,'lat_band','stats','ts']) + ext)


# END PARAMS ==================================================================
def read_monthly_data():
    # Get file paths and read files to df. Merge in single df by report_id
    file_header = os.path.join(inDir,'-'.join(['header',str(yr),'{:02d}'.format(mo)]) + '.psv')
    files_vars = dict(zip(vars_in,[ os.path.join(inDir,'-'.join([vars_file_prefix.get(vari),str(yr),'{:02d}'.format(mo)]) + '.psv') for vari in vars_in ]))
    df_header = pd.read_csv(file_header,usecols=[0,10,13,14],sep="|",skiprows=0,na_values=["NULL"])
    df_header.set_index('report_id',drop=True, inplace=True)
    df_varis = dict()
    vars_out = []
    for vari in vars_in:
        if os.path.isfile(files_vars.get(vari)):
            df_varis[vari] = pd.read_csv(files_vars.get(vari),usecols=[1,14,28],sep="|",skiprows=0,na_values=["NULL"])
            if len(df_varis[vari])>0:
                vars_out.append(vari)
                df_varis[vari].set_index('report_id',drop=True, inplace=True)
                df_varis[vari].rename({'observation_value':vari},axis='columns', inplace=True)
                df_varis[vari][vari] = df_varis[vari]*vars_factor.get(vari) + vars_offset.get(vari)
                df_varis[vari].rename({'quality_flag':vari+'_qc'},axis='columns', inplace=True)
                
    for vari in vars_out:
        df_header = pd.concat([df_header,df_varis.get(vari)],axis=1,sort=False)
        
    return df_header,vars_out

# MAIN ========================================================================
stats = stat_ini[:]
# 1. Set up monthly indexed DF by lat band where stats are stored
vars_idx = [vars_in[0]]*len(stat_ini)
for vari in vars_in[1:]:
    for stat in stat_ini: # Otherwise ugly flattening of string lists....
        vars_idx.append(vari)
        stats.append(stat)

tuples = list(zip(*[vars_idx,stats]))
columns = pd.MultiIndex.from_tuples(tuples)
ts_stats = dict()
for band in lat_band_tags:
    ts_stats[band] = dict()
    for vari in vars_in:
        ts_stats[band][vari] = pd.DataFrame(index = pd.date_range(start = datetime.datetime(y_ini,1,1),end = datetime.datetime(y_end,12,1),freq='MS'),columns = stat_ini)
        ts_stats[band][vari].index = pd.to_datetime(ts_stats[band][vari].index)
        ts_stats[band][vari].index = [ datetime.datetime(x.year,x.month,x.day) for x in ts_stats[band][vari].index ]

not_empty = dict()
for vari in vars_in:
    not_empty[vari] = False

edges=list(set([item for sublist in lat_band_edges for item in sublist]))
edges.sort()
# 2. Parse and compute file by file
for yr in range(y_ini,y_end + 1):
    for mo in range(1,13):
        dt = datetime.datetime(yr,mo,1)
        print('{0}-{1}'.format(yr,mo))
        mm = '{:02d}'.format(mo)
        try:
            df_header,vars_out = read_monthly_data()
        except Exception as e:
            print('Could not load data file:',yr,mo)
            print(e)
            continue
        for vari in vars_out:
            if all(pd.isna(df_header[vari])):
                print('Empty ' + vari + ' file')
                continue
            not_empty[vari] = True
            if mode == 'qc' and vari in vari_qc:
                locs=df_header.loc[df_header[vari+'_qc']==0].index
            else:
                locs=df_header.index
            band_median = df_header[vari].loc[locs].groupby(pd.cut(df_header['latitude'].loc[locs], edges,labels=lat_band_tags)).median()
            band_max = df_header[vari].loc[locs].groupby(pd.cut(df_header['latitude'].loc[locs], edges,labels=lat_band_tags)).max()
            band_min = df_header[vari].loc[locs].groupby(pd.cut(df_header['latitude'].loc[locs], edges,labels=lat_band_tags)).min()
            band_count = df_header[vari].groupby(pd.cut(df_header['latitude'], edges,labels=lat_band_tags)).count()
            if mode=='qc' and vari in vari_qc:
                band_count_qc0 = df_header[vari].loc[df_header[vari+'_qc']==0].groupby(pd.cut(df_header['latitude'].loc[df_header[vari+'_qc']==0], edges,labels=lat_band_tags)).count()
                band_count_qc01 = df_header[vari].loc[(df_header[vari+'_qc']==0) | (df_header[vari+'_qc']==1)].groupby(pd.cut(df_header['latitude'].loc[(df_header[vari+'_qc']==0) | (df_header[vari+'_qc']==1)], edges,labels=lat_band_tags)).count()
                band_count_qc012 = df_header[vari].loc[(df_header[vari+'_qc']==0) | (df_header[vari+'_qc']==1) | (df_header[vari+'_qc']==2)].groupby(pd.cut(df_header['latitude'].loc[(df_header[vari+'_qc']==0) | (df_header[vari+'_qc']==1) | (df_header[vari+'_qc']==2)], edges,labels=lat_band_tags)).count()
            band_p75 = df_header[vari].loc[locs].groupby(pd.cut(df_header['latitude'].loc[locs], edges,labels=lat_band_tags)).quantile(q=0.75)
            band_p25 = df_header[vari].loc[locs].groupby(pd.cut(df_header['latitude'].loc[locs], edges,labels=lat_band_tags)).quantile(q=0.25)
            for band in lat_band_tags:
                ts_stats[band][vari]['median'].loc[dt] =  band_median.loc[band]
                ts_stats[band][vari]['max'].loc[dt] =  band_max.loc[band]
                ts_stats[band][vari]['min'].loc[dt] =  band_min.loc[band]
                ts_stats[band][vari]['count'].loc[dt] =  band_count.loc[band]
                if mode=='qc' and vari in vari_qc:
                    ts_stats[band][vari]['count_qc0'].loc[dt] =  band_count_qc0.loc[band]
                    ts_stats[band][vari]['count_qc01'].loc[dt] =  band_count_qc01.loc[band]
                    ts_stats[band][vari]['count_qc012'].loc[dt] =  band_count_qc012.loc[band]
                ts_stats[band][vari]['p75'].loc[dt] =  band_p75.loc[band]
                ts_stats[band][vari]['p25'].loc[dt] =  band_p25.loc[band]


#logging.warning('Could not process date {0}. Error issued: {1}'.format("-".join([str(yr),mm]),e))
# Now find bound values for maps
max_values = dict()
min_values = dict()
max_counts = dict()
for vari in vars_in:
    max_values[vari] = np.nanmax( [ ts_stats[band][vari]['max'].max() for band in lat_band_tags ])
    min_values[vari] = np.nanmin( [ ts_stats[band][vari]['min'].min() for band in lat_band_tags ])
    max_counts[vari] = np.nanmax( [ ts_stats[band][vari]['count'].max() for band in lat_band_tags ])
# 3. Plot
plt.rc('legend',**{'fontsize':font_size_legend})          # controls default text sizes
plt.rc('axes', titlesize=axis_label_size)     # fontsize of the axes title
plt.rc('axes', labelsize=axis_label_size)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=tick_label_size)    # fontsize of the tick labels
plt.rc('ytick', labelsize=tick_label_size)    # fontsize of the tick labels
plt.rc('figure', titlesize=title_label_size)  # fontsize of the figure title

xmin = datetime.datetime(y_ini,1,1) - datetime.timedelta(days=15)
xmax = datetime.datetime(y_end,12,1) + datetime.timedelta(days=15)
bbox_props = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)

ax2 = ['']*len(lat_band_tags)
i = len(lat_band_tags)-1
for vari in vars_in:
    i = len(lat_band_tags)-1
    fig, ax = plt.subplots(nrows=len(lat_band_tags), ncols=1, figsize=figsize, dpi=150)

    for band in lat_band_tags:
        if (ts_stats[band][vari]['count'].sum()) > 0 and max_values[vari] == max_values[vari]:
            ax[i].fill_between(ts_stats[band][vari].index, ts_stats[band][vari]['p25'].astype('float'),  ts_stats[band][vari]['p75'].astype('float'),
                            facecolor='DarkBlue', alpha=0.25, interpolate=False, label='IQR',zorder=5)
            ax[i].plot(ts_stats[band][vari].index,ts_stats[band][vari]['max'],marker = '.',linestyle=' ',color='OrangeRed',label='_nolegend_',markersize = markersize,zorder=7)
            ax[i].plot(ts_stats[band][vari].index,ts_stats[band][vari]['min'],marker = '.',linestyle = ' ',color='OrangeRed',label='extremes',markersize = markersize,zorder=8)
            ax[i].plot(ts_stats[band][vari].index,ts_stats[band][vari]['median'],linestyle ='-',color='black',label='median',zorder=6)
            ax[i].set_ylabel(vars_units.get(vari), color='k')
            ax[i].grid(linestyle=':',which='major')
            ax2[i] = ax[i].twinx()
            if mode == 'qc' and vari in vari_qc:
                ax2[i].fill_between(ts_stats[band][vari].index,0,ts_stats[band][vari]['count_qc0'].astype('float'), facecolor='Green',alpha=0.15,interpolate=False,label='counts (qc=0)',zorder=1)
                ax2[i].fill_between(ts_stats[band][vari].index,ts_stats[band][vari]['count_qc0'].astype('float'),ts_stats[band][vari]['count_qc01'].astype('float'), facecolor='Red',alpha=0.15,interpolate=False,label='counts (qc=1)',zorder=2)
                ax2[i].fill_between(ts_stats[band][vari].index,ts_stats[band][vari]['count_qc01'].astype('float'),ts_stats[band][vari]['count_qc012'].astype('float'), facecolor='Gray',alpha=0.15,interpolate=False,label='counts (qc=2)',zorder=3)
                ax2[i].plot(ts_stats[band][vari].index,ts_stats[band][vari]['count'],linestyle=':',color='Grey',label='_nolegend_',linewidth=1,zorder=4)
            else:
                ax2[i].fill_between(ts_stats[band][vari].index,0,ts_stats[band][vari]['count'].astype('float'), facecolor='grey',alpha=0.15,interpolate=False,label='total counts',zorder=1)
            ax2[i].set_yscale("log")
            ax2[i].set_ylabel('counts', color='k')
            # Now send the histogram to the back
            ax[i].set_zorder(ax2[i].get_zorder()+1) # put ax in front of ax2
            ax[i].patch.set_visible(False) # hide the 'canvas'
            ax[i].set_xlim([xmin, xmax])
            ax_y_lim = [ min_values[vari] - 0.3*(max_values[vari]-min_values[vari]),max_values[vari] +  0.3*(max_values[vari]-min_values[vari]) ]
            ax[i].set_ylim(ax_y_lim)
            ax2[i].set_ylim([0, max_counts[vari] +  0.5*max_counts[vari] ])
            if i == len(lat_band_tags) - 1:
                lines, labels = ax[i].get_legend_handles_labels()
                lines2, labels2 = ax2[i].get_legend_handles_labels()
                if mode == 'qc' and vari in vari_qc:
                    ncols_leg=3
                else:
                    ncols_leg=4
                ax[i].legend(lines + lines2,labels + labels2,loc='center', bbox_to_anchor=(0.5, -0.25),ncol=ncols_leg)
            ax[i].text(0.99, 0.9, band , horizontalalignment='right',verticalalignment='center', transform=ax[i].transAxes)
            if i == 0:
                plt.title(sid_deck + ' ' + vars_labels['long_name_upper'].get(vari) + '\n' + " to ".join([str(y_ini),str(y_end)]))
                #plt.title(sid_deck + ' ' + vars_labels['long_name_upper'].get(vari) + '\n' + " to ".join([str(y_ini),str(y_end)]) + '\n' + band )
            #else:
            #    plt.title( band )
            plt.tight_layout();
        else:
            if mode == 'qc' and vari in vari_qc:
                print('{0} No valid data in band {1}'.format(vari,band))
            else:
                print('{0} No data in band {1}'.format(vari,band))
            ax[i].set_xlim([xmin, xmax])
            ax[i].set_ylim(vars_saturation.get(vari)[0],vars_saturation.get(vari)[1])
            xmid = xmin + (xmax-xmin)//2
            if mode == 'qc' and vari in vari_qc:
                ax[i].text(xmid,vars_saturation.get(vari)[0] + (vars_saturation.get(vari)[1]-vars_saturation.get(vari)[0])/2, "No valid data", ha="center", va="center", size=20,bbox=bbox_props)
            else:
                ax[i].text(xmid,vars_saturation.get(vari)[0] + (vars_saturation.get(vari)[1]-vars_saturation.get(vari)[0])/2, "No data", ha="center", va="center", size=20,bbox=bbox_props)
            if i == 0:
                ax[i].set_title(sid_deck + ' ' + vars_labels['long_name_upper'].get(vari) + '\n' + " to ".join([str(y_ini),str(y_end)]))
                #ax[i].set_title(sid_deck + ' ' + vars_labels['long_name_upper'].get(vari) + '\n' + " to ".join([str(y_ini),str(y_end)]) + '\n' + band )
            #else:
            #   ax[i].set_title( band )
            ax[i].text(0.99, 0.9, band , horizontalalignment='right',verticalalignment='center', transform=ax[i].transAxes)
        i = i - 1
    plt.savefig(out_path.get(vari),bbox_inches='tight',dpi = 300)
#logging.info('END')

#pd.DF.count()
#Count non-NA cells for each column or row.
#The values None, NaN, NaT, and optionally numpy.inf (depending on pandas.options.mode.use_inf_as_na) are considered NA.
#pd.DF.mean(), max(),min(),median()
#skipna : boolean, default True
#Exclude NA/null values when computing the result.
#pd.DF.quantile() ????
#DataFrame.quantile(q=0.5, axis=0, numeric_only=True, interpolation='linear')[source]
