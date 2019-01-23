#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


"""
from __future__ import absolute_import
from __future__ import division
import future        # pip install future
import builtins      # pip install future
import past          # pip install future
import six           # pip install six

import os
import sys
import logging
import pandas as pd
import __main__ as main
import glob
import logging
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import numpy as np
import matplotlib.dates as mdates
from common.file_functions import *
from common.var_properties import *
from common.file_functions import *
import shutil
import datetime

if sys.version_info[0] >= 3:
    py3 = True
    from io import StringIO
else:
    py3 = False
    from io import BytesIO

this_script = main.__file__

## PROCESS INPUT PARAMETERS AND INITIALIZE  AND CHECK SOME STUFF ===============
##==============================================================================
#yr = '2000'
#mo = '01'
#qc_path = '/Users/iregon/C3S/dessaps/CDSpy/test_data/'
#data_path = '/Users/iregon/C3S/dessaps/CDSpy/test_data/114-992'
#out_path = '/Users/iregon/C3S/dessaps/CDSpy/test_data/114-992'
# 1. From command line. Check output dirs, create log, clean output from
# previous runs on file
try:
    sid_deck = sys.argv[1]
    y_ini = int(sys.argv[2])
    y_end = int(sys.argv[3])
    qc_path = sys.argv[4]
    data_path = sys.argv[5]
    out = sys.argv[6]
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)


setup_log(file_mode='w') # Beware: 'w' might not work in python 2!!!!!
logging.info("{0} on files in {1}".format(this_script,data_path))

# PARAMETERS ===================================================================
qc_columns = dict()
qc_columns['sst'] = ['UID','bud', 'clim', 'nonorm', 'freez', 'noval', 'hardlimit']
qc_columns['at'] = ['UID','bud', 'clim', 'nonorm', 'noval', 'mat_blacklist', 'hardlimit']
qc_columns['slp'] = ['UID','bud', 'clim', 'nonorm', 'noval']
qc_columns['dpt'] = ['UID','bud', 'clim', 'nonorm', 'ssat', 'noval', 'rep', 'repsat']
qc_columns['pos'] = ['UID','trk', 'date', 'time', 'pos', 'blklst']
qc_dtype = {'UID':'object'}
qc_delimiter = ','
chunksize = 100000
vars_init = ['sst','at','slp','dpt']
vars_in = []
lat_band_edges = [[-90,-60],[-60,-30],[-30,30],[30,60],[60,90]]
lat_band_tags = ['60-90 S','30-60 S','30 S-30 N','30-60 N','60-90 N']
edges=list(set([item for sublist in lat_band_edges for item in sublist]))
edges.sort()
# 2. Some plotting options
font_size_legend = 11
axis_label_size = 11
tick_label_size = 9
title_label_size = 14
markersize = 2
figsize=(9, 14)
test_colors = dict()
test_colors['bud'] = 'OrangeRed'
test_colors['clim'] = 'DarkOrange'
test_colors['nonorm'] = 'SteelBlue'
test_colors['noval'] = 'DimGray'
test_colors['freez'] = 'DarkTurquoise'
test_colors['hardlimit'] = 'Gold'
test_colors['rep'] = 'LimeGreen'
test_colors['repsat'] = 'OliveDrab'
test_colors['ssat'] = 'ForestGreen'
test_colors['pos'] = 'Purple'
test_colors['mat_blacklist'] = 'Gray'

out_path = dict()
ext='.png'
for vari in vars_init:
    out_path[vari] = os.path.join(out,"_".join([sid_deck,vari,'lat_band','qc_assess','ts']) + ext)
# NOW GO  =====================================================================
# 0. INITIALIZE TO STORE MONTHLY PERCENT OF FAILS

out_assess = dict()
for band in lat_band_tags:
    out_assess[band] = dict()
    for vari in vars_init:
        vari_tests = qc_columns.get(vari)[1:]
        vari_tests.extend(['pos','counts','global'])
        out_assess[band][vari] = pd.DataFrame(index = pd.date_range(start = datetime.datetime(y_ini,1,1),end = datetime.datetime(y_end,12,1),freq='MS'),columns = vari_tests )

for yri in range(y_ini,y_end + 1):
    for moi in range(1,13):
        dt = datetime.datetime(yri,moi,1)
        yr = str(yri)
        mo = '{:02d}'.format(moi)
        # Check firstly header file. If not available or len(0), go to next iteration
        hdr_filename = os.path.join(data_path,"-".join(['header',yr,mo]) + '.psv')
        if not os.path.isfile(hdr_filename):
            logging.warning('NO HEADER TABLE FILE FOUND FOR {0}-{1}: continue'.format(yr,mo))
            continue
        hdr_data = pd.read_csv(hdr_filename,delimiter = "|",usecols=['report_id'],nrows = 2)
        if len(hdr_data) == 0:
            logging.warning('NO DATA IN HEADER TABLE FILE FOR {0}-{1}: continue'.format(yr,mo))
            continue
        # 1. READ POS QC DATA TO DF--------------------------------------------------------
        # Read the whole thing with no chunks. Even if lots of records, ncolumns is not large: otherwise difficult to match record ids with data file
        # Merge pos_qc tests to only qc flag. At this point we do not want that detail on this test
        qc_pos_filename = os.path.join(qc_path,yr,mo,"_".join(['POS','qc',yr+mo,'standard.csv']))
        logging.info('Reading position qc file: {}'.format(qc_pos_filename))
        qc_df_pos = pd.read_csv(qc_pos_filename,dtype = qc_dtype,usecols=qc_columns.get('pos'), delimiter = qc_delimiter, error_bad_lines = False, warn_bad_lines = True )
        qc_df_pos.set_index('UID',inplace=True,drop=True)
        qc_df_pos['total'] = qc_df_pos.sum(axis=1)
        qc_df_pos.drop(qc_columns.get('pos')[1:],axis = 1, inplace=True)
        qc_df_pos['global'] = qc_df_pos['total'].apply(lambda x: 0 if x == 0 else 1  )
        qc_df_pos.drop('total',axis = 1, inplace=True)
        qc_df_pos.rename({'global':'pos'},axis=1,inplace=True)
        if len(qc_df_pos) == 0:
            logging.warning('NO POS QC FLAGS TO APPLY TO {0}-{1}: continue'.format(yr,mo))
            continue
        # 2. LOOP THROUGH VARS --------------------------------------------------------
        # Read vari qc, merge with qc flag, get obs_id from observations file and compute percent of fails per test
        for vari in vars_init:
            qc_vari_filename = os.path.join(qc_path,yr,mo,"_".join([vars_labels['short_name_upper'].get(vari),'qc',yr+mo,'standard.csv']))
            logging.info('Reading {0} qc file: {1}'.format(vari,qc_vari_filename))
            qc_df_vari = pd.read_csv(qc_vari_filename,dtype = qc_dtype,usecols=qc_columns.get(vari), delimiter = qc_delimiter, error_bad_lines = False, warn_bad_lines = True )
            qc_df_vari.set_index('UID',inplace=True,drop=True)
            # Merge with pos now as string and build 'observation_id' as in observations-var table
            qc_df = qc_df_vari.join(qc_df_pos,how='inner')
            #qc_df['total'] = qc_df.sum(axis=1)
            #qc_df['quality_flag'] = qc_df['total'].apply(lambda x: '0' if x == 0 else '1'  )
            #qc_df.drop('total',axis = 1, inplace=True)
            qc_df['index'] = qc_df.index
            qc_df['observation_id'] = qc_df['index'].apply(lambda x: "-".join(['ICOADS-30',x,vars_labels['short_name_upper'].get(vari)]))
            qc_df.drop('index',axis = 1, inplace=True)
            qc_df.set_index('observation_id',drop=True,inplace=True)
            qc_df['total'] = qc_df.sum(axis=1)
            qc_df['global'] = qc_df['total'].apply(lambda x: 0 if x == 0 else 1  )
            qc_df.drop('total',axis = 1, inplace=True)
            if len(qc_df) == 0:
                logging.warning('NO COMBINED POS AND {} QC FLAGS TO APPLY: continue'.format(vari))
                continue

            # Open observations-vari table: only read observation_id, longitude, latitude and observation_value
            data_filename = os.path.join(data_path,"-".join(['observations',vars_labels['short_name_lower'].get(vari),yr,mo]) + '.psv')
            logging.info('Reading {0} data file: {1}'.format(vari,data_filename))
            df_vari = pd.read_csv(data_filename,usecols=[0,6,7,14],sep="|",skiprows=0,na_values=["NULL"])
            df_vari.set_index('observation_id',drop=True,inplace=True)

            qc_df_merged = df_vari.join(qc_df,how='inner') # 'inner', from index intersection: if not in qc_file, won't show
            n_reports = len(qc_df)
            if n_reports == 0:
                    logging.warning('NO COMBINED QC FLAGS AND OBSERVATIONS ({}): continue'.format(vari))
                    continue

            vari_tests = qc_columns.get(vari)[1:]
            vari_tests.extend(['pos'])
            # band_count: total number of quality controlled observations (inner intersection with qc files!!!) in lat band
            # band_failed: number of failed observations (qc_flag == 1, so it is easy peasy to count) per qc test
            band_count = qc_df_merged['latitude'].groupby(pd.cut(qc_df_merged['latitude'], edges,labels=lat_band_tags)).count()
            band_failed = qc_df_merged[vari_tests].groupby(pd.cut(qc_df_merged['latitude'], edges,labels=lat_band_tags)).sum()
            band_global = qc_df_merged['global'].groupby(pd.cut(qc_df_merged['latitude'], edges,labels=lat_band_tags)).sum()
            for band in lat_band_tags:
                out_assess[band][vari].loc[dt] = 100*band_failed.loc[band]/band_count.loc[band]
                out_assess[band][vari]['counts'].loc[dt] = band_count.loc[band]
                out_assess[band][vari]['global'].loc[dt] = band_global.loc[band]


max_counts = dict()
for vari in vars_init:
    max_counts[vari] = np.nanmax( [ out_assess[band][vari]['counts'].max() for band in lat_band_tags ])
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
for vari in vars_init:
    i = len(lat_band_tags)-1
    fig, ax = plt.subplots(nrows=len(lat_band_tags), ncols=1, figsize=figsize, dpi=150)
    idata = -9
    for band in lat_band_tags:
        if (out_assess[band][vari]['counts'].sum()) > 0:
            idata = i
            vari_tests = qc_columns.get(vari)[1:]
            vari_tests.extend(['pos'])
            z = 7
            for test in vari_tests:
                ax[i].plot(out_assess[band][vari].index,out_assess[band][vari][test],marker = '.',linestyle='-',color=test_colors.get(test),label=test,markersize = markersize,zorder=z)
                z = z + 1

            ax[i].set_ylabel('percent failed', color='k')
            ax[i].grid(linestyle=':',which='major')
            ax[i].set_yscale("log")
            ax2[i] = ax[i].twinx()
            ax2[i].fill_between(out_assess[band][vari].index,0,out_assess[band][vari]['counts'].astype('float'), facecolor='Green',alpha=0.15,interpolate=False,label='counts qced',zorder=1)
            ax2[i].fill_between(out_assess[band][vari].index,0,out_assess[band][vari]['global'].astype('float'), facecolor='Red',alpha=0.15,interpolate=False,label='counts any failed',zorder=2)

            ax2[i].set_yscale("log")
            ax2[i].set_ylabel('counts', color='k')
            # Now send the histogram to the back
            ax[i].set_zorder(ax2[i].get_zorder()+1) # put ax in front of ax2
            ax[i].patch.set_visible(False) # hide the 'canvas'
            ax[i].set_xlim([xmin, xmax])
            ax_y_lim = [0.01,100]
            ax[i].set_ylim(ax_y_lim)
            ax2[i].set_ylim([0.01, max_counts[vari] +  0.1*max_counts[vari] ])
            #if i == len(lat_band_tags) - 1:
            #    lines, labels = ax[i].get_legend_handles_labels()
            #    lines2, labels2 = ax2[i].get_legend_handles_labels()
            #    ncols_leg=5
            #    ax[i].legend(lines + lines2,labels + labels2,loc='center', bbox_to_anchor=(0.5, -0.25),ncol=ncols_leg)
            ax[i].text(0.99, 0.9, band , horizontalalignment='right',verticalalignment='center', transform=ax[i].transAxes)
            if i == 0:
                plt.title( sid_deck + ' ' + vars_labels['long_name_upper'].get(vari) + '\n' + " to ".join([str(y_ini),str(y_end)]))

            plt.tight_layout();
        else:
            print('{0} No qc-ed data in band {1}'.format(vari,band))
            ax[i].set_xlim([xmin, xmax])
            ax[i].set_ylim(0.01,100)
            xmid = xmin + (xmax-xmin)//2
            ax[i].text(xmid,vars_saturation.get(vari)[0] + (vars_saturation.get(vari)[1]-vars_saturation.get(vari)[0])/2, "No qc-ed data", ha="center", va="center", size=20,bbox=bbox_props)

            if i == 0:
                ax[i].set_title(sid_deck + ' ' + vars_labels['long_name_upper'].get(vari) + '\n' + " to ".join([str(y_ini),str(y_end)]))

            ax[i].text(0.99, 0.9, band , horizontalalignment='right',verticalalignment='center', transform=ax[i].transAxes)


        i = i - 1
    if idata >= 0:
        lines, labels = ax[idata].get_legend_handles_labels()
        lines2, labels2 = ax2[idata].get_legend_handles_labels()
        ncols_leg=5
        ax[i].legend(lines + lines2,labels + labels2,loc='center', bbox_to_anchor=(0.5, -0.25),ncol=ncols_leg)
    plt.savefig(out_path.get(vari),bbox_inches='tight',dpi = 300)
