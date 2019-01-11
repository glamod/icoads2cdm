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
    file_io = sys.argv[2]
    out = sys.argv[3]
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)
    
# 2. Some plotting options
font_size_legend = 11
axis_label_size = 11
tick_label_size = 9
title_label_size = 14
figsize=(14, 6)
# END PARAMS ==================================================================
def read_data():
    # Get file paths and read files to df. Merge in single df by report_id
    df_varis = pd.read_csv(file_io,sep=",",parse_dates=[0],header=[0])
    df_varis.set_index(['date'],inplace=True)
    return df_varis
# MAIN ========================================================================

# 3. Plot
plt.rc('legend',**{'fontsize':font_size_legend})          # controls default text sizes
plt.rc('axes', titlesize=axis_label_size)     # fontsize of the axes title
plt.rc('axes', labelsize=axis_label_size)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=tick_label_size)    # fontsize of the tick labels
plt.rc('ytick', labelsize=tick_label_size)    # fontsize of the tick labels
plt.rc('figure', titlesize=title_label_size)  # fontsize of the figure title

data=read_data()
fig, ax = plt.subplots(nrows=1, ncols=1, figsize=figsize, dpi=150) 
ax.plot(data.loc[data['imma']>0].index,data['imma'].loc[data['imma']>0],color='DarkSlateGrey',marker='o',markersize=3,linewidth=3,zorder = 1)
for vari in data.columns[2:]:
    ax.plot(data.loc[data[vari]>0].index,data[vari].loc[data[vari]>0],color=vars_color.get(vari),marker='o',markersize=2,linewidth=1)

ax.plot(data.loc[data['header']>0].index,data['header'].loc[data['header']>0],linestyle=":",color='DarkGrey',marker='o',markersize=3,linewidth=2,zorder=len(data.columns))

ax.grid(linestyle=":",color='grey')
ax.set_yscale("log")
ax.set_ylabel('counts', color='k')
ax.legend(loc='center',bbox_to_anchor=(0.5, -0.1),ncol=5)
plt.title(sid_deck + ' icoads2cds io')

plt.tight_layout();
plt.savefig(os.path.join(out,sid_deck + '_imma_process_io.png'),bbox_inches='tight',dpi = 300)

