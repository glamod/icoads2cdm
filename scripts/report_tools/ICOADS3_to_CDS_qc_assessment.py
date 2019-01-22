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
    y_ini = int(sys.argv[1])
    y_end = int(sys.argv[2])
    qc_path = sys.argv[3]
    data_path = sys.argv[4]
    #out_path = sys.argv[5]
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)

print(y_ini,y_end,qc_path,data_path)
#if  not os.path.isdir(out_path):
#    print("Output directory not found: {}".format(out_path))
#    exit(1)


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
# NOW GO  =====================================================================
# 0. INITIALIZE TO STORE MONTHLY PERCENT OF FAILS
out_assess = dict()
for vari in vars_init:
    out_assess[vari] = pd.DataFrame(index = pd.date_range(start = datetime.datetime(y_ini,1,1),end = datetime.datetime(y_end,12,1),freq='MS'),columns = qc_columns.get(vari)[1:])


print(out_assess)

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

             
            out_assess[vari].loc[dt] =  qc_df_merged.sum()/n_reports

print(out_assess)
