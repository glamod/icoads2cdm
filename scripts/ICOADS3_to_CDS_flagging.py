#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 20 11:43:22 2018

export PYTHONPATH="$(pwd)/py_tools:${PYTHONPATH}"

Script to reprocess and format ICOADS release 3.0. imma1 files to C3S-CDS CDM tables.

To run from command line:

    python(v) ICOADS3_to_CDS.py data_file YYYY m config_file outdir_data

Command line args:
    data_file: path to imma1 data file
    YYYY: year of data records
    m: month of data records
    config_file: path to script configuration json file with processing options
    outdir_data: path to output data files

Processing options (from config_file). Minimum requirements for this version:
        - "supplemental": dictionary with info on whether and how to process supplemental data
        - "attachments_out": list with imma1 attachments to read from the imma1 files
        - "variables_out": list with measured parameters to process and output
INPUT:
    1) imma1 data:
    - unique source-deck is assumed
    - developed for monthly data files (although no specific restrictions within code to ensure data file complies with this....)
    2) config_file: json file with processing options for the file.
    Minimum requirements for this version are keys:
        - "supplemental": dictionary with info on whether and how to process supplemental data ()
        - "attachments_out": list with imma1 attachements to read from the imma1 files
        - "variables_out": list with measured parameters to process and output

OUTPUT:
    1) in outdir_data: files with data in cdm like tables
        header-filename.psv
        observations-[variables_out]-filename.psv
    2) in outdir_data/logs:
        filename.log
        filename_[check_name]_all.json
        filename_[check_name]_indexes_all.json

"""
from __future__ import absolute_import
from __future__ import division
import future        # pip install future
import builtins      # pip install future
import past          # pip install future
import six           # pip install six

import os
import sys
sys.path.append('/Users/iregon/C3S/dessaps/CDSpy/py_tools')
import logging
import pandas as pd
import __main__ as main
import glob
import logging
from common.file_functions import *
from common.var_properties import *
from common.file_functions import *
import shutil

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
    yr = sys.argv[1]
    mo = sys.argv[2]
    qc_path = sys.argv[3]
    data_path = sys.argv[4]
    out_path = sys.argv[5]
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)

if  not os.path.isdir(out_path):
    print("Output directory not found: {}".format(out_path))
    exit(1)

outdir_logs = os.path.join(data_path,'logs')
logfile = os.path.join(outdir_logs,"_".join([yr + "-" + mo,'flagging.log']))

status = mkdir_ifNot(outdir_logs)
if status != 0:
    print('Could not create log output directory {0}: {1}'.format(outdir_logs,status))
    exit(1)

setup_log(log_file=logfile,file_mode='w') # Beware: 'w' might not work in python 2!!!!!
logging.info("{0} on files {1},{2}".format(this_script,yr,mo))
logging.info('Searching path {}'.format(os.path.join(out_path,"-".join(['observations','*',yr,mo]) + '_qc.psv')))
for f in glob.glob(os.path.join(out_path,"-".join(['observations','*',yr,mo]) + '_qc.psv')):
    logging.info('Removing file {}'.format(f))
    os.remove(f)

# PARAMETERS ==================================================================
qc_columns = dict()
qc_columns['sst'] = ['UID','bud', 'clim', 'nonorm', 'freez', 'noval', 'hardlimit']
qc_columns['at'] = ['UID','bud', 'clim', 'nonorm', 'noval', 'mat_blacklist', 'hardlimit']
qc_columns['slp'] = ['UID','bud', 'clim', 'nonorm', 'noval']
qc_columns['dpt'] = ['UID','bud', 'clim', 'nonorm', 'ssat', 'noval', 'rep', 'repsat']
qc_columns['pos'] = ['UID','trk', 'date', 'time', 'pos', 'blklst']
qc_dtype = {'UID':'object'}
qc_delimiter = ','
data_dtype = 'object'
data_delimiter = '|'
chunksize = 100000
vars_init = ['sst','at','slp','dpt']
vars_in = []
# NOW GO  =====================================================================
# 0. CHECK NECESSARY INPUT, ABANDON OTHERWISE
qc_pos_filename = os.path.join(qc_path,yr,mo,"_".join(['POS','qc',yr+mo,'standard.csv']))
if not os.path.isfile(qc_pos_filename):
    logging.error('POSITION QC file not found: {}'.format(qc_pos_filename))
    exit(1)
    
# Check firstly header file. If not available or len(0), just abandon with no error
hdr_filename = os.path.join(data_path,"-".join(['header',yr,mo]) + '.psv')
if not os.path.isfile(hdr_filename):
    logging.warning('NO HEADER TABLE FILE FOUND FOR {0}-{1}: exit with no error'.format(yr,mo))
    exit(0)

hdr_data = pd.read_csv(hdr_filename,delimiter = data_delimiter,usecols=['report_id'],nrows = 2)

if len(hdr_data) == 0:
    logging.warning('NO DATA IN HEADER TABLE FILE FOR {0}-{1}: exit with no error'.format(yr,mo))
    exit(0)
    
for vari in vars_init:
    qc_vari_filename = os.path.join(qc_path,yr,mo,"_".join([vars_labels['short_name_upper'].get(vari),'qc',yr+mo,'standard.csv']))
    data_filename = os.path.join(data_path,"-".join(['observations',vars_labels['short_name_lower'].get(vari),yr,mo]) + '.psv')
    if not os.path.isfile(qc_vari_filename) or not os.path.isfile(data_filename):
        logging.warning('Could not finc {0} qc or data file: {1},{2}'.format(vari,qc_vari_filename,data_filename))
        logging.warning('Flagging of {0} excluded'.format(vari))
    else:
        vars_in.append(vari)

if len(vars_in) == 0:
    logging.error('NO DATA OR QC FLAGS TO APPLY')
    exit(1)
# 1. READ POS QC DATA TO DF--------------------------------------------------------
# Read the whole thing with no chunks. Even if lots of records, ncolumns is not large: otherwise difficult to match record ids with data file
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
    logging.error('NO POS QC FLAGS TO APPLY')
    exit(1)
# 2. LOOP THROUGH VARS --------------------------------------------------------
# Read vari qc
for vari in vars_in:
    qc_vari_filename = os.path.join(qc_path,yr,mo,"_".join([vars_labels['short_name_upper'].get(vari),'qc',yr+mo,'standard.csv']))
    logging.info('Reading {0} qc file: {1}'.format(vari,qc_vari_filename))
    qc_df_vari = pd.read_csv(qc_vari_filename,dtype = qc_dtype,usecols=qc_columns.get(vari), delimiter = qc_delimiter, error_bad_lines = False, warn_bad_lines = True )
    qc_df_vari.set_index('UID',inplace=True,drop=True)
    qc_df_vari['total'] = qc_df_vari.sum(axis=1)
    qc_df_vari.drop(qc_columns.get(vari)[1:],axis = 1, inplace=True)
    qc_df_vari['global'] = qc_df_vari['total'].apply(lambda x: 0 if x == 0 else 1  )
    qc_df_vari.drop('total',axis = 1, inplace=True)
    qc_df_vari.rename({'global':vari},axis=1,inplace=True)
    # Merge with pos now as string and build 'observation_id' as in observations-var table
    qc_df = qc_df_vari.join(qc_df_pos,how='inner')
    qc_df['total'] = qc_df.sum(axis=1)
    qc_df.drop(['pos',vari],axis = 1, inplace=True)
    qc_df['quality_flag'] = qc_df['total'].apply(lambda x: '0' if x == 0 else '1'  )
    qc_df.drop('total',axis = 1, inplace=True)
    qc_df['index'] = qc_df.index
    qc_df['observation_id'] = qc_df['index'].apply(lambda x: "-".join(['ICOADS-30',x,vars_labels['short_name_upper'].get(vari)]))
    qc_df.drop('index',axis = 1, inplace=True)
    if len(qc_df) == 0:
        logging.error('NO COMBINED POS AND {} QC FLAGS TO APPLY'.format(vari))
        continue
    # Open observations-vari table
    data_filename = os.path.join(data_path,"-".join(['observations',vars_labels['short_name_lower'].get(vari),yr,mo]) + '.psv')
    data_filename_qc = os.path.join(out_path,"-".join(['observations',vars_labels['short_name_lower'].get(vari),yr,mo]) + '_qc.psv')
    data_tfrObj = pd.read_csv(data_filename,dtype = data_dtype,delimiter = data_delimiter, error_bad_lines = False, warn_bad_lines = True, keep_default_na = False, chunksize = chunksize )
    data_pd_ref = data_tfrObj.f
    data_pd_options = data_tfrObj.orig_options
    data_tfrObj,data_mismatches_tfrObj,matches,mismatches = replace_from_mirror(data_tfrObj,qc_df,('observation_id'),['quality_flag'],drop_mismatches = False,ignore_mismatches = False,value_mismatches = {'quality_flag':'2'})
    ichunk = 1
    header = True
    #shutil.move(os.path.join(src, filename), os.path.join(dst, filename))
    for df in data_tfrObj:
        header = True if ichunk == 1 else False
        mode = 'w' if ichunk == 1 else 'a'
        df.to_csv(data_filename_qc,index = False,header = header ,sep = "|", mode = mode, encoding = 'utf-8')
        ichunk += 1

    if os.path.isfile(data_filename_qc) and os.stat(data_filename_qc).st_size>0:
        shutil.move(data_filename_qc, data_filename)
    else:
        logging.error('QC flagging resulted in no file or empty file. Original data file not replaced.')
