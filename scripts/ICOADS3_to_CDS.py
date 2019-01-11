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
import glob
import io
from io import open # To allow for encoding definition
import __main__ as main
import json
import simplejson # Allows direct conversion from Nan to json null
import itertools
from common.file_functions import *
from mdf_reader import data_reader,checkers
from mdf_reader.common import properties as reader_properties
from cdm import mapper, table_creator

if sys.version_info[0] >= 3:
    py3 = True
    from io import StringIO
else:
    py3 = False
    from io import BytesIO

this_script = main.__file__

# PROCESS INPUT PARAMETERS AND INITIALIZE  AND CHECK SOME STUFF ===============
#==============================================================================
# 1. From command line. Check output dirs, create log, clean output from
# previous runs on file
try:
    infile = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    config_file = sys.argv[4]
    outdir_data = sys.argv[5]
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)
    
if  not os.path.isdir(outdir_data):
    print("Output directory not found: {}".format(outdir_data))
    exit(1)
    
file_basename = os.path.basename(infile).split(".")[0]
outdir_logs = os.path.join(outdir_data,'logs')
outdir_mismatches = os.path.join(outdir_data,'rejects_pub47')
logfile = os.path.join(outdir_logs,".".join([file_basename,'log']))

status = mkdir_ifNot(outdir_logs)
if status != 0:
    print('Could not create log output directory {0}: {1}'.format(outdir_logs,status))
    exit(1)

setup_log(log_file=logfile,file_mode='w') # Beware: 'w' might not work in python 2!!!!!
logging.info("{0} on file {1}".format(this_script, infile))

logging.info('REMOVING OUTPUT FILES FROM PREVIOUS RUNS ON INPUT FILE')
for f in glob.glob(os.path.join(outdir_logs,"*" + file_basename + "*")):
    if '.log' not in f:
        logging.info('Removing file {}'.format(f))
        os.remove(f)
logging.info('Searching path {}'.format(os.path.join(outdir_data,"*" + file_basename + "*.psv")))        
for f in glob.glob(os.path.join(outdir_data,"*" + file_basename + "*.psv")):
    logging.info('Removing file {}'.format(f))
    os.remove(f)   
     
# 2. From  process configuration file: check input
logging.info("READING PROCESSING OPTIONS FROM {}:".format(config_file))
with open(config_file) as options_file:
        myOptions = json.load(options_file)

basic_options = ["attachments_out","variables_out"]
for option in basic_options:
    try:
        myOptions.get(option)
        logging.info("{0}: {1}".format(option,",".join(myOptions.get(option))))
    except:
        logging.error("Option {0} not defined in configuration file".format(option))
        exit(1)

extra_options = [ key for key in myOptions if key not in basic_options ]
if "supplemental" in extra_options:
    supplemental = myOptions.get("supplemental")
    if supplemental.get("process"):
        if not supplemental.get("format"):
            logging.error("Supplemental data format not specified")
            exit(1)
        else:
            logging.info("Supplemental data format: {}".format(supplemental.get("format")))
            supp_format = supplemental.get("format")
        if not supplemental.get("replace"):
            logging.warning("Supplemental data replacement NOT requested")
            replacements = None
        else:
            logging.info("Supplemental data replacements requested")
            replacements = supplemental.get("replace")
    else:
        logging.warning("Supplemental data processing NOT requested")
        supp_format = None
        replacements = None
if "metadata" in extra_options:
    metadata = myOptions.get("metadata")
    if metadata.get("process"):
        logging.info("Metadata process required")
        if not metadata.get("format"):
            logging.error("Metadata format not specified")
            exit(1)
        else:
            metadata_format = metadata.get("format")
            logging.info("Metadata format: {}".format(metadata_format))
        if not metadata.get("path"):
            logging.error("Metadata path not specified")
            exit(1)
        else:
            metadata_path = metadata.get("path")
            logging.info("Metadata path {}".format(metadata_path))
    else:
        logging.warning("Metadata processing NOT requested")
else:
    metadata = None
scriptPath = os.path.dirname(os.path.abspath(__file__))
# 3. Harcoded parameters for this process
vars_imma = {'sst':'SST_core','at':'AT_core','slp':'SLP_core',
             'dpt':'DPT_core','wbt':'WBT_core',
             'ws':'W_core','wd':'D_core'}
drop_which_bad = ['LAT_core','LON_core']
for vari in myOptions.get('variables_out'):
	drop_which_bad.append(vars_imma.get(vari))

chunksize = 100000

# END PARAMS ==================================================================

# FUNCTIONS ===================================================================
def report_fails(test_name,outdir,perc_fails,number_rejects,number_observations,bad_lines):
    
    json_name = os.path.join(outdir,"_".join([file_basename,test_name,"indexes"]) + ".json")
    test_out = {'year':year,'month': month,'bad_records':bad_lines}

    # Could be simpler, except for python2....
    with io.open(json_name,'w',encoding="utf-8") as jn:
        json_str = simplejson.dumps(test_out, ignore_nan=True, ensure_ascii = False)
        jn.write(json_str)

    json_name = os.path.join(outdir,"_".join([file_basename,test_name]) + ".json")
    test_out = dict()
    test_out['year'] = year
    test_out['month'] = month
    test_out['number_observations'] = number_observations
    test_out['number_rejects'] = number_rejects
    test_out['percent_rejects'] = perc_fails
    with io.open(json_name,'w',encoding="utf-8") as jn:
        json_str = simplejson.dumps(test_out, ignore_nan=True, ensure_ascii = False)
        jn.write(json_str)
# END FUNCTIONS ===============================================================


# MAIN ========================================================================

# 1. READ ICOADS DATA TO DF----------------------------------------------------
format_reader = reader_properties.format_reader
# Core + attm to df; supp data to buffer
logging.info('READING INPUT IMMA1 FILE {}'.format(infile))
reader_main_str = ".".join(['data_reader',format_reader.get('imma1')])
reader_main = eval(reader_main_str)
[imma1_tfr_Obj,imma1_schema, supp_buff] = reader_main(infile, attmOut = myOptions.get("attachments_out"),chunksize = chunksize, log_file = logfile)
imma1_pd_ref = imma1_tfr_Obj.f
imma1_pd_options = imma1_tfr_Obj.orig_options
# Supp to df
if supplemental.get("process"):
    logging.info('READING SUPPLEMENTAL DATA ({} FORMAT) FROM BUFFER'.format(supp_format))
    reader_supp_str = ".".join(['data_reader',format_reader.get(supp_format)])
    reader_supp = eval(reader_supp_str)
    [supp_tfr_Obj, supp_schema ] = reader_supp(supp_buff, chunksize = chunksize, log_file = logfile)
    supp_pd_ref = supp_tfr_Obj.f
    supp_pd_options = supp_tfr_Obj.orig_options

# 2. APPLY SELECTION CRITERIA--------------------------------------------------
# First on supplemental (if) data based on imma section values:
criteria = myOptions.get('select')
if criteria:
    logging.info('APPLYING SELECTION CRITERIA')
    if supplemental.get("process"):
        supp_tfr_Obj = select_from_external_col_df_tfr(supp_tfr_Obj,supp_pd_options,criteria,tfr_external = imma1_tfr_Obj)
        imma1_tfr_Obj = restore_df_tfr(imma1_pd_ref, imma1_pd_options)
        supp_pd_ref = supp_tfr_Obj.f
        supp_pd_options = supp_tfr_Obj.orig_options
    
    imma1_tfr_Obj = select_from_external_col_df_tfr(imma1_tfr_Obj,imma1_pd_options,criteria)  
    imma1_pd_ref = imma1_tfr_Obj.f
    imma1_pd_options = imma1_tfr_Obj.orig_options  

# 3. DO SOME CHECKS ON DATA----------------------------------------------------
# If a test is not performed for any reason on a parameter which is subject to be tested
# its stat (percent of fails) is set to nan in test function

# 3.1. Range check
# Check parameters with column_type as float in schema: assumed measured variables
# (hour will enter this check in imma1 as is decimal hours.....)
logging.info('BASIC QUALITY CHECKS ON IMMA1 RECORDS')
logging.info('RANGE CHECK')
float_params = drop_which_bad #imma1_schema.loc[imma1_schema['column_type'].str.contains("float")].index
[range_perc_fails, range_fails_ln, range_number_rejects, range_number_observations] = checkers.ranges(imma1_tfr_Obj, float_params, imma1_schema)
imma1_tfr_Obj = restore_df_tfr(imma1_pd_ref, imma1_pd_options)

# 3.2. Date check
# This is so far data format dependent, as date times are given in different formats....
logging.info('DATE CHECK')
checker_date_main_str = ".".join(['checkers.datetimes()','imma1'])
checker_date_main = eval(checker_date_main_str)
[date_perc_fails, date_fails_ln, date_number_rejects, date_number_observations] = checker_date_main(imma1_tfr_Obj)
imma1_tfr_Obj = restore_df_tfr(imma1_pd_ref, imma1_pd_options)

# 3.3. Table codes check: function checks with a codetable defined in its schema
logging.info('CODED PARAMETERS')
checker_codes_main_str = ".".join(['checkers.tablecodes()','imma1'])
checker_codes_main = eval(checker_codes_main_str)
[code_perc_fails, code_fails_ln,code_number_rejects, code_number_observations] = checker_codes_main(imma1_tfr_Obj, imma1_schema)
imma1_tfr_Obj = restore_df_tfr(imma1_pd_ref, imma1_pd_options)


# 4. SAVE CHECK RESULTS -------------------------------------------------------
# percents and bad lines -> won't ouput bad lines to file,
# takes too much on large files.Instead, just output bad line indexes
logging.info('REPORTING CHECK RESULTS')
report_fails('range',outdir_logs,range_perc_fails,range_number_rejects, range_number_observations,range_fails_ln)
report_fails('code',outdir_logs,code_perc_fails,code_number_rejects, code_number_observations,code_fails_ln)
report_fails('date',outdir_logs,date_perc_fails,date_number_rejects, date_number_observations,date_fails_ln)

# 5. CLEAN DATA TO PROCESS ACCORDING TO CHECKS --------------------------------
# Discard not valid location,value and time (range/date tests)
# Add date fails to range dictionary to simplify here
# skiprows option does not seem to work on chunks, could have just changed reading options from read_csv
logging.info('CLEANING DATA')
drop_which_bad.append('date')
logging.info('Removing records failing the following tests: {}'.format(",".join(drop_which_bad)))
range_fails_ln['date'] = date_fails_ln['date']
idx_bad = list(set(itertools.chain.from_iterable([ range_fails_ln.get(x) for x in drop_which_bad ])))
idx_bad.sort()
if len(idx_bad) == 0:
    idx_bad = None
    logging.info('NO RECORDS TO REMOVE')
else:
    logging.info('{} RECORDS TO REMOVE'.format(len(idx_bad)))
    logging.info('Removing on imma1 data')
    imma1_tfr_Obj = remove_records_df_tfr(imma1_tfr_Obj,imma1_pd_options,idx_bad)
    imma1_pd_ref = imma1_tfr_Obj.f
    imma1_pd_options = imma1_tfr_Obj.orig_options
    imma1_tfr_Obj = restore_df_tfr(imma1_pd_ref, imma1_pd_options)

    if supplemental.get("process"):
        logging.info('Removing on supplemental data')
        supp_tfr_Obj = remove_records_df_tfr(supp_tfr_Obj,supp_pd_options,idx_bad)
        supp_pd_ref = supp_tfr_Obj.f
        supp_pd_options = supp_tfr_Obj.orig_options
        supp_tfr_Obj = restore_df_tfr(supp_pd_ref, supp_pd_options)

# 6. CREATE FINAL DF ON THE MAIN DATA FRAME: CHANGE DESIRED COLUMNS THERE
# Create the final data set schema
# Mmmm, potential problem where changed field is not mapped directly or via simple
# transformation to CDM:
#    Eg. swap LON: OK. Just disable/enable 0/360 to -180/180
#        swap HH.hh....CDM date as YYYY MM DD HH.hh from imma1 time fields
#                               as YYYYMMDD HHMM from meds time fields
#  We here, an so far ASSUME THAT REPLACEMENTS ARE ON FIELDS WITH SIMPLE TRANSLATION
if supplemental.get("process") and supplemental.get("replace"):
    logging.info('REPLACING DATA FROM SUPP RECORDS')
    L1_buff = StringIO() if py3 else BytesIO()
    for imma1_df,supp_df in zip(imma1_tfr_Obj, supp_tfr_Obj):
        for imma1_key in replacements.keys():
            imma1_df[imma1_key] = supp_df[replacements.get(imma1_key)]
        imma1_df.to_csv(L1_buff,columns = imma1_schema.index,mode = 'a', header=False, encoding = 'utf-8') # will have to append here

    L1_buff.seek(0)
    imma1_tfr_Obj = pd.read_csv( L1_buff, names = imma1_schema.index , chunksize = chunksize )#dtype = imma1_pd_options['dtype'],
    imma1_pd_ref = imma1_tfr_Obj.f
    imma1_pd_options = imma1_tfr_Obj.orig_options


# Now change schema: data has changes, schema must update accordingly: basically update precision  from replacements and names!!!
out_schema = imma1_schema.copy()
if supplemental.get("process") and replacements:
    for imma1_key in replacements.keys():
        out_schema.loc[imma1_key] = supp_schema.loc[replacements.get(imma1_key)]

# 7. INTO THE CDM SPACE
# Map the full thing, include in mapping parameters not actually in cdm tables, but needed to construct these: eg. reports: source and station configuration and so on
# Need to define measured parameter names (label) in a common way: the cdm way....no short name there. No label.....
logging.info('INTO THE CDM SPACE')
[cdm_df_tfr, precisions] = mapper.map_cdm(imma1_tfr_Obj,out_schema,'imma1',year,month,var_out = myOptions.get("variables_out"),replacements = replacements ,replace_format = supp_format,remove_null_fields = False, chunksize = chunksize)
cdm_pd_ref = cdm_df_tfr.f
cdm_pd_options = cdm_df_tfr.orig_options
cdm_df_tfr = restore_multiIdxColumn_df_tfr(cdm_pd_ref,cdm_pd_options)
#  8. METADATA

if metadata:
    # Read data files
    logging.info('READING METADATA')
    metadata_file = os.path.join(metadata_path,'-'.join([str(year),str(month),'01.csv']))
    [pub47_tfr_Obj,pub47_schema] = data_reader.read_fdf(metadata_file,metadata_format,field_delimiter = ";", chunksize=chunksize, log_file = None)
    pub47_cdm_tfr_Obj,pub47_precisions = mapper.map_cdm(pub47_tfr_Obj,pub47_schema,metadata_format,year,month,var_out = myOptions.get("variables_out"), replacements = None ,replace_format = None, remove_null_fields = False, log_file = None, chunksize=chunksize)
    pub47 = pub47_cdm_tfr_Obj.get_chunk()
    replacement_columns = pub47.columns.tolist()
    logging.info('ADDING METADATA')

    cdm_df_tfr = restore_multiIdxColumn_df_tfr(cdm_pd_ref,cdm_pd_options)
    cdm_df_tfr,cdm_df_mismatches_tfr,matches,mismatches = replace_from_mirror(cdm_df_tfr,pub47,('common','primary_station_id'),replacement_columns,drop_mismatches = True ,ignore_mismatches = True)
    cdm_pd_ref = cdm_df_tfr.f
    cdm_pd_options = cdm_df_tfr.orig_options
    cdm_pd_mismatches_ref = cdm_df_mismatches_tfr.f
    cdm_pd_mismatches_options = cdm_df_mismatches_tfr.orig_options
    if len(mismatches) > 0:
        status = mkdir_ifNot(outdir_mismatches)
        with io.open(os.path.join(outdir_mismatches,file_basename + '_call_sign_list.txt'),'w',encoding="utf-8") as f:
            if py3:
                f.write("\n".join(mismatches))
            else:
                try:
                    f.write("\n".join([str(x).encode('unicode-escape') for x in mismatches]))
                    #f.write(unicode("\n".join([str(x) for x in mismatches]), 'unicode-escape'))
                except Exception as e:
                    logging.error('Reporting pub47 mismatches: {}'.format(e))
    for key in precisions:
        precisions[key].update(pub47_precisions[key])
# 9. CREATE SQL LIKE TABLES FOR DB INSERTION
logging.info('CREATING CDM DB TABLE LIKE FILES')
status = table_creator.individual_records(cdm_df_tfr, os.path.join(outdir_data,'header-' + file_basename + '.psv') , 'header', precisions = precisions)
cdm_df_tfr = restore_multiIdxColumn_df_tfr(cdm_pd_ref,cdm_pd_options)
for vari in myOptions.get("variables_out"):
    status = table_creator.individual_records(cdm_df_tfr, os.path.join(outdir_data,'-'.join(['observations',vari,file_basename]) + '.psv') , 'observations', var_out = vari, precisions = precisions)
    cdm_df_tfr = restore_multiIdxColumn_df_tfr(cdm_pd_ref,cdm_pd_options)
# 10. WRITE MISTMATCHES TO OUTPUT/REJECTS
if metadata and len(mismatches) > 0:
    logging.info('CREATING HEADER CDM DB TABLE FOR METADATA MISMATCHES')
    status = table_creator.individual_records(cdm_df_mismatches_tfr, os.path.join(outdir_mismatches,'header-' + file_basename + '.psv') , 'header', precisions = precisions)	
    cdm_df_mismatches_tfr = restore_multiIdxColumn_df_tfr(cdm_pd_mismatches_ref,cdm_pd_mismatches_options)
    for vari in myOptions.get("variables_out"):
        status = table_creator.individual_records(cdm_df_mismatches_tfr, os.path.join(outdir_mismatches,'-'.join(['observations',vari,file_basename]) + '.psv') , 'observations', var_out = vari, precisions = precisions)
        cdm_df_mismatches_tfr = restore_multiIdxColumn_df_tfr(cdm_pd_mismatches_ref,cdm_pd_mismatches_options)


logging.info('END')
