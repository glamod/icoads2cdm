#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 10:38:05 2018

Maps data from different native formats (formati) to the Common Data Model (CDM) space.

- Input data in a pandas df as a TextFileReader object as large datasets need to be accounted for

- Mappings from formati to the CDM space are stored in json files (mapping files) @ ./mappings/formati/:
    - common.json:  fields that are "transversal" accross the whole record
    - vari.json:    fields that are particular to a measured variable

- formati df columns names must be consistent with:
    - formati schema index (where some information on the original data format is retrieved from)
    - data_key field in the formati - CDM mapping files

- Mapping from original data format to cdm can be:
    - Direct: a field in formati is directly translated to the CDM space:
        -> Tuple cdm_key - data_key in mapping file
    - Simple transform: a field in formati undergoes a simple transformation to the CDM space:
        -> Triplet cdm_key - data_key(1 only field) - transform in mapping file
        Eg. transform longitudes from 0/360 to -180/180
    - Multiple transformation: several fields in formati generate a field in the CDM space:
        -> Triplet cdm_key - data_key(multiple fields separated by commas) - transform in mapping file
        Eg. datetime from YYYY-MM-DD HH.hh
    - Direct assignement: a fixed value is assigned to the CDM field in the mapping file
        -> Tuple cdm_key - default apply

    ......

- Transfrom functions are stored in the common/functions module. More can be added to accomodate future needs.

- Ouput is a TextFileReader object containing a df that is multiindexed in the columns:
    - At the main level:  'common' and each of the 'vari' in the dataset.
    - The secondary level has all the individual fields applicable.

- Data types in the output df are not specifically stored so far.

@author: iregon
"""

from __future__ import print_function
from __future__ import absolute_import
from collections import OrderedDict # This is because python2 dictionaries do not keep key insertion order
# Import require libraries
import sys
sys.path.append('/Users/iregon/C3S/dessaps/CDSpy')
import os
import pandas as pd
import json
import logging
import time
import io
from .common.globals import *
from common.file_functions import *

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False


toolPath = os.path.dirname(os.path.abspath(__file__))
tablesPath = os.path.join(toolPath,'common')

#-------------------------- AUX FUNCTIONS  ------------------------------------
def print_int(df):
    return df.apply( lambda x: str(x) if x != imiss else 'NULL' )

def print_float(df,precision):
    if len(precision.split(".")) > 1:
        decimal_positions = len(precision.split(".")[1])
    else:
        decimal_positions = 0

    format_float='{:.' + str(int(decimal_positions)) + 'f}'
    return df.apply( lambda x: format_float.format(x) if x != fmiss else 'NULL' )

def print_object(df):
    return df.apply( lambda x: x if x != omiss else 'NULL' )

#---------------------- END AUX FUNCTIONS  ------------------------------------

#------------------------------ MAIN ------------------------------------------

def individual_records(df_tfr_obj, out_file , table_type, var_out = None, precisions = None, log_file = None):
    logging.info('CDM TABLE CONTENT CREATOR')
    if 'observations' in table_type:
        logging.info('Creating table {} from dataframe'.format("-".join([table_type,var_out])))
    else:
        logging.info('Creating table {} from dataframe'.format(table_type))
    if table_type not in ['header','observations']:
        logging.error('Input table type {0} not supported. Valid table types are {1}'.format(table_type,",".join(['common','observations'])))
        return None
    elif table_type == 'observations' and not var_out:
        logging.error('Output variable name must be provided for table type \'{}\'. No one has been provided'.format(table_type))
        return None

    df_level0 = 'common' if table_type == 'header' else var_out

    setup_log(log_file)
    tic = time.time()
    table_path = os.path.join(tablesPath,'tables_dtypes.json')
    try:
        with open(table_path) as json_file:
            tables_content = json.load(json_file, object_pairs_hook=OrderedDict)

        table_content = tables_content.get(table_type)
    except Exception as e:
        logging.error('Could not load CDM table content file {0}: {1}'.format(table_path,e))
        return None
    try:
        ichunk = 0
        for df in df_tfr_obj:
            hdr_level_columns = df['common'].columns
            out_level_columns = df[df_level0].columns
            table_df = pd.DataFrame()
            for cdm_field in table_content.keys():
                df_level = df_level0 if cdm_field in out_level_columns else ('common' if cdm_field in hdr_level_columns else None)
                if df_level:
                    ctype = table_content.get(cdm_field)
                    if 'float' in ctype:
                        precision = precisions[df_level].get(cdm_field)
                        if not precision:
                            precision = '0.01'
                            logging.warn('Precision not defined for {}. Set to 1 hundreth'.format(cdm_field))
                        table_df[cdm_field] = print_float(df[df_level][cdm_field],precision)
                    elif 'int' in ctype:
                        table_df[cdm_field] = print_int(df[df_level][cdm_field])
                    else:
                        table_df[cdm_field] = print_object(df[df_level][cdm_field])
                else:
                    table_df[cdm_field] = 'NULL'
            header = True if ichunk == 0 else False
            mode = 'w' if ichunk == 0 else 'a'
            if var_out:
                table_df = table_df.loc[table_df['observation_value'] != 'NULL' ]
            table_df.to_csv(out_file,index = False,header = header ,sep = "|", mode = mode, encoding = 'utf-8')
            ichunk += 1

    except Exception as e:
        logging.error('At ichunk {0}:{1}'.format(ichunk,e))
        #logging.error('At ichunk {0}-{1}: {2}'.format(ichunk,cdm_field,e))
        return None

    return True
#------------------------------  END MAIN -------------------------------------
