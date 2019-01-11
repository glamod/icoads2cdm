#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 15:14:51 2018

"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
# Import require libraries
import sys
import time
import os
import io
import pandas as pd
import math
import numpy as np
import logging
import json
from common.file_functions import *
from .common.functions import *
from .common.globals import *
from .common.properties import *

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False

toolPath = os.path.dirname(os.path.abspath(__file__))

dtype_col_code_mapping = dict()
dtype_col_code_mapping['int'] = 'int'
dtype_col_code_mapping['base36'] = 'int'
dtype_col_code_mapping['object'] = 'object'

def old_code_keys(table,coltype,table_path):
    try:
        mapping_key = [ key for key in dtype_col_code_mapping.keys() if key in coltype ][0]
        code_keys = pd.read_csv(os.path.join(table_path,table),delimiter = ";",usecols = [0], names = ['key'], skiprows = 1, dtype = dtype_col_code_mapping[mapping_key])['key'].tolist()
        if len(code_keys) > 0:
            return code_keys
        else:
            logging.error('Table {0} is empty'.format(table))
            return None
    except Exception as e:
        logging.error('Could not access to code values from table {0}:{1}'.format(table,e))
        return None

def code_keys(table,coltype,table_path):
    try:
        mapping_key = [ key for key in dtype_col_code_mapping.keys() if key in coltype ][0]
        with io.open(os.path.join(table_path,table), 'r',encoding='utf-8') as f:
            code_table = json.load(f)
        code_keys_enc = [ y for x in [x['codes'] for x in code_table] for y in x ] # Get 'codes' list for all elements in json and flatten list
        if not py3:
            code_keys = [ x.encode('utf-8') if isinstance(x, unicode) else x for x in code_keys_enc ]
        else:
            code_keys = code_keys_enc
        if len(code_keys) > 0:
            return code_keys
        else:
            logging.error('Table {0} is empty'.format(table))
            return None
    except Exception as e:
        logging.error('Could not access to code values from table {0}:{1}'.format(table,e))
        return None
    
def ranges(tfr_obj, col_list, schema_df, log_file = None):
    # WORKING ON IMISS AND FMISS VALUES. NOT NAN
    setup_log(log_file)
    tic= time.time()
    logging.info('RANGE TEST BASED ON DATA FORMAT SCHEMA VALUES')
    # Set up output dictionaries
    # Building with dict(zip()) made the list extend method below be applied to all the lists in the range_fails_ln dictionary (!?). This did not applied to the counts increase in number_rejects and number_observations
    range_fails_ln = dict()
    number_rejects = dict()
    number_observations = dict()
    for column in col_list:
        range_fails_ln[column] = []
        number_rejects[column] = 0
        number_observations[column] = 0
    
    logging.info('Check perfomed on requested parameters: {}'.format(",".join(col_list)))
    try:
        ctypes = dict(zip(col_list,schema_df['column_type'].loc[col_list]))
    except Exception as e:
        logging.error('Access to parameter data types in data format schema \"column_type\" column')
        return None
    try:
        valid_mins = dict(zip(col_list,schema_df['valid_min'].loc[col_list]))
        valid_maxs = dict(zip(col_list,schema_df['valid_max'].loc[col_list]))
    except Exception as e:
        logging.error('Access to valid min|max in data format schema \"valid_min\"|\"valid_max\" column')
        return None

    nread = 0
    chunkread = 0
    no_test = []
    for raw_df in tfr_obj:
        chunkread += 1
        logging.info('Processing dataframe chunk {}'.format(str(chunkread)))
        for column in col_list:
                # Will check here data type to assign missing value, however, supposed to be run
                # only on floats. Will perform on other data types if requested and possible (ranges found)
                missing = fmiss if 'float' in ctypes.get(column) else imiss
                number_observations[column] += len(raw_df[raw_df[column] != missing][column])
                if any([ x in ctypes.get(column) for x in ['float','int','base']]):
                    # Validate numeric fields
                    goodValues = raw_df[column].between(valid_mins.get(column) - tol, valid_maxs.get(column) + tol, inclusive=True)
                    goodValues.loc[raw_df[column] == missing ] = True
                    if not all(goodValues):
                        range_fails_ln[column].extend( goodValues.loc[ goodValues == False ].index.tolist() )
                    del goodValues
                else:
                    logging.warning('Non numeric format detected in column {}. Range test not performed'.format(column))
                    no_test.append(column)

        nread+= raw_df.shape[0]

    # QC summary
    range_perc_fails = dict()
    for column in range_fails_ln.keys():
        if column in no_test:
            logging.warning('Test was not performed to data in column {}. Test stats set to nan'.format(column))
            range_perc_fails[column] = np.nan
            number_rejects[column] = np.nan
        elif number_observations[column] == 0:
            logging.warning('No data in column {}. Test stats set to nan'.format(column))
            range_perc_fails[column] = np.nan
            number_rejects[column] = np.nan
        else:
            number_rejects[column] = len(range_fails_ln[column])
            range_perc_fails[column] = 100*number_rejects[column]/number_observations[column]

    toc = time.time()
    logging.info('Time to check ranges "{0:.3f}"'.format(toc - tic))

    return(range_perc_fails, range_fails_ln, number_rejects, number_observations)

class datetimes():
    def __init__(self):
        pass
    def imma1(self,tfr_obj, log_file = None):
        try:
            date_fails_ln = {'date':[]} # Store in dict 1 key dict just to be consistent with other checks and be able to manage info with same tools
            date_perc_fails = {'date':''}
            number_rejects = {'date':''}
            number_observations = {'date':''}
            tic= time.time()
            setup_log(log_file)
            logging.info('IMMA1 DATE-TIME CHECK')
            # Set up output
            nread = 0
            chunkread = 0
            for raw_df in tfr_obj:
                chunkread += 1
                logging.info('Processing dataframe chunk {}'.format(str(chunkread)))
                dateMask = raw_df.apply(lambda row: valid_date( year   = int(row['YR_core']),
                                                                  month  = int(row['MO_core']),
                                                                  day    = int(row['DY_core']),
                                                                  hour   = int( math.floor(row['HR_core']) ),
                                                                  minute = int( math.floor(60.0 * math.fmod(row['HR_core'], 1)) ),
                                                                  ), axis=1)
                if not all(dateMask):
                    date_fails_ln['date'].extend( dateMask.loc[dateMask==False].index.tolist() )
                nread+= raw_df.shape[0]

            # QC summary
            number_rejects['date']  = len(date_fails_ln['date'])
            number_observations['date']  = nread
            date_perc_fails['date'] = 100*number_rejects['date']/number_observations['date']
            toc = time.time()
            logging.info('Time to check dates "{0:.3f}"'.format(toc - tic))
        except Exception as e:
            logging.error('Datetime check failed: {}'.format(e))
            logging.error('Setting stats to nan')
            date_perc_fails['date'] = np.nan
            number_rejects['date'] = np.nan
            number_observations['date'] = np.nan

        return(date_perc_fails, date_fails_ln, number_rejects,number_observations)

class tablecodes():
    def __init__(self):
        self.tables_path = os.path.join(toolPath,"code_tables")

    def imma1(self,tfr_obj, schema_df, log_file = None):
        tic = time.time()
        setup_log(log_file)
        logging.info('IMMA1 TABLE CODES CONSISTENCY CHECK')
        imma1_tables = os.path.join(self.tables_path,code_tables.get('imma1'))
        missing = imiss # NO!!!!! some can be objects!!!: country By definition of table coded values
        # Set up input and output 
        codeTables_keys = dict()
        code_fails_ln = dict()
        number_rejects = dict()
        number_observations = dict()
        missing = dict()

        no_test = []
        logging.info('Reading code tables from {}'.format(imma1_tables))
        for section,table in zip(schema_df.loc[schema_df['codetable'] == schema_df['codetable']].index, schema_df.loc[schema_df['codetable'] == schema_df['codetable']]['codetable']):
            codeTables_keys[section] = code_keys(table,schema_df['column_type'].loc[section],imma1_tables)
            code_fails_ln[section] = []
            number_rejects[section] = 0
            number_observations[section] = 0
            dtype = schema_df['column_type'].loc[section]
            missing[section] = fmiss if 'float' in dtype else (omiss if 'obj' in dtype else imiss) # This order to account for base36 integers
            if not codeTables_keys[section]:
                no_test.append(section)
                logging.warning('Parameter {} will not be checked'.format(section))
        if len(codeTables_keys) == 0:
            logging.error('No code tables defined in current format schema found in {}'.format(imma1_tables))
            return None
        else:
            logging.info('Checking data fields with codetable entries: {}'.format(",".join(codeTables_keys.keys())))

        nread = 0
        chunkread = 0
        for raw_df in tfr_obj:
            chunkread += 1
            logging.info('Processing dataframe chunk {}'.format(str(chunkread)))
            for column in codeTables_keys.keys():
                if column not in no_test:
                    try:
                        boolCodes = raw_df[column].isin(codeTables_keys[column])
                        number_observations[column] += len(raw_df[raw_df[column] != missing[column] ][column])
                        if not all(boolCodes):
                            code_fails_ln[column].extend( boolCodes.loc[(boolCodes == False) & (raw_df[column] != missing[column] )].index.tolist() )
                    except Exception as e:
                        logging.error('Assessing column {0}:{1}'.format(column,e))
                        no_test.append(column)
            nread+= raw_df.shape[0]
        # QC summary
        code_perc_fails = dict()
        for column in code_fails_ln.keys():
            if column in no_test:
                logging.warning('Test was not performed to data in column {}. Test stats set to nan'.format(column))
                code_perc_fails[column] = np.nan
                number_rejects[column] = np.nan
                number_observations[column] = np.nan
            elif number_observations[column] == 0:
                logging.warning('No data in column {}. Test stats set to nan'.format(column))
                code_perc_fails[column] = np.nan
                number_rejects[column] = np.nan
            else:
                number_rejects[column] = len(code_fails_ln[column])
                code_perc_fails[column] = 100*number_rejects[column]/number_observations[column]

        toc = time.time()
        logging.info('Time to check codes "{0:.3f}"'.format(toc - tic))

        return(code_perc_fails, code_fails_ln, number_rejects,number_observations)
