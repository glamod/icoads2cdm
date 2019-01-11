#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 10:38:05 2018
Maps elements of a given data representation to the Common Data Model (CDM) space.
Elements from two main categories are mapped:
•	"common" elements as defined in common.json mapping file
•	elements specific to observed variables “var” as defined in observations_var.json mapping file.


- Input data in a pandas df as a TextFileReader object as large datasets need to be accounted for

- Mappings from formati to the CDM space are required in json files (mapping files) @ ./mappings/formati/:
    - common.json:  fields that are "transversal" accross the whole record: can accomodate more fields than just the info to be output to the header table
    - observations_vari.json:    fields that are particular to a measured variable  (vari)

- Dataframe column names in the input TextFileReader object must be consistent with:
    - formati schema index (where some information on the original data format is retrieved from)
    - data_key field in the formati - CDM mapping files

- Mapping from original data format to cdm can be:
    - Direct: a field in formati is directly translated to the CDM space:
        -> Tuple "cdm_key - data_key" in mapping file
    - Simple transform: a field in formati undergoes a simple transformation to the CDM space:
        -> Triplet "cdm_key - data_key(1 only field) - transform" in mapping file
        Eg. transform longitudes from 0/360 to -180/180
    - Multiple transformation: several fields in formati generate a field in the CDM space:
        -> Triplet "cdm_key - data_key(multiple fields separated by commas) - transform" in mapping file
        Eg. datetime from YYYY-MM-DD HH.hh
    - Direct assignement: a fixed value is assigned to the CDM field in the mapping file
        -> Tuple "cdm_key - default" in the mapping file applies

    ......

- Transfrom functions are stored in ./common/mapping_functions.py. More can be added to accomodate future needs.

- Ouput is a TextFileReader object containing a df that is multiindexed in the columns:
    - At the main level:  'common' and each of the 'vari' in the input dataset.
    - The secondary level has all the individual elements applicable.

DEVEL NOTES:
 Need to address how to specify numeric elements' precisions to the table_creator in a nicer way:
  - only numerical elements in the original format schema have their precisions specified
  but:
    - observation_value and numeric_precision can be different (like kelvin conversion)
    (this has been patched in this version, see precision_observed dict)
    - but there are elements introduced in processes independent of this mapping (bbox...)
    ....

@author: iregon
"""

from __future__ import print_function
from __future__ import absolute_import
from collections import OrderedDict # This is because python2 dictionaries do not keep key insertion order

import sys
import os
import pandas as pd
import json
import logging
import time
import glob
from .common.globals import *
from .common.mapping_functions import *
from common.file_functions import *

if sys.version_info[0] >= 3:
    py3 = True
    from io import StringIO
else:
    py3 = False
    from io import BytesIO


toolPath = os.path.dirname(os.path.abspath(__file__))
mappingsPath = os.path.join(toolPath,'mappings')
tablesPath = os.path.join(toolPath,'common')

precision_observed = {'observation_value':'numerical_precision','original_value':'original_precision'}

#-------------------------- AUX FUNCTIONS  ------------------------------------
def load_maps(iformat, var_out = None):
    files = dict()
    files['common'] = os.path.join(mappingsPath,iformat,'common.json')
    if not var_out: # ADD THIS FEATURE
        var_out = []
    else:
        for myVar in var_out:
            files[myVar] = os.path.join(mappingsPath,iformat,'observations_' + myVar + '.json')

    logging.info('Reading mappings for {0} files: {1}'.format(iformat,",".join(os.path.basename(x) for x in list(files.values()))))

    maps = dict()
    try:
        for key in files.keys():
            with open(files.get(key)) as json_file:
                maps[key] = json.load(json_file, object_pairs_hook=OrderedDict)
    except Exception as e:
        logging.error('Could not load mapping file {0}: {1}'.format(files.get(key),e))
        return None

    return maps

def load_code_maps(iformat):
    #### INT TYPE ASSUMED IN CDM / DATA ELEMENT
    files = glob.glob(os.path.join(mappingsPath,iformat,'code_tables','*.json'))
    if len(files) == 0: # ADD THIS FEATURE
        return None
    logging.info('Reading code mappings for {0}: {1}'.format(iformat,",".join(os.path.basename(x) for x in files)))
    code_maps = dict()
    try:
        for filepath in files:
            element_name = os.path.basename(filepath).split(".")[0]
            with open(filepath) as json_file:
                json_dict = json.load(json_file, object_pairs_hook=OrderedDict)
                code_maps[element_name] = {int(k):v for k,v in json_dict.items() }
    except Exception as e:
        logging.error('Could not load mapping file {0}: {1}'.format(filepath,e))
        return None
    return code_maps
#---------------------- END AUX FUNCTIONS  ------------------------------------

#------------------------------ MAIN ------------------------------------------

def map_cdm(df_tfr_obj,schema_df,data_format,yr,mo,var_out = None, replacements = None ,replace_format = None, remove_null_fields = False, log_file = None, chunksize=100000):

    setup_log(log_file)
    tic = time.time()
    cdm_dtypes_file = os.path.join(tablesPath,'tables_dtypes.json')
    # Load maps to apply:default common.json. Plus observations_[var_out[.json]
    cdm_maps = dict()
    cdm_maps[data_format] = load_maps(data_format,var_out = var_out)
    data_format_code_maps = load_code_maps(data_format)
    logging.info('Adding data format of replacement source ({}) to CDM maps'.format(replace_format))
    if replacements:
        cdm_maps[replace_format] = load_maps(replace_format, var_out = var_out)

    logging.info('Reading CDM data types from {}'.format(cdm_dtypes_file))
    with open(cdm_dtypes_file) as json_file:
        cdm_tables_dtypes = json.load(json_file, object_pairs_hook=OrderedDict)
    cdm_dtypes = dict()
    for dicti in cdm_tables_dtypes.values():
        cdm_dtypes.update(dicti)

    # Initialize from_schema class to use in mapping
    from_schema = from_schemas(schema_df)

    logging.info('Mapping {0} data to CDM...'.format(data_format))
    # Collect original precisions of input data to output and be used afterwards....
    precisions = dict()
    output_buff = StringIO() if py3 else BytesIO()
    ichunk = 0
    dtypes = dict()
    for raw_df in df_tfr_obj:
        logging.info('Mapping chunk {}'.format(ichunk))
        cdm_df_i = pd.DataFrame()
        try:
            for key in cdm_maps[data_format].keys():
                logging.info('Applying *{}.json map'.format(key))
                precisions[key] = dict()
                cdm_map = cdm_maps[data_format].get(key)
                if replacements:
                    cdm_map_supp = cdm_maps[replace_format].get(key)

                for cdm_key,data_key,codetable,function,default in [ [x['cdm_key'],x['data_key'],x['codetable'],x['transform'],x['default']] for x in cdm_map ]:
                    if (not data_key and not default and remove_null_fields): # do not output empty column if mapping not defined for cdm_key
                        continue
                    cdm_dtype = cdm_dtypes.get(cdm_key)
                    cdm_fill = fmiss if 'float' in cdm_dtype else ( omiss if 'obj' in cdm_dtype else imiss )
                    if data_key:# only valid for single data_keys....
                        # Mapping using column values: find data type, missing tag, precision, if value was replaced from other data format
                        if (replacements) and (any([ x == data_key for x in replacements.keys() ])):
                            rep_data_key = replacements.get(data_key).strip()
                            s_data_key = data_key.strip()
                            logging.info('Applying replacement format ({0}) mappging to {1}-{2}'.format(replace_format,data_key,cdm_key))
                            function = [ x['transform'] for x in cdm_map_supp if ((x['data_key'] == rep_data_key) and (x['cdm_key'] == cdm_key)) ][0]
                            if function is not None:
                                function = function.replace(rep_data_key,s_data_key)
                        ctypes = schema_df['column_type'].loc[data_key.split(",")]
                        if any('float' in s for s in ctypes):
                            precisionsi = dict(zip(cdm_key.split(","),schema_df['precision'].loc[data_key.split(",")]))
                            try:
                                precisions[key].update(precisionsi)
                            except:
                                pass
                        if any('int' in s for s in ctypes):
                            precisionsi = dict(zip(cdm_key.split(","),schema_df['precision'].loc[data_key.split(",")]))
                            try:
                                precisions[key].update(precisionsi)
                            except:
                                pass
                        #missing = dict(zip(data_key.split(","),[ fmiss if 'float' in dt else ( imiss if 'int' in dt else omiss ) for dt in ctypes]))
                    if function:
                        cdm = False
                        # Mapping with function:
                        # - data to CDM via transformation of source data
                        # - standalone...(like date_now)
                        fun = function.split(",")[0] # First is function.Then cdm dataframe level 0 (section). Then,actual function. Rest, input columns (level 1)  JAJA! should change order!
                        if fun.split(".")[0] == 'cdm': # Dirty last minute way to work on already built cdm elements
                            cdm = True
                            isection = fun.split(".")[1]
                            fun = ".".join(fun.split(".")[2:])
                            col = eval(fun)
                            tup = [ (isection,x) for x in function.split(",")[1:] ]
                            cdm_df_i[(key,cdm_key)] = col(cdm_df_i[tup])
                        else:
                            col = eval(fun)
                            inargs = function.split(",")[1:] if len(function.split(",")) > 0 else None
                            if data_key:
                                cdm_df_i[(key,cdm_key)] = col(raw_df[data_key.split(",")],inargs) if inargs else col(raw_df[data_key.split(",")])
                            else:
                                cdm_df_i[(key,cdm_key)] = col(inargs) if inargs else col()
                    elif codetable:
                        #print(raw_df[data_key])
                        table = data_format_code_maps[cdm_key]
                        # Fill in undefined in lut with appropiate tag for cdm table type
                        table.update(dict(zip([ key1 for key1 in table if table.get(key1) is None],[ cdm_fill for key1 in table if table.get(key1) is None])))
                        cdm_df_i[(key,cdm_key)] = raw_df[data_key].map(smart_dict(table, missing = cdm_fill ))
                    elif data_key:
                        # Direct mapping: data to CDM as was in source
                        cdm_df_i[(key,cdm_key)] = raw_df[data_key]
                    # let's try map. If if we would prefer/could use this in any/all of the other mapping cases
                    # df[column].map({v_o1:v_n1,v_o2:v_n2}...) if value in series not in dict keys, sets to NaN
                    # if we want to keep control on fmiss, imiss, etc...once the code mapping is read in a dictionary, just add a imiss:imiss pair to it.
                    # We are here going to assume that these codings come as integers -> defined as such in schema: need to track imiss correctly.
                    # Big mistake, some are defined as base36 in the schema: converted to
                    # Ok, add all exceptions to mapping dictionary: imiss,fmiss and omiss. And keep in misnt that these are going to be mapped as strings, objects as there might be non numerical codes....
                    # So coded elements in the table_dtypes need to be defined as objects.
                        #cdm_df_i[(key,cdm_key)] =   cdm_fill
                    else:
                        # Direct assignement: default value or null (value_miss)
                        cdm_df_i[(key,cdm_key)] = default if default is not None else cdm_fill


                    missing = fmiss if 'float' in cdm_dtype else ( omiss if 'obj' in cdm_dtype else imiss )

                    if default:
                        cdm_df_i[(key,cdm_key)] = cdm_df_i[(key,cdm_key)].swifter.apply(lambda x: default if x == missing else x)

                    cdm_df_i[(key,cdm_key)].astype(cdm_dtype)
                    dtypes[(key,cdm_key)] = cdm_dtype

                # Ugly way to update precisions for measured variables
                for element in precisions[key].keys():
                    if element in precision_observed.keys():
                        logging.info('Updating CDM reporting precision of {0} ({1})'.format(element,precisions[key][element]))
                        precisions[key][element] = cdm_df_i[(key,precision_observed.get(element))].max()
                        logging.info('New value taken from ({0}-{1}): {2}'.format(key,precision_observed.get(element),precisions[key][element]))


        except Exception as e:
            logging.error('Error mapping {0} to {1} in ichunk {2}: {3}'.format(data_key,cdm_key,str(ichunk),e))
            return None

        names = cdm_df_i.columns.tolist()
        cdm_df_i.columns = pd.MultiIndex.from_tuples(cdm_df_i.columns) # Create multiindex
        header = True if ichunk == 0 else False
        cdm_df_i.to_csv(output_buff,header=header, mode = 'a', encoding = 'utf-8',) # True only in first chunk


        ichunk += 1

    output_buff.seek(0)
    out_tfr_Obj = pd.read_csv( output_buff,skipinitialspace=True, chunksize = chunksize,dtype = dtypes,names=names,skiprows=2)
    toc = time.time()
    logging.info('Time to map data "{0:.3f}"'.format(toc - tic))

    return(out_tfr_Obj,precisions)
#------------------------------  END MAIN -------------------------------------
