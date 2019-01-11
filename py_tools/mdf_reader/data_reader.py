#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 15:14:51 2018

Reads data from buffer/file from various input formats using data format information
provided in a schema.
Outputs data in a pandas dataframe in a TextFileReader object, together with the
data format schema in a pandas dfself.

Different functions to read different data formats so far.

# imma1
Reads imma1 formatted data from buffer/file into dataframe, using imma1 format
 schema

    1. Read format json schema to dataframe
    2. (Read data to buffer)
    3. Read core data from buffer into dataframe
    4. Write core and selected attm to output buffer
    5. Read core and selected attm into dataframe from output buffer
    6. Check data consistency according to specifications: type and value

Returns:
    1. Dataframe (TextFileReader object) with raw data, decoded and rescaled.
    2. Buffer with supplemental data where applicable

Requires:
    1. ./schemas/imma1.json (will change to json)
    2. ./code_tables/imma1/tables(i)

# meds
Reads meds formatted data from buffer/file into dataframe, using meds format
 schema

DEV NOTES:
    1. Need to really be flexible and intelligent to know if input is buffer or filename
    2. Need to be rewritten (most probably only meds, as imma1 is quite peculiar)
     to be a function to read other data formats with similar structure: identify different missing values and so on.....

BEWARE PYTHON 2/3 COMPATIBILITY:   ISSUE WITH WRITE CSV IN PANDAS
https://stackoverflow.com/questions/13120127/how-can-i-use-io-stringio-with-the-csv-module

The Python 2.7 csv module doesn't support Unicode input: see the note at the beginning of the documentation.
It seems that you'll have to encode the Unicode strings to byte strings, and use io.BytesIO, instead of io.StringIO.
The examples section of the documentation includes examples for a UnicodeReader and UnicodeWriter wrapper classes (thanks @AlexeyKachayev for the pointer).

"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
from io import open # To allow for encoding definition
from io import StringIO as StringIO
import sys
import time
import os
import pandas as pd
import numpy as np
import logging
import swifter
#from mdf_reader import schema_reader
from . import schema_reader
from .common.functions import *
from .common.converters import *
from .common.globals import *
from common.file_functions import *

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False
    from io import BytesIO as BytesIO


toolPath = os.path.dirname(os.path.abspath(__file__))
schemaPath = os.path.join(toolPath,'schemas')

converters = {
    'int8'    : conv_int8,
    'int16'   : conv_int16,
    'int32'   : conv_int32,
    'float32' : conv_float32,
    'object'  : conv_object,
    'base36'  : conv_base36
}

def imma1_extract_from_buffer(bufferIn, core_df, schema_general_df, sections):
    tic = time.time()
    # get number of lines
    nrecords = core_df.shape[0]
    # rewind buffer to start and initialize output buffers
    bufferIn.seek(0)
    output = StringIO()
    output_modulo = StringIO()
    # Now go
    nlines = 0
    # Create empty string for each segment and get section-based info needed for parsing:
    # Do this outside loop, as evaluating df at each step is time consuming
    segments0 = dict( zip( schema_general_df.index, [ x.ljust(y) for x,y in zip(schema_general_df['sentinal'],schema_general_df['length'])] ))
    section_len = dict( zip( schema_general_df.index, schema_general_df['length'] ))
    sentinal_section = dict( zip( schema_general_df['sentinal'], schema_general_df.index ))
    core_len = schema_general_df['length'].loc['core']
    for ind in range( nrecords ):
        line = bufferIn.readline()
        segments = dict(segments0)
        # read in core section
        segments['core'] = line[  0 : core_len  ]
        offset = core_len - 1
        # ********* THIS NEEDS TO BE ADDRESSED ***************
        # NOTE: for multiple segments of the same type only the last will be kept.
        for attInd in range( core_df['ATTC'][ind] ):
            sentinal = line[ (offset+1):(offset+5) ]
            section = sentinal_section[ sentinal ]
            attmLength = section_len[ section ]
            if attmLength == 0:
                attmLength = len(line) - offset
            segments[ section ] = line[ (offset+1):(offset + attmLength + 1)]
            offset = offset + attmLength
        # Now join core data and requested attm (supp data: just output to buffer, without the sentinal tag)
        record = ''
        for segment in sections:
            record += segments[segment]
        record += '\n'
        output.write( record )
        output_modulo.write((segments['c99'][5:-1] + '\n') )
        nlines += 1
    toc = time.time()
    logging.info('Time to scan attachments "{0:.3f}"'.format(toc - tic))
    output.seek(0)
    output_modulo.seek(0)
    return output, output_modulo


def imma1(infile, attmOut = '', chunksize=100000, log_file = None):

    setup_log(log_file)
    logging.info('IMMA1 READER: READING IMMA1 DATA FROM FILE')
    logging.info('Reading file {}'.format(infile))
    # =========================================================================
    # 1. Load json schema defining format
    # =========================================================================
    [schema_general_df,schema_df] = schema_reader.imma1()
    # Now force types to know what we are working with: leave potential integers that might not be appl. to every case (eg. precision) as float to allow nan
    dtypes_general = {'minCount': 'uint8', 'maxCount': 'uint8', 'sentinal': 'object', 'length': 'uint16'}
    schema_general_df = schema_general_df.astype(dtype = dtypes_general)
    # =========================================================================
    # 2. Read file into internal buffer and determine contents
    # =========================================================================
    tic = time.time()
    with open(infile,'r', encoding='utf-8') as content_file: # Force unicode
        content = content_file.read() # str object

    content_buffer = StringIO(content)
    content_buffer.seek(0)
    toc = time.time()
    logging.info('Time to read data "{0:.3f}"'.format(toc - tic))
    # =========================================================================
    # 3. Read in data for core section to df
    # =========================================================================
    # Generate converters for pandas dataframe (core section)
    core_conv = map(  ( lambda column_type : converters.get(column_type) ) , schema_df['column_type'].loc['core'] )
    core_conv = dict( zip( schema_df['names'].loc['core'] , core_conv ) )
    tic = time.time()

    content_buffer.seek(0)
    core_df = pd.read_fwf(content_buffer, widths = schema_df['field_length'].loc['core'].astype('int16').values, header = None, names = schema_df['names'].loc['core'] , converters = core_conv, encoding = 'utf-8' )
    toc = time.time()
    logging.info('Time to read core "{0:.3f}"'.format(toc - tic))
    # =========================================================================
    # 4. Get core and requested attms to  output buffer
    #    Output supplementary data to separate buffer
    # =========================================================================
    # See what to output:
    if len(attmOut) == 0:
            attmOut = list(schema_general_df.index)
            attmOut.pop(0)
    sectionsOut = ['core'] + attmOut
    # Get core and attm in one buff and supp data in other
    [output_buff, output_modulo_buff] = imma1_extract_from_buffer(content_buffer, core_df, schema_general_df, sectionsOut )

    output_buff.seek(0)
    output_modulo_buff.seek(0)
    output_buff.seek(0)
    output_modulo_buff.seek(0)
    # =========================================================================
    # 5. Read core and selected attm into dataframe from output buffer
    # =========================================================================
    # Extract only output sections from schema and index like field-section
    schema_out = schema_df.loc[sectionsOut]
    schema_out['names-section'] = schema_out['names'] + '_' + schema_out.index
    schema_out.set_index('names-section', inplace = True,drop = True)

    #==========================================================================
    out_conv = map(  ( lambda column_type : converters.get(column_type) ) , schema_out['column_type'] )
    out_conv = dict( zip( schema_out.index , out_conv ) )
    tic = time.time()
    output_L0_buff = StringIO() if py3 else BytesIO()
    outraw_tfr_Obj = pandas.read_fwf( output_buff, widths = schema_out['field_length'].astype('int16').values, header = None, names = schema_out.index, converters=out_conv, chunksize = chunksize)
    # =========================================================================
    # 6. Process and prepare to output: scale
    # Process in chunks
    # Ouput processed to temporal buffer to read as TextFileReader DF object on output
    # =========================================================================
    nread = 0
    for raw_df in outraw_tfr_Obj:
        for column in raw_df:
            ctype = schema_out['column_type'].loc[column]
            value_miss = fmiss if ( 'float' in ctype) else imiss
            # find missing values and apply, then convert integers to integers and so on....
            try:
                if 'float' in ctype:
                    missing = np.array(value_miss).astype(ctype).item(0) # Make sure our missing value is in same format: probably:HERE WE LOOSE THE DECIMALS WE'VE BEEN WORKING WITH SO FAR
                    scale = schema_out['scale'].loc[column]
                    raw_df[column] = raw_df[column].swifter.apply( lambda x: fmiss if x == fmiss else x*scale) #[apply_scale(value, scale , fmiss ) for value in raw_df[column]]
                if 'int' in ctype:
                    raw_df[column] = raw_df[column].astype(ctype)
                if 'base36' in ctype:
                    raw_df[column] = raw_df[column].astype('int16')
            except Exception as e:
                logging.error('Column {}'.format(column))
                logging.error('Exception: {}'.format(e))
                raise
        # Here we might loose the precision (number of decimals) we've been working with....
        # We reduce the display precision (above) to limit this
        raw_df.to_csv(output_L0_buff,columns = schema_out.index,mode = 'a', header=False,  encoding = 'utf-8' ) # will have to append here
        nread+= raw_df.shape[0]
    # Final dtypes: actual after scaling and base conversions
    out_num_dtypes = dict(zip( schema_out.index , schema_out['column_type'] ) )
    out_num_dtypes = { k:('int16' if 'base' in v else v) for k, v in out_num_dtypes.items() }
    output_buff.seek(0)
    output_L0_buff.seek(0)
    # Now create TextFileReader  objets to output non processed (raw) df and consistency checked (l0) df outputs
    # deb: outraw_tfr_Obj = pd.read_fwf( output_buff, widths = schema_out['field_length'].astype('int16').values, header = None, names = schema_out.index, converters=out_conv, chunksize = chunksize, dtype = out_num_dtypes_i0, na_values = na_values )
    outL0_tfr_Obj = pd.read_csv( output_L0_buff, names = schema_out.index , chunksize = chunksize, dtype = out_num_dtypes) #,  encoding = 'utf-8' )

    toc = time.time()
    logging.info('Time to process data to output "{0:.3f}"'.format(toc - tic))

    return(outL0_tfr_Obj,schema_out,output_modulo_buff)


def meds(data_buffer, varOut = [], chunksize=100000, log_file = None):
    # NOW IT IS SUPPER MEDS SPECIFIC: ADDED QC FILTERING.......Including accepting "P" flag :(
    setup_log(log_file)
    data_buffer.seek(0)
    logging.info('MEDS READER: READING MEDS DATA FROM BUFFER')
    # =========================================================================
    # 1. Load json schema defining format
    # =========================================================================
    logging.info('Reading MEDS default schema file')
    try:
        schema_df = schema_reader.meds()
    except Exception as e:
        logging.error('Reading MEDS schema: {}'.format(e))
        return None
    # =========================================================================
    # Clean buffer: 714 is a "fake" csv: it has actual widths; if spaces not eliminated, pandas.read>csv cannot parse correctly
    # =========================================================================
    # rewind buffer to start
    logging.info('Eliminating buffer spaces')
    tic = time.time()
    output = StringIO()
    nlines = 0
    for line in data_buffer.readlines():
        # ipg7: 714 is like a "fake" csv: it has actual widths, if spaces not out, then pandas.read>csv cannot parse correctly
        output.write( line.replace(' ', '') )
        nlines += 1
    # =========================================================================
    # We are now ready to read in data into data frame
    # =========================================================================
    # Select out params
    if len(varOut) > 0:
        schema_out = schema_df.loc[varOut]
    else:
        schema_out = schema_df

    out_conv = map(  ( lambda column_type : converters.get(column_type) ) , schema_out['column_type'] )
    out_conv = dict( zip( schema_out['names'] , out_conv ) )
    dtypes = dict(zip(schema_out['names'],schema_out['column_type']))
    # Read data and fill in gaps
    output.seek(0)
    logging.info('Reading buffer to df: applying conversions where needed')
    # The following fails when reading the chunk afterwrds if there is a badly formed value (eg.'P' instead of integer....), converters fails, and pd does not throw the warning.
    df_tfrO = pd.read_csv( output, header = None, names = schema_out['names'], usecols =schema_out['names'], converters = out_conv, chunksize = chunksize,error_bad_lines = False, warn_bad_lines = True )
    # Now control dtypes: output as defined in schema
    logging.info('Assigning schema data types to df columns: loading to temporal buffer')
    output_L0_buff = StringIO() if py3 else BytesIO()
    ichunk = 0
    qced = dict([ (x,schema_out['qc_column'].loc[x]) for x in schema_out['names'] if schema_out['qc_column'].loc[x] == schema_out['qc_column'].loc[x] ])
    try:
        for raw_df in df_tfrO:
            #col = None
            #for col in raw_df:
            #    raw_df[col] = raw_df[col].values.astype(dtypes.get(col))
            raw_df = raw_df.astype(dtypes)   # tried above to see if faster....
            # Mask 3,4 as missing
            for col in qced.keys():
                raw_df[col] = raw_df[[col,qced.get(col)]].swifter.apply(lambda row: fmiss if row[qced.get(col)] not in ['0','1','P'] else row[col] )
            raw_df.to_csv(output_L0_buff,columns = schema_out.index,mode = 'a', header=False, encoding = 'utf-8')
            ichunk += 1
    except Exception as e:
        logging.error("Error reading chunk number {} ".format(ichunk))
        logging.error("{}".format(e))
        #if col:
        #    logging.error("Error casting column {} to data type".format(col))
        return None

    logging.info('Creating TextFileReader object of df')
    output_L0_buff.seek(0)
    outL0_tfr_Obj = pd.read_csv( output_L0_buff, names = schema_out.index, chunksize = chunksize, dtype = dtypes  )
    toc = time.time()
    logging.info('Time elapsed "{0:.3f}"'.format(toc - tic))

    return(outL0_tfr_Obj,schema_out)

def read_fdf(filename,data_format,field_delimiter = ",", chunksize=100000, log_file = None):
    #  Mmmmmm, should be general(and meds reader a case of this): now only to read wmo_pub47 MD as reprocessed by DB
    # There are hadrcodings like "MSNG"
    setup_log(log_file)
    logging.info('FIELD DELIMITED FILE READER: READING {0} DATA FROM FILE {1}'.format(data_format,filename))
    # =========================================================================
    # 1. Load json schema defining format
    # =========================================================================
    schema_filename = os.path.join(schemaPath,".".join([data_format,'json']))
    logging.info('Reading schema file {}'.format(schema_filename))
    try:
        schema_df = schema_reader.meds(schema_filename)
    except Exception as e:
        logging.error('Reading schema: {}'.format(e))
        return None
    # =========================================================================
    # We are now ready to read in data into data frame
    # =========================================================================
    logging.info('Parsing gaps and applying dtypes')
    dtypes = dict(zip(schema_df['names'],schema_df['column_type']))
    data_file = pd.read_csv(filename, header = None, skiprows = 1,names = schema_df['names'], usecols = schema_df['names'],delimiter = field_delimiter, error_bad_lines = False, warn_bad_lines = True )
    # Now handle gaps and set to appropriate data type: should do this with converters, but we have this MSNG.... 
    for element in schema_df.index:
        missing = omiss if 'object' in schema_df['column_type'].loc[element] else (imiss if 'int' in schema_df['column_type'].loc[element]  else fmiss)  
        data_file[element] = data_file[element].swifter.apply( lambda x: missing if (x!=x or x=='MSNG') else x).astype(schema_df['column_type'].loc[element])
    # =========================================================================
    # Prepare output as TFR_object
    # =========================================================================
    logging.info('Creating TextFileReader object of df')
    output_L0_buff = StringIO() if py3 else BytesIO()
    data_file.to_csv(output_L0_buff,columns = schema_df.index, header=False, encoding = 'utf-8')
    output_L0_buff.seek(0)
    data_tfr_Obj = pd.read_csv( output_L0_buff, names = schema_df.index, chunksize = chunksize, dtype = dtypes  )    

    return(data_tfr_Obj,schema_df)
