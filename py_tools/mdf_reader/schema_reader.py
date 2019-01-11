#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 15:14:51 2018


Read format json schema to dataframe

"""


from __future__ import print_function
from __future__ import absolute_import
# Import required libraries
import sys
import os
import pandas as pd
import json

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False

toolPath = os.path.dirname(os.path.abspath(__file__))
schemaPath = os.path.join(toolPath,'schemas')

def imma1(schema_file = None):
    schema_def = 'imma1.json'
    if not schema_file:
        schema_file = os.path.join(schemaPath, schema_def)
    # Load json schema defining format
    with open(schema_file) as schemaObj:
        schema = json.load(schemaObj,'utf-8') if not py3 else json.load(schemaObj)

    # Format schema dictionary to conveninent DF: index by section name
    schema_general_df = pd.DataFrame()
    schema_df = pd.DataFrame()
    for section in schema:
        schema_general_df = schema_general_df.append( pd.concat([pd.DataFrame(columns = ['section'], data = [section]),pd.DataFrame.from_dict([schema[section]['outline']])],axis = 1))
        schema_df = schema_df.append( pd.concat([pd.DataFrame(columns =['section'], data = [ section for x in range(len(schema[section]['content'])) ] ),pd.DataFrame.from_dict( schema[section]['content'])],axis = 1 ))

    schema_general_df.set_index('section', inplace = True,drop = True)
    schema_df.set_index('section', inplace = True,drop = True)

    return schema_general_df,schema_df

def meds(schema_file = None):
    # Output indexed by 'names'. 'names' column kept in df.
    schema_def = 'meds.json'
    if not schema_file:
        schema_file = os.path.join(schemaPath, schema_def)
    # Load json schema defining format
    with open(schema_file) as schemaObj:
        schema = json.load(schemaObj)
    # Format schema dictionary to conveninent DF: index by element name ('names')
    schema_df = pd.DataFrame(schema)
    schema_df.set_index('names', inplace = True,drop = False)

    return schema_df
