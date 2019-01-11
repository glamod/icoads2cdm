#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 15:14:51 2018

Creates station-ID summary data-frames from full cdm dataframes

station_configuration: super beta version...
"""

from __future__ import division
from __future__ import print_function
# Import require libraries
import sys
sys.path.append('/Users/iregon/C3S/dessaps/CDSpy')
import os
import pandas as pd
import json
import numpy as np
import logging
from collections import Counter as CCounter
from common.globals import *
from common.file_functions import *
from common.functions import *


if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False

toolPath = os.path.dirname(os.path.abspath(__file__))

def station_configuration(tfr_Obj, year, month, log_file = None):
# CHANGES TO REPORT PARAMETERS ESTIMATION WITH RESPECT TO PREVIOUS VERSION:
# PLATFORM TYPE: FROM MODE, NOT MEDIAN
# DELTAT: FROM MIN DELTA, NOT MEDIAN -> change to mode!!!
#
# Reports need:
# Find field to report in full cdm dataframe: sometimes same name, sometimes different
# Create new parameters from existing parameter -> Create at beginning
# Find extremes of original or new parameters  -> find max, min
# Find group tipical value of original or new parameters -> find mean ....just because median/mode is harder here because of chunking
# Find group tipical value of coded fields () -> find mode ...just because median is a bit harder owing to chunking
#
# Some fields are (should be) exactly equal: we will add to the grouping keys toghether with the primary_id. How check if differences?
# Information common to everything: should be in the "group by"
# primary_id : index to group by
#
# Extremes:


    setup_log(log_file)
    logging.info('CDM station configuration report')

    min_pieces = []
    max_pieces = []
    mean_pieces = []
    hist_pieces = []
    counts_common = dict()
    agg_counts_common = dict()
    counts_common['platform_type'] = []
    counts_vars = dict()
    counts_vars['at'] = []
    counts_vars['sst'] = []
    agg_counts_vars = dict()
    # Be carefull with the following if we ever decide not to map to the cdm information data format
    # is not able to provide as null. Column renaming will fail for those fields, have to account for this in future
    # Following is the info we are going to use from the header part of the cdm df. Some names are the same,
    # others are not: not able to just rename with multiindex columns (!!!)
    inherit_from_hdr = {'primary_station_id':'primary_id','secondary_station_id':'secondary_id',
    'primary_station_id_scheme':'primary_id_scheme','secondary_station_id_scheme':'secondary_id_scheme',
    'crs':'station_crs','latitude':'latitude','longitude':'longitude','station_type':'station_type',
    'platform_type':'platform_type','platform_sub_type':'platform_sub_type'}
    station_common = ['primary_id' ,'secondary_id','primary_id_scheme','secondary_id_scheme','station_crs','station_type','platform_type','platform_sub_type']
    station_common_indexes = [ ('header', s) for s in station_common ]
    group_keys = ['primary_id' ]
    group_indexes = [ ('header', s) for s in group_keys ]
    common_df = pd.DataFrame()
    for x in tfr_Obj:
        x.rename(columns = inherit_from_hdr,level=1,inplace=True)
        # Get station common info
        station_common_info = x.groupby( station_common_indexes , sort = False).groups
        common_df_i = pd.DataFrame(data=[ list(x) for x in station_common_info.keys()], columns = station_common)
        common_df_i.set_index(group_keys, inplace =True, drop = True)
        common_df = pd.concat([common_df,common_df_i])
        # seems pandas is not able to recognize dates on multilevel ( &^%%$ !!!!!! )
        x.loc[:,('header','report_timestamp')] = pd.to_datetime(x['header']['report_timestamp'])
        min_i = x.groupby(group_indexes , sort = False ).agg(
                     { ('header','longitude'):           {'bbox_min_lon': min},
                       ('header','latitude'):            {'bbox_min_lat': min},
                       ('header','report_timestamp'):    {'start_date': min,'delT': getDelT}
                     })
        max_i = x.groupby(group_indexes , sort = False ).agg(
                    { ('header','longitude'):           {'bbox_max_lon': max},
                        ('header','latitude'):            {'bbox_max_lat': max},
                        ('header','report_timestamp'):    {'end_date': max}
                    })
        min_pieces.append(min_i)
        max_pieces.append(max_i)
        # JUST AS INITIAL VERSION NOT WORKING.....beacuse agg does not work on multi-level either!!! pero qu'e mierda!!
        counts_common['platform_type'].append(x.groupby(group_indexes , sort = False ).agg({('header','platform_type'):{'nobs': getSize}}))
        for var in counts_vars.keys():
            counts_vars[var].append(x.groupby(group_indexes , sort = False ).agg(
            {(var,'observation_value'):{var: countGoodFloat, "_".join([var,'max']): float_null_max,"_".join([var,'min']):float_null_min }
            }))

    #Now remove higher levels of indexing
    for i in range(len(min_pieces)):
        min_pieces[i].columns = min_pieces[i].columns.droplevel([0,1])
        max_pieces[i].columns = max_pieces[i].columns.droplevel([0,1])
        for var in counts_common.keys():
            counts_common[var][i].columns = counts_common[var][i].columns.droplevel([0,1])
        for var in counts_vars.keys():
            counts_vars[var][i].columns = counts_vars[var][i].columns.droplevel([0,1])

    # And find global stat: aggregate results from chunks
    agg_min = pd.concat(min_pieces).groupby(level=0).min()
    agg_max = pd.concat(max_pieces).groupby(level=0).max()
    for var in counts_common.keys():
        agg_counts_common[var] = pd.concat(counts_common[var]).groupby(level=0).sum()
    for var in counts_vars.keys():
        agg_counts_vars[var] = pd.concat(counts_vars[var]).groupby(level=0).sum()

    # Now merge everything and rename index
    groups = pd.DataFrame()
    groups_extremes = pd.concat([agg_min,agg_max],axis = 1)
    groups_var = pd.concat([agg_counts_vars.get(x) for x in agg_counts_vars],axis = 1)
    groups_common = pd.concat([agg_counts_common.get(x) for x in agg_counts_common],axis = 1)
    groups = pd.concat([groups_extremes,groups_var,groups_common],axis = 1)
    #groups = pd.concat([groups_extremes,groups_var,groups_common,common_df],axis = 1)
    groups.index.names = group_keys

    # Add new vars
    observing_frequency = 86400 / groups['delT']
    observing_frequency[ observing_frequency == float('infinity')] = 0
    observing_frequency[ observing_frequency.isnull()] = 0

    nids = len(groups)
    groups = groups.assign( observing_frequency = observing_frequency )
    groups = groups.assign( year      = [year] * nids)
    groups = groups.assign( month     = [month] * nids)
    groups = groups.assign( reporting_time     = ['NULL'] * nids )

    # OK. Look at main program. Al this is to be done on the results on the chunks!
    grouped = groups.groupby(group_keys)
    _PT    = grouped['platform_type'].unique() # Better get histogram
    _PT    = _PT.apply( lambda x: '{'+','.join( map( str, x) )+'}' )
    _FREQ  = grouped['observing_frequency'].unique() # Better get histogram
    _FREQ = _FREQ.apply(lambda x: '{' + ','.join(map(str, x)) + '}')
    _TIMES = grouped['reporting_time'].unique() # Better get histogram
    _TIMES = _TIMES.apply(lambda x: '{' + ','.join(map(str, x)) + '}')
    _DELT   = grouped['delT'].unique() # Better get histogram
    _DELT = _DELT.apply(lambda x: '{' + ','.join(map(str, x)) + '}')

    grouped = grouped.agg({'bbox_min_lon': min, 'bbox_max_lon': max, 'bbox_min_lat': min, 'bbox_max_lat': max,
                               'start_date': min, 'end_date': max, 'nobs': sum} )

    grouped_vars = dict()
    groups.groupby(group_keys)
    for var in counts_vars.keys():
        G = groups.groupby(group_keys)
        grouped_vars[var] = G.agg({var: sum, "_".join([var,'max']): float_null_max,"_".join([var,'min']):float_null_min })

    vars_grouped = pd.concat([grouped_vars.get(x) for x in grouped_vars],axis = 1)
    final_groups = pd.concat([vars_grouped,grouped],axis = 1)
    # EOF - why?

    nstations = final_groups.shape[0]
    _VAR = final_groups.apply( list_variables_avail, axis = 1)

    final_groups = final_groups.assign( platform_type = _PT  )
    final_groups = final_groups.assign( YR = [year]*nstations )
    final_groups = final_groups.assign( MO = [month]*nstations )
    final_groups = final_groups.assign( observing_frequency = _FREQ )
    final_groups = final_groups.assign( delT = _DELT )
    final_groups = final_groups.assign( reporting_time = _TIMES )
    final_groups = final_groups.assign( observed_variables = _VAR )
    #final_groups = final_groups.reset_index()
    print(final_groups)
    return( final_groups )

# hist_pieces = []
# hist_pieces
# The following not really necessary if we assume should be unique
# hist_i = x.groupby(group_keys , sort = False ).agg(
#                     {('header','platform_type'):    {'platform_type': getDiscreteHistogram }
#                     })
#     hist_pieces.append(hist_i)
#     hist_pieces[i].columns = hist_pieces[i].columns.droplevel([0,1])
#     hist_pieces[i].index = hist_pieces[i].index.rename(hist_pieces[i].index.name[1])
#                   #('header','report_timestamp'):    {'tmin': min, 'tmax': max, 'delt': getDelT},
# agg_hist_dict = dict()
# for group_name, data in pd.concat(hist_pieces).groupby(level=0):
#     try:
#         aggs = sum((CCounter(x[0]) for x in data.loc[group_name].values), CCounter()) # here x[0] because we increase dimensionality with multiindex!!!!
#     except:
#         aggs = CCounter(data.loc[group_name].values[0])
#     agg_hist_dict[group_name] = aggs.most_common(1)[0][0] if len(aggs) > 0 else None
#
# agg_hist=pd.DataFrame.from_dict(agg_hist_dict,columns = ['pt'],orient='index')
# groups_extremes = pd.concat([agg_hist,agg_min,agg_max],axis = 1)
