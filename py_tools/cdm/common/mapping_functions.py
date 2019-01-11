from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import numpy as np
import os
import datetime
import pandas as pd
import math
import pytz
import swifter # df.swifter.apply!!!!!
from .globals import *
from .properties import *

class smart_dict(dict):
    missing = np.nan
    def __init__(self,*args,**kwargs):
        dict.__init__(self,*args,**kwargs)
        self.__dict__ = self

    def __getitem__(self, key):
        val = dict.__getitem__(self, key)
        return val

    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)

    def __missing__(self, key):
        key = ''
        if key == '' :
            key = self.missing
        return key
# row[df.columns[i]] without axis=1 at the end of apply, because of the way swifter works.
def dtime_imma12cdm(df):

    # Will leave checks here out as, when this is applied, date check has already been
    # past. However, as a function, it should provide for errors....
    dtime = df.swifter.apply(lambda row: datetime.datetime( year   = int(row[df.columns[0]]),
                                                          month  = int(row[df.columns[1]]),
                                                          day    = int(row[df.columns[2]]),
                                                          hour   = int( math.floor(row[df.columns[3]] ) ),
                                                          minute = int( math.floor(60.0 * math.fmod(row[df.columns[3]], 1))),
                                                          tzinfo = pytz.utc))
    return dtime

def dtime_meds2cdm(df):
    # Will leave checks here out as, when this is applied, date check has already been
    # past. However, as a function, it should provide for errors....
    dtime = df.swifter.apply(lambda row: datetime.datetime( year   = int(row[df.columns[0]][0:4]),
                                                          month  = int(row[df.columns[0]][4:6]),
                                                          day    = int(row[df.columns[0]][6:]),
                                                          hour   = int(row[df.columns[1]][0:2]),
                                                          minute = int(row[df.columns[1]][2:]),
                                                          tzinfo = pytz.utc), axis = 1)
    return dtime


def time_accuracy(df): #ti_core
    secs = {0: 3600,1: int(round(3600/10)),2: int(round(3600/60)),3: int(round(3600/100)),imiss:imiss}
    return df[df.columns[0]].map( smart_dict(secs,missing = imiss) )


def lon_360to180(df):
    return df[df.columns[0]].swifter.apply( lambda x: fmiss if x == fmiss else (-180 + math.fmod(x, 180) if x >= 180 else x) )

def iloc_accuracy(li,lat):
#    math.sqrt(111**2)=111.0
#    math.sqrt(2*111**2)=156.97770542341354
    deg_km = 111
    return max(1,int(round(li*math.sqrt((deg_km**2)*( 1 + math.cos(math.radians(lat))**2)))))

def location_accuracy(df): #(li_core,lat_core) math.radians(lat_core)
    degrees = {0: .1,1: 1,2: fmiss,3: fmiss,4: 1/60,5: 1/3600,imiss: fmiss}
    return df.swifter.apply( lambda row: imiss if ((degrees.get(row[df.columns[0]]) == fmiss) or (row[df.columns[1]] == fmiss)) else iloc_accuracy(degrees.get(row[df.columns[0]]),row[df.columns[1]]), axis = 1 )

def temperature_C2K(df):
    return df[df.columns[0]].swifter.apply( lambda x: fmiss if x == fmiss else x + 273.15 )

def float_oposite(df):
    return df[df.columns[0]].swifter.apply( lambda x: fmiss if x == fmiss else -x )

def float_get_first(df):
    return df.swifter.apply(lambda row: next((x for i, x in enumerate([ row[df.columns[y]] for y,c in enumerate(df.columns) ]) if x != fmiss), fmiss)  )

def float_scale(df,factor):
    return df[df.columns[0]].swifter.apply( lambda x: fmiss if x == fmiss else x*float(factor[0]) )

def integer_oposite(df):
    return df[df.columns[0]].swifter.apply( lambda x: imiss if x == imiss else -x )

def integer_to_float(df):
    return df[df.columns[0]].swifter.apply( lambda x: fmiss if x == imiss else float(x) )

def string_prepend(df,string_modifiers):
    return df[df.columns[0]].swifter.apply( lambda x: string_modifiers[1].join([string_modifiers[0],x]), axis = 1  )

def string_wrap(df,string_modifiers):
    return df[df.columns[0]].swifter.apply( lambda x: string_modifiers[2].join([string_modifiers[0],x,string_modifiers[1]]) )

def build_sensor_id(df):
    # Probably 100 way to do it better...
    return df.swifter.apply( lambda row: omiss if any(s in "-".join([str(x) for x in row]) for s in [omiss,str(imiss)]) else "-".join([str(x) for x in row]), axis = 1 )

def pt_observing_programme(df):
    #observing_programmes = { range(1, 5): '{7, 56}',7: '{5,7,9}'} see how to do a beautifull rnage dict in the future. Does not seem to be straighforward....
    # if no PT, assume ship
    # !!!! set only for drifting buoys. Rest assumed ships!
    return df[df.columns[0]].swifter.apply( lambda x: '{5,7,9}' if x == 7 else '{7,56}')

def string_prepend_join(df,string_modifiers):
    return df.swifter.apply( lambda row: string_modifiers[1].join([string_modifiers[0],string_modifiers[1].join([str(x) for x in row])]), axis = 1 )

def dtime_now_cdm():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class from_schemas():
    def __init__(self, schema):
        self.schema = schema

    def round_to_precision(self,df,field):
        precision = self.schema['precision'].loc[field].values[0]
        if len(precision.split(".")) > 1:
            decimal_positions = len(precision.split(".")[1])
        else:
            decimal_positions = 0

        return df[df.columns[0]].swifter.apply( lambda x: round(x,decimal_positions) if x != fmiss else x )

    def element_precision(self,field):
        return self.schema['precision'].loc[field].values[0]

    def kelvin_precision(self,field):
        precision = self.schema['precision'].loc[field].values[0]
        if len(precision.split(".")) > 1:
            decimal_positions = len(precision.split(".")[1])
            if decimal_positions > 1:
                return precision
            else:
                return '0.01'
        else:
            return '0.01'

    def pascal_precision(self,field):
        precision = self.schema['precision'].loc[field].values[0]
        if len(precision.split(".")) > 1:
            decimal_positions = len(precision.split(".")[1])
            if decimal_positions > 2:
                format_float='{:.' + str(int(decimal_positions-2)) + 'f}'
                return format_float.format(1/10**(decimal_positions-2))
            else:
                return '1'
        else:
            return '1'

    def location(self,df):
        # WARNING!!!!: these names will be format dependent if working with other than imma1
        # So far only working with other formats to replace imma1: only actually reading imma1 schema
        lat_schema_name = 'LAT_core'
        lon_schema_name = 'LON_core'

        lat_precision = self.schema['precision'].loc[lat_schema_name]
        if len(lat_precision.split(".")) > 1:
            lat_decimal_positions = len(lat_precision.split(".")[1])
            lat_format='{:.' + str(int(lat_decimal_positions)) + 'f}'
        else:
            lat_format='{:.0f}'
        lon_precision = self.schema['precision'].loc[lon_schema_name]
        if len(lon_precision.split(".")) > 1:
            lon_decimal_positions = len(lon_precision.split(".")[1])
            lon_format='{:.' + str(int(lon_decimal_positions)) + 'f}'
        else:
            lon_format='{:.0f}'
        return df.swifter.apply( lambda row: "SRID=4326;POINT( " + lon_format.format(row[df.columns[0]]) + " " + lat_format.format(row[df.columns[1]]) + ")", axis = 1 )
