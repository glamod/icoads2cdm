from __future__ import division
from __future__ import print_function
from common.globals import *
import numpy
import datetime
import pandas
import math
import hashlib
import pytz
import sys


# define functions
# ===========================
# functions for processing the different ICOADS data type
# ===========================
# convert character to int
def conv_int( value ):
    if( len(value) == 0 ):
        return imiss
    else:
        return int(value)
# ===========================
def conv_int8( value ):
    if( len(value) == 0 ):
        rval = imiss
    else:
        rval = int( value )
    assert numpy.int8( rval ) == numpy.int32( rval ) , " conv_int8: value out of range"
    return numpy.int8( rval )
# ===========================
def conv_int16( value ):
    if( len(value) == 0 ):
        rval = imiss
    else:
        rval = int( value )
    assert numpy.int16( rval ) == numpy.int32( rval ) , " conv_int16: value out of range"
    return numpy.int16( rval )
# ===========================
def conv_int32( value ):
    if( len(value) == 0 ):
        rval = imiss
    else:
        rval = int( value )
    assert numpy.int32( rval ) == numpy.int32( rval ) , " conv_int32: value out of range"
    return numpy.int32( rval )
# convert character to float
# ===========================
def conv_float32( value ):
    if( len(value) == 0):
        return fmiss
    else:
        return float( value )
# ===========================
# convert character to object
def conv_object( value ):
    if( len( value ) == 0):
        return "__EMPTY"
    else:
        return value
# ===========================
# convert character (b36) to int
# ===========================
def conv_base36( value ):
    if( len(value) == 0):
        return imiss
    else:
        return int(value, 36)
#===========================
def apply_scale( value, scale, msng ):
    if( value != msng ):
        return( value * scale )
    else:
        return( msng )

def validate_float( value, min, max):
    if( value != fmiss and ( (min - value > tol) or ( value - max > tol ) ) ):
        print( value, value - min , value - max)
        return True
    else:
        return False

def validate_int( value, min, max):
    if( value != imiss and ( value < min or value > max) ):
        return True
    else:
        return False

def list_variables( row , threshold = 50):
    res = list()
    if 100 * row['at'] / row['nobs'] > threshold:
        res.append('85')
    if 100 * row['sst'] / row['nobs'] > threshold:
        res.append('95')
    if 100 * row['slp'] / row['nobs'] > threshold:
        res.append('58')
    if 100 * row['w'] / row['nobs'] > threshold:
        res.append('107')
    if 100 * row['d'] / row['nobs'] > threshold:
        res.append('106')
    if 100 * row['wbt'] / row['nobs'] > threshold:
        res.append('41')
    if 100 * row['dpt'] / row['nobs'] > threshold:
        res.append('36')
    return '{' + ','.join( res ) +'}'



def list_variables_avail( row , threshold = 50):
    res = []
    for variable in inhouse_cdm_params.keys():
        if variable in row.index:
            if 100 * row[variable] / row['nobs'] > threshold:
                res.append(str(inhouse_cdm_params.get(variable)))
    return '{' + ','.join( res ) +'}'

def valid_date( year = None, month = None, day = None, hour = None, minute = None ):
    try:
        datetime.datetime(year = int(year), month = int(month), day = int(day), hour = int(hour), minute = int(minute))
        return True
    except ValueError:
        return False

def extract_source( inputdf , year, month ):
    inputdf = inputdf.assign(dcksid = inputdf['DCK_c1'].map(str) + ':' + inputdf['SID_c1'].map(str) )
    # get number of different sources in file
    nsource = inputdf.dcksid.unique().size
    # get min and max for different sources
    xmin    = ( inputdf.LON_core.groupby(inputdf.dcksid).min() )
    xmax    = ( inputdf.LON_core.groupby(inputdf.dcksid).max() )
    ymin    = ( inputdf.LAT_core.groupby(inputdf.dcksid).min() )
    ymax    = ( inputdf.LAT_core.groupby(inputdf.dcksid).max() )
    datemin = ( inputdf.date.groupby(inputdf.dcksid).min() )
    datemax = ( inputdf.date.groupby(inputdf.dcksid).max() )
    # convert to data frame
    sourcedf = pandas.DataFrame( {'xmin': xmin,
                                  'xmax': xmax,
                                  'ymin': ymin,
                                  'ymax': ymax,
                                  'tmin': datemin,
                                  'tmax': datemax,
                                  'YR': [year]*nsource,
                                  'MO': [month]*nsource,
                                   })
    sourcedf = sourcedf.reindex()
    return sourcedf

def countGoodFloat( theData ):
    ngood = theData[ theData != fmiss].count()
    if( math.isnan( ngood ) or math.isinf( ngood ) ):
        ngood = 0
    return( ngood )

def countGoodInt( theData ):
    ngood = theData[ theData != imiss].count()
    if( math.isnan( ngood ) or math.isinf( ngood ) ):
        ngood = 0
    return( ngood )

def getDelT( theData ):
    if theData.size > 1:
        delT = theData.sort_values().diff().median().total_seconds()
    else:
        delT = 0
    return(delT)

def getminDelT( theData ):
    if theData.size > 1:
        delT = theData.sort_values().diff().min().total_seconds()
    else:
        delT = 0
    return(delT)

def delThisto( theData ):
    if theData.size > 1:
        delT = getDiscreteHistogram( theData.sort_values().diff().apply( lambda x:  x.seconds ))
    else:
        delT = None
    return(delT)

def getSize( theData ):
    return( theData.size )

def getMedian( theData ):
    if theData.size > 1:
        med = theData.median()
    else:
        med = theData
    return( med )

def getDiscreteHistogram( theData ):
    try:
        counts = numpy.bincount([ int(d) for d in theData[~numpy.isnan(theData)]])
        bins = numpy.arange(len(counts))
        return(dict(zip(bins,counts)))
    except:
        return None

# def md5(fname):
#     hash_md5 = hashlib.md5()
#     with open(fname, "rb") as f:
#         for chunk in iter(lambda: f.read(4096), b""):
#             hash_md5.update(chunk)
#     return hash_md5.hexdigest()

def dtime_now_cdm():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def float_null_min( theData ):
    if theData.size > 0:
        clean = theData.replace(to_replace=fmiss, value=numpy.nan).dropna()
        if len(clean) > 0:
            return clean.min(skipna = True)
        else:
            return fmiss
    else:
        return fmiss

def float_null_max( theData ):
    if theData.size > 0:
        clean = theData.replace(to_replace=fmiss, value=numpy.nan).dropna()
        if len(clean) > 0:
            return clean.max(skipna = True)
        else:
            return fmiss
    else:
        return fmiss
