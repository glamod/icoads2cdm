from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from .globals import *
import numpy
import datetime
import pandas
import math
import hashlib
import pytz
import sys



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
    
def valid_date( year = None, month = None, day = None, hour = None, minute = None ):
    try:
        datetime.datetime(year = int(year), month = int(month), day = int(day), hour = int(hour), minute = int(minute))
        return True
    except ValueError:
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
