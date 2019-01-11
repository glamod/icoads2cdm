from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from .globals import *
import numpy as np


# define functions
# ===========================
# functions for processing the different ICOADS data type
# Need to address this in the future: fmiss as global, nans....
# Additionally other data formats than imma1 tag missing data
# ===========================
# convert character to int
def conv_int( value ):
    try:
        if( len(value) == 0 ):
            return imiss
        else:
            return int(value)
    except:
        return imiss
# ===========================
def conv_int8( value ):
    try:
        if( len(value) == 0 ):
            rval = imiss
        else:
            rval = int( value )
        assert np.int8( rval ) == np.int32( rval ) , " conv_int8: value out of range"
        return np.int8( rval )
    except:
        return imiss
# ===========================
def conv_int16( value ):
    try:
        if( len(value) == 0 ):
            rval = imiss
        else:
            rval = int( value )
        assert np.int16( rval ) == np.int32( rval ) , " conv_int16: value out of range"
        return np.int16( rval )
    except:
        return imiss
# ===========================
def conv_int32( value ):
    try:
        if( len(value) == 0 ):
            rval = imiss
        else:
            rval = int( value )
        assert np.int32( rval ) == np.int32( rval ) , " conv_int32: value out of range"
        return np.int32( rval )
    except:
        return imiss
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
# ===========================
# fill in base36 with omiss
# ===========================
#def conv_base36( value ):
#    if( len(value) == 0):
#        return "__EMPTY"
#    else:
#        return value
#===========================
