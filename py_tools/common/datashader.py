#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 14:51:54 2018

@author: iregon
"""

import pandas as pd
import datashader as ds
import datashader.transfer_functions as tf
from datashader import utils
from datashader.colors import inferno
from matplotlib.colors import rgb2hex
from matplotlib.cm import get_cmap

import numpy as np
#from cartopy import crs

plot_width  = 850
plot_height = 600
x_range = (-2.0e6, 2.5e6)
y_range = (4.1e6, 7.8e6)

def categorical_color_key(ncats,cmap):
    """Generate a color key from the given colormap with the requested number of colors"""
    mapper = get_cmap(cmap)
    return [str(rgb2hex(mapper(i))) for i in np.linspace(0, 1, ncats)]

def create_image(x_range=x_range, y_range=y_range, w=plot_width, h=plot_height, 
                 aggregator=ds.count(), categorical=None, black=False, cmap=None):
    opts={}
    if categorical and cmap:
        opts['color_key'] = categorical_color_key(len(df[aggregator.column].unique()),cmap)       

    cvs = ds.Canvas(plot_width=w, plot_height=h, x_range=x_range, y_range=y_range)
    agg = cvs.line(df, 'longitude', 'latitude',  aggregator)
    img = tf.shade(agg, cmap=inferno, **opts)
        
    if black: img = tf.set_background(img, 'black')
    return img

def tests_datashader():
    import datashader as ds
    import datashader.transfer_functions as tf
    import pandas as pd

    df = pd.read_csv('/Users/iregon/C3S/dessaps/test_data/imma_converter/observations-sst-2014-6.psv',usecols=[6,7,14],sep="|",skiprows=0) 

    agg_mean = cvs.points(df, 'longitude', 'latitude', ds.mean('observation_value'))
    agg_max = cvs.points(df, 'longitude', 'latitude', ds.max('observation_value'))
    agg_min = cvs.points(df, 'longitude', 'latitude', ds.min('observation_value'))
    agg_count = cvs.points(df, 'longitude', 'latitude', ds.count('observation_value'))
    #tf.shade(agg.where(agg > 0), cmap=["lightblue", "darkblue"])
    #img = tf.shade(agg.where(agg > 0), cmap=['green', 'yellow', 'red'], how='linear', span=[275,305])
    
 
    
df = pd.read_csv('/Users/iregon/C3S/dessaps/test_data/imma_converter/observations-sst-2014-6.psv',usecols=[6,7,14],sep="|",skiprows=0) 
bounds = dict(x_range = (-180, 180), y_range = (-90, 90))
plot_width = 360*10
plot_height = 180*10
canvas = ds.Canvas(plot_width=plot_width, plot_height=plot_height,**bounds)
agg_mean = canvas.points(df, 'longitude', 'latitude', ds.max('observation_value'))
img = tf.shade(agg_mean, cmap=['green', 'yellow', 'red'], how='linear', span=[275,305])
utils.export_image(img=img,filename='Oct2431doshade.png', fmt=".png", background=None)

points = hv.Points(df['observation_value'].values)
img = points.hist()