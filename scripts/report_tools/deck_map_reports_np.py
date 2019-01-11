#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 20 11:43:22 2018

Script to .

To run from command line:

    python(v)

Inargs:


INPUT:


OUTPUT:



@author: iregon
"""
from __future__ import absolute_import
from __future__ import division
import future        # pip install future
import builtins      # pip install future
import past          # pip install future
import six           # pip install six

import datetime
import os
import sys
import __main__ as main
sys.path.append('/Users/iregon/C3S/dessaps/CDSpy/py_tools')
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np

from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import make_axes_locatable
from mpl_toolkits.axes_grid1 import ImageGrid
import xarray as xr
import cartopy.crs as ccrs
import numpy.ma as ma

from common.file_functions import *
from common.var_properties import *
from common.deck_properties import *
from common.map_properties import *

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False

this_script = main.__file__

# PROCESS INPUT PARAMETERS AND INITIALIZE SOME STUFF ==========================
try:
    sid_deck = sys.argv[1]
    inDir = sys.argv[2]
    y_ini = int(sys.argv[3])
    y_end = int(sys.argv[4])
    out = sys.argv[5]
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)

vars_in = ['dpt','wbt','sst','at','slp','wd','ws']
grid_resolution = 1 # units: degrees
qc_mode = True # if qc mode, var axis are not shared nor saturated. Plots from min to max value in dataset.
descriptor_list = ['counts','min','max']
cmaps = dict()
for vari in vars_in:
    cmaps[vari] = plt.get_cmap(vars_colormap.get(vari))

cmaps['counts'] = plt.get_cmap(vars_colormap.get('counts'))
# END PARAMS ==================================================================

# END PARAMS ==================================================================
def griddata(x, y, z, binsize=0.01, retbin=True, retloc=True, xlim = None, ylim = None, agg = None):
    """
    Place unevenly spaced 2D data on a grid by 2D binning (nearest
    neighbor interpolation).
    
    Parameters
    ----------
    x : ndarray (1D)
        The idependent data x-axis of the grid.
    y : ndarray (1D)
        The idependent data y-axis of the grid.
    z : ndarray (1D)
        The dependent data in the form z = f(x,y).
    binsize : scalar, optional
        The full width and height of each bin on the grid.  If each
        bin is a cube, then this is the x and y dimension.  This is
        the step in both directions, x and y. Defaults to 0.01.
    retbin : boolean, optional
        Function returns `bins` variable (see below for description)
        if set to True.  Defaults to True.
    retloc : boolean, optional
        Function returns `wherebins` variable (see below for description)
        if set to True.  Defaults to True.
    xlim: list, optional
        x coordinate left and right limits
    ylim: list, optional
        y coordinate left and right limits
    agg: list, optional
        Aggregation functions. Defaults to nanmedian.
        Valid options are one or multiple of any of the np functions, without the "np"....
        eg. ['nanmax','max','mean','nanmean'].....
   
    Returns
    -------
    grid : ndarray (2D)
        The evenly gridded data.  The value of each cell is the median
        value of the contents of the bin.
    bins : ndarray (2D)
        A grid the same shape as `grid`, except the value of each cell
        is the number of points in that bin.  Returns only if
        `retbin` is set to True.
    wherebin : list (2D)
        A 2D list the same shape as `grid` and `bins` where each cell
        contains the indicies of `z` which contain the values stored
        in the particular bin.

    Revisions
    ---------
    2010-07-11  ccampo  Initial version
    https://scipy-cookbook.readthedocs.io/items/Matplotlib_Gridding_irregularly_spaced_data.html
    ipg:
        Added opt args xlim,ylim, agg
        Grid initialized to nan (was zeros)
    """
    # get extrema values.
    if xlim:
        xmin, xmax = xlim[0], xlim[1]
    else:
        xmin, xmax = x.min(), x.max()
        
    if ylim:
        ymin, ymax = ylim[0], ylim[1]
    else:
        ymin, ymax = y.min(), y.max()

    # make coordinate arrays.
    xi      = np.arange(xmin, xmax+binsize, binsize)
    yi      = np.arange(ymin, ymax+binsize, binsize)
    xi, yi = np.meshgrid(xi,yi)

    # make the grid.
    grid           = np.empty((len(agg),xi.shape[0],xi.shape[1]), dtype=x.dtype)*np.nan
    #grid           = np.zeros((len(agg),xi.shape[0],xi.shape[1]), dtype=x.dtype)
    nrow, ncol = grid[0].shape
    
    if retbin:
        bins = np.copy(grid[0])

    # create list in same shape as grid to store indices
    if retloc:
        wherebin = np.copy(grid[0])
        wherebin = wherebin.tolist()

    # Create aggregation functions
    agg_func = []
    if agg:
        for func in agg:
            agg_func.append(eval(".".join(['np',func])))
    else:
        agg_func.append(eval(".".join(['np','nanmedian'])))

    # fill in the grid.
    for row in range(nrow):
        for col in range(ncol):
            xc = xi[row, col]    # x coordinate.
            yc = yi[row, col]    # y coordinate.

            # find the position that xc and yc correspond to.
            posx = np.abs(x - xc)
            posy = np.abs(y - yc)
            ibin = np.logical_and(posx < binsize/2., posy < binsize/2.)
            ind  = np.where(ibin == True)[0]

            # fill the bin.
            bin = z[ibin]
            if retloc: wherebin[row][col] = ind
            if retbin: bins[row, col] = bin.size
            if bin.size != 0:
                #binval         = np.median(bin)
                #grid[row, col] = np.median(bin)
                grid[:,row, col] = [ fun(bin) for fun in agg_func ]
#            else:
#                grid[:, row, col] = np.nan   # fill empty bins with nans.
    # return the grid
    if retbin:
        if retloc:
            return grid, bins, wherebin
        else:
            return grid, bins
    else:
        if retloc:
            return grid, wherebin
        else:
            return grid
        
def read_monthly_data():
    # Get file paths and read files to df. Merge in single df by report_id
    file_header = os.path.join(inDir,'-'.join(['header',str(yr),'{:02d}'.format(mo)]) + '.psv')
    files_vars = dict(zip(vars_in,[ os.path.join(inDir,'-'.join([vars_file_prefix.get(vari),str(yr),'{:02d}'.format(mo)]) + '.psv') for vari in vars_in ]))
    df_header = pd.read_csv(file_header,usecols=[0,10,13,14],sep="|",skiprows=0,na_values=["NULL"])
    df_header.set_index('report_id',drop=True, inplace=True)
    df_varis = dict()
    for vari in vars_in:
        df_varis[vari] = pd.read_csv(files_vars.get(vari),usecols=[1,14],sep="|",skiprows=0,na_values=["NULL"])
        df_varis[vari].set_index('report_id',drop=True, inplace=True)
        df_varis[vari].rename({'observation_value':vari},axis='columns', inplace=True)
        df_varis[vari] = df_varis[vari] + vars_offset.get(vari)
    for vari in vars_in:
        df_header = pd.concat([df_header,df_varis.get(vari)],axis=1)
    return df_header


def map_on_subplot(subplot_ax):
    # Plots a map on subplot cartopy axis
    # The axis is divided (an map re-scaled accordingly) to accomodate a colorbar. Colorbar is only drawn if show_colorbar = True.
    # Otherwise, added axis is made transparent. This axis is alway added to keep same scaling between maps in subplots.
    # This colorbar approach so that the colorbar height matches the height of cartopy's axes...!!!!!
    # colorbar_w = percent of axis width: have to declare equal to other subplots in figure so that re-scaling is the same
    #
    # To be used locally: all variables/params to be set in script before invocation (but subplot_axis)
    #
    # Could potentially, more or less easily, choose to have an horiontal colorbar...just a couple of parameters more....
    t1 = subplot_ax.pcolormesh(lons,lats,z,transform=ccrs.PlateCarree(),cmap = colormap, vmin = min_value , vmax = max_value )
    subplot_ax.coastlines(linewidth = coastline_width)
    gl = subplot_ax.gridlines(crs=ccrs.PlateCarree(),color = 'k', linestyle = ':', linewidth = grid_width, alpha=0.3, draw_labels=True)
    gl.xlabels_bottom = False
    gl.ylabels_right = False
    gl.xlabel_style = {'size': grid_label_size}
    gl.ylabel_style = {'size': grid_label_size}
    # following https://matplotlib.org/2.0.2/mpl_toolkits/axes_grid/users/overview.html#colorbar-whose-height-or-width-in-sync-with-the-master-axes
    # we need to set axes_class=plt.Axes, else it attempts to create
    # a GeoAxes as colorbar
    divider = make_axes_locatable(subplot_ax)
    new_axis_w = str(colorbar_w) + '%'
    if colorbar_orien == 'v':
        cax = divider.new_horizontal(size = new_axis_w, pad = 0.08, axes_class=plt.Axes)
        orientation = 'vertical'
    else:
        cax = divider.new_vertical(size = new_axis_w, pad = 0.08, axes_class=plt.Axes,pack_start=True)
        orientation = 'horizontal'
    f.add_axes(cax)
    if show_colorbar:
        #cb_v = plt.colorbar(t1, cax=cax) could do it this way and would work: would just have to add label and tick as 2 last lines below. But would have to do arrangements anyway if we wanted it to be horizontal. So we keep as it is...
        norm = mpl.colors.Normalize(vmin=min_value, vmax=max_value)
        cb = mpl.colorbar.ColorbarBase(cax, cmap=colormap,norm = norm, orientation=orientation)
        cb.set_label(colorbar_title, size = colorbar_title_size)
        cb.ax.tick_params(labelsize = colorbar_label_size)
    else:
        cax.axis('off')
# MAIN ========================================================================

# 1. COMPUTE MAPS -------------------------------------------------------------
# Create structure to organize maps: descriptors[type][var]
descriptors = dict()
for descriptor in descriptor_list:
        descriptors[descriptor] = dict()
        for vari in vars_in:
            descriptors[descriptor][vari] = dict()

i = 1
for yr in range(y_ini,y_end + 1):
    for mo in range(1,13):
        logging.info('{0}-{1}'.format(yr,mo))
        mm = '{:02d}'.format(mo)
        # Read monthly data (header and observed vars) to df indexed by report_id
        try:
            df_mo = read_monthly_data()
        except Exception as e:
            print(yr,mm,e)
            continue
        # For each observed variable in df, compute stats and aggregate to its monhtly composite.
        for vari in vars_in:
            aggs,counts = griddata(df_mo.longitude, df_mo.latitude, df_mo.sst, binsize=grid_resolution, retbin=True, retloc=False, xlim = [0,360], ylim=[-90,90],agg=['nanmin','nanmax'])
            if mm in descriptors['counts'][vari].keys():
                descriptors['counts'][vari][mm] = np.nansum((np.descriptors['counts'][vari][mm],counts),axis=0)
                descriptors['max'][vari][mm] = np.nanmax((descriptors['max'][vari][mm],aggs[1]),axis = 0)
                descriptors['min'][vari][mm] = np.nanmin((descriptors['min'][vari][mm],aggs[0]),axis = 0)
#                descriptors['ave'][vari][mm] = np.nanmean(descriptors['mean'][vari][mm],aggs[2])
            else:
                descriptors['counts'][vari][mm] = counts
                descriptors['max'][vari][mm] = aggs[1]
                descriptors['min'][vari][mm] = aggs[0]
#                descriptors['ave'][vari][mm] = aggs[2]

#             Also add monthly stats to the global aggregate
            if 'global' in descriptors['counts'][vari].keys():
                descriptors['counts'][vari]['global'] = np.nansum((descriptors['counts'][vari]['global'],counts),axis=0)
                descriptors['max'][vari]['global'] = np.nanmax((descriptors['max'][vari]['global'],aggs[1]),axis = 0)
                descriptors['min'][vari]['global'] = np.nanmin((descriptors['min'][vari]['global'],aggs[0]),axis = 0)
#                descriptors['ave'][vari]['global'] = np.nanmean(descriptors['mean'][vari]['global'],aggs[2])
            else:
                descriptors['counts'][vari]['global'] = counts
                descriptors['max'][vari]['global'] = aggs[1]
                descriptors['min'][vari]['global'] = aggs[0]
#                descriptors['ave'][vari]['global'] = aggs[2]


# Now find bound values for maps
max_values = dict()
min_values = dict()
max_counts = dict()
for vari in vars_in:
    max_values[vari] = vars_saturation[vari][1] if not qc_mode else np.nanmax(descriptors['max'][vari]['global']).item()
    min_values[vari] = vars_saturation[vari][0] if not qc_mode else np.nanmin(descriptors['min'][vari]['global']).item()
    max_counts[vari] = np.amax(descriptors['counts'][vari]['global']).item()
if not qc_mode:
    max_counts[vari] = np.nanmax( [ max_counts.get(x) for x in max_counts ])
#mins =  np.nanmin(aggs,axis=(1,2))
#maxs =  np.nanmax(aggs,axis=(1,2))
# 2. PLOT MAPS ----------------------------------------------------------------
# HOW ARE WE PLOTTING:
# We plot using matplotlib on a cartopy axis
# Have to do some adjustments to matplotlib because of cartopy.
# Why cartopy, and not basemap?:   https://github.com/SciTools/cartopy/issues/920
# But we might face the need of a projection not yet in cartopy......
proj = ccrs.PlateCarree()
lons = np.arange(0, 360+grid_resolution, grid_resolution)
lats = np.arange(-90, 90+grid_resolution, grid_resolution)

# 2.1. Map: individual --------------------------------------------------------
# Set mapping function params
colorbar_w = 4
show_colorbar = True
coastline_width = line_width['coastlines']
grid_width = line_width['grid']
grid_label_size = label_size['grid']['label']
colorbar_title_size = label_size['colorbar']['title']
colorbar_label_size = label_size['colorbar']['label']

for agg_period in ['global']: #descriptors['max'][vari].keys():
    if agg_period != 'global':
        agg_name = datetime.date(1900, int(agg_period), 1).strftime('%B')
    else:
        agg_name = 'full period'
    for descriptor in descriptor_list:
        for vari in vars_in:
            colormap = cmaps.get('counts')  if descriptor == 'counts' else cmaps.get(vari)
            colorbar_title = vars_units.get('counts') if descriptor == 'counts' else descriptor + "(" + vars_units.get(vari) + ")"
            colorbar_orien = 'v'
            plt.title('-'.join([sid_deck,vari]) + ": " + " ".join([agg_name,descriptor]) + '\n' + "-".join([str(y_ini),str(y_end)]) + ' composite')
            f, ax = plt.subplots(1, 1, subplot_kw=dict(projection=proj),figsize=(14,7),dpi = 150)  # It is important to try to estimate dimensions that are more or less proportional to the layout of the mosaic....
            if descriptor == 'counts':
                min_value = 0; max_value = max_counts[vari]
                descriptors[descriptor][vari][agg_period][descriptors[descriptor][vari][agg_period]==0] = np.nan
                z = ma.masked_invalid(descriptors[descriptor][vari][agg_period])
            else:
                min_value = min_values[vari]; max_value = max_values[vari]
                z = ma.masked_invalid(descriptors[descriptor][vari][agg_period])
            #z = counts
            map_on_subplot(ax)
            plt.savefig(os.path.join(out, '_'.join([sid_deck,vari,descriptor,agg_period,'qc_map']) + '.png'),bbox_inches='tight')
            plt.close()

# 2.2. Map: mosaic ------------------------------------------------------------
show_colorbar = True
colorbar_w = 6
coastline_width = line_width['mosaic']['coastlines']
grid_width = line_width['mosaic']['grid']
grid_label_size = label_size['mosaic']['grid']['label']
colorbar_title_size = label_size['mosaic']['colorbar']['title']
colorbar_label_size = label_size['mosaic']['colorbar']['label']

# With mosaic, to have a good resolution, have to increase dpi on savefig...and here state a "normal" one. If try to increase here, does strange, harmful things....
# with same figsize, increasing dpi's results in larger subplots ??????
# SUBPLOTS DOES NOT SEEM TO BE GOOD ENOUGH TO CONTROL SIZES IN MOSAIC...
# MAYBE TRY ADD_SUBPLOT?
#for agg_period in descriptors['max'][vari].keys():
for agg_period in ['global']:
    f, ax = plt.subplots(1, 3, subplot_kw=dict(projection=proj),figsize=(14,7),dpi = 180)
    if agg_period != 'global':
        agg_name = datetime.date(1900, int(agg_period), 1).strftime('%B')
    else:
        agg_name = 'full period'
    for vari in vars_in:
        c = 0
        for descriptor in descriptor_list:
            colormap = cmaps.get('counts')  if descriptor == 'counts' else cmaps.get(vari)
            colorbar_title = vars_units.get('counts') if descriptor == 'counts' else descriptor + "(" + vars_units.get(vari) + ")"
            colorbar_orien = 'h'
            if descriptor == 'counts':
                min_value = 0; max_value = max_counts[vari]
                descriptors[descriptor][vari][agg_period][descriptors[descriptor][vari][agg_period]==0] = np.nan
                z = ma.masked_invalid(descriptors[descriptor][vari][agg_period])
            else:
                min_value = min_values[vari]; max_value = max_values[vari]
                z = ma.masked_invalid(descriptors[descriptor][vari][agg_period])
            #if not qc_mode:
            #    show_colorbar = True if (c == 2 or descriptor == 'counts') else False
            map_on_subplot(ax[c])
            c += 1

        wspace = 0.08# if qc_mode else 0.05
        dpi = 400 if qc_mode else 300
        plt.subplots_adjust(wspace=wspace,hspace=0.05)#  Force small separation (default is .2, to keep in mind "transparent" colorbar between subplots....THIS WILL DEPEND ON THE FIGURE WIDTH
        f.savefig(os.path.join(out,'_'.join([sid_deck,vari,'mosaic',agg_period,'qc_map']) + '.png'),bbox_inches='tight',dpi = 300)# 'tight' here, not in plt.tight_layout: here it realizes the new size because of add_axes, but not in plt.tight_layout....
        plt.close()
#
### Now map colorbars
##for vari in vars_in:
##    # Set the colormap and norm to correspond to the data for which
##    # the colorbar will be used.
##    # ColorbarBase derives from ScalarMappable and puts a colorbar
##    # in a specified axes, so it has everything needed for a
##    # standalone colorbar.  There are many more kwargs, but the
##    # following gives a basic continuous colorbar with ticks
##    # and labels.
##    cmap = cmaps.get(vari)
##    norm = mpl.colors.Normalize(vmin=min_value_global, vmax=max_value_global)
##    fig_v = plt.figure(i,figsize = (1.8,7), dpi = 150)
##    ax_v = fig_v.add_axes([0.05, 0.05, 0.45, 0.9])
##    cb_v = mpl.colorbar.ColorbarBase(ax_v, cmap=cmap,norm=norm,orientation='vertical')
##    cb_v.set_label(vari + ' (' +  vars_units.get(vari)+ ' )', size = colorbar_title_size)
##    cb_v.ax.tick_params(labelsize = colorbar_label_size)
##    plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/L1a/maps/' + '-'.join([str(sid_deck),vari,'vertical_cb']) + '.png',bbox_inches='tight')
##    plt.close(i)
##    fig_h = plt.figure(i,figsize = (14,1.8), dpi = 150);
##    ax_h = fig_h.add_axes([0.05, 0.5, 0.9, 0.45])
##    cb_h = mpl.colorbar.ColorbarBase(ax_h, cmap=cmap,norm=norm,orientation='horizontal')
##    cb_h.set_label(vari + ' (' +  vars_units.get(vari)+ ' )', size = colorbar_title_size)
##    cb_h.ax.tick_params(labelsize = colorbar_label_size)
##    plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/L1a/maps/' + '-'.join([str(sid_deck),vari,'horizontal_cb']) + '.png',bbox_inches='tight')
##    plt.close(i)
##
##vari= 'counts'
##cmap = cmaps.get(cmap_counts)
##norm = mpl.colors.Normalize(vmin=min_value_global, vmax=max_value_global)
### Vertical + horizontal colorbars
##fig_v = plt.figure(i,figsize = (1.8,7), dpi = 150)
##ax_v = fig_v.add_axes([0.05, 0.05, 0.35, 0.9])
##cb_v = mpl.colorbar.ColorbarBase(ax_v, cmap=cmap,norm=norm,orientation='vertical')
##cb_v.set_label(vari, size = colorbar_title_size)
##cb_v.ax.tick_params(labelsize = colorbar_label_size)
##plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/L1a/maps/' + '-'.join([str(sid_deck),vari,'vertical_cb']) + '.png',bbox_inches='tight')
##plt.close(i)
##fig_h = plt.figure(i + 1,figsize = (14,1.8), dpi = 150)
##ax_h = fig_h.add_axes([0.05, 0.5, 0.9, 0.35])
##cb_h = mpl.colorbar.ColorbarBase(ax_h, cmap=cmap,norm=norm,orientation='horizontal')
##cb_h.set_label(vari, size = colorbar_title_size)
##cb_h.ax.tick_params(labelsize = colorbar_label_size)
##plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/L1a/maps/' + '-'.join([str(sid_deck),vari,'horizontal_cb']) + '.png',bbox_inches='tight')
##plt.close(i)

