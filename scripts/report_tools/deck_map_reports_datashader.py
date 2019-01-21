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

DEV NOTES:
    Need to clean
    Need to handle better when no data in files
    So far only mapping global stats, not monthly: no data at all in certain months will not
    affect as it is


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
plt.switch_backend('agg')
import matplotlib as mpl
import numpy as np

import datashader as ds
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import make_axes_locatable
from mpl_toolkits.axes_grid1 import ImageGrid
import xarray as xr
import cartopy.crs as ccrs

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
    mode = sys.argv[6] # 'qc' or whatever
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)


vars_in = ['dpt','wbt','sst','at','slp','wd','ws']
vari_qc=['dpt','sst','at','slp']
grid_resolution = 1 # units: degrees
qc_mode = True # if qc mode, var axis are not shared nor saturated. Plots from min to max value in dataset.
descriptor_list = ['counts','min','max']

cmaps = dict()
for vari in vars_in:
    cmaps[vari] = plt.get_cmap(vars_colormap.get(vari))

cmaps['counts'] = plt.get_cmap(vars_colormap.get('counts'))#, autumn

ext='.png'
# END PARAMS ==================================================================

# END PARAMS ==================================================================
def read_monthly_data():
    # Get file paths and read files to df. Merge in single df by report_id
    file_header = os.path.join(inDir,'-'.join(['header',str(yr),'{:02d}'.format(mo)]) + '.psv')
    files_vars = dict(zip(vars_in,[ os.path.join(inDir,'-'.join([vars_file_prefix.get(vari),str(yr),'{:02d}'.format(mo)]) + '.psv') for vari in vars_in ]))
    df_header = pd.read_csv(file_header,usecols=[0,10,13,14],sep="|",skiprows=0,na_values=["NULL"])
    df_header.set_index('report_id',drop=True, inplace=True)
    df_varis = dict()
    vars_out = []
    for vari in vars_in:
        if os.path.isfile(files_vars.get(vari)):
            df_varis[vari] = pd.read_csv(files_vars.get(vari),usecols=[1,14,28],sep="|",skiprows=0,na_values=["NULL"])
            if len(df_varis[vari])>0:
                vars_out.append(vari)
                df_varis[vari].set_index('report_id',drop=True, inplace=True)
                df_varis[vari].rename({'observation_value':vari},axis='columns', inplace=True)
                df_varis[vari][vari] = df_varis[vari]*vars_factor.get(vari) + vars_offset.get(vari)
                df_varis[vari].rename({'quality_flag':vari+'_qc'},axis='columns', inplace=True)

    for vari in vars_out:
        df_header = pd.concat([df_header,df_varis.get(vari)],axis=1,sort=False)

    return df_header,vars_out


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
    t1 = subplot_ax.pcolormesh(lons,lats,z,transform=ccrs.PlateCarree(),cmap = colormap, vmin = min_value , vmax = max_value,norm=normalization )
    gl = subplot_ax.gridlines(crs=ccrs.PlateCarree(),color = 'k', linestyle = ':', linewidth = grid_width, alpha=0.3, draw_labels=True)
    subplot_ax.coastlines(linewidth = coastline_width)
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
        cb = mpl.colorbar.ColorbarBase(cax, cmap=colormap,norm = normalization, orientation=orientation)
        cb.set_label(colorbar_title, size = colorbar_title_size)
        cb.ax.tick_params(labelsize = colorbar_label_size)
    else:
        cax.axis('off')
# MAIN ========================================================================

# 1. COMPUTE MAPS -------------------------------------------------------------
# Do first part with holoviews, lets track what kind of data structure we are working with....
# Create canvas to grid on to
# bound = 20026376.39 mercator!
bounds = dict(x_range = (-180, 180), y_range = (-90, 90)) # Do not think we have to change this with resolution.....have to check: Atuplerepresentingtheboundsinclusivespace[min, max]alongtheaxis.
plot_width = int(360*(1./grid_resolution)) #Width and height of the output aggregate in pixels. Must be integer!!!
plot_height = int(180*(1./grid_resolution))
cvs = ds.Canvas(plot_width=plot_width, plot_height=plot_height, **bounds)   # type(cvs): <class 'datashader.core.Canvas'>

# Create structure to organize maps: descriptors[type][var]
descriptors = dict()
for descriptor in descriptor_list:
        descriptors[descriptor] = dict()
        for vari in vars_in:
            descriptors[descriptor][vari] = dict()

not_empty = dict()
for vari in vars_in:
    not_empty[vari] = False

i = 1
for yr in range(y_ini,y_end + 1):
    for mo in range(1,13):
        print('{0}-{1}'.format(yr,mo))
        mm = '{:02d}'.format(mo)
        # Read monthly data (header and observed vars) to df indexed by report_id
        try:
            df_mo,vars_in_file = read_monthly_data()
        except Exception as e:
            print('Could not load data file:',yr,mo)
            print(e)
            continue
        # For each observed variable in df, compute stats and aggregate to its monhtly composite.
        for vari in vars_in_file:
            if all(pd.isna(df_mo[vari])):
                print('Empty '+ vari + ' file')
                continue
            not_empty[vari] = True

            if mode=='qc' and vari in vari_qc:
                locs=df_mo.loc[df_mo[vari+'_qc']==0].index
            else:
                locs=df_mo.index
            if mm in descriptors['counts'][vari].keys():
                descriptors['counts'][vari][mm] = descriptors['counts'][vari][mm] + cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.count(vari))
                descriptors['max'][vari][mm] = np.fmax(descriptors['max'][vari][mm],cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.max(vari)))
                descriptors['min'][vari][mm] = np.fmin(descriptors['min'][vari][mm],cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.min(vari)))
                # All this mess, because addition propagates nan's in xarrays!...have to check this
#                mean_mm = cvs.points(df_mo[['latitude','longitude',vari]], 'longitude', 'latitude', ds.mean(vari))
#                max_frame = np.fmax(descriptors['ave'][vari][mm],mean_mm)
#                min_frame = np.fmin(descriptors['ave'][vari][mm],mean_mm)
#                descriptors['ave'][vari][mm] = 0.5*max_frame + 0.5*min_frame
            else:
                descriptors['counts'][vari][mm] = cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.count(vari)) # <class 'xarray.core.dataarray.DataArray'>
                descriptors['max'][vari][mm] = cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.max(vari))
                descriptors['min'][vari][mm] = cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.min(vari))
#                mean_mm = cvs.points(df_mo[['latitude','longitude',vari]], 'longitude', 'latitude', ds.mean(vari))
#                descriptors['ave'][vari][mm] = mean_mm

            # Also add monthly stats to the global aggregate
            if 'global' in descriptors['counts'][vari].keys():
                descriptors['counts'][vari]['global'] = descriptors['counts'][vari]['global'] + cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.count(vari))
                descriptors['max'][vari]['global'] = np.fmax(descriptors['max'][vari]['global'],cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.max(vari)))
                descriptors['min'][vari]['global'] = np.fmin(descriptors['min'][vari]['global'],cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.min(vari)))
#                max_frame = np.fmax(descriptors['ave'][vari]['global'],mean_mm)
#                min_frame = np.fmin(descriptors['ave'][vari]['global'],mean_mm)
#                descriptors['ave'][vari]['global'] = 0.5*max_frame + 0.5*min_frame
            else:
                descriptors['counts'][vari]['global'] = cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.count(vari))
                descriptors['max'][vari]['global'] = cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.max(vari))
                descriptors['min'][vari]['global'] = cvs.points(df_mo[['latitude','longitude',vari]].loc[locs], 'longitude', 'latitude', ds.min(vari))
#                descriptors['ave'][vari]['global'] = mean_mm

# da = xr.concat(das, pd.Index(dts, name='date'))!!!!
# Now find bound values for maps
max_values = dict()
min_values = dict()
max_counts = dict()
for vari in vars_in:
    if not_empty.get(vari):
        max_values[vari] = vars_saturation[vari][1] if not qc_mode else np.amax(descriptors['max'][vari]['global']).item()
        min_values[vari] = vars_saturation[vari][0] if not qc_mode else np.amin(descriptors['min'][vari]['global']).item()
        max_counts[vari] = np.amax(descriptors['counts'][vari]['global']).item()
        if not qc_mode:
            max_counts[vari] = np.amax( [ max_counts.get(x) for x in max_counts ])

print(max_counts)

# 2. PLOT MAPS ----------------------------------------------------------------
# HOW ARE WE PLOTTING:
# We plot using matplotlib (not xarray direct plotting) on a cartopy axis
# Have to do some adjustments to matplotlib because of cartopy.
# Why cartopy, and not basemap?:   https://github.com/SciTools/cartopy/issues/920
# But we might face the need of a projection not yet in cartopy......
proj = ccrs.PlateCarree()

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
            if not_empty.get(vari) and max_counts.get(vari)>0:
                colormap = cmaps.get('counts')  if descriptor == 'counts' else cmaps.get(vari)
                colorbar_title = vars_units.get('counts') if descriptor == 'counts' else descriptor + " (" + vars_units.get(vari) + ")"
                colorbar_orien = 'v'
                if descriptor == 'counts':
                    min_value = 1; max_value = max_counts[vari]
                    z = descriptors[descriptor][vari][agg_period].where(descriptors[descriptor][vari][agg_period]> 0).values
                    normalization = LogNorm(vmin=min_value, vmax=max_value)
                else:
                    normalization = mpl.colors.Normalize(vmin=min_value, vmax=max_value)
                    min_value = min_values[vari]; max_value = max_values[vari]
                    z = descriptors[descriptor][vari][agg_period].values
                plt.title('-'.join([sid_deck,vari]) + ": " + " ".join([agg_name,descriptor]) + '\n' + "-".join([str(y_ini),str(y_end)]) + ' composite')
                f, ax = plt.subplots(1, 1, subplot_kw=dict(projection=proj),figsize=(14,7),dpi = 150)  # It is important to try to estimate dimensions that are more or less proportional to the layout of the mosaic....
                lons = descriptors[descriptor][vari][agg_period]['longitude']
                lats = descriptors[descriptor][vari][agg_period]['latitude']
                map_on_subplot(ax)
                plt.savefig(os.path.join(out, '_'.join([sid_deck,vari,descriptor,agg_period,'map']) + ext),bbox_inches='tight')
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
    if agg_period != 'global':
        agg_name = datetime.date(1900, int(agg_period), 1).strftime('%B')
    else:
        agg_name = 'full period'
    for vari in vars_in:
        if not_empty.get(vari) and max_counts.get(vari)>0:
            f, ax = plt.subplots(1, 3, subplot_kw=dict(projection=proj),figsize=(14,7),dpi = 180)
            c = 0
            for descriptor in descriptor_list:
                colormap = cmaps.get('counts')  if descriptor == 'counts' else cmaps.get(vari)
                colorbar_title = vars_units.get('counts') if descriptor == 'counts' else descriptor + " (" + vars_units.get(vari) + ")"
                colorbar_orien = 'h'
                if descriptor == 'counts':
                    z = descriptors[descriptor][vari][agg_period].where(descriptors[descriptor][vari][agg_period]> 0).values
                    min_value = 1; max_value = max_counts[vari]
                    normalization = LogNorm(vmin=min_value, vmax=max_value)
                else:
                    normalization = mpl.colors.Normalize(vmin=min_value, vmax=max_value)
                    z = descriptors[descriptor][vari][agg_period].values
                    min_value = min_values[vari]; max_value = max_values[vari]
                lons = descriptors[descriptor][vari][agg_period]['longitude']
                lats = descriptors[descriptor][vari][agg_period]['latitude']
                #if not qc_mode:
                #    show_colorbar = True if (c == 2 or descriptor == 'counts') else False
                map_on_subplot(ax[c])
                c += 1

            wspace = 0.08# if qc_mode else 0.05
            dpi = 400 if qc_mode else 300
            plt.subplots_adjust(wspace=wspace,hspace=0.05)#  Force small separation (default is .2, to keep in mind "transparent" colorbar between subplots....THIS WILL DEPEND ON THE FIGURE WIDTH
            print(os.path.join(out,'_'.join([sid_deck,vari,'mosaic',agg_period,'map']) + ext))
            f.savefig(os.path.join(out,'_'.join([sid_deck,vari,'mosaic',agg_period,'map']) + ext),bbox_inches='tight',dpi = 300)# 'tight' here, not in plt.tight_layout: here it realizes the new size because of add_axes, but not in plt.tight_layout....
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
