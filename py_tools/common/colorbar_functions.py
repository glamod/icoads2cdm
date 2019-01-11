#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 09:42:02 2018

@author: iregon
"""

for vari in vars_in:
    # Set the colormap and norm to correspond to the data for which
    # the colorbar will be used.
    cmap = cmaps.get(vari)
    norm = mpl.colors.Normalize(vmin=min_value_global, vmax=max_value_global)
    # Vertical + horizontal colorbars
    fig = plt.figure(i,figsize = (1.8,7), dpi = 150);
    ax_v = fig.add_axes([0.05, 0.05, 0.45, 0.9])
    fig = plt.figure(i + 1,figsize = (14,1.8), dpi = 150);
    ax_h = fig.add_axes([0.05, 0.5, 0.9, 0.45])
    # ColorbarBase derives from ScalarMappable and puts a colorbar
    # in a specified axes, so it has everything needed for a
    # standalone colorbar.  There are many more kwargs, but the
    # following gives a basic continuous colorbar with ticks
    # and labels.
    cb_v = mpl.colorbar.ColorbarBase(ax_v, cmap=cmap,norm=norm,orientation='vertical')
    cb_v.set_label(vari + ' (' +  vars_units.get(vari)+ ' )', size = colorbar_title_size)
    cb_v.ax.tick_params(labelsize = colorbar_label_size)
    plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/' + '-'.join([str(deck),vari,'vertical_cb']) + '.png',bbox_inches='tight')
    cb_h = mpl.colorbar.ColorbarBase(ax_h, cmap=cmap,norm=norm,orientation='horizontal')
    cb_h.set_label(vari + ' (' +  vars_units.get(vari)+ ' )', size = colorbar_title_size)
    cb_h.ax.tick_params(labelsize = colorbar_label_size)
    plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/' + '-'.join([str(deck),vari,'horizontal_cb']) + '.png',bbox_inches='tight')
    plt.close(i)
    plt.close(i+1)
    i +=2

vari= 'counts'
cmap = cmaps.get(cmap_counts)
norm = mpl.colors.Normalize(vmin=min_value_global, vmax=max_value_global)
# Vertical + horizontal colorbars
fig = plt.figure(i,figsize = (1.8,7), dpi = 150);
ax_v = fig.add_axes([0.05, 0.05, 0.45, 0.9])
fig = plt.figure(i + 1,figsize = (14,1.8), dpi = 150);
ax_h = fig.add_axes([0.05, 0.5, 0.9, 0.45])    
cb_v = mpl.colorbar.ColorbarBase(ax_v, cmap=cmap,norm=norm,orientation='vertical')
cb_v.set_label(vari, size = colorbar_title_size)
cb_v.ax.tick_params(labelsize = colorbar_label_size)
plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/' + '-'.join([str(deck),vari,'vertical_cb']) + '.png',bbox_inches='tight')
cb_h = mpl.colorbar.ColorbarBase(ax_h, cmap=cmap,norm=norm,orientation='horizontal')
cb_h.set_label(vari, size = colorbar_title_size)
cb_h.ax.tick_params(labelsize = colorbar_label_size)
plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/' + '-'.join([str(deck),vari,'horizontal_cb']) + '.png',bbox_inches='tight')
plt.close(i)
plt.close(i+1)