#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  2 09:46:52 2018

@author: iregon
"""

vars_file_prefix = {'sst':'observations-sst','at':'observations-at','slp':'observations-slp','wd':'observations-wd','ws':'observations-ws','dpt':'observations-dpt','wbt':'observations-wbt'}
vars_offset = {'sst':-273.15,'at':-273.15,'slp':0,'wd':0,'ws':0,'dpt':-273.15,'wbt':-273.15}
vars_factor = {'sst':1,'at':1,'slp':0.01,'wd':1,'ws':1,'dpt':1,'wbt':1}
vars_units = {'sst':'$^\circ$C','at':'$^\circ$C','counts':'counts','slp':'hPa','wd':'degrees','ws':'ms$^{-1}$','dpt':'$^\circ$C','wbt':'$^\circ$C'}

vars_labels = dict()
vars_labels['long_name_lower'] = {'sst':'sea surface temperature','at':'air temperature','slp':'sea level pressure','wd':'wind direction','ws':'wind speed'
                                ,'dpt':'dew point temperature','wbt':'wet bulb temperature'}
vars_labels['long_name_upper'] = {'sst':'Sea Surface Temperature','at':'Air Temperature','slp':'Sea Level Pressure','wd':'Wind Wirection','ws':'Wind Speed'
                                 ,'dpt':'Dew Point Temperature','wbt':'Wet Bulb Temperature'}
vars_labels['short_name_upper'] = {'sst':'SST','at':'AT','slp':'SLP','wd':'WD','ws':'WS','dpt':'DPT','wbt':'WBT'}
vars_labels['short_name_lower'] = {'sst':'sst','at':'at','slp':'slp','wd':'wd','ws':'ws','dpt':'dpt','wbt':'wbt'}

vars_colormap = dict()
vars_colormap['sst'] = 'jet'
vars_colormap['at'] = 'jet'
vars_colormap['slp'] = 'jet'
vars_colormap['wd'] = 'jet'
vars_colormap['ws'] = 'jet'
vars_colormap['dpt'] = 'jet'
vars_colormap['wbt'] = 'jet'
vars_colormap['counts'] = 'winter'

vars_color = dict()
vars_color['sst'] = 'OrangeRed'
vars_color['at'] = 'DarkRed'
vars_color['slp'] = 'BlueViolet'
vars_color['wd'] = 'DeepSkyBlue'
vars_color['ws'] = 'SteelBlue'
vars_color['dpt'] = 'LimeGreen'
vars_color['wbt'] = 'Chartreuse'
vars_color['counts'] = 'Black'

vars_saturation = dict()
vars_saturation['sst'] = [-5,35]
vars_saturation['at'] = [-5,35]
vars_saturation['slp'] = [870,1050]
vars_saturation['wd'] = [1,360]
vars_saturation['ws'] = [0,100]
vars_saturation['dpt'] = [-5,35]
vars_saturation['wbt'] = [-5,35]
vars_saturation['counts'] = [0.01,1000000]
