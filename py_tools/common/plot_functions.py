from __future__ import print_function
import matplotlib 
import matplotlib.pyplot as plt
import socket
if not 'noc' in socket.gethostname():
    plt.switch_backend('agg')  
import numpy as np
from matplotlib.ticker import ScalarFormatter, FormatStrFormatter


def subplt_logbar(names, data, fig = None, ax_share = None, ixsubplots = None, nrows = None, ncols = None, title = None, figure_number = 1, ylabel = ''): 
    
    if not fig:
        ncols = 1 if not ncols else ncols
        nrows = 1 if not nrows else nrows
        ixsubplots = 100*nrows + 10*ncols + 1
        fig = plt.figure(figure_number, figsize=(10, 10), dpi=100)
        ax = fig.add_subplot(ixsubplots)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=75, fontsize = 8)
        
    if (ixsubplots > 11 + 100):
        ax = fig.add_subplot(ixsubplots,sharex = ax_share, sharey = ax)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=75, fontsize = 8)
        
    
    ax.bar( range(len(data)),data,tick_label=names, log=True)
    idx_nan = []
    data_nan = []
    label_nan = []
    for i,x in enumerate(data):
        if np.isnan(x):
            idx_nan.append(i)
            data_nan.append(1)
            label_nan.append(names[i])
            
    if len(data_nan) > 0:        
        ax.bar( idx_nan,data_nan,color = 'OrangeRed', log=True)
        
    if title:
        ax.set_title(title)

#    if ylabel == 'percent':    
#        ax.set_ylim(top=100)
#        ax.set_ylim(bottom=0.1)
#        ax.set_yticks([1,10,50,100], minor=False)
#        ax.set_yticks([5,20,30,40,60,70,80,90], minor=True)
    #ax.set_yscale('log')
    ax.grid(which='major', axis='y', color='grey', linestyle='-', linewidth=1)    
    ax.set_ylabel(ylabel)
    ax.set_axisbelow(True)
    plt.tight_layout()
    
    return fig, ax, ixsubplots
