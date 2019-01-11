# De  http://introtopython.org/visualization_earthquakes.html

# Had to go down to matplotlib 2.2.0 for basemap to work (,atplotlib.cbook.is_scalar issue)

from mpl_toolkits.basemap import Basemap
import matplotlib
import matplotlib.pyplot as plt
#plt.switch_backend('agg')
import numpy as np
import csv
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes
#from mpl_toolkits.axes_grid1.inset_locator import mark_inset
import matplotlib.patches as mplPatch
from matplotlib.patches import Polygon
import matplotlib.patheffects as path_effects

############################  FUNCTIONS  #####################################
def drawScreenPoly(lons,lats,m,col,alfa):
    x, y = m( lons, lats ) #paso a coordenadas de dibujo
    xy=np.array([x,y]) #convierto en matriz
    xy=xy.T #transpuesta de la matriz
    #creo poligono con color y transparencia
    poly = mplPatch.Polygon( xy, facecolor=col, alpha=alfa)
    plt.gca().add_patch(poly)   
    print('Drawn')
      
############################ END  FUNCTIONS  ##################################

###############################  MAIN  ########################################
def map_polygons(groups):
    
    # Set some hardcoded defaults and map settings    
    nLAT=89
    sLAT=-89
    wLON=-179
    eLON=179
    cLAT=sLAT + (nLAT-sLAT)/2
    cLON=wLON + (eLON-wLON)/2
    scale_map=0.4
    font = {'weight' : 'normal',
            'size'   : 2}
    
    matplotlib.rc('font', **font) 
    
    # Read data into vertices
    polygons = dict()
    polygons['labels'] = dict(zip(range(len(groups.index)),groups.index))
    polygons['LONS'] = dict(zip(range(len(groups.index)),[ [x,y,z,k] for x,y,z,k in zip(groups['xmin'],groups['xmin'],groups['xmax'],groups['xmax'])]))
    polygons['LATS'] = dict(zip(range(len(groups.index)),[ [x,y,z,k] for x,y,z,k in zip(groups['ymin'],groups['ymax'],groups['ymax'],groups['ymin'])]))
                                        
    keys=sorted([x for x in polygons['LATS'].keys()])
    
    # 1. CREATE OBJECTS FOR MAP
    fig = plt.figure(figsize=(8, 14), dpi=150)
    ax = fig.add_subplot(111)
    
    myMap = Basemap(projection='cyl', 
                  lat_0=cLAT, lon_0=cLON,
                  llcrnrlat=sLAT,urcrnrlat=nLAT,
                  llcrnrlon=wLON,urcrnrlon=eLON,
                  resolution=None)
    #myMap.shadedrelief(scale=scale_map)
    myMap.bluemarble(scale=scale_map)
    #path_effectsTxt=[path_effects.withSimplePatchShadow()]
    
    # 2. NOW ADD POLYGONS
    ax.grid(color='k')
    for key in keys:
        xyList = [[a,b] for a,b in zip(polygons['LONS'][key], polygons['LATS'][key])]#list(zip(polygons['LONS'][key], polygons['LATS'][key]))
        p = Polygon(xyList, alpha=0.3,facecolor='yellow',edgecolor=None)
        ax.add_artist(p)
        plt.text(polygons['LONS'][key][2],polygons['LATS'][key][1],polygons['labels'][key],weight='bold',va='bottom',ha='right',color='yellow') #,path_effects=path_effectsTxt)
    
    path_effectsLine=[path_effects.SimpleLineShadow(),
                           path_effects.Normal()]
    #plt.show()
    # 3. SAVE OR RETURN FIG
    return fig
    
    #polygons['color']={3:'Blue', \
    #                    1: 'Cyan', \
    #                    2: 'Cyan', \
    #                    6: 'Magenta', \
    #                    4:'Lime',\
    #                    5:'Lime',\
    #                    0:'Gray'}