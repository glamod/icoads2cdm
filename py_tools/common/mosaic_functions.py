v# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import ImageGrid
import matplotlib.image as mpimg

maps_paths = []
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-SST-counts-global.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-AT-counts-global.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-counts-vertical_cb.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-SST-ave-global.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-AT-ave-global.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-AT-vertical_cb.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-SST-max-global.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-AT-max-global.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-AT-vertical_cb.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-SST-min-global.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-AT-min-global.png')
maps_paths.append('/Users/iregon/C3S/dessaps/CDSpy/714-AT-vertical_cb.png')


F = plt.figure(1, dpi = 150)
grid = ImageGrid(F, 111,  # similar to subplot(111)
                 nrows_ncols=(4, 3),
                 axes_pad=(0.1,0.1),
                 add_all=True,
                 label_mode="1",
                 direction = 'row'
                 )
for i, im in enumerate(maps_paths):
    im = mpimg.imread(maps_paths[i])
    ax = grid[i]
    ax.set_axis_off()
    ax.imshow(im, interpolation='none')

plt.show() 

plt.tight_layout();
plt.savefig('/Users/iregon/C3S/dessaps/CDSpy/AA_mosaic.png',dpi = 700,bbox_inches='tight')




#deck = 714
#y_ini = 1980
#y_end = 2010
#param = 'SST'
#
#
#var_names = dict()
#var_names['AT'] = 'Air Temperature'
#var_names['SST'] = 'Sea Surface Temperature'
#
##=============================== to print....==================================
## 1. HEADER. TEXT AND TEXT PROPERTIES. One per header line
#hdr_txt = []
#hdr_txt.append(' '.join([str(deck),'ICOADS R3.0']))
#hdr_txt.append(var_names.get(param))
#hdr_txt.append(' to '.join([str(y_ini),str(y_end)]) + ' summary')
#
#hdr_properties = dict()
#hdr_properties['size'] = [16,16,14]
#hdr_properties['effects'] = ['B','','I']
#hdr_properties['align'] = ['C','C','C']
## 2. CAPTION TEXT PROPERTIES. Global to all captions
#caption_properties = dict()
#caption_properties['size'] = 9
#caption_properties['effects'] = 'I'
#caption_properties['align'] = 'C'
## 3. MOSAIC TEXT PROPERTIES. Global to all titles in mosaic. Will be always centered
#mosaic_layout = [4,2]
#mosaic_label_rows = ['COUNTS','AVE','MAX','MIN']
#mosaic_label_cols = ['SST','AT']
#mosaic_properties = dict()
#mosaic_properties['size'] = 12
#mosaic_properties['effects'] = ''
#
## GLOB PROPERTIES
#global_properties = dict()
#global_properties['font'] = 'Arial'
##==============================================================================
#def print_text(pdf,txt,properties):
#    for i,x in enumerate(txt):
#        pdf.set_font(global_properties['font'], style = properties['effects'][i], size=properties['size'][i])
#        pdf.cell(width,8, txt=txt[i], ln=1, align=properties['align'][i])
#
#def rotated_text(x,y,txt,angle,font,size):
#    x_width = pdf.get_string_width(txt) #sum([ pdf.get_string_width(s) for s in txt ])
#    pdf.rotate(angle,x,y)
#    pdf.set_font(font, size = size)
#    pdf.text(x -  0.5*x_width,y - 0.5*pdf.get_string_width('S') ,txt)
#    pdf.rotate(0)
#
#def add_image(image_path,width,caption):
#    pdf.image(image_path, w = width, x = pdf.l_margin)
#    pdf.set_font(global_properties['font'], style = caption_properties['effects'], size = caption_properties['size'])
#    pdf.cell(width,caption_properties['size'], txt=caption, ln=1, align=caption_properties['align'])
#
#def add_mosaic(images_path,n_rows,n_columns,label_rows = None,label_cols = None, w_scale = 1, caption = None):
#    if label_rows:
#        x_offset = 1.5*mosaic_properties['size']
#    x0 = x_offset + pdf.l_margin + 0.5*(width - width*w_scale)
#    mosaic_width = width*w_scale - x_offset
#    c = 0
#    if label_cols:
#        pdf.set_font(global_properties['font'], size=mosaic_properties['size'])
#        for i,label in enumerate(label_cols):
#            pdf.text(x0 + c*mosaic_width/n_columns + 0.5*mosaic_width/n_columns,pdf.get_y(),label)
#            c += 1
#    c = 0
#    l = 0  
#    pdf.ln(h = mosaic_properties['size']*0.2)
#    for i,imi in enumerate(images_path):
#        if c == n_columns - 1:
#            y_prev = y
#            y = None # Positions cursor in current y, plus goes to below image afterwards          
#        else:
#            y = pdf.get_y()
#        if c == n_columns:
#            rotated_text(x0,y_prev + (y - y_prev)/2,label_rows[l],90.,font = global_properties['font'],size = mosaic_properties['size'])
#            l += 1
#            c = 0
#        pdf.image(imi, x = x0 + c*mosaic_width/n_columns, w = mosaic_width/n_columns, y = y)
#        c = c + 1
#        
#    y = pdf.get_y()
#    rotated_text(x0,y_prev + (y - y_prev)/2,label_rows[l],90.,font = global_properties['font'],size = mosaic_properties['size'])
#    pdf.set_font(global_properties['font'], style = caption_properties['effects'], size = caption_properties['size'])
#    pdf.cell(width,8, txt=caption, ln=1, align=caption_properties['align'])
##==============================================================================
#pdf = FPDF()
#pdf.set_auto_page_break(False)
#width = pdf.w - pdf.l_margin - pdf.r_margin
#pdf.add_page()
#single_image_width = width
#
#
## 1. PRINT HEADER
#print_text(pdf,hdr_txt,hdr_properties)
#pdf.ln()
## 2. ADD TIMESERIES IMAGE AND CAPTION
#add_image(ts_path,single_image_width,'Monthly bla bla')
#pdf.ln()
## 3. ADD MAP MOSAIC
#add_mosaic(maps_paths,mosaic_layout[0],mosaic_layout[1], label_rows = mosaic_label_rows,label_cols = mosaic_label_cols,w_scale = 0.8, caption = 'Maps')
#
#pdf.output("simple_demo.pdf")