# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from fpdf import FPDF
import sys
import os
from common.var_properties import *
from common.deck_properties import *
from common.sid_properties import *

try:
    sid_deck = sys.argv[1]
    inDir = sys.argv[2] # general to reports
    y_ini = int(sys.argv[3])
    y_end = int(sys.argv[4])
    out = sys.argv[5] # specific to no_qc
except Exception as e:
    print("Error processing line argument input to script: ", e)
    exit(1)

sid = sid_deck.split("-")[0]
deck = sid_deck.split("-")[1]
vars_in = ['dpt','wbt','sst','at','slp','wd','ws']
#=============================== to print....==================================
# 0. COVER. TEXT AND TEXT PROPERTIES. One per COVER line
cvr_txt = []
cvr_txt.append('CDS BETA RELEASE ( PRE-QC )')
cvr_txt.append(sid_labels['long_name'].get(str(sid)))
cvr_txt.append(deck_labels['long_name'].get(str(deck)))
cvr_txt.append(' to '.join([str(y_ini),str(y_end)]) + ' summary')
cvr_txt.append("(" + sid_deck + ")")

cvr_properties = dict()
cvr_properties['size'] = [22,16,16,14,14]
cvr_properties['effects'] = ['B','','','I','']
cvr_properties['align'] = ['C','C','C','C','C']
# 1. HEADER. TEXT AND TEXT PROPERTIES. One per header line
hdr_txt = dict()
for vari in vars_in:
    hdr_txt[vari] = []
    hdr_txt[vari].append(vars_labels['long_name_upper'].get(vari))

hdr_txt['IO'] = ['RECORD NUMBERS IN CDM TABLES']
hdr_txt['NO_PUB47']= ['IMMA1 FILE RECORDS (PT-c1 subset: 1,2,3,4,5) NOT IN PUB47']

hdr_properties = dict()
hdr_properties['size'] = [16]
hdr_properties['effects'] = ['']
hdr_properties['align'] = ['C']
# 2. CAPTION TEXT PROPERTIES. Global to all captions
caption_properties = dict()
caption_properties['size'] = 8
caption_properties['effects'] = 'I'
caption_properties['align'] = 'C'
# 3. MOSAIC TEXT PROPERTIES. Global to all titles in mosaic. Will be always centered
mosaic_layout = [4,2]
mosaic_label_rows = ['COUNTS','AVE','MAX','MIN']
mosaic_label_cols = ['SST','AT']
mosaic_properties = dict()
mosaic_properties['size'] = 12
mosaic_properties['effects'] = ''

# GLOB PROPERTIES
global_properties = dict()
global_properties['font'] = 'Arial'
#==============================================================================
def print_text(pdf,txt,properties, item = None):
    if item or item == 0:
        i = item
        pdf.set_font(global_properties['font'], style = properties['effects'][i], size=properties['size'][i])
        pdf.cell(epw,8, txt=txt[i], ln=1, align=properties['align'][i])
    else:
        for i,x in enumerate(txt):
            pdf.set_font(global_properties['font'], style = properties['effects'][i], size=properties['size'][i])
            pdf.cell(epw,8, txt=txt[i], ln=1, align=properties['align'][i])

def rotated_text(x,y,txt,angle,font,size):
    x_width = pdf.get_string_width(txt) #sum([ pdf.get_string_width(s) for s in txt ])
    pdf.rotate(angle,x,y)
    pdf.set_font(font, size = size)
    pdf.text(x -  0.5*x_width,y - 0.5*pdf.get_string_width('S') ,txt)
    pdf.rotate(0)

def add_image(image_path,caption,width=None,height=None):
    if width:
        pdf.image(image_path, w = width, x = pdf.l_margin + (epw - width)/2)#, x = pdf.l_margin)
    if height:
        pdf.image(image_path, h = height)#, x = pdf.l_margin)
    pdf.set_font(global_properties['font'], style = caption_properties['effects'], size = caption_properties['size'])
    pdf.cell(epw,caption_properties['size'], txt=caption, ln=1, align=caption_properties['align'])

def add_mosaic(images_path,n_rows,n_columns,label_rows = None,label_cols = None, w_scale = 1, caption = None):
    if label_rows:
        x_offset = 1.5*mosaic_properties['size']
    x0 = x_offset + pdf.l_margin + 0.5*(epw - epw*w_scale)
    mosaic_width = epw*w_scale - x_offset
    c = 0
    if label_cols:
        pdf.set_font(global_properties['font'], size=mosaic_properties['size'])
        for i,label in enumerate(label_cols):
            pdf.text(x0 + c*mosaic_width/n_columns + 0.5*mosaic_width/n_columns,pdf.get_y(),label)
            c += 1
    c = 0
    l = 0
    pdf.ln(h = mosaic_properties['size']*0.2)
    for i,imi in enumerate(images_path):
        if c == n_columns - 1:
            y_prev = y
            y = None # Positions cursor in current y, plus goes to below image afterwards
        else:
            y = pdf.get_y()
        if c == n_columns:
            rotated_text(x0,y_prev + (y - y_prev)/2,label_rows[l],90.,font = global_properties['font'],size = mosaic_properties['size'])
            l += 1
            c = 0

        pdf.image(imi, x = x0 + c*mosaic_width/n_columns, w = mosaic_width/n_columns, y = y)
        c = c + 1

    y = pdf.get_y()
    rotated_text(x0,y_prev + (y - y_prev)/2,label_rows[l],90.,font = global_properties['font'],size = mosaic_properties['size'])
    pdf.set_font(global_properties['font'], style = caption_properties['effects'], size = caption_properties['size'])
    pdf.cell(epw,8, txt=caption, ln=1, align=caption_properties['align'])
#==============================================================================
pdf = FPDF()
pdf.set_auto_page_break(False)
epw = pdf.w - pdf.l_margin - pdf.r_margin
eph = pdf.h - pdf.t_margin - pdf.b_margin
single_image_width = epw
pdf.add_page()
# COVER
pdf.ln(h = eph*0.35)
for i,x in enumerate(cvr_txt):
    print_text(pdf,cvr_txt,cvr_properties,item = i)
    pdf.ln()

# IO PLOT
pdf.add_page()
pdf.ln()
print_text(pdf,hdr_txt.get('IO'),hdr_properties)
pdf.ln()
io_path = os.path.join(inDir,"_".join([sid_deck,"imma_process_io.png"]))
if os.path.isfile(io_path):
    add_image(io_path,'Monthly number of records of raw input files (imma) and cdm tables (header,observations)',width = epw/1.1)
else:
    pdf.ln()
    print_text(pdf,["IO TIME SERIES NOT AVAILABLE"],hdr_properties)
    pdf.ln()
io_rejects_path = os.path.join(inDir,'rejects_pub47',"_".join([sid_deck,"imma_process_io.png"]))
if os.path.isfile(io_rejects_path):
    pdf.ln()
    print_text(pdf,hdr_txt.get('NO_PUB47'),hdr_properties)
    add_image(io_rejects_path,'Records not matching pub47 call signs. Monthly number of records of raw input files (imma) and cdm tables (header,observations)',width = epw/1.1)
else:
    pdf.ln()
    print_text(pdf,["NO PUB 47 MISMATCHES FOUND"],hdr_properties)
    pdf.ln()

for vari in vars_in:
    pdf.add_page()
    # 1. PRINT HEADER
    pdf.ln()
    print_text(pdf,hdr_txt.get(vari),hdr_properties)
    pdf.ln()
    # 2. ADD TIMESERIES IMAGE AND CAPTION
    ts_path = os.path.join(out,"_".join([sid_deck,vari,"lat_band_stats_ts.png"]))
    if os.path.isfile(ts_path):
        add_image(ts_path,'Monthly ' + vars_labels['long_name_lower'].get(vari) + ' statistics aggregated by latitudinal bands',width = epw/1.6)
    else:
        pdf.ln()
        print_text(pdf,["TIME SERIES NOT AVAILABLE"],hdr_properties)
        pdf.ln()
    # 3. ADD MOSAIC
    map_path = os.path.join(out,"_".join([sid_deck,vari,"mosaic_global_map.png"]))
    if os.path.isfile(map_path):
        add_image(map_path,"Full period " + vars_labels['long_name_lower'].get(vari) + ' statistics',width = epw/1)
    else:
        pdf.ln()
        print_text(pdf,["MAP NOT AVAILABLE"],hdr_properties)
        pdf.ln()

pdf.output(os.path.join(out,sid_deck + '_report.pdf'))
