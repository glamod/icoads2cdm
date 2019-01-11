from __future__ import print_function
import os
import pandas as pd
import numpy as np
import logging
import sys
if sys.version_info[0] >= 3:
    py3 = True
    from io import StringIO as StringIO
else:
    py3 = False
    from io import BytesIO as BytesIO

class smart_dict(dict):
    missing = np.nan
    def __init__(self,*args,**kwargs):
        dict.__init__(self,*args,**kwargs)
        self.__dict__ = self 
 
    def __getitem__(self, key):
        val = dict.__getitem__(self, key)
        return val
 
    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)
 
    def __missing__(self, key):
        key = ''
        if key == '' :
            key = self.missing
        return key
    
def setup_log(log_file = None,file_mode = 'a'):
    if log_file:
        logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                    level=logging.INFO,datefmt='%Y%m%d %H:%M:%S',filename=log_file,filemode = file_mode)
        logging.info('Log file mode'.format(file_mode))
    else:
        logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                    level=logging.INFO,datefmt='%Y%m%d %H:%M:%S',filename=None)

#def replace_from_mirror(df_main_tfrObj,df_replacements,reference_column,replace_columns,drop_mismatches = False,ignore_mismatches = True):
def replace_from_mirror(df_main_tfrObj,df_replacements,reference_column,replace_columns,drop_mismatches = False,ignore_mismatches = True, value_mismatches = None):
    # df_main_tfrObj: pandas df as a textfile reader object
    # df_replacements: pandas df as pandas df
    # reference_column: name of column used as reference value to replace from df2 to df1 
    # replace_columns: list of columns to replace from df2 to df1
    # drop_mismatches: drop records from df1 for which values in reference column are not found in df2. Defaults to False
    # ignore_mismatches: if true df1 values for reference column values not found in df2 are kept. Defaults to True
    #                   Otherwise, value mismatches must be provided as a dictionary with element:value pairs
    # value_mismatches: dictionary with values to tag mismatching records with if ignore_mismatches = False. One column:value pair per column in replace_coumns
    # 
    # WARNING CHANGES AFTER BETA RELEASE PROCESSING FOR QC FLAG INSERTION:
    #    value_mismatches option
    #    if not drop_mismatches and ignore_mismatches: instead of "if not drop_mismatches"
    #    on final read_csv: # WARNING: keep_default_na = False ADDED AFTER ALL PROCESSING TO ADD QC FLAGS, SHOULD NOT AFFECT....BUT MIGHT!!!

    df_buff = StringIO() if py3 else BytesIO()
    df_buff_rejects = StringIO() if py3 else BytesIO()
    tfrObj_options = df_main_tfrObj.orig_options
    df_replacements.set_index(reference_column,drop=False,inplace=True)
    
    matches = []
    mismatches = []
    ichunk = 0
    for df in df_main_tfrObj:
        logging.info('Replacing chunk {}'.format(ichunk))
        idx = df.index
        df.set_index(reference_column,drop=False,inplace=True)
        matchesi = list(set([ x for x in df.index if x in df_replacements.index ]))
        matches.extend(matchesi)
        mismatchesi = list(set([ x for x in df.index if x not in df_replacements.index ]))
        mismatches.extend(mismatchesi)
        df_mismatches = df.loc[mismatchesi].copy()
       
        for element in replace_columns:
            logging.info("Replacing {}".format(element))
            if drop_mismatches or ignore_mismatches:
                missing = np.nan
            elif not ignore_mismatches:
                #logging.warning('Feature "IGNORE_MISMATCHES = FALSE" not yet supported. Will treat as ignore True')
                missing = value_mismatches.get(element)
            new = df.index.map(smart_dict(df_replacements[element],missing = missing))
            if not drop_mismatches and ignore_mismatches:
                new.fillna(df[element],inplace = True)
            df[element] = new
            bef = len(df)
            if drop_mismatches:
       
                df.dropna( subset = [element],inplace = True) # Drop rows where the replaced element has resulted in nan: not found in replacement df
                if bef - len(df) > 0:
                    logging.info('Dropped {} unmatching records'.format(bef - len(df)))  
           
        header = True if ichunk == 0 else False
        df_mismatches.to_csv(df_buff_rejects, mode = 'a', header = header, encoding = 'utf-8',index=False)
        df.to_csv(df_buff, mode = 'a', header = header, encoding = 'utf-8',index=False)   
        ichunk += 1
        
    df_buff.seek(0)
    df_buff_rejects.seek(0)
    # WARNING: keep_default_na = False ADDED AFTER ALL PROCESSING TO ADD QC FLAGS, SHOULD NOT AFFECT....BUT MIGHT!!!
    pd_tfr_rejects = pd.read_csv( df_buff_rejects,skipinitialspace=True, chunksize = tfrObj_options['chunksize'],dtype = tfrObj_options['dtype'],names=tfrObj_options['names'],skiprows=tfrObj_options['skiprows'], keep_default_na = False )
    pd_tfr = pd.read_csv( df_buff,skipinitialspace=True, chunksize = tfrObj_options['chunksize'],dtype = tfrObj_options['dtype'],names=tfrObj_options['names'],skiprows=tfrObj_options['skiprows'],keep_default_na =False )
    return pd_tfr,pd_tfr_rejects,list(set(matches)),list(set(mismatches))


def restore_df_tfr(file_ref,options):
    file_ref.seek(0)
    pd_tfr = pd.read_csv( file_ref, names = options['names'],chunksize = options['chunksize'], dtype = options['dtype']) #, skiprows = options['skiprows'])
    return pd_tfr

def restore_multiIdxColumn_df_tfr(file_ref,options):
    file_ref.seek(0)
    pd_tfr = pd.read_csv( file_ref,skipinitialspace=True, chunksize = options['chunksize'],dtype=options['dtype'],names=options['names'],skiprows=options['skiprows'])
    return pd_tfr

def remove_records_df_tfr(tfr,options,lines):
    df_buff = StringIO() if py3 else BytesIO()
    ichunk = 0
    for df in tfr:
        logging.info('Removing lines from chunk {}'.format(ichunk))
        df.drop(df.loc[df.index.isin(lines) ].index.tolist(),inplace=True)

        df.to_csv(df_buff,columns = options['names'],mode = 'a', header=False, encoding = 'utf-8')
        ichunk += 1
        
    df_buff.seek(0)
    pd_tfr = pd.read_csv( df_buff ,names = options['names'],chunksize = options['chunksize'], dtype = options['dtype'])
    return pd_tfr

def select_from_external_col_df_tfr(tfr,options,criteria,tfr_external = None):
    df_buff = StringIO() if py3 else BytesIO()
    ichunk = 0
    if tfr_external:
        for df_external,df in zip(tfr_external,tfr):
            logging.info('Parsing chunk {}'.format(ichunk))
            for key in criteria:
                logging.info('Selecting by {0}:{1}'.format(key,','.join([str(x) for x in criteria.get(key)])))
                df = df.loc[df_external[key].isin(criteria.get(key))]
            logging.info('{0}'.format(len(df)))
            df.to_csv(df_buff,columns = options['names'],mode = 'a', header=False, encoding = 'utf-8')
            ichunk += 1
    else:
        for df in tfr:
            logging.info('Parsing chunk {}'.format(ichunk))
            for key in criteria:
                logging.info('Selecting by {0}:{1}'.format(key,','.join([str(x) for x in criteria.get(key)])))
                df = df.loc[df[key].isin(criteria.get(key))]
            logging.info('{0}'.format(len(df)))
            df.to_csv(df_buff,columns = options['names'],mode = 'a', header=False, encoding = 'utf-8')
            ichunk += 1
        
    df_buff.seek(0)
    pd_tfr = pd.read_csv( df_buff ,names = options['names'],chunksize = options['chunksize'], dtype = options['dtype'],header=None)
    return pd_tfr

def setup_outdir(out_directory):
    if  not os.path.isdir(out_directory):
        try:
            os.mkdir(out_directory)
            return True
        except Exception as e:
            logging.error('Could not create output directory {}'.format(out_directory))

def mkdir_ifNot(name_dir):
    try:
        if not os.path.isdir(name_dir):
            os.mkdir(name_dir)
        return 0
    except Exception as e:
        return e
