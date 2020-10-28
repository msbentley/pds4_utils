#!/usr/bin/python
"""
pds_utils.py - a collection of utilities for dealing with PDS4 files"""

from pds4_tools import pds4_read
from pds4_tools.reader.table_objects import TableManifest
# import pds4_tools
from pathlib import Path
from lxml import etree
import pandas as pd
import sys
import yaml
import os
import logging

try:
   import cPickle as pickle
except:
   import pickle

# set up module level logging

# only show warning or higher messages from PDS4 tools
pds4_logger = logging.getLogger('PDS4ToolsLogger')
pds4_logger.setLevel(logging.WARNING)

class DuplicateFilter(logging.Filter):

    def filter(self, record):
        current_log = (record.module, record.levelno, record.msg)
        last = getattr(self, "last_log", None)
        if (last is None) or (last != current_log):
            self.last_log = current_log
            return True  # i.e. log the message
        else:
            return False  # i.e. do not log the message

log = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.WARNING)


default_config = os.path.join(
    os.environ.get('APPDATA') or
    os.environ.get('XDG_CONFIG_HOME') or
    os.path.join(os.environ['HOME'], '.config'),
    "pds_dbase.yml")


def index_products(directory='.', pattern='*.xml'):
    """
    Accepts a directory containing PDS4 products, indexes the labels and returns a 
    Pandas data-frame containng meta-data for each product.
    """

    from lxml import etree
    import pandas as pd

    # define the PDS namespace
    pds_ns = 'http://pds.nasa.gov/pds4/pds/v1'
    
    # recursively find all labels
    labels = select_files(pattern, directory=directory, recursive=True)

    cols = ['filename', 'product_type', 'lid', 'vid', 'start_time', 'stop_time']
    index = pd.DataFrame([], columns=cols)

    for label in labels:
        
        #  get the root element
        root = etree.parse(label).getroot()

        # returns the namespace mapping
        ns = root.nsmap

        # XPath doesn't like "default" namespace (nsmap return None)
        # so if present, we replace here with the key pds
        if None in ns and pds_ns == ns[None]:
            ns['pds'] = ns.pop(None)

        product_type = root.xpath('name(/*)', namespaces=ns)
        if not product_type.startswith('Product_'):
            log.warn('XML file {:s} is not a PDS4 label, skipping'.format(Path(label).name))
            continue

        start_time = root.xpath('//pds:Time_Coordinates/pds:start_date_time', namespaces=ns)
        start_time = pd.to_datetime(start_time[0].text) if len(start_time)>0 else pd.NaT
        stop_time = root.xpath('//pds:Time_Coordinates/pds:stop_date_time', namespaces=ns)
        stop_time = pd.to_datetime(stop_time[0].text) if len(stop_time)>0 else pd.NaT

        index = index.append({
            'filename': label,
            'product_type': product_type,
            'lid': root.xpath('pds:Identification_Area/pds:logical_identifier', namespaces=ns)[0].text,
            'vid': root.xpath('pds:Identification_Area/pds:version_id', namespaces=ns)[0].text,
            'start_time': start_time,
            'stop_time': stop_time
            }, ignore_index=True)

    index['bundle'] = index.lid.apply(lambda x: x.split(':')[3])
    index['collection'] = index.lid.apply(lambda x: x.split(':')[4] if len(x.split(':'))>4 else None)
    index['product_id'] = index.lid.apply(lambda x: x.split(':')[-1])
    # removed before publishing to GitHub as PSA specific
    # index['sc_hk'] = index.apply(lambda row: row.product_id.split('_')[2] if row.product_type=='Product_Observational' else None, axis=1)
    index.start_time=pd.to_datetime(index.start_time).dt.tz_localize(None)
    index.stop_time=pd.to_datetime(index.stop_time).dt.tz_localize(None)

    log.info('{:d} PDS4 labels indexed'.format(len(index)))

    return index



class Database:

    def __init__(self, files=None, directory='.', config_file=default_config):
        
        from yaml.scanner import ScannerError

        # read configuration file (YAML format)
        try:
            f = open(config_file, 'r')
            self.config = yaml.load(f, Loader=yaml.SafeLoader)
            log.debug('configuration file loaded with {:d} templates'.format(len(self.config)-1))
        except FileNotFoundError:
            log.error('config file {:s} not found'.format(config_file))
            return None
        except ScannerError as e:
            log.error('error loading YAML configuration file (error: {:s})'.format(e.problem))
            return None


        # build an initial index
        if files is not None:
            self.index = index_products(directory=directory, pattern=files)

            # initialise the dictionary of tables
            self.dbase = {}

            # build the database according to the config file and indexed
            self.build()

        
    def build(self):

        # group by product types
        prod_types = self.index.product_type.unique()

        for prod_type in prod_types:

            if prod_type not in self.config.keys():
                log.warn('no configuration found for product type {:s}, skipping ingestion'.format(prod_type))
                continue

            rules = self.config[prod_type]

            # see which products in the index match this LID pattern and product type
            for name in rules:

                log.debug('processing {:s} products for rule {:s}'.format(prod_type, name))
                lid = self.config[prod_type][name]['lid']
                prods = self.index[ (self.index.product_type==prod_type) & (self.index.lid.str.contains(lid))]

                if len(prods) == 0:
                    continue

                # create a new dataframe (index as per the product index)
                keywords = self.config[prod_type][name]['keywords'].keys()
                dbase = pd.DataFrame([], columns=keywords, index=prods.index)

                # now we need to parse the file for the listed meta-data
                for idx, product in prods.iterrows():
                    label = etree.parse(product.filename)
                    log.debug('processing label {:s}'.format(product.filename))
                    ns = label.getroot().nsmap.copy() 
                    ns['pds'] = ns.pop(None) 
                    
                    # for each keyword/xpath pair in the config, populate the database
                    for keyword in keywords:
                        path = self.config[prod_type][name]['keywords'][keyword]
                        result = label.xpath(path, namespaces=ns)
                        if len(result)==0:
                            dbase[keyword].loc[idx] = None
                            log.warning('meta-data for keyword {:s} not found in product {:s}'.format(
                                keyword, product.product_id))
                        elif len(result)==1:
                            dbase[keyword].loc[idx] = result[0].text
                        else:
                            dbase[keyword].loc[idx] = [r.text for r in result]

                self.dbase.update({name: dbase})
                log.info('database table {:s} created for {:d} products'.format(name, len(dbase)))

        return 

    def list_tables(self):

        if len(self.dbase) == 0:
            log.warn('no tables found - use build() to create')
        else:
            log.info('{:d} tables found: {:s}'.format(len(self.dbase), ', '.join(self.dbase.keys())))
        return

    def get_table(self, table):

        if table not in self.dbase.keys():
            log.error('table {:s} not found'.format(table))
            return None
        else:
            return self.index.join(self.dbase[table], how='inner')

    def save_dbase(self, filename='database.pkl', directory='.'):
        
        pkl_f = open(os.path.join(directory, filename), 'wb')
        pickle.dump((self.index, self.dbase), file=pkl_f, protocol=pickle.HIGHEST_PROTOCOL)

    def load_dbase(self, filename='database.pkl'):

        f = open(filename, 'rb')
        self.index, self.dbase = pickle.load(f)
        self.list_tables()




class pds4_df(pd.DataFrame):
    """
    Sub-class of pd.DataFrame adding extra meta-data:
    -- filename
    -- path
    """

    _metadata = ['filename', 'path']

    @property
    def _constructor(self):
        return pds4_df


def read_tables(label, label_directory='.', recursive=False, table_name=None, index_col=None, add_filename=False, quiet=False):
    """
    Accepts a directory and file-pattern or list and attempts to load the specified table
    (or first table, if none is specified) into a merged DataFrane. If the tables 
    have different columns, tables will be merged.

    index_col= can be used to specify the column which will be used as an index to the frame.
    add_filename= will add a new column including the label filename if True - this can be 
        useful to distinguish between otherwise identical records from several products.
    """

    if recursive:
        selectfiles = select_files(label, directory=label_directory, recursive=recursive)
        file_list = [file for file in selectfiles]
    elif type(label) == list:
        if label_directory != '.':
            file_list = [os.path.join(label_directory, file) for file in label]
        else:
            file_list = label
    else:
        import glob
        file_list = glob.glob(os.path.join(label_directory, label))

    if len(file_list) == 0:
        log.warning('no files found matching pattern {:s}'.format(label))
        return None

    table = None

    # de-dupe list
    file_list = list(set(file_list))

    handler.addFilter(DuplicateFilter())
    filter_inst = log.handlers[0].filters[-1]

    for f in file_list:
        if table is None:
            table = read_table(f, table_name=table_name, index_col=index_col, quiet=quiet)
            if add_filename:
                table['filename'] = table.filename
        else:
            temp_table = read_table(f, table_name=table_name, index_col=index_col, quiet=quiet)
            if add_filename:
                temp_table['filename'] = temp_table.filename
            table = table.append(temp_table)

    handler.removeFilter(filter_inst)

    table.sort_index(inplace=True)

    log.info('{:d} files read with {:d} total records'.format(len(file_list), len(table)))

    return table




def read_table(label_file, table_name=None, index_col=None, quiet=True):
    """
    Reads data from a PDS4 product using pds4_tools. Data are
    converted to a Pandas DataFrame and any columns that are
    using PDS4 time data types are converted to Timestamps.

    By default the first table is read, otherwise the
    table_name can be used to specify.

    If index_col is set, this field will be used as an index in 
    the returned pandas DataFrame, otherwise if a time field
    is present this will be used.

    NOTE: only simple 2D tables can currently be read. Group
    fields are skipped with a warning message!
    """

    data = pds4_read(label_file, quiet=True)

    num_arrays = 0
    tables = []

    for structure in data.structures:
        if structure.is_array():
            num_arrays += 1
        elif structure.is_table():
            tables.append(structure.id)

    if len(tables) == 0:
        log.error('no tables found in this product')
        return None

    if not quiet:
        log.info('product has {:d} tables and {:d} arrays'.format(len(tables), num_arrays))

    if table_name is not None:
        if table_name in tables:
            table = data[table_name]
        else:
            log.error('table name {:s} not found in product'.format(table_name))
            return None
    else:
        table = data[tables[0]]
    
    if not quiet:
        log.info('using table {:s}'.format(table.id))

    # clunky way to get the names of group fields to ignore for now
    table_manifest = TableManifest.from_label(data[table.id].label)

    time_cols = []
    fields = []
    group_fields = []

    for i in range(len(table_manifest)):
        if table_manifest[i].is_group():
            continue
        name = table_manifest[i].full_name()
        if table_manifest.get_parent_by_idx(i):
            group_fields.append(table_manifest[i].full_name())
            continue
        fields.append(name)

        data_type = table_manifest[i]['data_type']
        if 'Date' in data_type:
           time_cols.append(name)

        # TODO: fix nested tables (group fields)
        # TODO: fix handling of masked arrays (in particular missing vals in CSVs trigger this)

    data = pds4_df(table.data, columns=fields)
    for field in fields:
        data[field] = table.data[field]

    for group_field in group_fields:
        field_name = group_field.split(',')[1].strip()
        field_data = table[group_field]
        if field_data.shape[0] != len(data):
            log.warn('group field length does not match table length - skipping!')
            continue
        data[field_name] = None
        for idx in range(len(data)):
            data[field_name].iat[idx] = field_data[idx]

    path, filename = os.path.split(label_file)
    data.path = path
    data.filename = filename

    for col in time_cols:
        data[col] = pd.to_datetime(data[col]).dt.tz_localize(None)
    
    if index_col is not None:
        if index_col in fields:
            data.set_index(index_col, drop=True, inplace=True)        
        else:
            log.warn('requested index field {:s} not found'.format(index_col))
            index_col=None

    if index_col is None:
        if len(time_cols)==0:
            log.warning('no time-based columns found, returned data will not be time-indexed')
        elif len(time_cols)==1:
            data.set_index(time_cols[0], drop=True, inplace=True)
            log.debug('data time indexed with field {:s}'.format(time_cols[0]))
        else:
            if 'TIME_UTC' in data.columns:
                data.set_index('TIME_UTC', drop=True, inplace=True)
                log.debug('data time indexed with field {:s}'.format(time_cols[0]))
            else:
                data.set_index(time_cols[0], drop=True, inplace=True)
                log.debug('data time indexed with field {:s}'.format(time_cols[0]))

    return data




def select_files(wildcard, directory='.', recursive=False):
    """Create a file list from a directory and wildcard - recusively if
    recursive=True"""

    # recursive search
    # result = [os.path.join(dp, f) for dp, dn, filenames in os.walk('.') for
    # f in filenames if os.path.splitext(f)[1] == '.DAT']

    if recursive:
        selectfiles = locate(wildcard, directory)
        filelist = [file for file in selectfiles]
    else:
        import glob
        filelist = glob.glob(os.path.join(directory, wildcard))

    filelist.sort()

    return filelist


def locate(pattern, root_path):
    """Returns a generator using os.walk and fnmatch to recursively
    match files with pattern under root_path"""

    import fnmatch

    for path, dirs, files in os.walk(os.path.abspath(root_path)):
        for filename in fnmatch.filter(files, pattern):
            yield os.path.join(path, filename)
