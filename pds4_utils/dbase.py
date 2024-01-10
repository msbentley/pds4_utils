#!/usr/bin/python
"""
dbase.py
"""

try:
   import cPickle as pickle
except:
   import pickle

from . import common

import yaml
from lxml import etree
import pandas as pd
from pathlib import Path
import os

import logging
log = logging.getLogger(__name__)


default_config = os.path.join(
    os.environ.get('APPDATA') or
    os.environ.get('XDG_CONFIG_HOME') or
    os.path.join(os.environ['HOME'], '.config'),
    "pds_dbase.yml")


def index_products(directory='.', pattern='*.xml', recursive=True):
    """
    Accepts a directory containing PDS4 products, indexes the labels and returns a 
    Pandas data-frame containng meta-data for each product.
    """

    from lxml import etree
    import pandas as pd

    # recursively find all labels
    labels = common.select_files(pattern, directory=directory, recursive=recursive)

    cols = ['filename', 'product_type', 'lid', 'vid', 'start_time', 'stop_time']
    # index = pd.DataFrame([], columns=cols)
    index = []

    for label in labels:
        
        #  get the root element
        root = etree.parse(label).getroot()

        # returns the namespace mapping
        ns = root.nsmap

        # XPath doesn't like "default" namespace (nsmap return None)
        # so if present, we replace here with the key pds
        if None in ns and common.pds_ns == ns[None]:
            ns['pds'] = ns.pop(None)

        product_type = root.xpath('name(/*)')#, namespaces=ns)
        if not product_type.startswith('Product_'):
            log.warn('XML file {:s} is not a PDS4 label, skipping'.format(Path(label).name))
            continue

        start_time = root.xpath('//pds:Time_Coordinates/pds:start_date_time', namespaces=ns)
        start_time = pd.to_datetime(start_time[0].text) if len(start_time)>0 else pd.NaT
        stop_time = root.xpath('//pds:Time_Coordinates/pds:stop_date_time', namespaces=ns)
        stop_time = pd.to_datetime(stop_time[0].text) if len(stop_time)>0 else pd.NaT

        meta = {
            'filename': label,
            'product_type': product_type,
            'lid': root.xpath('pds:Identification_Area/pds:logical_identifier', namespaces=ns)[0].text,
            'vid': root.xpath('pds:Identification_Area/pds:version_id', namespaces=ns)[0].text,
            'start_time': start_time,
            'stop_time': stop_time
            }

        index.append(meta)

    index = pd.DataFrame(index, columns=cols)

    index['bundle'] = index.lid.apply(lambda x: x.split(':')[3])
    index['collection'] = index.lid.apply(lambda x: x.split(':')[4] if len(x.split(':'))>4 else None)
    index['product_id'] = index.lid.apply(lambda x: x.split(':')[-1])

    # make sure timestamps are stripped of timezones
    index.start_time=pd.to_datetime(index.start_time.fillna(pd.NaT)).dt.tz_localize(None)
    index.stop_time=pd.to_datetime(index.stop_time.fillna(pd.NaT)).dt.tz_localize(None)

    log.info('{:d} PDS4 labels indexed'.format(len(index)))

    return index

class Database:

    def __init__(self, files=None, directory='.', config_file=default_config, recursive=True):
        
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
            self.index = index_products(directory=directory, pattern=files, recursive=recursive)
        else:
            self.index = index_products(directory=directory, pattern='*.xml', recursive=recursive)
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
                        try:
                            result = label.xpath(path, namespaces=ns)
                        except etree.XPathEvalError:
                            log.warn('could not evaluate xpath: {:s}'.format(path))
                            continue
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

