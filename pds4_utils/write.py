#!/usr/bin/python
"""
write.py
"""

from . import dbase
from . import common
from . import read
import os
from pathlib import Path
import pandas as pd
from lxml import etree
from pathlib import Path
# from pds4_tools import pds4_read

import logging
log = logging.getLogger(__name__)

def generate_collection(template, directory='.', pattern='*.xml', recursive=True):
    """Generates or updates a collection inventory from a set of files and a label template. 
    This can be an existing collection label. If the label points to an existing inventory
    any secondary members will be retained and any primary members will be replaced with the
    real contents of the specified collection."""

    index = dbase.index_products(directory, pattern, recursive)
    if len(index)==0:
        log.error('no valid products found')
        return None

    if len(index.bundle.unique()) > 1:
        log.error('cannot mix products from multiple bundles')
        return None

    if len(index.collection.unique()) > 1:
        log.error('cannot mix products from multiple collections')
        return None
    
    collection_name = index.collection.unique()[0]
    collection_csv = 'collection_{:s}.csv'.format(collection_name)
    collection_xml = 'collection_{:s}.xml'.format(collection_name)
    
    # remove any collection and bundle inventories from the product list
    bad_types = ['Product_Collection', 'Product_Bundle']
    bad_idx = bad = index[index.product_type.isin(bad_types)].index
    index.drop(bad_idx, inplace=True)

    # check for the template
    if not Path(template).exists():
        log.error('could not open template file {:s}'.format(template.name))
        return None
    else:
        tree = etree.parse(template)
        root = tree.getroot()
        ns = root.nsmap
        if None in ns and common.pds_ns == ns[None]:
            ns['pds'] = ns.pop(None)

    # read the existing collection inventory, if any
    inventory = read.read_table(template)
    secondary = inventory[inventory.iloc[:,0]=='S'] # regardless of name

    # add LID+VID => LIDVID
    index['lidvid'] = index.apply(lambda row: '{:s}::{:s}'.format(row.lid, row.vid), axis=1)

    # add a column for primary/secondary - all those products in this collection are primary
    index['primary'] = 'P'
    index = index[['primary','lidvid']]

    # merge secondary, if any, coming from the template
    final = pd.concat([index, secondary.rename(columns={'Member Status':'primary','LIDVID_LID':'lidvid'})], ignore_index=True)


    # write the CSV
    csv_file = os.path.join(directory, collection_csv)
    final.to_csv(csv_file, header=False, index=False, lineterminator='\r\n')

    # check if the template is a Product_Collection
    if root.tag != '{http://pds.nasa.gov/pds4/pds/v1}Product_Collection':
        log.error('template is not a Product_Collection')
        return None

    def set_if_exists(root, xpath, value):
        if len(root.xpath(xpath, namespaces=ns))==1:
            el = root.xpath(xpath, namespaces=ns)[0]
            el.text = str(value)

    # write the filename, checksum and number of records into the template
    set_if_exists(root, '/pds:Product_Collection/pds:File_Area_Inventory/pds:Inventory/pds:records', len(index))
    set_if_exists(root, '/pds:Product_Collection/pds:File_Area_Inventory/pds:File/pds:records', len(index))
    set_if_exists(root, '/pds:Product_Collection/pds:File_Area_Inventory/pds:File/pds:file_name', collection_csv)
    set_if_exists(root, '/pds:Product_Collection/pds:File_Area_Inventory/pds:File/pds:md5_checksum', common.md5_hash(csv_file))
    set_if_exists(root, '/pds:Product_Collection/pds:File_Area_Inventory/pds:File/pds:file_size', os.path.getsize(csv_file))

    tree.write(collection_xml, xml_declaration=True, encoding=tree.docinfo.encoding) 

    return 



