#!/usr/bin/python
"""
read.py
"""

from . import common
import os
from pathlib import Path
import pandas as pd
from pds4_tools import pds4_read
from pds4_tools.reader.table_objects import TableManifest

# only show warning or higher messages from PDS4 tools
import logging
pds4_logger = logging.getLogger('PDS4ToolsLogger')
pds4_logger.setLevel(logging.WARNING)

log = logging.getLogger(__name__)

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

    if quiet:
        logging.disable(logging.INFO)
    else:
        logging.disable(logging.NOTSET)

    if recursive:
        selectfiles = common.select_files(label, directory=label_directory, recursive=recursive)
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

    for f in file_list:
        if table is None:
            table = read_table(f, table_name=table_name, index_col=index_col, quiet=quiet)
            cols = table.columns
            if add_filename:
                table['filename'] = table.filename
        else:
            temp_table = read_table(f, table_name=table_name, index_col=index_col, quiet=quiet)
            if temp_table is None:
                continue
            if set(temp_table.columns) != set(cols):
                log.warning('product has different columns names, skipping')
                continue
            if add_filename:
                temp_table['filename'] = temp_table.filename
            # table = table.append(temp_table)
            table = pd.concat([table, temp_table], axis=0, join='inner')

    table.sort_index(inplace=True)

    logging.disable(logging.NOTSET)

    log.info('{:d} files read with {:d} total records'.format(len(file_list), len(table)))

    return table




def read_table(label_file, table_name=None, index_col=None, quiet=True):
    """
    Reads data from a PDS4 product using pds4_tools. Data are
    converted to a Pandas DataFrame and any columns that are
    using PDS4 time data types are converted to Timestamps.

    By default the first table is read, otherwise the
    table can be used to specify either the 0-based index of
    the table or the name (string).

    If index_col is set, this field will be used as an index in 
    the returned pandas DataFrame, otherwise if a time field
    is present this will be used.

    NOTE: only one level of group fields are handled here
    """

    if quiet:
        logging.disable(logging.INFO)
    else:
        logging.disable(logging.NOTSET)

    data = pds4_read(label_file, lazy_load=True, quiet=True)
    labelpath = Path(label_file)

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

    log.info('product {:s} has {:d} tables and {:d} arrays'.format(labelpath.name, len(tables), num_arrays))

    if table_name is not None:
        if isinstance(table_name, str):
            if table_name in tables:
                table = data[table_name]
            else:
                log.error('table name {:s} not found in product'.format(table_name))
                return None
        elif isinstance(table_name, int):
            if table_name <= len(tables):
                table = data[table_name]
            else:
                log.error('table number {:d} not found in product'.format(table_name))
                return None
    else:
        table = data[tables[0]]
    
    log.info('using table {:s}'.format(table.id))

    # clunky way to get the names of group fields
    table_manifest = TableManifest.from_label(data[table_name].label)

    time_cols = []
    fields = []
    group_fields = []

    # Work around https://github.com/Small-Bodies-Node/pds4_tools/issues/68
    # Rename field names with colons to have underscores instead

    for i in range(len(table_manifest)):
        if table_manifest[i].is_group():
            continue
        name = table_manifest[i].full_name().replace(':', '_')
        if table_manifest.get_parent_by_idx(i):
            group_fields.append(table_manifest[i].full_name().replace(':', '_'))
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
            log.info('data indexed with field {:s}'.format(time_cols[0]))   
        else:
            log.warn('requested index field {:s} not found'.format(index_col))
            index_col=None

    if index_col is None:
        if len(time_cols)==0:
            log.warning('no time-based columns found, returned data will not be time-indexed')
        elif len(time_cols)==1:
            data.set_index(time_cols[0], drop=True, inplace=True)
            log.info('data time-indexed with field {:s}'.format(time_cols[0]))
        else:
            if 'TIME_UTC' in data.columns:
                data.set_index('TIME_UTC', drop=True, inplace=True)
                log.info('data time-indexed with field {:s}'.format(time_cols[0]))
            else:
                data.set_index(time_cols[0], drop=True, inplace=True)
                log.info('data time-indexed with field {:s}'.format(time_cols[0]))

    logging.disable(logging.NOTSET)

    return data


