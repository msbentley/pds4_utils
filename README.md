# pds4_utils
Utilities for working with NASA Planetary Data System v4 (PDS4) data files

## Dependencies

The following dependencies must be met:
- python 3
- pandas
- pyyaml
- lxml
- PDS4 tools

## Installation

First, clone this repository. If you are using conda, the dependencies can be installed in a new environment using the provided environment file:

```conda env create -f environment.yml```

The newly created environment can be activated with:

```
conda activate pds4utils
```

Otherwise, please make sure the dependencies are installed with your system package manager, or a tool like `pip`. Use of a conda environment or virtualenv is recommended!

The package can then be installed with:

```python setup.py install```

## Contents

The module contains a few simple functions and a class. A brief overview is given here:

`read_table`
- reads 2D tables from PDS4 products
- one level of group fields are supported
- returns a pandas dataframe
  - group field data are returned as an array in each pandas cell
  - if `table_name` is not given, the first table is returned
  - the DataFrame is indexed by the first time field, if any
    - this can be set using the `index_col` parameter


`read_tables`
- reads multiple tables using `read_table`
- useful for building a large dataframe from many similar data products
- set `add_filename=True` to add the product name to each row, to track which product the data came from


`index_products(directory='.', pattern='*.xml')`
- searches for PDS4 labels recursively in `directory` matching `pattern`
- returns a pandas DataFrame with one row per product
- returned data include:
  - LID + VID
  - bundle, collection and product identifier
  - start and stop time, if present


`Database`
- this class builds one or more DataFrames containing custom meta-data from a set of PDS4 products
- a YAML formatted configuration file is required to determine which attributes to read
  - the Xpath to each attribute must be known
  - see [example.yml](https://nbviewer.jupyter.org/github/msbentley/pds4_utils/blob/master/example.yml) for more information
  - if no config file is specified when instantiating the class, a default is looked for
    - `pds_dbase.yml` in the user's home directory, or pointed to by `APPDATA` or `XDG_CONFIG_HOME`
- each entry in the configuration file produces one database table (one Pandas dataframe)
  - to see which tables have been loaded, use `list_tables()`
  - to return a table, use `get_table(table)`
  - to save or restore a database using `save_dbase()` or `load_dbase()`


## Example

The Jupyter notebook included with this repository shows an example of pds4_utils in use. To view the notebook, click [here](https://nbviewer.jupyter.org/github/msbentley/pds4_utils/blob/master/pds4_utils_example.ipynb).
