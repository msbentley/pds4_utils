#!/usr/bin/python
"""
common.py
"""

import os
import logging
log = logging.getLogger(__name__)


pds_ns = 'http://pds.nasa.gov/pds4/pds/v1'


def md5_hash(filename):

    import hashlib

    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    with open(filename, 'rb') as f:
        buf = f.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(BLOCKSIZE)
    return hasher.hexdigest()


def select_files(wildcard, directory='.', recursive=False):
    """Create a file list from a directory and wildcard - recursively if
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
