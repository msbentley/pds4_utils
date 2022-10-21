#!/usr/bin/env python
# encoding: utf-8
"""
__init__.py

"""
__all__ = ['common', 'read', 'write', 'dbase']

# Set up the nullhandler - user applications can configure how to
# display log messages etc.
import logging
import sys
# logging.getLogger(__name__).addHandler(logging.NullHandler())

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

logformat = format='%(levelname)s %(asctime)s (%(name)s): %(message)s'
stream = logging.StreamHandler(sys.stdout)
stream.setFormatter(logging.Formatter(logformat))
stream.setLevel(logging.INFO)
# stream.propagate = False
log.addHandler(stream)
