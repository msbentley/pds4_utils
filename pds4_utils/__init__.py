#!/usr/bin/env python
# encoding: utf-8
"""
__init__.py

"""
__all__ = ['common', 'read', 'write', 'dbase']

# Set up the nullhandler - user applications can configure how to
# display log messages etc.
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

