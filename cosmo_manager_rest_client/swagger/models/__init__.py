#!/usr/bin/env python
"""Add all of the modules in the current directory to __all__"""
import os
import sys

__all__ = []

if getattr(sys, 'frozen', None):
    basedir = sys._MEIPASS + '/swagger'
else:
    basedir = os.path.dirname(__file__)

for module in os.listdir(basedir):
    if module != '__init__.py' and module[-3:] == '.py':
        __all__.append(module[:-3])
