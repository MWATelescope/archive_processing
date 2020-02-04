#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name='archive_processing',
      version='1.0.0',
      description='MWA Archive Processing Utilities',
      author='Greg Sleap',
      author_email='operatuibs@mwatelescope.org',
      url='http://www.mwatelescope.org',
      packages=find_packages(),
      entry_points={'console_scripts': ['test_processor = scripts.test_processor:main']})
