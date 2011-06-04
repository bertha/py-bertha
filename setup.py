#!/usr/bin/env python

from setuptools import setup
from get_git_version import get_git_version

setup(name='bertha',
      version=get_git_version(),
      description='Python client library and console util for BerthaD',
      author='Bas Westerbaan',
      author_email='bas@westerbaan.name',
      url='http://github.com/bwesterb/py-bertha/',
      packages=['bertha'],
      zip_safe=True,
      package_dir={'bertha': 'src'},
      install_requires = ['docutils>=0.3'],
      entry_points = {
              'console_scripts': [
                      'bertha = bertha.main:main',
              ]
      }
      )
