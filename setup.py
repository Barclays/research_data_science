from setuptools import setup, find_packages
from importlib import import_module
from os import path
import warnings


pckg_name = 'ctrlaltdata'
cwd = path.dirname(path.abspath(__file__))

with open(path.join(cwd, 'README.md')) as fh:
    long_description = fh.read()

with open(path.join(cwd, 'requirements.txt')) as fh:
    install_requires = fh.read().splitlines()

setup(name=pckg_name,
      version='0.1',
      packages=find_packages(),
      test_suite='tests',
      long_description=long_description,
      long_description_content_type='text/markdown',
      install_requires=install_requires
      )

warnings.warn(f"Setup the configuration file at: {path.join(import_module(pckg_name).__path__[0], 'config.py')}!")
