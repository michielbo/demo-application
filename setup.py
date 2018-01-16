#!/usr/bin/env python3

from distutils.core import setup
from setuptools import find_packages

setup(name='App',
      version='1.0',
      description='Alternate Presentation Program',
      author='Wouter De Borger',
      author_email='wouter.deborger@inmanta.com',
      package_dir={"": "src"},
      packages=find_packages("src"),
      install_requires=["tornado"]
      )
