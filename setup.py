#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'microns_dashboard_api', 'version.py')) as f:
    exec(f.read())

with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().split()

setup(
    name='microns-dashboard',
    version=__version__,
    description='Schemas and notebooks for the MICrONS Jupyterhub Dashboard',
    author='Stelios Papadopoulos, Christos Papadopoulos',
    author_email='spapadop@bcm.edu, cpapadop@bcm.edu',
    packages=find_packages(exclude=[]),
    install_requires=requirements
)