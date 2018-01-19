#!/usr/bin/env python

import setuptools

setuptools.setup(
    name='pywandio',
    version='0.1',
    description='High-level file IO library',
    url='https://github.com/CAIDA/pywandio',
    author='Alistair King, Chiara Orsini',
    author_email='bgpstream-info@caida.org',
    packages=setuptools.find_packages(),
    install_requires=[
        'python-keystoneclient', 'python-swiftclient',
    ],
    entry_points={'console_scripts': [
        'pywandio-cat = wandio.wandio:main'
    ]}
)
