#!/usr/bin/env python

from distutils.core import setup
from glob import glob

setup(
    name='htcondenser',
    version='0.2.0',
    description="htcondenser is a simple library for submitting"
    " simple jobs & DAGs on the Bristol machines.",
    author='Robin Aggleton',
    author_email='',
    url='https://github.com/raggleton/htcondenser',
    packages=['htcondenser'],
    scripts=glob('bin/*'),
    package_data={'htcondenser': ['templates/*']},
    include_package_data=True,
)
