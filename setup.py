#!/usr/bin/env python

from distutils.core import setup

VERSION = '1.1.0'

cls_txt = \
"""
Development Status :: 4 - Beta
Intended Audience :: Developers
Intended Audience :: Information Technology
Intended Audience :: Science/Research
License :: OSI Approved :: GNU General Public License v2 (GPLv2)
Programming Language :: Python
Topic :: Scientific/Engineering :: Atmospheric Science
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: POSIX :: Linux
Operating System :: MacOS :: MacOS X
"""

short_desc = "Python Sciamachy read and sqlite3 library"
long_desc = \
"""
The pynadc package provides (limited) access to Sciamachy level 0 and 1b 
products from Python. Access is restricted to product headers and (G)ADS
datasets within the products. Implementation to access to DSR's is not 
foreseen.

The scripts are intended for SRON internal usage.
"""

setup( 
    name = 'pynadc',
    version = VERSION,
    description = short_desc,
    long_description = long_desc,
    classifiers = [x for x in cls_txt.split("\n") if x],
    author = 'Richard van Hees',
    author_email = 'rm dot vanhees at gmail dot com',
    maintainer = 'Richard van Hees',
    maintainer_email = 'rm dot vanhees at gmail dot com',
    download_url = 'https://github.com/rmvanhees/pynadc.git',
    packages=['scia'],
    package_dir={'scia': 'src/scia'},
    scripts=['scripts/inquire_db2.py', 'scripts/sdmf_calibSMR.py',
             'scripts/scia_lv0.py', 'scripts/scia_lv1.py',
             'scripts/collect_stateDefs.py']
)
