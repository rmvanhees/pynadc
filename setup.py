#!/usr/bin/env python
import subprocess
from distutils.core import setup

try:
    version_git = subprocess.check_output(["git", "describe"])
    VERSION = str(version_git.decode('ascii').rstrip())[1:]
except:
    VERSION = '1.1.4-no_git'

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

short_desc = "Python Sciamachy/GOSAT/Tropomi read and SQLite3 library"
long_desc = \
"""
The pynadc package provides (limited) read access to Sciamachy level 0 and 1b 
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
    package_dir={'': 'sources'},
    packages=[ 'pynadc',
               'pynadc.gosat',
               'pynadc.scia',
               'pynadc.tropomi' ],
    scripts=[ 'scripts/add_entry_gosat.py',
              'scripts/add_entry_scia.py',
              'scripts/add_entry_s5p.py',
              'scripts/collect_stateDefs.py',
              'scripts/inquire_db.py',
              'scripts/inquire_gosat.py',
              'scripts/inquire_scia.py',
              'scripts/inquire_s5p.py',
              'scripts/scia_lv0.py',
              'scripts/scia_lv1.py',
              'scripts/sdmf_calibSMR.py' ]
)
