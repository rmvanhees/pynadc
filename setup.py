#!/usr/bin/env python

from distutils.core import setup

setup( name='pynadc',
       version='1.1',
       description='Python Sciamachy read and sqlite3 library',
       author='Richard van Hees',
       author_email='rm.vanhees@gmail.com',
       packages=['scia'],
       package_dir={'scia': 'src/scia'},
       scripts=['scripts/inquire_db2.py', 'scripts/sdmf_calibSMR.py',
                'scripts/scia_lv0.py', 'scripts/scia_lv1.py',
                'scripts/collect_stateDefs.py']
       )
