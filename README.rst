pyNADC
======
The pyNADC package contains the following tools and python modules:


Access to Sciamachy data-products
---------------------------------
* Fast read access to Sciamachy (ENVISAT) level 0 (format PDS/ESA). The
   software attempt to read all data and offers convenient access to the
   parameter through structured Numpy arrays.
   The script 'scia_lv0.py' offers a guideline of how to use the module
   'pynadc.scia.lv0'.
* Fast read access to Sciamachy (ENVISAT) level 1b (format PDS/ESA). The
   software attempt to read all data and offers convenient access to the
   parameter through structured Numpy arrays.
   Both Science and calibration datasets can be read into memory, however,
   currently calibration of the science data is not included.
   The script 'scia_lv1.py' offers a guideline of how to use the module
   'pynadc.scia.lv1'.
* The 'collect_scia_states.py' can be used to build a database with all
   instrument configurations used during the mission of Sciamachy. This requires
   that the complete Sciamachy level 1b archive is available.
   Definitions of OCR states are not included in the level 1b products, and are
   therefore hard-coded in the software. These could be obtained from level 0
   products, but this is not yet implemented.


Access to GOSAT and Sciamachy (local) archives
----------------------------------------------
Databases of files in your archives, can be very handy when you want to query on
presence, completeness, date of archiving, double entries, etc. Specially, when
you are collecting data during a satellite mission. The package pyNADC contains
software to create SQLite databases of archives with GOSAT FTS level 1, GOSAT
CAI level 2 and Sciamachy level 0, 1b and 2 (PDS/ESA format). And tools to query
these databases.


Installation
------------
* python3 setup.py install


Websites
--------
* Source code: http://github.com/rmvanhees/pynadc
* SRON Netherlands Institute for Space Research: https://www.sron.nl/
* Sciamachy:
  https://earth.esa.int/web/guest/missions/esa-operational-eo-missions/envisat/instruments/sciamachy
* GOSAT: http://global.jaxa.jp/projects/sat/gosat


Disclaimer
----------
The software is developed for SRON internal usage, but generously shared without
any warranty.


Reporing bugs
-------------
Issues and bugs should be reported to <r.m.van.hees at sron.nl>
