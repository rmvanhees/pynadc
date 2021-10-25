pynadc
======

The pynadc package contains the following tools and Python modules:


Access to Sciamachy data-products
---------------------------------

* Fast read access to Sciamachy (ENVISAT) level 0
   The software attempt to read all data and offers convenient access to the
   parameter through structured numpy arrays.
   The script *scia_lv0.py* offers a guideline of how to use the module
   *pynadc.scia.lv0*. Products compressed by gzip can be read directly.
* Fast read access to Sciamachy (ENVISAT) level 1b
   The software attempt to read all data and offers convenient access to the
   parameter through structured numpy arrays.
   Both Science and calibration datasets can be read into memory, however,
   currently calibration of the science data is not included.
   The script *scia_lv1.py* offers a guideline of how to use the module
   *pynadc.scia.lv1*.  Products compressed by gzip can be read directly.
* Build a database with Sciamachy instrument settings
   The script *collect_scia_states.py* can be used to build a database with all
   instrument configurations used during the mission of Sciamachy. This requires
   that the complete Sciamachy level 1b archive is available.
   Definitions of OCR states are not included in the level 1b products, and are
   therefore hard-coded in the software. These could be obtained from level 0
   products, but this is not yet implemented.

The Sciamachy level 0 and level 1 (up to versions 8) are in a special ENVISAT
format which require dedicated reading S/W tools. The ENVISAT format is a
simple byte stream with a header containing byte offsets that allows the
extraction of specific datasets. While this type of data format is very space
efficient (all data bytes are just written one after the other with a small
ASCII header), it is not future proof (dedicated S/W is needed) nor self
descriptive. The latest level 1 products (from version 9 and up) are provided
in netCDF4 format. This format is self-descriptive and can be read by widely
available standard reading tools that are provided in all major programming
languages.


Access to GOSAT and Sciamachy (local) archives
----------------------------------------------

Databases of files in your archives, can be very handy when you want to query on
presence, completeness, date of archiving, double entries, etc. Specially, when
you are collecting data during a satellite mission. The package pynadc contains
software to create SQLite databases of archives with GOSAT FTS level 1, GOSAT
CAI level 2 and Sciamachy level 0, 1b and 2 (PDS/ESA format). And tools to query
these databases.


Installation
------------

Installation instructions are provided in the INSTALL file.

We installed the software succesfully on Linux and MacOS.


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

Issues and bugs should be reported to <r.m.van.hees[at]sron.nl>
