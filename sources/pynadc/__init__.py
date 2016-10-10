'''
This file is part of pynadc

https://github.com/rmvanhees/pynadc

This is the main Python package for all Python PyNADC software

The package is subdivided into subpackages and modules as follows:

   pynadc
      gosat       # contains GOSAT modules
      scia        # contains Sciamachy modules
      tropomi     # contains Tropomi modules

For details on the available modules inside a subpackage, see the subpackage
documentation.
For details on the modules, see the documentation of the corresponding module.

Copyright (c) 2012-2016 SRON - Netherlands Institute for Space Research 
   All Rights Reserved

License:  Standard 3-clause BSD

'''
__all__ = ['version', 'stats', 'extendedrainbow_with_outliers',
           'gosat', 'scia', 'tropomi']
