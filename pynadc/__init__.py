# This file is part of pynadc
#
# https://github.com/rmvanhees/pynadc
#
# This is the main Python package for all Python PyNADC software
#
# The package is subdivided into subpackages and modules as follows:
#
#   pynadc
#      gosat       # contains GOSAT modules
#      scia        # contains Sciamachy modules
#
# For details on the available modules inside a subpackage, see the subpackage
# documentation.
# For details on the modules, see the documentation of the corresponding module.
#
# Copyright (c) 2012--2019 SRON - Netherlands Institute for Space Research
#   All Rights Reserved
#
# License:  BSD-3-Clause
"""
"""

from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    pass
