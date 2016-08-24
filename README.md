pynadc
======

The pynadc package contains software to build SQLite databases to search you
archives with products of several satellite instruments designed spectral
atmospheric observations. The following products are supported:
 * Sciamachy (ENVISAT) :  level 0 and 1b products
 * TANSO (GOSAT) : FTS level 1b & CAI level 2
 * Tropomi (S5p) : Tropomi ICM products

In addition, the pynadc package provides read access to several satellite
instruments designed spectral atmospheric observations:
 * Sciamachy (ENVISAT) :  limited to Sciamachy level 0 and 1b products
      Read access is restricted to product headers and (G)ADS within these
      products. Implementation to access to DSR's is not foreseen.
 * Tropomi (S5p) :  limited to Tropomi ICM products

The software is intended for SRON internal usage.
