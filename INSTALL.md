Installing pynadc
=================

Install from source
-------------------

The latest release of pynadc is available from
[gitHub](https://github.com/rmvanhees/pynadc).
Where you can download the source code as a tar-file or zipped archive.
Or you can use git do download the repository:

    git clone https://github.com/rmvanhees/pynadc.git

Before you can install pynadREc, you need:

 * Python version 3.7+ with development headers
 * HDF5 version 1.10+ with development headers

And have the following Python modules available:

 * setuptools v57+
 * setuptools-scm v6+
 * bitstring v3.1+
 * numpy v1.19+
 * h5py v3.40+

You can install pySpexCal once you have satisfied the requirements listed above.
Run at the top of the source tree:

    python3 -m build  
    pip3 install dist/pynadc-<version>.whl [--user]

The Python scripts can be found under `/usr/local/bin` or `$USER/.local/bin`.
