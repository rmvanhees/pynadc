from setuptools import setup


def readme():
    with open('README.rst') as fp:
        return fp.read()


setup( 
    name = 'pynadc',
    description='Python Sciamachy/GOSAT/Tropomi read and SQLite3 library',
    long_description=readme(),
    use_scm_version={"root": ".", "relative_to": __file__},
    setup_requires=['setuptools_scm'],
    classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: BSD License (standard 3-clause)',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Topic :: Scientific/Engineering :: Atmospheric Science',
    ],
    url='https://github.com/rmvanhees/pynadc',
    author='Richard van Hees',
    author_email='r.m.van.hees@sron.nl',
    maintainer='Richard van Hees',
    maintainer_email='r.m.van.hees@sron.nl',
    license='BSD-3-Clause',
    packages=[
        'pynadc',
        'pynadc.gosat',
        'pynadc.gosat2',
        'pynadc.scia'
    ],
    scripts=[
        'scripts/add_entry_gosat.py',
        'scripts/add_entry_gosat2.py',
        'scripts/add_entry_scia.py',
        'scripts/collect_scia_states.py',
        'scripts/inquire_gosat.py',
        'scripts/inquire_gosat2.py',
        'scripts/inquire_scia.py',
        'scripts/scia_lv0.py',
        'scripts/scia_lv1.py'
    ],
    install_requires=[
        'bitstring>=3.1',
        'numpy>=1.16',
        'h5py>=2.9'
    ],
    zip_safe=False
)
