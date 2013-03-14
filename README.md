python-lsf
==========

Python API for the LSF batch job scheduler

Installation
------------

    python setup.py install

If you are missing the right to write to the system wide python directories,
use

    python setup.py install --user

to install the package in ~/.local. You might then have to

    export PATH=$PATH:~/.local/bin

in order to make the scripts available on the command line.

Usage
-----

ejobs and ehosts has essentially the same interface as LFS' bjobs and bhosts.
Check

    ejobs -h
    ehosts -h
    man bjobs
    man bhosts
