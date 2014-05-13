python-lsf
==========

Python API for the LSF batch job scheduler


Requirements
------------
* bjobs, bhosts, bsub, and lshosts available from the command line
* python version 2.7.x
* (developed with LSF version 9.1.2.0)


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

ejobs and ehosts have essentially the same interfaces as LSF' bjobs and bhosts.
Check

    ejobs -h
    ehosts -h
    esub -h
    man bjobs
    man bhosts
    man bsub
