python-lsf
==========

Python API for the LSF batch job scheduler


Requirements
------------

* python version 2.7.x
* LSF version 9.1.2.0
* bjobs, bhosts, bsub, and lshosts available


Installation
------------

    python setup.py install

If you are missing the right to write to the system wide python directories,
use

    python setup.py install --user

to install the package in ~/.local.  You might then have to

    export PATH=$PATH:~/.local/bin

in order to make the scripts available on the command line.


Usage
-----

ejobs, ehosts, and esub have essentially the same interfaces as LSF' bjobs,
bhosts, and bsub.
Check

    ejobs -h
    ehosts -h
    esub -h
    man bjobs
    man bhosts
    man bsub
