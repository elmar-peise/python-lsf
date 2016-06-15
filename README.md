python-lsf
==========

Improved Interface for the LSF batch job scheduler


Requirements
------------

* python version 2.7.x
* LSF version 9.1.3.0
* `bjobs`, `bhosts`, `bsub`, and `lshosts` available


Installation
------------

    python setup.py install

If you are missing the right to write to the system wide python directories,
use

    python setup.py install --user

to install the package in `~/.local`.  You might then have to

    export PATH=$PATH:~/.local/bin

in order to make the scripts available on the command line.


Usage
-----

`ejobs`, `ehosts`, and `esub` have essentially the same interfaces as LSF's
`bjobs`, `bhosts`, and `bsub`.
Check

    ejobs -h
    ehosts -h
    esub -h
    man bjobs
    man bhosts
    man bsub


User Alias Resolution
---------------------

To resolve commonly encountered and possibly cryptic user names, `ejobs` and
`ehosts` provide a mechanism to replace such user names by user defined aliases.
These aliases are read from `~/.useraliases` (if existing), which needs to be in
the following format:  each user is on its own line; the first word on the line
is the user name, all following words are the user alias. E.g.:

    ep123456    Elmar Peise
