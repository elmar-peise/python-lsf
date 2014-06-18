#!/usr/bin/env python
from __future__ import division, print_function

import os


def getuseralias(user):
    if getuseralias.aliases is None:
        filename = os.environ["HOME"] + "/.useraliases"
        if os.path.isfile(filename):
            with open(filename) as fin:
                getuseralias.aliases = dict(line.split() for line in fin)
        else:
            getuseralias.aliases = {}
    if user in getuseralias.aliases:
        return getuseralias.aliases[user]
    else:
        return user
getuseralias.aliases = None
