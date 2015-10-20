#!/usr/bin/env python
"""Username to real name aliasing utilities."""
from __future__ import division, print_function

import os

useraliases = None


def loadaliases():
    """Load all aliases from ~/.useraliases."""
    global useraliases
    if useraliases is None:
        filename = os.environ["HOME"] + "/.useraliases"
        if os.path.isfile(filename):
            with open(filename) as fin:
                useraliases = dict(line.strip().split(None, 1) for line in fin)
        else:
            useraliases = {}
    return useraliases


def getuseralias(user):
    """Look up the alias for a user."""
    aliases = loadaliases()
    if user in aliases:
        return aliases[user]
    else:
        return user


def lookupalias(alias):
    """Look up the user for an alias."""
    aliases = loadaliases()
    try:
        return next(k for k, v in aliases.iteritems() if v == alias)
    except:
        return alias
