#!/usr/bin/env python
from __future__ import division, print_function

import os

useraliases = None


def loadaliases():
    global useraliases
    if useraliases is None:
        filename = os.environ["HOME"] + "/.useraliases"
        if os.path.isfile(filename):
            with open(filename) as fin:
                useraliases = dict(line.split() for line in fin)
        else:
            useraliases = {}
    return useraliases


def getuseralias(user):
    aliases = loadaliases()
    if user in aliases:
        return aliases[user]
    else:
        return user


def lookupalias(alias):
    aliases = loadaliases()
    usernames = [k for k, v in aliases.iteritems() if v == alias]
    if not usernames:
        usernames = [alias]
    return usernames
