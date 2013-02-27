#!/usr/bin/env python
from __future__ import print_function, division

import sys


def color(string, c):
    if sys.stdout.isatty():
        names = {"r": 31, "g": 32, "y": 33}
        if c in names:
            c = names[c]
        return "\033[{}m{}\033[0m".format(c, string)
    else:
        return string


def format_time(t):
    if t == 0:
        return "            0"
    # seconds
    s = "{:0>2}".format(t % 60)
    t //= 60
    # minutes
    s = "{}:".format(t % 60) + s
    t //= 60
    if t == 0:
        return "       " + s
    s = s.rjust(5, "0")
    # hours
    s = "{:>2}:".format(t % 24) + s
    t //= 24
    if t == 0:
        return "    " + s
    # days
    s = "{:>2}d ".format(t) + s
    return s
