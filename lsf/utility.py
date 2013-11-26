#!/usr/bin/env python
from __future__ import print_function, division

import sys
from time import strftime, gmtime


def color(string, c):
    if sys.stdout.isatty():
        names = {"r": 31, "g": 32, "y": 33}
        if c in names:
            c = names[c]
        return "\033[{}m{}\033[0m".format(c, string)
    else:
        return string


def format_duration(t):
    t = int(t)
    if t == 0:
        return "            0"
    # seconds
    s = "{:0>2}".format(t % 60)
    t //= 60
    # minutes
    if t >= 60:
        s = "{:0>2}:".format(t % 60) + s
    else:
        s = "{:>2}:".format(t % 60) + s
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


def format_time(t):
    return strftime("%d.%m.%Y %H:%M:%S", gmtime(t))


def format_mem(s):
    i = 0
    while s >= 1024:
        s //= 1024
        i += 1
    return str(s) + ["B  ", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"][i]
