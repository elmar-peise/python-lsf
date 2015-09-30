#!/usr/bin/env python
"""Read hosts from LSF."""
from __future__ import print_function, division

from subprocess import Popen, PIPE, check_output
import re


def parseval(val):
    """Parse a value that could be int, float, % or contain a memory unit."""
    if val == "-":
        return None
    if re.match("\d+$", val):
        return int(val)
    if re.match("\d+(.\d+)?([eE][+-]\d+)?$", val):
        return float(val)
    if re.match("\d+(.\d+)?%$", val):
        return 100 * float(val[:-1])
    if re.match("\d+(.\d+)?[KMGT]$", val):
        e = {"K": 1, "M": 2, "G": 3, "T": 4}[val[-1]]
        return int(float(val[:-1]) * 1024 ** e)
    return val


def readhosts(args, fast=False):
    """Read hosts from LSF."""
    # read bhosts for dynamic information
    p = Popen(["bhosts", "-l"] + args, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if err:
        return []
    lines = out.splitlines()
    lines.reverse()
    hostorder = []
    hosts = {}
    host = None
    stage = None
    while lines:
        line = lines.pop()
        if not line:  # ignore empty lines
            continue
        tokens = line.split()
        if tokens[0] == "HOST":
            if host:
                hostorder.append(host["host_name"])
                hosts[host["host_name"]] = host
            host = {
                "host_name": tokens[1],
                "load": {},
                "threshold": {},
                "comment": None,
            }
            stage = None
        elif tokens[0] == "STATUS":
            keys = line.lower().split()
            try:
                vals = lines.pop().split()
                for key, val in zip(keys, vals):
                    host[key] = parseval(val)
            except:
                pass
        elif tokens[0] == "CURRENT":
            stage = "load"
        elif tokens[0] == "LOAD":
            stage = "threshold"
        elif tokens[0] == "ADMIN":
            host["comment"] = " ".join(tokens[3:])[1:-1]
        elif stage in ("load", "threshold"):
            keys = tokens
            try:
                total = map(parseval, lines.pop().split()[1:])
                used = map(parseval, lines.pop().split()[1:])
                new = {k: v for k, v in zip(keys, zip(total, used))}
                host[stage].update(new)
            except:
                pass
    hostorder.append(host["host_name"])
    hosts[host["host_name"]] = host
    if fast:
        return [hosts[hn] for hn in hostorder]
    # read lshosts for static information
    out = check_output(["lshosts", "-w"] + hostorder)
    lines = out.splitlines()
    keys = lines[0].lower().split()
    for line in lines[1:]:
        vals = line.split()
        host = hosts[vals[0]]
        for key, val in zip(keys[1:], vals[1:]):
            host[key] = parseval(val)
            if key in ("server"):
                host[key] = val == "Yes"
        resources = vals[len(keys) - 1:]
        resources[0] = resources[0][1:]  # get rid of ()
        resources[-1] = resources[-1][:-1]
        host[keys[-1]] = resources
    return [hosts[hn] for hn in hostorder]
