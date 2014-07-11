#!/usr/bin/env python
from __future__ import division, print_function

from utility import findstringpattern

from collections import defaultdict


def sumhosts(hosts):
    sumhost = {}
    for key in hosts[0]:
        if key in ("host_name"):
            # find string pattern
            sumhost[key] = findstringpattern([host[key] for host in hosts
                                              if host[key]])
        elif key in ("max", "njobs", "run", "ssusp", "ususp", "rsv", "ncpus",
                     "maxmem", "maxswp"):
            # sum
            sumhost[key] = sum(host[key] for host in hosts if host[key])
        elif key in ("status", "server", "type", "comment"):
            sumhost[key] = defaultdict(int)
            for host in hosts:
                sumhost[key][host[key]] += 1
        elif key in ("load", "threshold"):
            # sum up free/used pairs
            sumhost[key] = dict()
            for key2 in hosts[0][key]:
                free, used = zip(*[host[key][key2] for host in hosts])
                if all(x is None for x in free):
                    free = None
                else:
                    free = sum(x for x in free if x)
                if all(x is None for x in used):
                    used = None
                else:
                    used = sum(x for x in used if x)
                sumhost[key][key2] = [free, used]
        else:
            # colect
            sumhost[key] = []
            for host in hosts:
                if host[key] and host[key] not in sumhost[key]:
                    sumhost[key].append(host[key])
    sumhost["host_names"] = [host["host_name"] for host in hosts]
    return sumhost
