#!/usr/bin/env python
"""Wrapper script with bhosts functionality."""
from __future__ import print_function, division

from readhosts import readhosts
from printhosts import printhosts
from grouphosts import grouphosts
from sumhosts import sumhosts

from readjobs import readjobs

import sys
import re
import argparse


def ehosts(args, bhostsargs):
    """Wrapper script with bhosts functionality."""
    # construct -R argument
    select = None
    if args.aices and args.aices2:
        select = "aices || aices2"
    elif args.aices:
        select = "aices"
    elif args.aices2:
        select = "aices2"
    elif args.aices24:
        select = "aices24"
    if args.model:
        if select:
            select = "(%s) && model==%s" % (select, args.model)
        else:
            select = "model==" + args.model
    if select:
        if "-R" not in bhostsargs:
            bhostsargs += ["-R", "select[%s]" % select]
        else:
            i = bhostsargs.index("-R") + 1
            req = bhostsargs[i]
            if "select" in req:
                bhostsargs[i] = req.replace("select[",
                                            "select[(%s) & " % select, 1)
            else:
                bhostsargs[i] = "(%s) & (%s)" % (req, select)

    # read
    hosts = readhosts(bhostsargs, fast=args.fast)

    if not hosts:
        return

    # read jobs
    if args.fast:
        jobs = []
    else:
        hostnames = [h["host_name"] for h in hosts]
        jobs = readjobs(["-u", "all", "-r", "-m", " ".join(hostnames)])

    # sort
    if not args.nosort:
        hosts.sort(key=lambda h: h["host_name"])

    # no grouping
    if not args.groupby or args.groupby not in hosts[0]:
        if args.sum:
            printhosts([sumhosts(hosts)], wide=args.wide, header=not
                       args.noheader)
        else:
            printhosts(hosts, jobs, wide=args.wide, header=not args.noheader)
        return

    # grouping
    hostgroups = grouphosts(hosts, args.groupby)
    if args.sum:
        hosts = []
        for title in sorted(hostgroups.keys()):
            hostgroup = hostgroups[title]
            sumhost = sumhosts(hostgroup)
            sumhost["title"] = title
            hosts.append(sumhost)
        printhosts(hosts, jobs, wide=args.wide, header=not args.noheader)
    else:

        for title in sorted(hostgroups.keys()):
            hosts = hostgroups[title]
            printhosts(hosts, jobs, wide=args.wide, header=not args.noheader,
                       title=title)


def main():
    """Main program entry point."""
    global args
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bhosts."
    )
    parser.add_argument(
        "-w", "--wide",
        help="show more detailed info",
        action="store_true"
    )
    parser.add_argument(
        "-sum",
        help="summarize across hosts",
        action="store_true"
    )
    parser.add_argument(
        "--groupby",
        help="group jobs by KEY",
        metavar="KEY"
    )
    parser.add_argument(
        "--fast",
        help="read less info frim LSF",
        action="store_true"
    )
    parser.add_argument(
        "--model",
        help="short for -R model==MODEL"
    )
    parser.add_argument(
        "-aices",
        help="short for -R aices",
        action="store_true"
    )
    parser.add_argument(
        "-aices2",
        help="short for -R aices2",
        action="store_true"
    )
    parser.add_argument(
        "-aices24",
        help="short for -R aices24",
        action="store_true"
    )
    parser.add_argument(
        "--noheader",
        help="don't show the header",
        action="store_true"
    )
    parser.add_argument(
        "--nosort",
        help="don't sort lexigraphically",
        action="store_true"
    )
    parser.add_argument_group(
        "further arguments",
        description="are passed to bhosts"
    )

    args, bhostsargs = parser.parse_known_args()

    try:
        ehosts(args, bhostsargs)
    except (KeyboardInterrupt, IOError):
        pass


if __name__ == "__main__":
    main()
