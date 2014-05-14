#!/usr/bin/env python
from __future__ import print_function, division

from readhosts import readhosts
from printhosts import printhosts
from printhostssum import printhostssum
from grouphosts import grouphosts

from readjobs import readjobs

import sys
import re
import argparse


def ehosts(args, bhostsargs):
    # construct -R argument
    select = None
    if args.aices and args.aices2:
        select = "aices || aices2"
    elif args.aices:
        select = "aices"
    elif args.aices2:
        select = "aices2"
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
            if "select" in req:
                bhostsargs[i].replace("select[", "select[(%s) &&" % select, 1)
            else:
                bhostsargs[i] += " select[%s]" % select

    # read
    hosts = readhosts(bhostsargs, fast=args.fast)
    if args.fast:
        jobs = []
    else:
        hostnames = [h["host_name"] for h in hosts]
        jobs = readjobs(["-u", "all", "-r", "-m", " ".join(hostnames)])

    # summarize?
    if args.sum:
        printhostsfun = printhostssum
    else:
        printhostsfun = printhosts

    # sort
    if not args.nosort:
        hosts.sort(key=lambda h: h["host_name"])

    # no grouping
    if not args.groupby:
        printhostsfun(hosts, jobs, wide=args.wide, header=not args.noheader)
        return

    # grouping
    hostgroups = grouphosts(hosts, args.groupby)
    for title in sorted(hostgroups.keys()):
        hosts = hostgroups[title]
        printhostsfun(hosts, jobs, wide=args.wide, header=not args.noheader,
                      title=title)


def main():
    global args
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bhosts."
    )
    parser.add_argument(
        "-w", "--wide",
        help="don't shorten strings",
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
