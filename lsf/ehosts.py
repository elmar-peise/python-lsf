#!/usr/bin/env python
from __future__ import print_function, division

from readhosts import readhosts
from readjobs import readjobs
from printhosts import printhosts
# from hostlist import Hostlist
# from utility import color

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
    hosts = readhosts(bhostsargs)
    if args.nojobs:
        jobs = []
    else:
        hostnames = [h["host_name"] for h in hosts]
        jobs = readjobs(["-u", "all", "-r", "-m", " ".join(hostnames)])

    # sort
    if not args.nosort:
        hosts.sort(key=lambda h: h["host_name"])
    # print
    printhosts(hosts, jobs, wide=args.wide, header=not args.noheader)


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
    parser.add_argument(
        "--nojobs",
        help="don't show jobs",
        action="store_true"
    )
    parser.add_argument(
        "--model",
        help="short for -R model==MODEL"
    )
    parser.add_argument_group(
        "further arguments",
        description="are passed to bhosts"
    )

    args, bhostsargs = parser.parse_known_args()

    try:
        ehosts(args, bhostsargs)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
