#!/usr/bin/env python
from __future__ import print_function, division

from job import submit
from error import LSFError
from utility import color

import sys
import argparse
import re


def esub(args, bsubargs, jobscript):
    data = {"Command": ""}
    if args.aices:
        data["-P"] = "aices"
    if args.aices2:
        data["-P"] = "aices2"
    last = False
    cmd = False
    for arg in bsubargs:
        if cmd:
            data["Command"] += " " + arg
        if arg[0] == "-":
            if last:
                data[last] = True
            last = arg
        else:
            if last:
                data[last] = arg
            else:
                cmd = True
                data["Command"] += " " + arg
    for line in jobscript.splitlines(True):
        if line.startswith("#BSUB"):
            match = re.match("#BSUB (-\w+)$", line)
            if match:
                data[match.groups()[0]] = True
            match = re.match("#BSUB (-\w+) \"?(.*?)\"?$", line)
            if match:
                data[match.groups()[0]] = match.groups()[1]
        else:
            data["Command"] += line
    try:
        job = submit(data)
        print(job["Job"])
    except LSFError as e:
        print(color(e.strerror, "r"))
        sys.exit(-1)


def main():
    parser = argparse.ArgumentParser(
        description="Wrapper for bsub."
    )
    parser.add_argument(
        "-aices",
        help="short for -P aices",
        action="store_true",
    )
    parser.add_argument(
        "-aices2",
        help="short for -P aices2",
        action="store_true",
    )
    parser.add_argument_group("further arguments",
                              description="are passed to bsub")

    args, bsubargs = parser.parse_known_args()

    jobscript = sys.stdin.read()

    esub(args, bsubargs, jobscript)


if __name__ == "__main__":
    main()
