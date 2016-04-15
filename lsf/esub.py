#!/usr/bin/env python
"""Wrapper script with bsub functionality."""
from __future__ import print_function, division

from submitjob import submitjob
from utility import color

import shlex
import sys
import os
import argparse
import re
import subprocess


def esub(args, bsubargs, jobscript):
    """Wrapper script with bsub functionality."""
    data = {"command": ""}
    scriptargs = []
    for line in jobscript.splitlines(True):
        if line.startswith("#!"):
            data["command"] += line
        elif line.startswith("#BSUB "):
            scriptargs += shlex.split(line[6:].split("#")[0])
        else:
            data["command"] += line.split("#")[0]
    bsubargs = scriptargs + bsubargs
    last = False
    cmd = False
    for arg in bsubargs:
        if cmd:
            data["command"] += " " + arg
            continue
        if arg[0] == "-":
            if last:
                data[last] = True
            last = arg
        else:
            if last:
                data[last] = arg
                last = False
            else:
                cmd = True
                data["command"] = arg
    if last:
        data[last] = True
    try:
        jobid = submitjob(data)
        if args.jid:
            print(jobid)
        else:
            subprocess.Popen(["ejobs", "--noheader", jobid])
    except Exception as e:
        print(color(e.strerror, "r"))
        sys.exit(-1)


def main():
    """Main program entry point."""
    parser = argparse.ArgumentParser(
        description="Wrapper for bsub."
    )
    parser.add_argument(
        "--jid",
        help="only print submitted job's id",
        action="store_true"
    )
    parser.add_argument_group("further arguments",
                              description="are passed to bsub")

    args, bsubargs = parser.parse_known_args()

    jobscript = sys.stdin.read()
    try:
        esub(args, bsubargs, jobscript)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
