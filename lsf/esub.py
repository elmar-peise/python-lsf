#!/usr/bin/env python
from __future__ import print_function, division

from job import submit
from error import LSFError
from utility import color

import sys
import os
import argparse
import re
import subprocess


def esub(args, bsubargs, jobscript):
    if args.show:
        try:
            os.unlink(".esubrecord")
        except:
            pass
        return
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
            line = "#" + line.split("#")[1]
            match = re.match("#BSUB (-\w+)$", line)
            if match:
                data[match.groups()[0]] = True
            match = re.match("#BSUB (-\w+) \"?(.*?)\"?$", line)
            if match:
                key, value = match.groups()
                if key in data:
                    if not isinstance(data[key], list):
                        data[key] = [data[key]]
                    data[key].append(value)
                else:
                    data[key] = value
        else:
            data["Command"] += line
    try:
        job = submit(data)
        if args.record:
            if not os.path.isfile(".esubrecord"):
                with open(".esubrecord", "a") as fout:
                    fout.write("esub temporary file; can safely be deleted")
                subprocess.call(["ejobs", str(job["Job"])])
            else:
                subprocess.call(["ejobs", "--noheader", str(job["Job"])])
        else:
            subprocess.call(["ejobs", str(job["Job"])])
    except LSFError as e:
        print(color(e.strerror, "r"))
        sys.exit(-1)


def main():
    parser = argparse.ArgumentParser(
        description="Wrapper for bsub."
    )
    parser.add_argument(
        "--record",
        help="record submitted job ids",
        action="store_true",
    )
    parser.add_argument(
        "--show",
        help="list recorded jobs",
        action="store_true",
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

    jobscript = None
    if not args.show:
        jobscript = sys.stdin.read()

    esub(args, bsubargs, jobscript)


if __name__ == "__main__":
    main()
