#!/usr/bin/env python
from __future__ import print_function, division

from hostlist import Hostlist
from utility import color

import sys
import argparse


def main_raising():
    global args
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bhosts.")
    parser.add_argument(
        "-w", "--wide",
        help="don't shorten strings",
        action="store_true",
    )
    parser.add_argument(
        "-aices",
        help="short for -R select[aices]",
        action="store_true",
    )
    parser.add_argument_group("further arguments",
                              description="are passed to bjobs")

    args, bjobsargs = parser.parse_known_args()

    if args.aices:
        bjobsargs = ["-R", "select[aices]"] + bjobsargs

    print("Reading host list from LSF ...", end="\r")
    sys.stdout.flush()
    hostlist = Hostlist(bjobsargs)
    print("                              ", end="\r")
    hostlist.sort()
    hostlist.display()


def main():
    try:
        main_raising()
    except KeyboardInterrupt:
        pass
    except SystemExit:
        pass
    except:
        print(color("ERROR -- probably a job status changed while " +
                    sys.argv[0] + " processed it", "r"), file=sys.stderr)

if __name__ == "__main__":
    main()
