#!/usr/bin/env python
from __future__ import print_function, division

from utility import color

from readjobs import readjobs
from printjobs import printjobs
from groupjobs import groupjobs

import sys
import argparse
import re

pendingcolors = {
    "Running an exclusive job": "y",
    "Job's requirement for exclusive execution not satisfied": "y",
    "An exclusive job has reserved the host": "y",
    "Job slot limit reached": "y",
    "Not enough processors to meet the job's spanning requirement": "y",
    "Not enough slots or resources for whole duration of the job": "r",
    "Not enough hosts to meet the job's spanning requirement": "r",
    "Job requirements for reserving resource (mem) not satisfied": "r",
}


def ejobs(args, bjobsargs):
    if args.pending:
        bjobsargs = ["-p"] + bjobsargs
        args.groupby = "pend_reason"
    if args.sort:
        args.sortby = "jobid"
    if args.aices:
        bjobsargs = ["-P", "aices", "-G", "p_aices"] + bjobsargs
    if args.aices2:
        bjobsargs = ["-P", "aices2", "-G", "p_aices"] + bjobsargs

    # read
    jobs = readjobs(bjobsargs)

    # sort
    if args.sortby:
        jobs.sort(key=lambda j: j[args.sortby])

    # no grouping
    if not args.groupby:
        printjobs(jobs, wide=args.wide, header=not args.noheader)
        return

    # grouping
    jobgroups = groupjobs(jobs, args.groupby)
    for title in sorted(jobgroups.keys()):
        jobs = jobgroups[title]
        if args.pending:  # grouped by pend_reason
            reasons = jobs[0]["pend_reason"]
            if len(reasons) > 1:
                title = None
            else:  # only use singular reason as title
                title = reasons[0].items()[0]
                if isinstance(resons[0][1], int):
                    title += ": %d" % reasons[0][1]
        printjobs(jobs, wide=args.wide, header=not args.noheader, title=title)
        if args.pending:
            if len(reasons) > 1:
                # show pending reasons
                for reason, count in reasons.iteritems():
                    if reason in pendingcolors:
                        reason = color(reason, pendingcolors[reason])
                    if count is True:
                        print("        " + reasona)
                    else:
                        print("  %4d  %s" % (count, reason))
                # show potential hosts
                if jobs[0]["effective_resreq"]:
                    req = jobs[0]["effective_resreq"]
                    req = re.sub(" && \(hostok\)", "", req)
                    req = re.sub(" && \(mem>\d+\)", "", req)
                    hosts = readhosts(["-R", req])
                    hosts.sort(key=lambda h: h["host_name"])
                    printhosts(hosts, wide=args.wide, header=not args.noheader)


def main():
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bjobs."
    )
    parser.add_argument(
        "-l", "--long",
        help="long job description",
        action="store_true"
    )
    parser.add_argument(
        "-w", "--wide",
        help="don't shorten strings",
        action="store_true"
    )
    exg = parser.add_mutually_exclusive_group()
    exg.add_argument(
        "-p", "--pending",
        help="show pending jobs with reasons and potential hosts",
        action="store_true"
    )
    exg.add_argument(
        "--groupby",
        help="group jobs by KEY",
        metavar="KEY"
    )
    exg.add_argument(
        "--sortby",
        help="sort jobs by KEY",
        metavar="KEY"
    )
    parser.add_argument(
        "--sort",
        help="short for --sortby jobid",
        action="store_true"
    )
    parser.add_argument(
        "-aices",
        help="short for -P aices",
        action="store_true"
    )
    parser.add_argument(
        "-aices2",
        help="short for -P aices2",
        action="store_true"
    )
    parser.add_argument(
        "--noheader",
        help="don't show the header",
        action="store_true"
    )
    parser.add_argument(
        "-X",
        help=argparse.SUPPRESS,
        action="store_true"
    )
    parser.add_argument_group(
        "further arguments",
        description="are passed to bjobs"
    )

    args, bjobsargs = parser.parse_known_args()

    try:
        ejobs(args, bjobsargs)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
