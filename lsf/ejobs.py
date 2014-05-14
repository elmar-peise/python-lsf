#!/usr/bin/env python
from __future__ import print_function, division

from utility import color

from readjobs import readjobs
from printjobs import printjobs
from printjobssum import printjobssum
from readhosts import readhosts
from printhosts import printhosts
from printhostssum import printhostssum
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
    if args.a:
        bjobsargs += ["-a"]

    # read
    jobs = readjobs(bjobsargs, fast=args.fast)

    # sort
    if args.sortby:
        jobs.sort(key=lambda j: j[args.sortby])

    # summarize?
    if args.sum:
        printjobsfun = printjobssum
        printhostsfun = printhostssum
    else:
        printjobsfun = printjobs
        printhostsfun = printhosts

    # no grouping
    if not args.groupby:
        printjobsfun(jobs, wide=args.wide, long=args.long, header=not
                     args.noheader)
        return

    # grouping
    jobgroups = groupjobs(jobs, args.groupby)
    for title in sorted(jobgroups.keys()):
        jobs = jobgroups[title]
        if args.pending:  # grouped by pend_reason
            reasons = jobs[0]["pend_reason"]
            if not reasons or len(reasons) != 1:
                title = None
            else:  # only use singular reason as title
                reason = reasons[0]
                title = reason[0]
                if not isinstance(reason[1], bool):
                    title += ": %d" % reason[1]
        printjobsfun(jobs, wide=args.wide, header=not args.noheader,
                     title=title)
        if args.pending:
            if reasons and len(reasons) > 1:
                # show pending reasons
                for reason, count in reasons:
                    if reason in pendingcolors:
                        reason = color(reason, pendingcolors[reason])
                    if count is True:
                        print("        " + reason)
                    else:
                        print("  %4d  %s" % (count, reason))
                # show potential hosts
                if jobs[0]["resreq"] and not args.fast:
                    req = jobs[0]["resreq"]
                    req = re.sub(" && \(hostok\)", "", req)
                    req = re.sub(" && \(mem>\d+\)", "", req)
                    hosts = readhosts(["-R", req])
                    hostnames = [h["host_name"] for h in hosts]
                    jobs = readjobs(["-u", "all", "-r", "-m",
                                     " ".join(hostnames)])
                    hosts.sort(key=lambda h: h["host_name"])
                    printhostsfun(hosts, jobs, wide=args.wide, header=not
                                  args.noheader)


def main():
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bjobs."
    )
    parser.add_argument(
        "-w", "--wide",
        help="don't shorten strings",
        action="store_true"
    )
    parser.add_argument(
        "-l", "--long",
        help="long job description",
        action="store_true"
    )
    parser.add_argument(
        "-sum",
        help="summarize across jobs",
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
    parser.add_argument(
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
        "--fast",
        help="read less info from LSF",
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
        "-a",
        help=argparse.SUPPRESS,
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
    except (KeyboardInterrupt, IOError):
        pass


if __name__ == "__main__":
    main()
