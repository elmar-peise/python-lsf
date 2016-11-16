#!/usr/bin/env python
"""Wrapper script with bjobs functionality."""

from __future__ import print_function

import sys
import re
import argparse

from utility import color
from useraliases import lookupalias
from shortcuts import ejobsshortcuts

from readjobs import readjobs
from printjobs import printjobs
from groupjobs import groupjobs
from sumjobs import sumjobs

from readhosts import readhosts
from printhosts import printhosts

# highlighting color for pending reasing
pendingcolors = {
    "Running an exclusive job": "y",
    "Job's requirement for exclusive execution not satisfied": "y",
    "An exclusive job has reserved the host": "y",
    "Job slot limit reached": "y",
    "Not enough processors to meet the job's spanning requirement": "y",
    "Not enough slots or resources for whole duration of the job": "r",
    "Not enough hosts to meet the job's spanning requirement": "r",
    "Job('s)? requirements for reserving resource \(.*\) not satisfied": "r",
}

# order of status identifiers
statorder = {
    "RUN": 4,
    "PROV": 4,
    "PSUSP": 3,
    "USUSP": 3,
    "SSUSP": 3,
    "PEND": 2,
    "WAIT": 2,
    "UNKWN": 1,
    "DONE": 0,
    "ZOMBI": 0,
    "EXIT": 0,
}


def ejobs(args, bjobsargs):
    """Wrapper script with bjobs functionality."""
    # handle arguments
    if args.pending:
        bjobsargs = ["-p"] + bjobsargs
        args.groupby = "pend_reason"
    if args.sort:
        args.sortby = "jobid"
    for shortcutname, shortcutargs in ejobsshortcuts.items():
        if getattr(args, shortcutname):
            bjobsargs = shortcutargs + bjobsargs
    for l in list("rsda"):
        if args.__dict__[l]:
            bjobsargs = ["-" + l] + bjobsargs
    if args.u:
        unames = map(lookupalias, args.u.split())
        bjobsargs = ["-u", " ".join(unames)] + bjobsargs
    if args.output:
        args.output = sum([fields.split() for fields in args.output], [])

    # read
    jobs = readjobs(bjobsargs, fast=args.fast or args.jid)

    if not jobs:
        return

    # sort
    jobs.sort(key=lambda j: j["submit_time"])
    jobs.sort(key=lambda j: j["priority"], reverse=True)  # can be None
    jobs.sort(key=lambda j: -j["run_time"])
    jobs.sort(key=lambda j: -statorder[j["stat"]])
    if args.sortby:
        try:
            jobs.sort(key=lambda j: j[args.sortby])
        except:
            print("Unknown sorting key \"%s\"!" % args.sortby, file=sys.stderr)

    if args.jid:
        for job in jobs:
            print(job["id"])
        return

    # no grouping
    if not args.groupby or args.groupby not in jobs[0]:
        if args.sum:
            jobs = [sumjobs(jobs)]
        printjobs(jobs, wide=args.wide, long=args.long, output=args.output,
                  header=not args.noheader)
        return

    # grouping
    jobgroups = groupjobs(jobs, args.groupby)
    if not args.pending:
        if args.sum:
            jobs = []
            for title in sorted(jobgroups.keys()):
                sumjob = sumjobs(jobgroups[title])
                sumjob["title"] = title
                jobs.append(sumjob)
            printjobs(jobs, wide=args.wide, long=args.long, output=args.output,
                      header=not args.noheader)
        else:
            for title in sorted(jobgroups.keys()):
                printjobs(jobgroups[title], wide=args.wide, long=args.long,
                          output=args.output, header=not args.noheader,
                          title=title)
        return

    # pending
    for title in sorted(jobgroups.keys()):
        jobs = jobgroups[title]
        reasons = jobs[0]["pend_reason"]
        resreq = jobs[0]["resreq"]
        hostreq = jobs[0]["host_req"]
        if not reasons or len(reasons) != 1:
            title = None
        else:
            # use singular reason as title
            reason = reasons[0]
            title = reason[0]
            if not isinstance(reason[1], bool):
                title += ": %d" % reason[1]
        if args.sum:
            jobs = [sumjobs(jobs)]
        printjobs(jobs, wide=args.wide, long=args.long, output=args.output,
                  header=not args.noheader, title=title)
        if reasons and len(reasons) > 1:
            # show pending reasons
            for reason, count in reasons:
                for pattern in pendingcolors:
                    if re.match(pattern, reason):
                        reason = color(reason, pendingcolors[pattern])
                        break
                if count is True:
                    print("        " + reason)
                else:
                    print("  %4d  %s" % (count, reason))
            # show potential hosts
            if resreq and not args.fast:
                resreq = re.sub(" && \(hostok\)", "", resreq)
                resreq = re.sub(" && \(mem>\d+\)", "", resreq)
                hosts = readhosts(["-R", resreq] + hostreq)
                hostnames = [h["host_name"] for h in hosts]
                jobs = readjobs(["-u", "all", "-r", "-m", " ".join(hostnames)])
                hosts.sort(key=lambda h: h["host_name"])
                printhosts(hosts, jobs, wide=args.wide,
                           header=not args.noheader)
                if len(jobgroups) > 1:
                    print()


def main():
    """Main program entry point."""
    # argument parser and options
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bjobs."
    )
    exg = parser.add_mutually_exclusive_group()
    exg.add_argument(
        "-w", "--wide",
        help="show more detailed info",
        action="store_true"
    )
    exg.add_argument(
        "-l", "--long",
        help="long job description",
        action="store_true"
    )
    exg.add_argument(
        "-o", "--output",
        help="show value of FIELD_NAME",
        action="append",
        metavar="FIELD_NAME"
    )
    exg.add_argument(
        "--jid",
        help="only job ids",
        action="store_true"
    )
    parser.add_argument(
        "--sum",
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
        "--fast",
        help="read less info from LSF",
        action="store_true"
    )
    parser.add_argument(
        "--noheader",
        help="don't show the header",
        action="store_true"
    )

    # shortcuts
    shortcuts = parser.add_argument_group("shortcuts")
    shortcuts.add_argument(
        "--sort",
        help="for \"--sortby jobid\"",
        action="store_true"
    )
    for shortcutname, shortcutargs in ejobsshortcuts.items():
        shortcuts.add_argument(
            "-" + shortcutname,
            help="for \"%s\"" % " ".join(shortcutargs),
            action="store_true"
        )

    # hide or discard some arguments
    parser.add_argument(
        "-X",  # discard
        help=argparse.SUPPRESS,
        action="store_true"
    )
    parser.add_argument(
        "-u",  # for username lookup
        help=argparse.SUPPRESS,
    )
    # pass the following on to allow combining (e.g. with -p or -l)
    for l in list("rsda"):
        parser.add_argument(
            "-" + l,
            help=argparse.SUPPRESS,
            action="store_true"
        )

    # bjobs arguments hint
    parser.add_argument_group(
        "further arguments",
        description="are passed to bjobs"
    )

    # parse arguments
    args, bjobsargs = parser.parse_known_args()

    # run ejobs
    try:
        ejobs(args, bjobsargs)
    except (KeyboardInterrupt, IOError):
        pass


if __name__ == "__main__":
    main()
