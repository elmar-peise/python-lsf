#!/usr/bin/env python
from __future__ import print_function, division

from utility import color
from useraliases import lookupalias

from readjobs import readjobs
from printjobs import printjobs
from groupjobs import groupjobs
from sumjobs import sumjobs

from readhosts import readhosts
from printhosts import printhosts

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
    if args.aices24:
        bjobsargs = ["-P", "aices-24", "-G", "p_aices"] + bjobsargs
    for l in list("rsda"):
        if args.__dict__[l]:
            bjobsargs = ["-" + l] + bjobsargs
    if args.u:
        unames = []
        for alias in args.u.split():
            unames += lookupalias(alias)
        bjobsargs += ["-u", " ".join(unames)]

    # read
    jobs = readjobs(bjobsargs, fast=args.fast)

    if not jobs:
        return

    # sort
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
    jobs.sort(key=lambda j: j["submit_time"])
    jobs.sort(key=lambda j: j["priority"], reverse=True)
    jobs.sort(key=lambda j: -j["run_time"])
    jobs.sort(key=lambda j: -statorder[j["stat"]])
    if args.sortby:
        jobs.sort(key=lambda j: j[args.sortby])

    # no grouping
    if not args.groupby or args.groupby not in jobs[0]:
        if args.sum:
            printjobs([sumjobs(jobs)], wide=args.wide, long=args.long,
                      header=not args.noheader)
        else:
            printjobs(jobs, wide=args.wide, long=args.long,
                      header=not args.noheader)
        return

    # grouping
    jobgroups = groupjobs(jobs, args.groupby)
    if not args.pending:
        if args.sum:
            jobs = []
            for title in sorted(jobgroups.keys()):
                jobgroup = jobgroups[title]
                sumjob = sumjobs(jobgroup)
                sumjob["title"] = title
                jobs.append(sumjob)
            printjobs(jobs, wide=args.wide, long=args.long,
                      header=not args.noheader)
        else:
            for title in sorted(jobgroups.keys()):
                jobs = jobgroups[title]
                printjobs(jobs, wide=args.wide, long=args.long,
                          header=not args.noheader, title=title)
        return

    # pending
    for title in sorted(jobgroups.keys()):
        jobs = jobgroups[title]
        reasons = jobs[0]["pend_reason"]
        if not reasons or len(reasons) != 1:
            title = None
        else:  # only use singular reason as title
            reason = reasons[0]
            title = reason[0]
            if not isinstance(reason[1], bool):
                title += ": %d" % reason[1]
        if args.sum:
            printjobs([sumjobs(jobs)], wide=args.wide, long=args.long,
                      header=not args.noheader, title=title)
        else:
            printjobs(jobs, wide=args.wide, long=args.long,
                      header=not args.noheader, title=title)
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
                jobs = readjobs(["-u", "all", "-r", "-m", " ".join(hostnames)])
                hosts.sort(key=lambda h: h["host_name"])
                printhosts(hosts, jobs, wide=args.wide,
                           header=not args.noheader)
                if len(jobgroups) > 1:
                    print()


def main():
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bjobs."
    )
    exg = parser.add_mutually_exclusive_group()
    exg.add_argument(
        "-w", "--wide",
        help="shore more detailed info",
        action="store_true"
    )
    exg.add_argument(
        "-l", "--long",
        help="long job description",
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
        "-aices24",
        help="short for -P aices-24",
        action="store_true"
    )
    parser.add_argument(
        "--noheader",
        help="don't show the header",
        action="store_true"
    )
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
