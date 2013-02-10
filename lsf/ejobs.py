#!/usr/bin/env python
from __future__ import print_function, division

from job import Joblist
from host import Hostlist
from utility import color

import sys
import os
import argparse


def main():
    global args
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bjobs.",
        epilog="Any non-listed arguments are passed to bjobs.")
    parser.add_argument(
        "-l", "--long",
        help="long job description",
        action="store_true",
    )
    parser.add_argument(
        "-w", "--wide",
        help="don't shorten strings",
        action="store_true",
    )
    exg = parser.add_mutually_exclusive_group()
    exg.add_argument(
        "-p", "--pending",
        help="show pending jobs with reasons and potential hosts",
        action="store_true",
    )
    exg.add_argument(
        "--group",
        help="group jobs by attribute",
        metavar="BY",
    )
    parser.add_argument(
        "-aices",
        help="short for -G p_aices",
        action="store_true",
    )
    parser.add_argument_group("further arguments",
                              description="are passed to bjobs")

    args, bjobsargs = parser.parse_known_args()

    if args.pending:
        args.group = "PENDING REASONS"
    if args.aices:
        bjobsargs = ["-G", "p_aices"] + bjobsargs

    whoami = os.getenv("USER")

    print("Reading job list from LSF ...", end="\r")
    sys.stdout.flush()
    joblist = Joblist(bjobsargs)
    print("                             ", end="\r")
    if args.pending:
        joblists = joblist.groupby("Status")
        if "PEND" in joblists:
            joblist = joblists["PEND"]
        else:
            joblist = Joblist()
    joblists = joblist.groupby(args.group)

    if not args.pending:
        for group, joblist in joblists.items():
            if group:
                groupn = group
                if args.group == "User":
                    groupn = joblist[0]["Userstr"]
                title = "{} = {} [{}]".format(args.group, groupn, len(joblist))
            else:
                title = None
            joblist.display(args.long, args.wide, title)
    else:
        for reasons in sorted(joblists.keys(), key=len):
            pendjobs = joblists[reasons]
            if len(reasons) == 1 and reasons[0][1] is True:
                title = "{} [{}]".format(reasons[0][0], len(pendjobs))
                pendjobs.display(args.long, args.wide, title)
                continue
            lists = {}
            resgrouped = pendjobs.groupby("Requested Resources")
            for res, rlist in resgrouped.iteritems():
                hostgrouped = rlist.groupby("Specified Hosts")
                for hosts, hlist in hostgrouped.iteritems():
                    lists[res, hosts] = hlist
            for case, list in lists.iteritems():
                title = "[{}]".format(len(list))
                list.display(args.long, args.wide, title)
                print()
                print("Pending reasons:")
                for reason, count in reasons:
                    if reason in ["Not enough slots or resources for "
                                  "whole duration of the job"]:
                        print("\t{}\t{}".format(count, color(reason, "r")))
                    else:
                        print("\t{}\t{}".format(count, reason))
                req = ["-R", case[0].replace(" && (hpcwork) && (hostok)", "")]
                if case[1]:
                    req = [case[1]]
                print("Reading host list from LSF ...", end="\r")
                sys.stdout.flush()
                hl = {h["HOST"]: h for h in Hostlist(req)}
                print("Reading job list from LSF ... ", end="\r")
                sys.stdout.flush()
                jl = Joblist(["-m", " ".join(hl.keys()), "-u", "all"])
                byproc = jl.groupby("Processors")
                print("potential hosts:              ")
                for hostname in sorted(hl.keys()):
                    host = hl[hostname]
                    freeslots = host["MAX"] - host["RUN"]
                    if hostname in byproc:
                        if byproc[hostname][0]["Exclusive Execution"]:
                            freeslots = 0
                    if freeslots == 0:
                        c = "r"
                    elif host["RUN"] == 0:
                        c = "g"
                    else:
                        c = "y"
                    freeslots = color("{:>2}*free".format(freeslots), c)
                    print("\t{}: {}".format(host, freeslots), end="")
                    if hostname in byproc:
                        jobs = byproc[hostname]
                        users = []
                        for job in jobs:
                            if job["Exclusive Execution"]:
                                js = "-x "
                            else:
                                js = str(job["Processors"][hostname]).ljust(2)
                                js += "*"
                            if job["User"] == whoami:
                                js += color(job["User"], "g")
                            else:
                                js += job["User"]
                            users.append(js)
                        print("\t{}".format("\t".join(users)), end="")
                    print()
                print("conflicting users:")
                for user, jobs in jl.groupby("User").items():
                    procs = {}
                    for job in jobs:
                        idx = "Processors" if args.wide else "Hostgroups"
                        for p, c in job[idx].iteritems():
                            if not p in procs:
                                procs[p] = 0
                            if job["Exclusive Execution"]:
                                procs[p] += job["Hosts"][0]["MAX"]
                            else:
                                procs[p] += c
                    procsstr = "\t".join("{:>3}*{}".format(c, p)
                                         for p, c in procs.iteritems())
                    hosts = set(sum((job["Hosts"] for job in jobs), []))
                    if jobs[0]["User"] == whoami:
                        c = "g"
                    else:
                        c = 0
                    ustr = jobs[0]["Userstr"].ljust(40)
                    print("\t" + color(ustr, c) + procsstr)

if __name__ == "__main__":
    main()
