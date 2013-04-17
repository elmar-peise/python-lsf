#!/usr/bin/env python
from __future__ import print_function, division

from joblist import Joblist
from hostlist import Hostlist
from utility import color

import sys
import argparse
import re


def ejobs(args, bjobsargs):
    if args.pending:
        args.group = "PENDING REASONS"
    if args.aices:
        bjobsargs = ["-G", "p_aices"] + bjobsargs

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
            joblist.display(args.long, args.wide, title,
                            parallel=not args.nopar)
        return
    for reasons in sorted(joblists.keys(), key=len):
        pendjobs = joblists[reasons]
        if len(reasons) == 1 and reasons[0][1] is True:
            if reasons[0][0] in (
                "New job is waiting for scheduling",
                "Dependency condition invalid or never satisfied",
            ):
                title = "{} [{}]".format(reasons[0][0], len(pendjobs))
                pendjobs.display(args.long, args.wide, title,
                                 parallel=not args.nopar)
                continue
        lists = {}
        resgrouped = pendjobs.groupby("Requested Resources")
        for res, rlist in resgrouped.iteritems():
            hostgrouped = rlist.groupby("Specified Hosts")
            for hosts, hlist in hostgrouped.iteritems():
                lists[res, hosts] = hlist
        for case, casejobs in lists.iteritems():
            title = "[{}]".format(len(casejobs))
            casejobs.display(args.long, args.wide, title,
                             parallel=not args.nopar)
            print()
            print("Pending reasons:")
            cs = {
                "Job's requirement for exclusive execution not satisfied": "y",
                "An exclusive job has reserved the host": "y",
                "Not enough slots or resources "
                "for whole duration of the job": "r",
                "Not enough hosts to meet the job's spanning requirement": "r",
            }
            for reason, count in reasons:
                s = reason
                if reason in cs:
                    s = color(reason, cs[reason])
                if count is True:
                    print("\t       " + s)
                else:
                    print("\t{:>4}  ".format(count) + s)
            if case[1]:
                req = [case[1]]
            else:
                req = case[0]
                req = re.sub(" && \(hostok\)", "", req)
                req = re.sub(" && \(mem>\d+\)", "", req)
                req = ["-R", req]
            print("Potential hosts:")
            print("Reading host list from LSF ...", end="\r")
            sys.stdout.flush()
            hl = Hostlist(req)
            hl.sort()
            hl.display(wide=args.wide, indent="    ", parallel=not args.nopar)
            hl = {h["HOST"]: h for h in Hostlist(req)}


def main():
    parser = argparse.ArgumentParser(
        description="More comprehensive version of bjobs."
    )
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
    parser.add_argument(
        "--nopar",
        help="faster response, longer runtime",
        action="store_true",
    )
    parser.add_argument(
        "-d", "--debug",
        help="show debug info on errors",
        action="store_true",
    )
    parser.add_argument_group("further arguments",
                              description="are passed to bjobs")

    args, bjobsargs = parser.parse_known_args()

    if args.debug:
        ejobs(args, bjobsargs)
    else:
        try:
            ejobs(args, bjobsargs)
        except KeyboardInterrupt:
            pass
        except Exception:
            print(color("ERROR -- probably a job status changed while " +
                        sys.argv[0] + " processed it", "r"), file=sys.stderr)


if __name__ == "__main__":
    main()
