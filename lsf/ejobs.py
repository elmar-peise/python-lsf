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
        bjobsargs = ["-P", "aices", "-G", "p_aices"] + bjobsargs
    if args.aices2:
        bjobsargs = ["-P", "aices2", "-G", "p_aices"] + bjobsargs
    if args.a:
        bjobsargs += ["-a"]

    if sys.stdout.isatty():
        print("Reading job list from LSF ...", end="\r")
    sys.stdout.flush()
    joblist = Joblist(bjobsargs)
    if sys.stdout.isatty():
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
                            parallel=not args.nopar, header=not args.noheader)
        return
    for reasons in sorted(joblists.keys(), key=len):
        pendjobs = joblists[reasons]
        if len(reasons) == 1 and reasons[0][1] is True:
            if reasons[0][0] in (
                "New job is waiting for scheduling",
                "Dependency condition invalid or never satisfied",
                "The schedule of the job is postponed for a while",
                "Job array has reached its running element limit",
                "The queue's pre-exec command exited with non-zero status",
            ):
                title = "{} [{}]".format(reasons[0][0], len(pendjobs))
                pendjobs.display(args.long, args.wide, title,
                                 parallel=not args.nopar,
                                 header=not args.noheader)
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
                             parallel=not args.nopar, header=not args.noheader)
            print()
            print("Pending reasons:")
            cs = {
                "Running an exclusive job": "y",
                "Job's requirement for exclusive execution not satisfied": "y",
                "An exclusive job has reserved the host": "y",
                "Job slot limit reached": "y",
                "Not enough processors to meet the job's spanning requirement":
                "y",
                "Not enough slots or resources for whole duration of the job":
                "r",
                "Not enough hosts to meet the job's spanning requirement": "r",
                "Job requirements for reserving resource (mem) not satisfied":
                "r",
            }
            for reason, count in reasons:
                s = reason
                if reason in cs:
                    s = color(reason, cs[reason])
                if count is True:
                    print("           " + s)
                else:
                    print("    {:>4}  ".format(count) + s)
            if case[1]:
                req = list(case[1])
            else:
                req = case[0]
                req = re.sub(" && \(hostok\)", "", req)
                req = re.sub(" && \(mem>\d+\)", "", req)
                req = ["-R", req]
            print("Potential hosts:")
            if sys.stdout.isatty():
                print("Reading host list from LSF ...", end="\r")
            sys.stdout.flush()
            hl = Hostlist(req)
            hl.sort()
            print("                              ", end="\r")
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
        help="short for -P aices",
        action="store_true",
    )
    parser.add_argument(
        "-aices2",
        help="short for -P aices2",
        action="store_true",
    )
    parser.add_argument(
        "--nopar",
        help="faster response, longer runtime",
        action="store_true",
    )
    parser.add_argument(
        "--debug",
        help="show debug info on errors",
        action="store_true",
    )
    parser.add_argument(
        "--noheader",
        help=argparse.SUPPRESS,
        action="store_true",
    )
    parser.add_argument(
        "--noa",
        help=argparse.SUPPRESS,
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
        except IOError:
            pass
        except Exception:
            print(color("ERROR -- probably a job status changed while " +
                        sys.argv[0] + " processed it", "r"), file=sys.stderr)


if __name__ == "__main__":
    main()
