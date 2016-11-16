"""Sort the jobs in groups by attributes."""

from collections import defaultdict


def grouphosts(jobs, key):
    """Sort the jobs in groups by attributes."""
    result = defaultdict(list)
    for job in jobs:
        if isinstance(job[key], dict):
            for val in job[key]:
                result[val].append(job)
        else:
            result[job[key]].append(job)
    return dict(result)
