#!/usr/bin/env python
"""Argument shortcuts for ejobs/ehosts/esub."""
from __future__ import division, print_function

ejobsshortcuts = {
    "aices": ["-G", "p_aices", "-P", "aices"],
    "aices2": ["-G", "p_aices", "-P", "aices2"],
    "aices24": ["-G", "p_aices", "-P", "aices-24"],
}

ehostsshortcuts = {
    "aices": "aices",
    "aices2": "aices2",
    "aices24": "aices24",
}
