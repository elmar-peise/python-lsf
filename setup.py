#!/usr/bin/env python
from distutils.core import setup

setup(name="lsf",
      version="1.2",
      description="LSF job scheduler utilities",
      author="Elmar Peise",
      author_email="peise@aices.rwth-aachen.de",
      url="http://github.com/elmar-peise/python-lsf",
      packages=["lsf"],
      scripts=["scripts/ejobs", "scripts/ehosts", "scripts/esub"]
      )
