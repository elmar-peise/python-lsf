#!/usr/bin/env python
from distutils.core import setup

setup(name="lsf",
      version="1.1",
      description="LSF job scheduler utilities",
      author="Elmar Peise",
      author_email="peise@aices.rwth-aachen.de",
      packages=["lsf"],
      scripts=["scripts/ejobs", "scripts/ehosts"]
      )
