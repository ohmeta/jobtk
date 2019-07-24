#!/usr/bin/env python

from __future__ import print_function
import sys
import pydoc

for path in sys.path:
    print(path)

if not sys.argv[1] is None:
    print(pydoc.help(sys.argv[1]))
