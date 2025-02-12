#!/usr/bin/env python

import re
import subprocess

cmd = "qstat -F vf,p -q st.q"
output = re.split(r'---+', subprocess.getoutput(cmd))

print("\t".join(re.split(r'\s+', output[0]) + ["num_proc", "virtual_free"]))

for line in output[1:]:
    line_list = line.strip().split("\n")
    first_line = re.split('\s+', line_list[0].strip())
    if len(first_line) == 6:
        num_proc = line_list[1].split("=")[-1]
        virtual_free = line_list[2].split("=")[-1]
        print("\t".join(first_line + [num_proc, virtual_free]))
