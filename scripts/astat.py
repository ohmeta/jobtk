#!/usr/bin/env python

import subprocess
import sys
import re
import os

cmd = "qstat -j %s" % str(sys.argv[1])
output = subprocess.getoutput(cmd).split("\n")

is_array_job = False
work_dir = ""
prefix = ""
script_file = ""
sh_files = []
o_files = []
e_files = []


def print_content(file_path, format):
    if not format.endswith("p"):
        if os.path.exists(file_path):
            ih = open(file_path, 'r')
            print(ih.read().rstrip())
            ih.close()
        else:
            print("%s does not exists!" % file_path)
    else:
        print(file_path)


for line in output:
    if line.startswith("sge_o_workdir"):
        work_dir = re.split(r"\s+", line)[-1]
        continue
    if line.startswith("stdout_path_list"):
        prefix = re.split(r":|\$", line)[3]
        continue
    if line.startswith("script_file"):
        script_file = os.path.join(work_dir, re.split(r"\s+", line)[-1])
        continue
    if line.startswith("job-array"):
        is_array_job = True
        continue
    if line.startswith("usage") and is_array_job:
        num = re.split(r'\s+', line)[1].rstrip(":")
        sh_files.append("%s%s.sh" % (os.path.join(work_dir, prefix), num))
        o_files.append("%s%s.o" % (os.path.join(work_dir, prefix), num))
        e_files.append("%s%s.e" % (os.path.join(work_dir, prefix), num))
        continue

if len(sys.argv) > 2:
    format = sys.argv[2]
    if is_array_job:
        for sh_file, o_file, e_file in zip(sh_files, o_files, e_files):
            if format == "sh" or format == "shp":
                print_content(sh_file, format)
            elif format == "o" or format == "op":
                print_content(o_file, format)
            elif format == "e" or format == "ep":
                print_content(e_file, format)
            else:
                print("please: sh | shp | o | op | e | ep")
    else:
        print_content(script_file, format)
else:
    if is_array_job:
        for sh_file in sh_files:
            print(sh_file)
    else:
        print(script_file)
