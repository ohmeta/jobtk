#!/usr/bin/env bash
# get st.q queue compute node resource information

qstat -u \* | \
    tail -n +3 | \
    awk -F'[ @]+' '$5=="r" && $8=="st_supermem.q" {print $9}' | \
    awk -F'.' '{print $1}' | \
    sort -V | \
    uniq | \
    tr '\n' ' ' | \
    xargs -I xxx echo qhost -h xxx | \
    xargs -I xxx bash -c xxx | \
    python3 `dirname $0`/sge_summary.py
