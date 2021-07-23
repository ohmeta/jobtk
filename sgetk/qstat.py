#!/usr/bin/env python

from typing import OrderedDict
import pandas as pd
import numpy as np
import subprocess as subp
from io import StringIO
import xmltodict
import sgetk
from pprint import pprint
import xml.dom.minidom
import lxml.etree as etree


def combine_string(str_old, str_list=[
        '-xml', '-ext', '-r', '-t', '-pri']):
    str_new = str_old
    for i in str_list:
        if i not in str_new:
            str_new += f" {i}"

    return str_new


def qstat2xml(qstat_cmd, str_list=[
        '-xml', '-ext', '-r', '-t', '-pri']):
    """
    Returns
    -------
    qstatxml : string
        The xml stdout string of the 'qstat -xml' call

    -xml: display the information in XML format
    -ext: view additional attributes
      -r: show requested resources of job(s)
      -t: show task information (implicitly -g t)
      -pri: display job priority information

    ["-xml", "-ext", "-r", "-t", "-pri"]
    """
    qstat_cmd_new = combine_string(qstat_cmd, str_list)
    try:
        qstatxml = subp.check_output(qstat_cmd_new,
                                     shell=True, stderr=(subp.STDOUT))
    except subp.CalledProcessError as e:
        try:
            print('qstat returncode: ', e.returncode)
            print('qstat std output: ', e.output)
            raise
        finally:
            e = None
            del e

    return qstatxml


def xml2data_frame(xml_str, query_key='job_list'):
    """
    xml_str is string, xmltodict.parse can parse: string, a file-like object, or a generator of strings
    when search job info, use "job_list"
    when search query info, use "Queue-List"
    """
    x = xmltodict.parse(xml_str)

    queue_info = x['job_info']['queue_info']
    job_info = x['job_info']['job_info']

    queue_df = pd.DataFrame()
    job_df = pd.DataFrame()

    if queue_info is not None:
        if query_key in queue_info:
            if isinstance(queue_info[query_key], list):
                queue_df = pd.DataFrame(queue_info[query_key])
            else:
                queue_df = pd.DataFrame([queue_info[query_key]])
        else:
            print(f"{query_key} is not in xml output, please check")
    if job_info is not None:
        if query_key in job_info:
            if isinstance(job_info[query_key], list):
                job_df = pd.DataFrame(job_info[query_key])
            else:
                job_df = pd.DataFrame([job_info[query_key]])
        else:
            print(f"{query_key} is not in xml output, please check")

    type_dict = {
        '@state': str,
        'cpu_usage': float,
        'mem_usage': float,
        'io_usage': float,
        'slots': float,
        'JAT_prio': float,
        'hard_req_queue': str,
        'JB_owner': str,
        'JB_department': str,
        'JB_project': str
    }
    type_dict_ = {}
    all_df = pd.concat([queue_df, job_df])
    for i in type_dict:
        if i in all_df.columns:
            type_dict_[i] = type_dict[i]
    all_df = all_df.astype(type_dict_)
    return all_df


def qstat(qstat_cmd, query_key='job_list', str_list=['-xml', '-ext', '-r', '-t', '-pri']):
    xml_str = qstat2xml(qstat_cmd, str_list)
    return xml2data_frame(xml_str, query_key)


def extract_mem_core(x):
    """
    Args:
      x:
        [OrderedDict([('@name', 'num_proc'),
                      ('@resource_contribution', '800.000000'),
                      ('#text', '8')])

         OrderedDict([('@name', 'virtual_free'),
                      ('@resource_contribution', '0.000000'),
                      ('#text', '5g')])]

        [OrderedDict([('@name', 'high_priority'),
                      ('@resource_contribution', '100000.000000'),
                      ('#text', 'TRUE')]),
         OrderedDict([('@name', 'num_proc'),
                      ('@resource_contribution', '400.000000'),
                      ('#text', '4')]),
         OrderedDict([('@name', 'virtual_free'),
                      ('@resource_contribution', '0.000000'),
                      ('#text', '10.5g')])]
    """
    core = 0
    mem = 0
    if isinstance(x, list):
        for i in x:
            if isinstance(i, OrderedDict):
                if i['@name'] == 'num_proc':
                    core = int(i['#text'])
                elif i['@name'] == 'virtual_free':
                    mem_str = str(i['#text'])
                    if mem_str[(-1)].isdigit():
                        mem_str += 'B'
                    mem = sgetk.sge_summary.human2bytes(mem_str)
            else:
                print('detect non ordered dict')
                pprint(i)

    elif isinstance(x, OrderedDict):
        if x['@name'] == 'num_proc':
            core = int(x['#text'])
        elif x['@name'] == 'virtual_free':
            mem_str = str(x['#text'])
            if mem_str[(-1)].isdigit():
                mem_str += 'B'
            mem = sgetk.sge_summary.human2bytes(mem_str)
    return (
        mem, core)


def extract_mem_core_v2(x):
    core = 0
    mem = ''
    for i in x:
        if i['@name'] == 'num_proc':
            core = int(i['#text'])
        if i['@name'] == 'virtual_free':
            mem = str(i['#text'])

    return (
        mem, core)


def user_running_job_info(x):
    job_count = 0.0
    for i in x['slots']:
        job_count += float(i)

    job_host = []
    for i in x['queue_name']:
        job_host.append(i)

    cpu_usage = sum(x['cpu_usage'])
    cpu_usage_average = cpu_usage / job_count
    mem_usage = sum(x['mem_usage'])
    mem_usage_average = mem_usage / job_count
    io_usage = sum(x['io_usage'])
    io_usage_average = io_usage / job_count
    JAT_prio_average = sum(x['JAT_prio']) / job_count
    mem_request = 0.0
    core_request = 0.0
    for i in x['hard_request']:
        mem, core = extract_mem_core(i)
        core_request += core
        mem_request += mem

    mem_request_average = mem_request / job_count
    core_request_average = core_request / job_count
    core_binding = 0.0
    for i in x['binding']:
        if i is not None:
            try:
                str_int = float(i.split(':')[(-1)])
            except:
                print(f'''error core binnind: {x["JB_owner"]} {x["JB_job_number"]} {i}''')
                core_binding += 1
        else:
            core_binding += 1

    core_binding_average = core_binding / job_count
    return pd.Series({'job_count': job_count,  'job_host': job_host,
                      'JAT_prio_average': JAT_prio_average,
                      'cpu_usage_total(hour)': cpu_usage / 3600,
                      'cpu_usage_per_job(hour)': cpu_usage_average / 36000,
                      'cpu_usage_average_per_job_per_core(hour)': cpu_usage_average / core_request_average / 3600,
                      'mem_usage_total': sgetk.sge_summary.bytes2human(mem_usage) if not pd.isna(mem_usage) else 0,
                      'mem_usage_average': sgetk.sge_summary.bytes2human(mem_usage_average) if not pd.isna(mem_usage_average) else 0,
                      'mem_request_total': sgetk.sge_summary.bytes2human(mem_request) if not pd.isna(mem_request) else 0,
                      'mem_request_average': sgetk.sge_summary.bytes2human(mem_request_average) if not pd.isna(mem_request_average) else 0,
                      'core_request_total': core_request,
                      'core_request_average': core_request_average,
                      'core_binding': core_binding,
                      'core_binding_average': core_binding_average,
                      'io_usage_total': io_usage,
                      'io_usage_average': io_usage_average})


def pretty_xml(xml_str):
    x = etree.fromstring(xml_str)
    print(etree.tostring(x, pretty_print=True))


def print_xml(xml_str):
    print(xml.dom.minidom.parseString(xml_str).toprettyxml())
# okay decompiling sgetk/__pycache__/qstat.cpython-37.pyc
