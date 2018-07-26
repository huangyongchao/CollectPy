# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-16.
# TODO: task线程类.
import logging
import threading
import traceback

from multiprocessing import Process

from collector import collect_pipe


def get_real_task_id(task_id, suffix):
    return "%s-%s" % (task_id, suffix)


def run_task(task, suffix):
    task_id = task['taskid']
    group = "collector"
    name = "%s-%s-%s" % (group, task_id, suffix)

    p = Process(name=name, target=exe_task,
                args=(task, suffix))
    p.start()
    print(p.name, 'process started')


def exe_task(task, suffix):
    task_id = task['taskid']
    input_conf = task['input']
    filter_conf = task['filter']
    outputs_conf = task['outputs']
    tricing_id = task['input']['tracingid']
    tracing_time = task['input']['tracingtime']
    interval_sec = task['interval_sec']
    suffix = suffix
    while True:
        try:
            collect_pipe.processor(task_id, input_conf, filter_conf, outputs_conf, tricing_id, tracing_time,
                                   interval_sec, suffix)
        except Exception as e:
            msg = traceback.format_exc()
            print("%s exec error  %s " % (task_id, msg))
            logging.error("%s exec error  %s " % (task_id, msg))
            continue
