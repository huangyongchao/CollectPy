# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-16.
# TODO: task线程类.
import logging
import threading
import traceback

from collector import collect_pipe


class CollectTask(threading.Thread):

    def __init__(self, task, suffix):
        threading.Thread.__init__(self)
        self.task = task
        self.suffix = suffix

    def _stop(self):
        super()._stop()

    def run(self):
        task_id = self.task['taskid']
        input_conf = self.task['input']
        filter_conf = self.task['filter']
        outputs_conf = self.task['outputs']
        tricing_id = self.task['input']['tracingid']
        tracing_time = self.task['input']['tracingtime']
        interval_sec = self.task['interval_sec']
        suffix = self.suffix
        while True:

            try:
                collect_pipe.processor(task_id, input_conf, filter_conf, outputs_conf, tricing_id, tracing_time,
                                       interval_sec, suffix)
            except Exception as e:
                msg = traceback.format_exc()
                print("%s exec error  %s " % (task_id, msg))
                logging.error("%s exec error  %s " % (task_id, msg))
                continue


