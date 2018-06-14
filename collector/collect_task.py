# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-16.
# TODO: task线程类.
import logging
import threading
import time
import traceback

from collector import collect_cct, collect_pipe


class CollectTask(threading.Thread):

    def __init__(self, task, suffix):
        threading.Thread.__init__(self)
        self.task = task
        self.suffix = suffix

    def _stop(self):
        super()._stop()

    def run(self):
        taskid = self.task['taskid']
        input_conf = self.task['input']
        filter_conf = self.task['filter']
        outputs_conf = self.task['outputs']
        tricing_id = self.task['input']['tracingid']
        tracingtime = self.task['input']['tracingtime']
        interval_sec = self.task['interval_sec']
        suffix = self.suffix
        while True:

            try:
                real_taskid = "%s-%s" % (taskid, suffix)
                time.sleep((interval_sec if (interval_sec > 5) else 30))

                # 获取CCT
                cct = collect_cct.get_cct(real_taskid)
                if not cct:
                    cct = collect_cct.get_conf_args(input_conf)
                logging.info("%s params: %s " % (real_taskid, cct.__dict__.__str__()))
                # 根据 input 获取 链接 以及sql
                sql = self.get_mysql_fmt_sql(input_conf, cct, suffix=suffix)
                logging.info("%s has generated  sql %s: " % (real_taskid, sql))
                # 根据sql获取数据集
                datas = collect_pipe.collect_get_input_data(sql, input_conf=input_conf)
                logging.info("%s  has queried data . " % real_taskid)

                # 过滤数据集
                datas_filtered = collect_pipe.collect_filter(datas, filter_conf=filter_conf)

                logging.info("%s  has filtered data . " % real_taskid)

                # 输出数据集
                collect_pipe.collect_output(datas_filtered, outputs_conf=outputs_conf)
                # 更新cct
                collect_cct.update_cct(real_taskid, cct, datas, tricing_id, tracingtime)
                logging.info("%s exec over . " % real_taskid)
            except Exception as e:
                msg = traceback.format_exc()
                print("%s exec error  %s " % (real_taskid, msg))
                logging.error("%s exec error  %s " % (real_taskid, msg))

    def get_mysql_fmt_sql(self, mysql_input, cct, suffix):
        """
        根据mysql input配置以及cct获取执行的sql
        :param mysql_input:
        :param cct:
        :return:
        """
        sql = str(mysql_input['sql'])
        if cct.and_id:
            condition = ("%s  = '%s' and  %s  > %s  order by %s  asc , %s  asc    limit  %s   " % (
                mysql_input['tracingtime'], cct.tracing_time, mysql_input['tracingid'], cct.tracing_id,
                mysql_input['tracingtime'],
                mysql_input['tracingid'], mysql_input['pagesize']))
            return sql.replace("{conditions}", condition).replace("{suffix}", suffix)

        else:

            condition = ("%s  >= '%s'  order by %s  asc , %s  asc    limit  %s  " % (
                mysql_input['tracingtime'], cct.tracing_time, mysql_input['tracingtime'],
                mysql_input['tracingid'], mysql_input['pagesize']))
            return sql.replace("{conditions}", condition).replace("{suffix}", suffix)
