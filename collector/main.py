#!/usr/bin/python3
# TODO(huangyongchao): 2018-05-16.
# TODO: 程序入口文件.

import json
import logging
import time

import sys

from collector import web_console
from collector.collect_task import CollectTask


def init_log():
    """
    初始化log
    :return None:
    """
    filename = "../logs/collector"
    log_level = logging.INFO

    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(filename)s  %(funcName)s %(lineno)d %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=filename + '.%s.log' % time.strftime("%Y-%m-%d"))


def init_task_conf():
    """
    加载./task.json 文件里面配置的task
    :return task json obj:
    """
    logging.info("loading  tasks  file :  ../conf/task.json :")
    with open('../conf/task.json', 'r') as f:
        tasks = json.load(f)
    logging.info(tasks)
    return tasks


def task_checker(taskconf):
    """
    根据元数据检查task.json的格式和配置属性
    :param taskconf:
    :return:
    """
    # 待完善
    return True


def start_task(taskconf):
    """
    根据task的配置,逐一去启动task线程
    :param taskconf:
    :return: None
    """
    for task in list(taskconf):
        task_thread = CollectTask(task)
        task_thread.start()
    logging.info("tasks started !")


def launcher():
    """
    检查并且启动配置的任务
    :return:
    """
    init_log()

    taskconf = init_task_conf()

    if task_checker(taskconf):
        start_task(taskconf)


if __name__ == '__main__':
    # 打印环境变量目录
    print(sys.path)
    launcher()
    web_console.start_console()
