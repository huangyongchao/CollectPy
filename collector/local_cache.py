# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-14.
# TODO: 记录任务的执行状态到.
import threading

__LOCAL_STATS = {}


def sub_local(taskid):
    if not __LOCAL_STATS.__contains__(taskid):
        __LOCAL_STATS[taskid] = {}
    return dict(__LOCAL_STATS.get(taskid))


def set_local(taskid, sub_key, value):
    l = sub_local(taskid)
    l[sub_key] = value
    __LOCAL_STATS[taskid] = l


def get_local(taskid, sub_key):
    return sub_local(taskid)[sub_key]


def has_local(taskid, sub_key):
    return sub_local(taskid).__contains__(sub_key)
