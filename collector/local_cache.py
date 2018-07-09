# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-14.
# TODO: 记录任务的执行状态到.
import threading

__LOCAL_STATS = {}


def __sub_local():
    t_id = str(threading.current_thread().ident)
    if not __LOCAL_STATS.__contains__(t_id):
        __LOCAL_STATS[t_id] = {}
    return dict(__LOCAL_STATS.get(t_id))


def set_local(sub_key, value):
    t_id = str(threading.current_thread().ident)
    l = __sub_local()
    l[sub_key] = value
    __LOCAL_STATS[t_id] = l


def get_local(sub_key):
    return __sub_local()[sub_key]


def has_local(sub_key):
    return __sub_local().__contains__(sub_key)
