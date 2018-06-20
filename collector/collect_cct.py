# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-14.
# TODO: 记录任务的执行状态到threadloacl 以及 redis集群里.

from datetime import datetime, timedelta

from rediscluster import StrictRedisCluster
import threading
from collector import sys_conf

REDIS_CON = StrictRedisCluster(
    startup_nodes=sys_conf.CCT_REDIS_NODES,
    skip_full_coverage_check=True,
    max_connections=sys_conf.CCT_REDIS_MAX_CONNECTIONS)

CCT_LOCAL = threading.local()


def get_key(task_id):
    return (sys_conf.CCT_PREFIX + "%s") % (task_id)


def set_cct(task_id, cct):
    """设置上下文状态
    Args:
        task_id: 收集任务的标志
        cct: 上下文状态对象
    Raises:
        IOError: An error occurred accessing the bigtable.Table object.

    """
    CCT_LOCAL.cct = cct.__dict__;
    REDIS_CON.set(get_key(task_id), cct.__dict__, sys_conf.CCT_REDIS_EXPIRETIME)


def get_cct(task_id):
    """获取上下文状态

    Args:
        task_id: 收集任务的标志
    Returns:
        A _CctState obj 上下文状态对象
    Raises:
        IOError: An error occurred accessing the bigtable.Table object.

    """
    cct = _CctState('', '', False)
    try:
        if CCT_LOCAL.cct:
            cct.__dict__ = CCT_LOCAL.cct
            return cct
    except:
        pass

    cache_cct = REDIS_CON.get(get_key(task_id))
    if cache_cct:

        cct.__dict__ = eval(cache_cct)
        return cct
    else:
        return None


def get_conf_args(input_dict):
    """
    根据配置的初始化参数返回默认CCT
    :param input_dict:
    :return: cct
    """
    return _CctState(input_dict['tracingid_val'], input_dict['tracingtime_val'], False)


def update_cct(taskid, cct, datas, tricing_id, tracingtime):
    """
    cct -> new cct
    1 datas = []  cct.and_id  :True
      tracing_time +1sec

    2 datas = []  cct.and_id  :False
      pass

    3 datas !=[]
      cct.and_id  : True
      cct.tracing_id=max(datas.id)

    4 datas  !=[]
      cct.and_id  : False
      cct.tracing_id=-1
      cct.tracing_time=max(datas.tracing_time)

    :param cct:
    :param mysqldatas:
    :param tracing:
    :return:
    """
    dl = len(datas)
    if dl <= 0:
        if cct.and_id:
            cct.tracing_time = str(datetime.strptime(cct.tracing_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1))
            cct.tracing_id = -1
            cct.and_id = False
        else:
            pass
    else:
        minstmp = datas[0][tracingtime]
        maxstmp = datas[dl - 1][tracingtime]
        maxid = datas[dl - 1][tricing_id]
        if minstmp == maxstmp:
            cct.and_id = True
            cct.tracing_time = minstmp
            cct.tracing_id = maxid
        else:
            cct.and_id = False
            cct.tracing_time = maxstmp
            cct.tracing_id = -1
    set_cct(taskid, cct)
    return cct


class _CctState(object):
    """存储执行状态的上下文的实体类
    Attributes:
        tracing_id: mysql作为input的时候查询条件里的ID
        tracing_time: mysql作为input的时候查询条件里的时间戳
        and_id:  boolean 为True 表示 sql查询的条件里 id  and  timestamp False 时候只依赖timestamp 一个条件.

    """

    tracing_id = -1
    tracing_time = '1970-01-01 00:00:00'
    and_id = False

    def __init__(self, tracing_id, tracing_time, and_id):
        self.tracing_id = tracing_id
        self.and_id = and_id
        self.tracing_time = tracing_time


def main():
    """自定义main函数"""
    pass


if __name__ == '__main__':
    main()
