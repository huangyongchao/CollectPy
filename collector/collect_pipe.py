# -*- coding: utf-8 -*-
import datetime
import json
import logging
import threading
import traceback
import time
import pymysql
from elasticsearch import helpers, Elasticsearch
from kafka import KafkaProducer
from rediscluster import StrictRedisCluster
from DBUtils.PooledDB import PooledDB

from collector import sys_conf, json_encoder, collect_cct, local_cache, collect_task, output_mysql, output_redis
from collector.collect_filter import CollectFilter

pymysql.install_as_MySQLdb()

# 连接池的本地缓存
__conn_cache = {}
__conn_cache_state = {}


def processor(task_id, input_conf, filter_conf, outputs_conf, tricing_id, tracing_time, interval_sec, suffix):
    real_taskid = collect_task.get_real_task_id(task_id, suffix)
    time.sleep((interval_sec if (interval_sec >= 5) else 30))

    # 获取CCT
    cct = collect_cct.get_cct(real_taskid)
    if not cct:
        cct = collect_cct.get_conf_args(input_conf)
    logging.info("%s params: %s " % (real_taskid, cct.__str__()))

    # 根据 input 获取 链接 以及sql
    sql = get_mysql_fmt_sql(input_conf, cct, suffix=suffix)
    logging.info("%s has generated  sql %s: " % (real_taskid, sql))

    # 根据sql获取数据集
    datas = collect_get_input_data(real_taskid, sql, input_conf=input_conf)
    logging.info("%s  has queried data . %s" % (real_taskid, datas.__len__()))
    # 过滤数据集
    datas_filtered = collect_filter(datas, filter_conf=filter_conf)
    logging.info("%s  has filtered data . " % real_taskid)
    # 输出数据集
    collect_output(real_taskid, cct, datas_filtered, outputs_conf=outputs_conf, suffix=suffix)
    # 更新cct
    collect_cct.update_cct(real_taskid, cct, datas, tricing_id, tracing_time)
    logging.info("%s exec over . " % real_taskid)


def get_pagesize(mysql_input):
    """
    处理默认分页
    :param mysql_input:
    :return:
    """
    if dict(mysql_input).__contains__('pagesize') and int(mysql_input['pagesize']) <= sys_conf.MAX_PAGESIZE:
        page_size = mysql_input['pagesize']
    else:
        page_size = sys_conf.MAX_PAGESIZE
    return page_size


def get_limit_stmp():
    now = datetime.datetime.now()
    limit_stmp = now + datetime.timedelta(seconds=-sys_conf.DELAY_SECONDS)
    return limit_stmp.strftime('%Y-%m-%d %H:%M:%S')


def get_mysql_fmt_sql(mysql_input, cct, suffix):
    """
    根据mysql input配置以及cct获取执行的sql
    :param mysql_input:
    :param cct:
    :return:
    """
    page_size = get_pagesize(mysql_input)

    sql = str(mysql_input['sql'])
    if cct.and_id:
        condition = ("%s  = '%s' and %s  < '%s' and   %s  > %s  order by %s  asc , %s  asc    limit  %s   " % (
            mysql_input['tracingtime'], cct.tracing_time, mysql_input['tracingtime'], get_limit_stmp(),
            mysql_input['tracingid'],
            cct.tracing_id,
            mysql_input['tracingtime'],
            mysql_input['tracingid'], page_size))
        return sql.replace("{conditions}", condition).replace("{suffix}", suffix)

    else:

        condition = ("%s  >= '%s' and %s  < '%s' order by %s  asc , %s  asc    limit  %s  " % (
            mysql_input['tracingtime'], cct.tracing_time, mysql_input['tracingtime'], get_limit_stmp(),
            mysql_input['tracingtime'],
            mysql_input['tracingid'], page_size))
        return sql.replace("{conditions}", condition).replace("{suffix}", suffix)


def collect_get_input_data(taskid, sql, input_conf):
    """
    执行mysql查询，增加链接缓存 以及 mysql链接失效重连
    :param taskid:
    :param sql:
    :param input_conf:
    :return:
    """
    try:
        conn = pymysql.connect(use_unicode=True, charset="utf8", **input_conf['mysql'])
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)
        return cursor.fetchall()
    except:
        conn.ping()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)
        return cursor.fetchall()


def init_pool(taskid, sql, input_conf):
    try:
        pool_lock = threading.Lock()
        pool_lock.acquire()
        if __conn_cache.__contains__(taskid):
            pool = __conn_cache[taskid]
        else:
            pool = PooledDB(pymysql, 30, **input_conf['mysql'])  # 5为连接池里的最少连接数
            __conn_cache[taskid] = pool
    finally:
        pool_lock.release()
    return pool


def collect_filter(datas, filter_conf):
    """
    数据集过滤，目前只增加时间格式过滤，必要的可以在SQL处理
    :param datas:
    :param filter_conf:
    :return:
    """
    if dict(filter_conf).__contains__('time_fmt'):
        timefmt = (True if (filter_conf['time_fmt']) else False)
        for rec in datas:
            if timefmt:
                fmt = filter_conf['time_fmt']['fmt']
                for field in filter_conf['time_fmt']['fields']:
                    if field in rec and rec[field] != None:
                        try:
                            rec[field] = CollectFilter.time_fmt(rec[field], fmt)
                        except:
                            rec[field] = None

    if dict(filter_conf).__contains__('camel_case'):
        """
        驼峰命名暂时没有什么高效的方式实现，建议SQL字段直接 别名
        """
        camelcase = (True if (filter_conf['camel_case'] == 'true') else False)
        pass

    return datas


def collect_output(taskid, cct, datas, outputs_conf, suffix):
    task_state = "%s%s" % (str(cct.__dict__), datas.__len__())

    if (not local_cache.has_local(taskid, 'out_state')) or local_cache.get_local(taskid, 'out_state') != task_state:
        for outputconf in outputs_conf:

            if outputconf['type'] == 'elasticsearch':
                logging.info(
                    "send to elasticsearch start : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                index = outputconf['index']
                doctype = outputconf['doctype']
                docid = outputconf['docid']
                routing = None
                parent = None
                if dict(outputconf).__contains__("routing"):
                    routing = outputconf['routing']
                if dict(outputconf).__contains__("parent"):
                    parent = outputconf['parent']

                nodes = [{"host": str(x).split(":")[0], "port": str(x).split(":")[1]} for x in outputconf['nodes']]
                actions = []
                i = 1
                # if local_cache.has_local(taskid, 'es'):
                #     es = local_cache.get_local(taskid, 'es')
                # else:
                #     es = Elasticsearch(hosts=nodes,
                #                        http_auth=tuple(outputconf['auth']),
                #                        sniff_on_start=True,
                #                        sniff_on_connection_fail=True,
                #                        sniffer_timeout=60)
                #     local_cache.set_local(taskid, 'es', es)
                es = Elasticsearch(hosts=nodes,
                                   http_auth=tuple(outputconf['auth']),
                                   sniff_on_start=True,
                                   sniff_on_connection_fail=True,
                                   sniffer_timeout=60)
                local_cache.set_local(taskid, 'es', es)
                for rec in datas:
                    action = {"_index": str(index).replace("{suffix}", suffix),
                              "_type": str(doctype).replace("{suffix}", suffix), "_id": rec[docid],
                              "_source": json.dumps(rec, cls=json_encoder.OutputEncoder,
                                                    ensure_ascii=False)}
                    if routing:
                        action["_routing"] = rec[routing]
                    if parent:
                        action["_parent"] = rec[parent]
                    i += 1
                    actions.append(action)
                r, errors = helpers.bulk(es, actions, request_timeout=60)

                if errors.__len__() > 0:
                    # 如果有失败的数据，增加一次失败重试
                    try:
                        helpers.bulk(es, errors, request_timeout=60)
                        logging.error(errors)
                    except:
                        raise Exception()

                logging.info("send to elasticsearch end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            elif outputconf['type'] == 'redis':

                logging.info("send to redis start : %s  " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                output_redis.out_put(taskid, outputconf, datas)

                logging.info("send to redis end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            elif outputconf['type'] == 'kafka':
                logging.info("send to kafka start : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                bootstrap_servers = outputconf['nodes']
                topic = outputconf['topic']

                # if local_cache.has_local(taskid, 'producer'):
                #     producer = local_cache.get_local(taskid, 'producer')
                # else:
                #     producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
                #     local_cache.set_local(taskid, 'producer', producer)
                producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

                for rec in datas:
                    producer.send(str(topic).replace("{suffix}", suffix),
                                  json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False).encode())
                logging.info("send to kafka end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            elif outputconf['type'] == 'mysql':

                logging.info("send to mysql start : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                output_mysql.out_put(outputconf, datas)
                logging.info("send to mysql end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        local_cache.set_local(taskid, 'out_state', task_state)
