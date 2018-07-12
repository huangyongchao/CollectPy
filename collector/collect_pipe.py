# -*- coding: utf-8 -*-
import datetime
import json
import logging
import threading

import pymysql
from elasticsearch import helpers, Elasticsearch
from kafka import KafkaProducer
from rediscluster import StrictRedisCluster
from DBUtils.PooledDB import PooledDB

from collector import sys_conf, json_encoder, collect_cct, local_cache
from collector.collect_filter import CollectFilter

pymysql.install_as_MySQLdb()

# 连接池的本地缓存
__conn_cache = {}


def collect_get_input_data(real_taskid, sql, input_conf):
    pool = init_pool(real_taskid, sql, input_conf)
    conn = pool.connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(sql)
    return cursor.fetchall()


def init_pool(real_taskid, sql, input_conf):
    try:
        pool_lock = threading.Lock()
        pool_lock.acquire()
        if __conn_cache.__contains__(real_taskid):
            pool = __conn_cache[real_taskid]
        else:
            pool = PooledDB(pymysql, 10, **input_conf['mysql'])  # 5为连接池里的最少连接数
            __conn_cache[real_taskid] = pool
    finally:
        pool_lock.release()
    return pool


def collect_filter(datas, filter_conf):
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
        camelcase = (True if (filter_conf['camel_case'] == 'true') else False)
        pass

    return datas


def collect_output(taskid, cct, datas, outputs_conf, suffix):
    task_state = "%s%s" % (str(cct.__dict__), datas.__len__())

    if (not local_cache.has_local('out_state')) or local_cache.get_local('out_state') != task_state:
        for outputconf in outputs_conf:

            if outputconf['type'] == 'elasticsearch':
                logging.info(
                    "send to elasticsearch start : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                index = outputconf['index']
                doctype = outputconf['doctype']
                docid = outputconf['docid']
                nodes = [{"host": str(x).split(":")[0], "port": str(x).split(":")[1]} for x in outputconf['nodes']]
                actions = []
                i = 1
                if local_cache.has_local('es'):
                    es = local_cache.get_local('es')
                else:
                    es = Elasticsearch(hosts=nodes,
                                       http_auth=tuple(outputconf['auth']),
                                       sniff_on_start=True,
                                       sniff_on_connection_fail=True,
                                       sniffer_timeout=500)
                    local_cache.set_local('es', es)

                for rec in datas:
                    action = {"_index": str(index).replace("{suffix}", suffix),
                              "_type": str(doctype).replace("{suffix}", suffix), "_id": rec[docid],
                              "_source": rec}
                    i += 1
                    actions.append(action)
                    if len(actions) == 10000:
                        helpers.bulk(es, actions)
                        del actions[0:len(actions)]

                if len(actions) > 0:
                    helpers.bulk(es, actions, {"timeout": 300})
                logging.info("send to elasticsearch end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            elif outputconf['type'] == 'redis':
                logging.info("send to redis start : %s  " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                nodes = outputconf['nodes']

                if local_cache.has_local('rc'):
                    rc = local_cache.get_local('rc')
                else:
                    rc = StrictRedisCluster(
                        startup_nodes=nodes, skip_full_coverage_check=True,
                        max_connections=sys_conf.CCT_REDIS_MAX_CONNECTIONS)
                    local_cache.set_local('rc', rc)

                datatype = outputconf['datatype']
                key = outputconf['key']
                keyfields = outputconf['keyfields']
                hkey = outputconf['hkey']
                hkeyfields = outputconf['hkeyfields']
                valuetype = outputconf['valuetype']
                expiresec = outputconf['expiresec']
                for rec in datas:

                    realk = key
                    n = str(key).count("%s")
                    if n > 0:
                        kf = ''
                        for f in keyfields[0:n]:
                            kf += ",'" + ("" if (rec[f] == None) else rec[f]) + "'"
                        realk = eval("'" + key + "'" + " %( " + kf[1: kf.__len__()] + ") ")

                    realk = str(realk).replace("{suffix}", suffix)

                    if datatype == "string":
                        if valuetype == "json":
                            if expiresec and (expiresec > 0):
                                rc.set(realk,
                                       json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False).encode(),
                                       ex=expiresec)
                            else:
                                rc.set(realk,
                                       json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False).encode())


                    elif datatype == "hash":
                        if valuetype == "json":
                            pass
                    elif datatype == "list":
                        if valuetype == "json":
                            pass
                    elif datatype == "set":
                        if valuetype == "json":
                            pass

                logging.info("send to redis end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            elif outputconf['type'] == 'kafka':
                logging.info("send to kafka start : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                bootstrap_servers = outputconf['nodes']
                topic = outputconf['topic']

                if local_cache.has_local('producer'):
                    producer = local_cache.get_local('producer')
                else:
                    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
                    local_cache.set_local('producer', producer)

                for rec in datas:
                    producer.send(str(topic).replace("{suffix}", suffix),
                                  json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False).encode())
                logging.info("send to kafka end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            else:
                pass
        local_cache.set_local('out_state', task_state)
