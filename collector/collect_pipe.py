# -*- coding: utf-8 -*-
import logging

import MySQLdb
import datetime
from elasticsearch import helpers, Elasticsearch
from kafka import KafkaProducer
import json

from rediscluster import StrictRedisCluster

from collector import sys_conf, json_encoder

from collector.collect_filter import CollectFilter


def collect_get_input_data(sql, input_conf):
    conn = MySQLdb.connect(use_unicode=True, charset='utf8', **input_conf['mysql'])
    cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    return cursor.fetchall()


def collect_filter(datas, filter_conf):
    timefmt = (True if (filter_conf['time_fmt']) else False)
    camelcase = (True if (filter_conf['camel_case'] == 'true') else False)
    for rec in datas:
        if timefmt:
            fmt = filter_conf['time_fmt']['fmt']
            for field in filter_conf['time_fmt']['fields']:
                if field in rec and rec[field] != None:
                    try:
                        rec[field] = CollectFilter.time_fmt(rec[field], fmt)
                    except:
                        rec[field] = None
        if camelcase:
            pass
    return datas


def collect_output(datas, outputs_conf, suffix):
    for outputconf in outputs_conf:

        if outputconf['type'] == 'elasticsearch':
            logging.info("send to elasticsearch start : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            index = outputconf['index']
            doctype = outputconf['doctype']
            docid = outputconf['docid']
            nodes = [{"host": str(x).split(":")[0], "port": str(x).split(":")[1]} for x in outputconf['nodes']]
            actions = []
            i = 1
            es = Elasticsearch(nodes,
                               http_auth=tuple(outputconf['auth']))
            for rec in datas:
                action = {"_index": str(index).replace("{suffix}", suffix),
                          "_type": str(doctype).replace("{suffix}", suffix), "_id": rec[docid],
                          "_source": rec}
                i += 1
                actions.append(action)
                if len(actions) == 2000:
                    helpers.bulk(es, actions)
                    del actions[0:len(actions)]

            if len(actions) > 0:
                helpers.bulk(es, actions)
            logging.info("send to elasticsearch end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        elif outputconf['type'] == 'redis':
            logging.info("send to redis start : %s  " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            nodes = [{"host": str(x).split(":")[0], "port": str(x).split(":")[1]} for x in outputconf['nodes']]
            rc = StrictRedisCluster(
                startup_nodes=nodes,
                decode_responses=True,
                max_connections=sys_conf.CCT_REDIS_MAX_CONNECTIONS)

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
                            rc.set(realk, json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False).encode(),
                                   ex=expiresec)
                        else:
                            rc.set(realk, json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False).encode())


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
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            for rec in datas:
                producer.send(str(topic).replace("{suffix}", suffix),
                              json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False).encode())
            logging.info("send to kafka end : %s " % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        else:
            pass
