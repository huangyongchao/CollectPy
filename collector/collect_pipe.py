# -*- coding: utf-8 -*-
import MySQLdb
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
                if rec[field]:
                    rec[field] = CollectFilter.time_fmt(rec[field], fmt)
        if camelcase:
            pass
    return datas


def collect_output(datas, outputs_conf):
    for outputconf in outputs_conf:

        if outputconf['type'] == 'elasticsearch':
            index = outputconf['index']
            doctype = outputconf['doctype']
            docid = outputconf['docid']
            nodes = [{"host": str(x).split(":")[0], "port": str(x).split(":")[1]} for x in outputconf['nodes']]
            actions = []
            i = 1
            es = Elasticsearch(nodes,
                               http_auth=tuple(outputconf['auth']))
            for rec in datas:
                action = {"_index": index, "_type": doctype, "_id": rec[docid], "_source": rec}
                i += 1
                actions.append(action)
                if len(actions) == 2000:
                    helpers.bulk(es, actions)
                    del actions[0:len(actions)]

            if len(actions) > 0:
                helpers.bulk(es, actions)

        elif outputconf['type'] == 'redis':
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
                if datatype == "string":
                    realk = key
                    n = str(key).count("%s")
                    if n > 0:
                        kf = ''
                        for f in keyfields[0:n]:
                            kf += ",'" + rec[f]+"'"
                        realk = eval( "'" + key+ "'"+ " %( " + kf[1 : kf.__len__()] + ") ")

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


        elif outputconf['type'] == 'kafka':
            bootstrap_servers = outputconf['nodes']
            topic = outputconf['topic']
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            for rec in datas:
                producer.send(topic, json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False).encode())

        else:
            pass
