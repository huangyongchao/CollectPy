from rediscluster import StrictRedisCluster

from collector import local_cache, sys_conf


def out_put(taskid, outputconf, datas):
    nodes = outputconf['nodes']

    rc = StrictRedisCluster(
        startup_nodes=nodes, skip_full_coverage_check=True,
        max_connections=sys_conf.CCT_REDIS_MAX_CONNECTIONS)
    multicmds = outputconf['multi-cmds']
    for data in datas:
        for cmds in multicmds:
            if dict(cmds).__contains__("condition"):

                condition = dict(cmds["condition"])
                if (eval(str(data[condition["field"]]) + str(condition["op"]) + str(condition["value"]))):
                    exec_cmds(rc=rc, obj=data, cmds=list(cmds["commands"]))

            else:
                exec_cmds(rc=rc, obj=data, cmds=list(cmds["commands"]))


def exec_cmds(rc, obj, cmds):
    for command in cmds:
        fun = command["cmd"]
        args = dict(command["args"])
        for k in args.keys():
            argstr = argstr + k + "='" + str(args[k]) + "',"
        eval("rc." + fun + "(" + argstr[0:argstr.__len__() - 1] + ")")


def convert_value(fun, k, v):
    if fun == "del":
        fun = "delete"
    if k == "values":
        v = list(v)

        # "key": "{suffix}_order:%s%s",
        # "keyfields": ["order_no", "product_no"],


        # datatype = outputconf['datatype']
        # key = outputconf['key']
        # keyfields = outputconf['keyfields']
        # hkey = outputconf['hkey']
        # hkeyfields = outputconf['hkeyfields']
        # valuetype = outputconf['valuetype']
        # expiresec = outputconf['expiresec']
        # for rec in datas:
        #
        #     realk = key
        #     n = str(key).count("%s")
        #     if n > 0:
        #         kf = ''
        #         for f in keyfields[0:n]:
        #             kf += ",'" + ("" if (rec[f] == None) else rec[f]) + "'"
        #         realk = eval("'" + key + "'" + " %( " + kf[1: kf.__len__()] + ") ")
        #
        #     realk = str(realk).replace("{suffix}", suffix)
        #
        #     if datatype == "string":
        #         if valuetype == "json":
        #             if expiresec and (expiresec > 0):
        #                 rc.setex(name=realk, time=expiresec,
        #                          value=json.dumps(rec, cls=json_encoder.OutputEncoder,
        #                                           ensure_ascii=False))
        #             else:
        #                 rc.set(realk,
        #                        json.dumps(rec, cls=json_encoder.OutputEncoder, ensure_ascii=False))
        #
        #     elif datatype == "hash":
        #         if valuetype == "json":
        #             pass
        #     elif datatype == "list":
        #         if valuetype == "json":
        #             pass
        #     elif datatype == "set":
        #         if valuetype == "json":
