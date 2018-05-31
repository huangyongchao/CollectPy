# encoding=utf-8
# TODO(huangyongchao): 2018-05-16.
# TODO: 程序级别的配置信息.
# redis cluster的nodes
CCT_REDIS_NODES = [{"host": "221.122.127.103", "port": "14000"}, {"host": "221.122.127.127", "port": "14000"}]
# redis的key的过期时间,单位s
CCT_REDIS_EXPIRETIME = 3600 * 300
CCT_REDIS_MAX_CONNECTIONS = 50
CCT_PREFIX = "collect_py_"
