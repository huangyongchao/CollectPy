# encoding=utf-8
import yaml

# TODO(huangyongchao): 2018-05-16.
# TODO: 程序级别的配置信息.
# redis cluster的nodes
__f = open('../conf/application.yaml')

# 导入
__appconf = yaml.load(__f)
CONSOLE_PORT = __appconf.get("webconsole")["port"]
LOGGING_LEVEL = __appconf.get("logging")["level"]
CCT_REDIS_NODES = __appconf.get("cctredis")["nodes"]
# redis的key的过期时间,单位s
CCT_REDIS_EXPIRETIME = __appconf.get("cctredis")["expire_seconds"]
CCT_REDIS_MAX_CONNECTIONS = __appconf.get("cctredis")["max_connections"]
CCT_PREFIX = __appconf.get("cctredis")["prefix"]
