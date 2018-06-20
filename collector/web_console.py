# -*- coding: utf-8 -*-
from wsgiref.simple_server import make_server

# 自己编写的application函数:

from collector import sys_conf, main, collect_cct


def application(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])
    tasks = main.init_task_conf()
    taskids = [t["taskid"] for t in tasks]

    body = ''
    for tid in taskids:
        body += "<b>" + tid + ":</b>  "
        body += str((collect_cct.get_cct() .REDIS_CON.get(sys_conf.CCT_PREFIX + tid)))

    return [body.encode('utf-8')]


def start_console():
    # 创建一个服务器，IP地址为空，端口是8000，处理函数是application:
    httpd = make_server('127.0.0.1', sys_conf.CONSOLE_PORT, application)
    print('WebConsole Serving HTTP on 127.0.0.1:%s...' % sys_conf.CONSOLE_PORT)
    # 开始监听HTTP请求:
    httpd.serve_forever()
