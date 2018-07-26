# -*- coding: utf-8 -*-
from wsgiref.simple_server import make_server

# 自己编写的application函数:

from collector import sys_conf, main, collect_cct, collect_task, local_cache


def application(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])
    tasks = main.init_task_conf()
    body = ''
    for task in list(tasks):
        if "suffix" in task["input"]:
            for suffix in task["input"]["suffix"]:
                real_taskid = collect_task.get_real_task_id(task["taskid"], suffix)
                body += "<b>" + real_taskid + ":</b>  "
                body += eval(collect_cct.get_redis_cct(real_taskid)).__str__()
                body += "<br>"


        else:
            real_taskid = collect_task.get_real_task_id(task["taskid"], "")
            body += "<b>" + real_taskid + ":</b>  "
            body += eval(collect_cct.get_redis_cct(real_taskid)).__str__()
            body += "<br>"

    return [body.encode('utf-8')]


def start_console():
    # 创建一个服务器，IP地址为空，端口是8000，处理函数是application:
    httpd = make_server('localhost', sys_conf.CONSOLE_PORT, application)
    print('Web Console Serving HTTP', end="\n")
    print('http://127.0.0.1:%s...' % sys_conf.CONSOLE_PORT)

    # 开始监听HTTP请求:
    httpd.serve_forever()
