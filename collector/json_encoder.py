# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-16.
# TODO: 自定义的JSON序列化类,去掉序列化后的Decimal 以及None等无法序列化成json的类型.
import json
from _decimal import Decimal

from datetime import datetime, date


class OutputEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return "%.4f" % obj
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        if isinstance(obj, None):
            return ''
        return json.JSONEncoder.default(self, obj)
