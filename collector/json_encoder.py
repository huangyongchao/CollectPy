# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-16.
# TODO: 自定义的JSON序列化类,去掉序列化后的Decimal 以及None等无法序列化成json的类型.
import json
from _decimal import Decimal

class OutputEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return "%.2f" % obj
        if isinstance(obj, None):
            return ''
        return json.JSONEncoder.default(self, obj)
