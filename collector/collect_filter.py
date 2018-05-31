# -*- coding: utf-8 -*-
import datetime

class CollectFilter:
    @staticmethod
    def time_fmt(in_time, fmt):
        return in_time.strftime(fmt)

    @staticmethod
    def camel_case(input_str, separator):
        """
        根据输入的字符串和分隔符，做字符串转驼峰
        :param input_str:
        :param separator:
        :return: camel_case_str
        """
        string_list = str(input_str).split(separator)
        first = string_list[0].lower()
        others = string_list[1:]
        others_capital = [word.capitalize() for word in others]
        others_capital[0:0] = [first]
        camel_case_str = ''.join(others_capital)
        return camel_case_str


f = getattr(CollectFilter, 'time_fmt', None)
f1 = getattr(CollectFilter, 'camel_case', None)
