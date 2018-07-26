from _decimal import Decimal
from datetime import datetime, date

import pymysql


def out_put(outputconf, datas):
    sql = batch_sql(outputconf, datas)
    if sql:
        conn = pymysql.connect(use_unicode=True, charset='utf8', **outputconf['mysql'])
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()


def batch_sql(outputconf, datas):
    if not datas:
        return

    sql_fields = ""

    for f in datas[0].keys():
        sql_fields += f + ","

    sql_values = []  # 数据库值
    for data in datas:
        row = [convert_mysql_type(v) for v in data.values()]
        sql_values.append(tuple(row))

    table = outputconf["table"]
    op = "insert"
    ignore = ""
    if dict(outputconf).__contains__("conflict"):
        conflict = outputconf["conflict"]
        if conflict == "replace":
            op = "replace"
        if conflict == "ignore":
            ignore = "ignore"
    vstr = sql_values.__str__().replace("None", "null")
    return "%s %s  into  %s (%s) values %s" % (
        op, ignore, table, sql_fields[0:sql_fields.__len__() - 1], vstr[1:vstr.__len__() - 1])


def convert_mysql_type(obj):
    if isinstance(obj, Decimal):
        return "%.4f" % obj
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")

    return obj
