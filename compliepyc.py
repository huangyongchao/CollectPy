# -*- coding: utf-8 -*-
# TODO(huangyongchao): 2018-05-14.
# TODO: 将项目下的colletor下面的py编译成pyc,放到bin下面的collector下,改文件目录修改会引起寻找文件等异常.

import os, shutil
import py_compile

currDir = os.path.dirname(os.path.abspath(__file__))
dstDir = currDir + "/bin/collector"
srcDir = currDir + "/collector"


# 清空目录
def clean_dir(dir):
    print('clean_dir ' + dir + '...')

    for entry in os.scandir(dir):
        if entry.name.startswith('.'):
            continue
        if entry.is_file():
            os.remove(entry.path)  # 删除文件
        else:
            shutil.rmtree(entry.path)  # 删除目录


# 编译当前文件夹下所有.py文件
def comple_path():
    global dstDir
    global srcDir
    if os.path.exists(dstDir):  # 如果存在，清空
        clean_dir(dstDir)
    else:  # 如果不存在，创建
        os.mkdir(dstDir)

    for filename in os.listdir(srcDir):
        if not filename.endswith('.py'):
            continue
        srcFile = os.path.join(srcDir, filename)
        if srcFile == os.path.abspath(__file__):  # 自身
            continue
        dstFile = os.path.join(dstDir, filename + 'c')
        print(srcFile + ' --> ' + dstFile)
        py_compile.compile(srcFile, cfile=dstFile)


if __name__ == "__main__":
    comple_path()
    print(".....complie completed")
