# Mysql Data Distribution Tools
> ###### 通过配置轮询mysql数据库表 分发数据到redis elasticsearch kafka等 本人QQ75184655 欢迎多交流

### 环境配置
###### *PYTHON3.x*
#### 本地导出依赖
###### 1. pip freeze > requirements.txt   
######  如果存在此文件可以直接进行下一步
#### 服务器上导入依赖
###### 1. pip install -r requirements.txt

#### 编译源码成pyc(非必要步骤)
###### 执行项目下的compliepyc.py

#### 运行项目
###### export PYTHONPATH=$PYTHONPATH:(项目Linux路径)
###### 如编译源码成Pyc那么直接运行bin里的start.sh
###### 也可以直接运行源码的main.py 或者针对各个平台pyinstaller
