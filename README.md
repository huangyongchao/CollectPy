# Mysql Data Distribution Tools
> ###### 通过配置轮询mysql数据库表 分发数据到redis elasticsearch kafka等 本人QQ75184655 欢迎多交流

### 环境配置
###### *PYTHON3.x*


1.在本地工程路径{project_dir}下执行

```
pip freeze >requirements.txt
```
> 生成requirements.txt文件，也就是项目依赖的包，注意这里的包来自于你本地的环境的所以最好本地也是虚拟环境,这样导出的包只当前项目依赖的包不会夹杂其他项目的环境

2.进入服务器部署目录创建虚拟环境

```
python -m  venv  venv(这个是虚拟环境名称)
```
> 这样服务器路径下就有了一个venv的目录，注意这个指令针对python3.4+版本，默认创建需要的pip wheel easy_install等工具,python3.4之前的版本需要python -m  venv  venv --without-pip 等参数

3.激活虚拟环境(进入venv虚拟环境的bin目录下)执行如下指令:

```
source activate
```

4.上传项目以及前面生成的requirements.txt到服务器

5.导入项目所需依赖
```
pip install -r requirements.txt
```
6.添加本地项目到python sys.path


```
export PYTHONPATH=$PYTHONPATH:{project_dir}
```
> 这种方法是设置linux环境变量,执行py程序会自动追加路径到sys.path的目录下，也可以通过程序追加进去 

7.进入collector目录下执行主程序(over!!!!)
```
python main.py
```
