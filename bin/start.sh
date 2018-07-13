#!/bin/bash
cp=`pwd`
cd  $cp
source ./venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$cp
python ./collector/main.pyc &