#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：test03_spark_dict.py
@Author  ：itcast
@Date    ：2022/12/15 16:48 
'''
import os

from pyspark.sql import SparkSession

SPARK_HOME = '/export/server/spark'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME

if __name__ == '__main__':
    ur = {
        '1': 5,
        '2': 6
    }

    print(ur[str(1)])
