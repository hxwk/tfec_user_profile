#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：01_pyspark_create_sparksession.py
@Author  ：itcast
@Date    ：2022/12/14 17:08 
'''

# 创建 sparkSQL - SparkSession 入口 session
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder \
    .master("local[2]") \
    .appName('create_spark_sessiom_sample') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.sql.autoBroadcastJoinThreshold', '50485760 ') \
    .getOrCreate()

print(spark)
