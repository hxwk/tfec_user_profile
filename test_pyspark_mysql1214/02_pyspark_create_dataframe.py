#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：02_pyspark_create_dataframe.py
@Author  ：itcast
@Date    ：2022/12/14 17:18 
'''

# 通过sparksession 创建一个 DataFrame 对象
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder \
    .master("local[2]") \
    .appName('create_spark_sessiom_sample') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.sql.autoBroadcastJoinThreshold', '50485760') \
    .getOrCreate()
sc: SparkContext = spark.sparkContext()
sc.setLogLevel("WARN")

df = spark.createDataFrame(data=[('1', 'zhangsan', 20), ('2', 'wangwu', 25), ('3', 'JayChou', 41)],
                           schema=['sno', 'sname', 'age'])

# 打印表结构
df.printSchema()
# 打印输出
df.show(truncate=False)
