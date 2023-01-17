#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：test01_spark_es_read.py
@Author  ：itcast
@Date    ：2022/12/15 9:50 
'''
# spark 如何读取 es 中的数据
# 创建 SparkSession 对象
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame

# 2-服务器路径
SPARK_HOME = '/export/server/spark'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME

spark: SparkSession = SparkSession.builder \
    .master('local[2]') \
    .appName('test01_spark_es_read') \
    .config('spark.sql.shuffle.partitions', '4') \
    .getOrCreate()

# 设置打印日志的级别
sc: SparkContext = spark.sparkContext
sc.setLogLevel('WARN')
# 读取 es 中的数据
# es.nodes=up01
# es.port=9200
# es.index.auto.create=yes
# es.read.field.include=empty
# es.resource= index/type

# esDF: DataFrame = spark.read \
#     .format("es") \
#     .option("es.nodes", "up01") \
#     .option("es.port", "9200") \
#     .option("es.index.auto.create", "yes") \
#     .option("es.read.field.include", "username,age") \
#     .option("es.resource", "user_profile") \
#     .load()

esDF: DataFrame = spark.read \
    .format("es") \
    .option("es.nodes", "up01") \
    .option("es.port", "9200") \
    .option("es.index.auto.create", "yes") \
    .option("es.resource", "user_profile") \
    .load()

esDF.printSchema()
esDF.show(truncate=False)

# spark write es
esDF.write \
    .format("es") \
    .option("es.nodes", "up01") \
    .option("es.port", "9200") \
    .option("es.write.operation", 'upsert') \
    .option("es.mapping.id",'username')\
    .option("es.resource", "user_profile_es_bak") \
    .mode("append") \
    .save()
