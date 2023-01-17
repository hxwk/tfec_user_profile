#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：05_pyspark_mysql_read.py
@Author  ：itcast
@Date    ：2022/12/14 17:48 
'''

# pyspark 读取mysql中的数据
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
import os

# 2-服务器路径
SPARK_HOME = '/export/server/spark'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME

spark: SparkSession = SparkSession.builder \
    .master("local[2]") \
    .appName('create_spark_sessiom_sample') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.sql.autoBroadcastJoinThreshold', '50485760 ') \
    .getOrCreate()

jdbcDF: DataFrame = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://up01:3306/pyspark_mysql_test?characterEncoding=utf8") \
    .option("dbtable", "t_user") \
    .option("user", "root") \
    .option("password", "123456") \
    .load()

jdbcDF.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://up01:3306/pyspark_mysql_test?characterEncoding=utf8") \
    .option("dbtable", "t_user_bak") \
    .option("user", "root") \
    .option("password", "123456") \
    .mode("overwrite") \
    .save()
