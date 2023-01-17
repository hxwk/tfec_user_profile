#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：test1_consumerperiod.py
@Author  ：itcast
@Date    ：2022/12/20 9:42 
'''
# 2-服务器路径
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, functions as F

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

df: DataFrame = spark.createDataFrame(
    [(1, '苹果14Max', '2022-11-20'), (2, '联想笔记本', '2022-11-25'), (3, '小米mate10', '2022-11-25'), (1, '包', '2022-12-02'),
     (1, '手表', '2022-12-10')], schema=['memberid', 'productName', 'finishtime'])

# df.createOrReplaceTempView('t_order')
# spark.sql('select memberid,max(finishtime) as max_finishtime from t_order group by memberid').show()
# df.groupby(df.memberid).max('finishtime').show()

df2: DataFrame = df.groupby(df.memberid).agg(F.max(df.finishtime).alias('lastest_consume'))
df2.withColumn('current_date', F.current_date())

df2.select(
    df2.memberid,
    F.datediff(F.current_date(), df2.lastest_consume).alias('diffDays')
).show(truncate=False)
