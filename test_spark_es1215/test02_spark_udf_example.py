#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：test02_spark_udf_example.py
@Author  ：itcast
@Date    ：2022/12/15 15:18 
'''
# 模拟数据集实现，用户名字转为大写，age年龄字段增加1岁，求用户名长度，id加2

# 2-服务器路径
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StringType, IntegerType

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

# 创建 dataframe 包含三个字段(用户编号,用户姓名,用户年龄)
df: DataFrame = spark.createDataFrame(data=[(1, 'JackMa', 25), (2, 'JayChou', 23), (3, 'Peter', 26)],
                                      schema=['id', 'name', 'age'])


# 模拟数据集实现，用户名字转为大写，age年龄字段增加1岁，求用户名长度，id加2
@F.udf
def convertName(name: str):
    return name.upper() + " family"


@F.udf(returnType=IntegerType())
def addOne(age: int):
    return age + 1


# 第三种实现 udf

def toLen(name):
    return len(name)


toLength = F.udf(toLen)

addIdOne = F.udf(lambda id: id + 1, returnType=IntegerType())

df.select(df.id, addIdOne(df.id), convertName(df.name), toLength(df.name), addOne(df.age)).show(truncate=False)
