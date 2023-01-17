#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：test2_payment_way.py
@Author  ：itcast
@Date    ：2022/12/20 14:55 
'''
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, functions as F, Window

SPARK_HOME = '/export/server/spark'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder \
        .master('local[2]') \
        .appName('test01_spark_es_read') \
        .config('spark.sql.shuffle.partitions', '4') \
        .getOrCreate()

    # 设置打印日志的级别
    sc: SparkContext = spark.sparkContext
    sc.setLogLevel('WARN')

    df: DataFrame = spark.createDataFrame(
        [
            ('123', 'alipay'),
            ('123', 'alipay'),
            ('123', 'alipay'),
            ('123', 'chinapay'),
            ('123', 'weixin'),
            ('123', 'alipay'),
            ('123', 'cod'),
            ('123', 'alipay'),
            ('123', 'weixin'),
            ('123', 'alipay'),
            ('123', 'alipay'),
            ('124', 'alipay'),
            ('124', 'alipay'),
            ('124', 'weixin'),
            ('124', 'weixin'),
            ('125', 'alipay'),
            ('125', 'weixin'),
            ('125', 'weixin'),
            ('125', 'alipay'),
            ('125', 'weixin'),
            ('125', 'weixin'),
            ('125', 'cod'),
            ('125', 'chinapay'),
        ],
        schema=['memberid', 'paymentcode']
    )

    # 使用 SQL 来实现每个用户、不同支付方式的次数
    # 取出来支付次数排名第一的用户
    # select * from(
    # select memberid,paymentcode,cnt
    # row_number() over(partition by memberid order by cnt desc) as rn
    # from(
    # select memberid,paymentcode,count(1) cnt from t_order group by memberid,paymentcode )tmp
    # ) t
    # where t.rn = 1
    df2: DataFrame = df.groupBy(df.memberid, df.paymentcode).agg(F.count(df.paymentcode).alias('paycount'))
    ## 1. 使用SparkSQL的方式实现逻辑
    df2.createOrReplaceTempView('t_order')
    # df3: DataFrame = spark.sql(
    #     'select memberid,paymentcode,paycount, row_number() over (partition by memberid order by paycount desc) rn from t_order'
    # )
    # df3.show()

    ## 2.使用 sparksql DSL 语句来实现排名第一支付方式
    df3: DataFrame = df2.withColumn(
        'rn',
        F.row_number().over(Window.partitionBy(df2.memberid).orderBy(df2.paycount.desc()))
    )

    # 取出来排名第1的数据
    df3.where(df3.rn == 1).show()
