#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：GenderModel.py
@Author  ：itcast
@Date    ：2022/12/15 16:17 
'''
import os

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession, functions as F

from cn.itcast.tags.bean.ESMeta import ESMeta, strToESMeta


@F.udf
def genderToTagsId(rule):
    return fiveMap[str(rule)]

@F.udf
def mergeToTagsId(newTagId, oldTagId):
    if newTagId == None:
        return oldTagId
    if oldTagId == None:
        return newTagId
    tagList = oldTagId.split(',')
    tagList.append(newTagId)
    return ','.join(set(tagList))


if __name__ == '__main__':
    """
    0.准备Spark开发环境
    1.读取MySQL中的数据【优化：只读四级标签和对应的五级标签】
    2.读取和年龄段标签相关的4级标签rule并解析
    3.根据解析出的rule读取es数据
    4.读取和年龄段标签相关的5级标签(根据4级标签的id作为pid查询)
    5.将esDF和fiveDS进行匹配计算, 得到的数据继续处理:
      5.1.将fiveDF转为map, 方便后续自定义UDF操作
      5.2.使用单表 + UDF完成esDF和fiveDS的匹配
    6.查询ES中的oldDF
    7.合并newDF和oldDF
    8.将最终结果写到ES ： tfec_userprofile_result
    """
    # 0.创建spark环境，获得SparkSession对象
    # 2-服务器路径
    SPARK_HOME = '/export/server/spark'
    # 导入路径
    os.environ['SPARK_HOME'] = SPARK_HOME

    spark: SparkSession = SparkSession.builder \
        .master('local[2]') \
        .appName('gender_model') \
        .config('spark.sql.shuffle.partitions', '4') \
        .getOrCreate()

    # 设置打印日志的级别
    sc: SparkContext = spark.sparkContext
    sc.setLogLevel('WARN')
    # 	1.读取mysql的业务标签规则数据 tfec_tags.tbl_basic_tag
    jdbcDF: DataFrame = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://up01:3306/tfec_tags?characterEncoding=utf8") \
        .option("dbtable", "tbl_basic_tag") \
        .option("user", "root") \
        .option("password", "123456") \
        .load()
    # 1.读取MySQL中的数据【优化：只读四级标签和对应的五级标签】
    fourTagsId = 4
    fourRuleDF: DataFrame = jdbcDF.where(f'id={fourTagsId} or pid={fourTagsId}').select(jdbcDF.rule)
    # 2.读取和年龄段标签相关的4级标签rule并解析
    fourRuleStr = fourRuleDF.rdd.map(lambda row: row.rule).collect()[0]
    # 	3.根据4级标签rule，作为es元数据信息，查询es源索引表数据
    esMeta: ESMeta = strToESMeta(fourRuleStr)

    # 3.根据解析出的rule读取es数据
    esDF: DataFrame = spark.read \
        .format("es") \
        .option("es.nodes", esMeta.esNodes) \
        .option("es.index.auto.create", "yes") \
        .option("es.resource", esMeta.esIndex) \
        .option("es.read.field.include", esMeta.selectFields) \
        .load()

    # 4.读取和年龄段标签相关的5级标签(根据4级标签的id作为pid查询)
    fiveRuleDF: DataFrame = jdbcDF.where(f'pid={fourTagsId}').select(jdbcDF.id, jdbcDF.rule)
    fiveRuleDF.show(truncate=False)
    # 5.将esDF和fiveDS进行匹配计算, 得到的数据继续处理:
    #   5.1.将fiveDF转为map, 方便后续自定义UDF操作
    fiveMap = fiveRuleDF.rdd.map(lambda row: (row.rule, row.id)).collectAsMap()
    #   5.2.使用单表 + UDF完成esDF和fiveDS的匹配
    newDF: DataFrame = esDF.join(
        other=fiveRuleDF,
        on=esDF.gender == fiveRuleDF.rule,
        how='left'
    )\
        .select(esDF.id.alias('userId'), fiveRuleDF.id.alias('tagsId'))
        #.select(esDF.id.alias('userId'), genderToTagsId(fiveRuleDF.rule).alias('tagsId'))

    # 6.查询ES中的oldDF
    oldDF: DataFrame = spark.read \
        .format("es") \
        .option("es.nodes", esMeta.esNodes) \
        .option("es.index.auto.create", "yes") \
        .option("es.resource", 'tfec_userprofile_result') \
        .option("es.read.field.include", 'userId, tagsId') \
        .load()
    # 7.合并newDF和oldDF
    resultDF = newDF.join(
        other=oldDF,
        on=newDF.userId == oldDF.userId,
        how='left'
    ).select(
        newDF.userId,
        mergeToTagsId(newDF.tagsId, oldDF.tagsId).alias('tagsId')
    )

    resultDF.show(truncate=False)

    # 8.将最终结果写到ES ： tfec_userprofile_result
    resultDF.write \
        .format("es") \
        .option("es.nodes", esMeta.esNodes) \
        .option("es.write.operation", 'upsert') \
        .option("es.mapping.id", 'userId') \
        .option("es.resource", "tfec_userprofile_result") \
        .mode("append") \
        .save()