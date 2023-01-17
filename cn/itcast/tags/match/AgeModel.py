#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：AgeModel.py
@Author  ：itcast
@Date    ：2022/12/15 11:33 
'''
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, functions as F

from cn.itcast.tags.bean import ESMeta
from cn.itcast.tags.bean.ESMeta import strToESMeta

if __name__ == '__main__':
    """
    开发步骤：
    0.创建spark环境，获得SparkSession对象
	1.读取mysql的业务标签规则数据 tfec_tags.tbl_basic_tag
	2.从mysql业务标签规则数据中过滤出4级标签数据
	3.根据4级标签rule，作为es元数据信息，查询es源索引表数据
	4.从mysql业务标签规则数据中过滤出5级标签数据
	5.根据标签规则，实现标签匹配逻辑
	6.把标签计算结果写入到es结果索引表中 tfec_userprofile_result
    """
    # 0.创建spark环境，获得SparkSession对象
    # 2-服务器路径
    SPARK_HOME = '/export/server/spark'
    # 导入路径
    os.environ['SPARK_HOME'] = SPARK_HOME

    spark: SparkSession = SparkSession.builder \
        .master('local[2]') \
        .appName('age_model') \
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
    # jdbcDF.show(truncate=False)
    # 	2.从mysql业务标签规则数据中过滤出4级标签数据,将年龄标签取出来
    fourTagsId = 14
    fourRuleDF: DataFrame = jdbcDF.where(f'id={fourTagsId}').select(jdbcDF.rule)
    # 将年龄段的四级 rule 字符串取出来
    fourRuleStr = fourRuleDF.rdd.map(lambda row: row.rule).collect()[0]
    # 	3.根据4级标签rule，作为es元数据信息，查询es源索引表数据
    esMeta: ESMeta = strToESMeta(fourRuleStr)

    esDF: DataFrame = spark.read \
        .format("es") \
        .option("es.nodes", esMeta.esNodes) \
        .option("es.index.auto.create", "yes") \
        .option("es.resource", esMeta.esIndex) \
        .option("es.read.field.include", esMeta.selectFields) \
        .load()
    # 	4.从mysql业务标签规则数据中过滤出5级标签数据
    ##将 birthday 转换成 20050425
    esDF2: DataFrame = esDF.select(esDF.id, F.regexp_replace(F.substring(esDF.birthday, 1, 10), '-', '').alias('birth'))

    fiveRuleDF: DataFrame = jdbcDF.where(f'pid={fourTagsId}').select(jdbcDF.id, jdbcDF.rule)
    fiveRuleDF.show(truncate=False)
    # 	5.根据标签规则，实现标签匹配逻辑
    ## 将一个字段转换成两个字段
    fiveRuleDF2: DataFrame = fiveRuleDF.select(
        fiveRuleDF.id,
        F.split(fiveRuleDF.rule, '-')[0].alias('startDate'),
        F.split(fiveRuleDF.rule, '-')[1].alias('endDate')
    )

    resultDF: DataFrame = esDF2.join(
        fiveRuleDF2,
    ).where(esDF2.birth.between(fiveRuleDF2.startDate, fiveRuleDF2.endDate)) \
        .select(esDF2.id.alias('userId'), fiveRuleDF2.id.cast('string').alias('tagsId'))
    resultDF.show(truncate=False)
    # 	6.把标签计算结果写入到es结果索引表中 tfec_userprofile_result
    resultDF.write \
        .format("es") \
        .option("es.nodes", esMeta.esNodes) \
        .option("es.write.operation", 'upsert') \
        .option("es.mapping.id", 'userId') \
        .option("es.resource", "tfec_userprofile_result") \
        .mode("append") \
        .save()
