#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：AbstractModelBase.py
@Author  ：itcast
@Date    ：2022/12/17 15:10 
'''
import os
from abc import ABCMeta, abstractmethod
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, functions as F

from cn.itcast.tags.bean import ESMeta
from cn.itcast.tags.bean.ESMeta import strToESMeta

SPARK_HOME = '/export/server/spark'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME


@F.udf
def mergeToTagsId(newTagId, oldTagId):
    if newTagId == None:
        return oldTagId
    if oldTagId == None:
        return newTagId
    tagList = oldTagId.split(',')
    tagList.append(str(newTagId))
    return ','.join(set(tagList))


class AbstractModelBase(metaclass=ABCMeta):
    # 0.  创建spark环境，获得SparkSession对象
    def __init__(self, taskName):
        self.spark = SparkSession.builder \
            .master('local[2]') \
            .appName('age_model') \
            .config('spark.sql.shuffle.partitions', '4') \
            .getOrCreate()

        # 设置打印日志的级别
        sc: SparkContext = self.spark.sparkContext
        sc.setLogLevel('WARN')

    # 1.  读取mysql的业务标签规则数据
    @abstractmethod
    def getFourTagsId(self):
        pass

    # 2.  从mysql业务标签规则数据中过滤出4级标签数据 => 4级标签id需要传入
    def __readMysqlTagData(self, fourTagsId):
        jdbcDF: DataFrame = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://up01:3306/tfec_tags?characterEncoding=utf8") \
            .option("query", f"select * from tbl_basic_tag where id={fourTagsId} or pid = {fourTagsId}") \
            .option("user", "root") \
            .option("password", "123456") \
            .load()
        return jdbcDF

    # 读取四级标签中的 es 规则字符串
    def __readFourRuleData(self, mysqlDF, fourTagsId):
        fourDF = mysqlDF.where(f'id = {fourTagsId}').select(mysqlDF.rule)
        return fourDF

    # 根据4级标签的rule，获得ESMeta对象
    def __getEsMetaFromFourRule(self, fourDF) -> ESMeta:
        fourRuleStr = fourDF.rdd.map(lambda row: row.rule).collect()[0]
        # print(fourRuleStr)
        esMeta = strToESMeta(fourRuleStr)
        return esMeta

    # 3.  根据4级标签rule，作为es元数据信息，查询es源索引表数据
    def __getEsWithEsMeta(self, esMeta):
        esDF: DataFrame = self.spark.read \
            .format("es") \
            .option("es.nodes", esMeta.esNodes) \
            .option("es.index.auto.create", "yes") \
            .option("es.resource", esMeta.esIndex) \
            .option("es.read.field.include", esMeta.selectFields) \
            .load()
        return esDF

    # 4.  从mysql业务标签规则数据中过滤出5级标签数据 => 4级标签id需要传入
    def __readFiveRuleData(self, jdbcDF, fourTagsId):
        fiveRuleDF: DataFrame = jdbcDF.where(f'pid={fourTagsId}').select(jdbcDF.id, jdbcDF.rule)
        fiveRuleDF.show(truncate=False)
        return fiveRuleDF

    # 5.  根据标签规则，实现标签匹配逻辑,返回newDF => 需要在子类实现匹配、统计、挖掘逻辑
    @abstractmethod
    def compute(self, esDF, fiveDF):
        pass

    # 6.  读取旧的标签结果数据,返回oldDF
    def __readOldTagsResultData(self, esMeta):
        oldDF: DataFrame = self.spark.read \
            .format("es") \
            .option("es.nodes", esMeta.esNodes) \
            .option("es.index.auto.create", "yes") \
            .option("es.resource", 'tfec_userprofile_result') \
            .option("es.read.field.include", 'userId, tagsId') \
            .load()
        return oldDF

    # 7.  根据用户id相同，进行新旧标签合并处理，返回resultDF
    def __mergeNewDFAndOldDF(self, newDF, oldDF):
        resultDF = newDF.join(
            other=oldDF,
            on=newDF.userId == oldDF.userId,
            how='left'
        ).select(
            newDF.userId,
            mergeToTagsId(newDF.tagsId, oldDF.tagsId).alias('tagsId')
        )
        return resultDF

    # 8.  把标签计算结果写入到es结果索引表中
    def __writeResultDFToEs(self, resultDF, esMeta):
        resultDF.write \
            .format("es") \
            .option("es.nodes", esMeta.esNodes) \
            .option("es.write.operation", 'upsert') \
            .option("es.mapping.id", 'userId') \
            .option("es.resource", "tfec_userprofile_result") \
            .mode("append") \
            .save()

    def execute(self):
        fourTagsId = self.getFourTagsId()
        jdbcDF = self.__readMysqlTagData(fourTagsId)
        fourDF = self.__readFourRuleData(jdbcDF, fourTagsId)
        esMeta: ESMeta = self.__getEsMetaFromFourRule(fourDF)
        esData = self.__getEsWithEsMeta(esMeta)
        fiveRuleData = self.__readFiveRuleData(jdbcDF, fourTagsId)
        newDF = self.compute(esData, fiveRuleData)
        oldDF = self.__readOldTagsResultData(esMeta)
        resultDF = self.__mergeNewDFAndOldDF(newDF, oldDF)
        self.__writeResultDFToEs(resultDF, esMeta)
