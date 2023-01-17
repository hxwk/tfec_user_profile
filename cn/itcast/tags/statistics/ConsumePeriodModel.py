#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：ConsumePeriodModel.py
@Author  ：itcast
@Date    ：2022/12/20 10:30 
'''
from cn.itcast.tags.base.AbstractModelBase import AbstractModelBase
from pyspark.sql import functions as F, DataFrame

"""
此类主要用于计算消费周期，消费周期的规则：
24,近7天,0-7
25,近14天,8-14
26,近30天,15-30
27,近60天,31-60
28,近90天,61-900
计算属于统计类标签
"""


class ConsumePeriodModel(AbstractModelBase):

    def getFourTagsId(self):
        return 23

    def compute(self, esDF, fiveDF):
        # 1. 计算出来最近一次消费的时间
        esDF2: DataFrame = esDF.groupBy(esDF.memberid).agg(
            F.max(esDF.finishtime).alias('lastest_consume')
        )
        # 2. 计算出来离当前时间的多少天未消费（datediff）
        esDF3: DataFrame = esDF2.select(
            esDF2.memberid.alias('userId'),
            F.datediff(F.current_date(), F.from_unixtime(esDF2.lastest_consume)).alias('diffDays')
        )
        # 3. 0-7 将五级标签的规则 用 - 截取出来并 start 和 end 时间
        fiveDF2: DataFrame = fiveDF.select(
            fiveDF.id.alias('tagsId'),
            F.split(fiveDF.rule, '-')[0].alias('start_date'),
            F.split(fiveDF.rule, '-')[1].alias('end_date')
        )
        # 4. 匹配 datediff >= start and datediff < end 将标签id和用户id取出来
        newDF:DataFrame = esDF3.join(fiveDF2)\
        .where(esDF3.diffDays.between(fiveDF2.start_date,fiveDF2.end_date))\
        .select(esDF3.userId,fiveDF2.tagsId.cast('string'))

        return newDF


if __name__ == '__main__':
    consume_period = ConsumePeriodModel('consumer_period_model')
    consume_period.execute()
