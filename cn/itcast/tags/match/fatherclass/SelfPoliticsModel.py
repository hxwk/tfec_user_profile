#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile
@File    ：SelfPoliticsModel.py
@Author  ：itcast
@Date    ：2022/12/17 16:52 
'''
from pyspark.sql import DataFrame

from cn.itcast.tags.base.AbstractModelBase import AbstractModelBase


class SelfPoliticsModel(AbstractModelBase):
    def getFourTagsId(self):
        return 61

    def compute(self, esDF, fiveRuleDF):
        #   5.1.将fiveDF转为map, 方便后续自定义UDF操作
        # fiveMap = fiveRuleDF.rdd.map(lambda row: (row.rule, row.id)).collectAsMap()
        #   5.2.使用单表 + UDF完成esDF和fiveDS的匹配
        newDF: DataFrame = esDF.join(
            other=fiveRuleDF,
            on=esDF.politicalface == fiveRuleDF.rule,
            how='left'
        ) \
            .select(esDF.id.alias('userId'), fiveRuleDF.id.alias('tagsId'))
        return newDF


if __name__ == '__main__':
    gender_model = SelfPoliticsModel('politics_model_task')
    gender_model.execute()
