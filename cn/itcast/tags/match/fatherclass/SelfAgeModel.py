#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：SelfAgeModel.py
@Author  ：itcast
@Date    ：2022/12/17 16:08 
'''
from pyspark.sql import DataFrame, functions as F

from cn.itcast.tags.base.AbstractModelBase import AbstractModelBase


class SelfAgeModel(AbstractModelBase):

    def getFourTagsId(self):
        return 14

    def compute(self, esDF, fiveRuleDF):
        esDF2: DataFrame = esDF.select(esDF.id,
                                       F.regexp_replace(F.substring(esDF.birthday, 1, 10), '-', '').alias('birth'))
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

        return resultDF


if __name__ == '__main__':
    age_model = SelfAgeModel('age_model')
    age_model.execute()
