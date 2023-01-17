#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：PayWayModel.py
@Author  ：itcast
@Date    ：2022/12/20 15:50
'''
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.functions import udf

from cn.itcast.tags.base.AbstractModelBase import AbstractModelBase


class PayWayModel(AbstractModelBase):
    """
        基于标签实现基类标签分析
    -   1.支付方式基类准备
    -   2.根据用户id+支付方式进行分组并计数
    -   3.使用开窗函数进行组内排序，求Top1
        -   方式1：DSL风格的开窗函数实现需求**【二选一】**
        -   方式2：SQL风格的开窗函数实现需求**【二选一】**
    -   4.将排序取Top1的DF与5级标签的DF进行匹配
        -   4.1.将rankDF中的支付方式转换为tagsId
        -   4.2.将5级标签的fiveDF转换为map字典形式
    -   5.得到并返回newDF
    -   6.调用代码
    """

    def getFourTagsId(self):
        return 29

    def compute(self, esDF: DataFrame, fiveDF: DataFrame):
        # 计算出来每个用户，不同支付方式的次数
        esDF2: DataFrame = esDF.groupBy(esDF.memberid, esDF.paymentcode) \
            .agg(F.count(esDF.paymentcode).alias('paycount'))
        # 对不同的支付方式排名
        esDF3: DataFrame = esDF2.withColumn(
            'rn',
            F.row_number().over(Window.partitionBy(esDF2.memberid).orderBy(esDF2.paycount.desc()))
        )
        # 取出排名第一的支付方式
        esDF4: DataFrame = esDF3.where(esDF3.rn == 1)

        # 将 5 级标签 fiveDF 转换成 map 字典
        fiveDict = fiveDF.rdd.map(lambda row: (row.rule, row.id)).collectAsMap()

        @udf
        def paymentcodeToTagsId(paymentcode):
            return fiveDict[paymentcode]

        # 将五级标签的 rule 和 id 生成一个 map
        newDF = esDF4.select(
            esDF4.memberid.alias('userId'),
            paymentcodeToTagsId(esDF4.paymentcode).alias('tagsId')
        )
        return newDF


if __name__ == '__main__':
    payWay = PayWayModel('pay_way_model')
    payWay.execute()
