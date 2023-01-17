#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：test04_spark_merge_user_profile.py
@Author  ：itcast
@Date    ：2022/12/15 17:07 
'''
if __name__ == '__main__':
    oldTag = '1,10,20'
    newTag = '10'

    tagList = oldTag.split(',')
    tagList.append(newTag)
    str = ','.join(set(tagList))
    print(str)
