#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：Doctor.py
@Author  ：itcast
@Date    ：2022/12/17 10:54 
'''
from test_python_abc1217.Human import Human


class Doctor(Human):
    def eat(self):
        print('一日四餐')

    # abstractmethod 方法不需要重写，就可以使用 pass 跳过即可
    def drink(self):
        pass

    def sleep(self):
        print('每日 5 个小时睡眠')

    def acid(self):
        print("每日 100 个人做核酸")

if __name__ == '__main__':
    d = Doctor()
    d.execute()
    d.acid()