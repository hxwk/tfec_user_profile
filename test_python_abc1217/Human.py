#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：Human.py
@Author  ：itcast
@Date    ：2022/12/17 10:41 
'''
# 实现 Human 的抽象类，此类作为 抽象基类 ，包含三个抽象方法
from abc import abstractmethod, ABCMeta


class Human(metaclass=ABCMeta):

    def eat(self):
        print('一日三餐...')

    @abstractmethod
    def drink(self):
        pass

    def sleep(self):
        print('每日8个小时睡眠')

    def execute(self):
        self.eat()
        self.drink()
        self.sleep()
