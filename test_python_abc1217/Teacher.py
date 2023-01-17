#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：Teacher.py
@Author  ：itcast
@Date    ：2022/12/17 10:46 
'''
from test_python_abc1217.Human import Human


class Teacher(Human):
    # def eat(self):
    #     print("陈老师喜欢吃饺子...")

    def drink(self):
        print("陈老师喜欢喝酒")


    # def sleep(self):
    #     print("陈老师喝多了就睡")

if __name__ == '__main__':
    # 当直接初始化抽象类及抽象方法，会报错
    # 抽象类中的抽象方法必须要重写
    t = Human()
    #t = Teacher()
    # 抽象类中具体方法（已经实现的方法）可以重写、也可以不重写
    t.execute()