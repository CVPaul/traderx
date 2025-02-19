#!/usr/bin/env python
#-*- coding:utf-8 -*-


import random
import string


def uuid(length=4):
    return ''.join(random.choices(
        string.ascii_lowercase + string.digits, k=length))


if __name__ == "__main__":
    print(uuid())
