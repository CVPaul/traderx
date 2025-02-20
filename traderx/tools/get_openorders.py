#!/usr/bin/env python
#-*- coding:utf-8 -*-


import os
import sys

from traderx.utils.common import create_rest_api
from traderx.utils.const import API_KEY, API_SECRET


if __name__ == "__main__":
    cli = create_rest_api(API_KEY, API_SECRET)
    rsp = cli.get_orders()
    # final
    cli.close()
