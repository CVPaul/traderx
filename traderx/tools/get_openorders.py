#!/usr/bin/env python
#-*- coding:utf-8 -*-


import os
import sys
import pandas as pd

from traderx.utils.common import create_rest_api
from traderx.utils.const import API_KEY, API_SECRET


if __name__ == "__main__":
    cli = create_rest_api(API_KEY, API_SECRET, is_online=True)
    rsp = cli.get_all_orders(sys.argv[1], limit=int(sys.argv[2]))
    df = pd.DataFrame(rsp)
    df['updateTime'] = pd.to_datetime(df.updateTime * 1e6)
    df.to_csv("trades.csv")
    print(df)
